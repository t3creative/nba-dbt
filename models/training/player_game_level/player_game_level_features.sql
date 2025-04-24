{{
    config(
        schema='features',
        materialized='table',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        indexes=[
            {'columns': ['player_game_key']},
            {'columns': ['game_id', 'player_id']},
            {'columns': ['player_id', 'game_date']}
        ]
    )
}}

with traditional_boxscores as (
    select * from {{ ref('int__player_traditional_bxsc') }}
),

advanced_boxscores as (
    select * from {{ ref('int__player_advanced_bxsc') }}
),

hustle_boxscores as (
    select * from {{ ref('int__player_hustle_bxsc') }}
),

usage_boxscores as (
    select * from {{ ref('int__player_usage_bxsc') }}
),

scoring_boxscores as (
    select * from {{ ref('int__player_scoring_bxsc') }}
),

-- Get team context features from int_game_context
team_game_context as (
    select * from {{ ref('int_game_context') }}
),

-- Get all distinct player-game combinations with dates
player_games as (
    select distinct
        player_game_key,
        player_id,
        game_id,
        game_date,
        season_year,
        team_tricode,
        team_id
    from traditional_boxscores
    order by player_id, game_date
),

-- Add game context features (rest days, back-to-backs, etc.)
game_context_base as (
    select
        pg.player_game_key,
        pg.player_id,
        pg.game_id,
        pg.game_date,
        pg.season_year,
        pg.team_tricode,
        pg.team_id,
        -- Check if first game of season for player
        case when row_number() over(partition by pg.player_id, pg.season_year order by pg.game_date) = 1
             then 1 else 0 end as is_first_game_of_season,
             
        -- Days since last game (NULL if first game of season)
        case 
            when lag(pg.season_year) over(partition by pg.player_id order by pg.game_date) != pg.season_year 
                then NULL -- First game of a new season
            else pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)
        end as days_since_last_game,
        
        -- Categorize by rest days
        case 
            when lag(pg.season_year) over(partition by pg.player_id order by pg.game_date) != pg.season_year 
                then 'Season Start' -- First game of a new season
            when (pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)) >= 7
                then '7+ Days' -- Week or more rest
            when (pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)) >= 4
                then '4-6 Days' -- Extended rest
            when (pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)) >= 2
                then '2-3 Days' -- Normal rest
            when (pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)) = 1
                then '1 Day' -- Back-to-back
            else 'Same Day' -- Same day game (unusual)
        end as rest_day_range,
        
        -- Is back-to-back (played yesterday)
        case 
            when lag(pg.season_year) over(partition by pg.player_id order by pg.game_date) != pg.season_year 
                then 0 -- Different season
            when (pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)) = 1 
                then 1 
            else 0 
        end as is_back_to_back,
        
        -- Is second game of back-to-back
        case 
            when lag(pg.season_year) over(partition by pg.player_id order by pg.game_date) != pg.season_year 
                then 0 -- Different season
            when (pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)) = 1 
                then 1 
            else 0 
        end as is_second_game_of_back_to_back,
        
        -- Prepare streak calculation data
        case 
            when lag(pg.season_year) over(partition by pg.player_id order by pg.game_date) != pg.season_year 
                then 0 -- New season starts new streak
            when (pg.game_date - lag(pg.game_date) over(partition by pg.player_id order by pg.game_date)) > 3
                then 0 -- Break in streak if more than 3 days between games
            else 1 -- Continue streak
        end as streak_continue_flag,
        
        -- Game number in season for player
        row_number() over(partition by pg.player_id, pg.season_year order by pg.game_date) as game_number_in_season,
        
        -- Total games in season so far for player
        count(*) over(partition by pg.player_id, pg.season_year) as total_games_in_season_so_far,
        
        -- Percentage of season completed (based on 82 games standard season)
        (row_number() over(partition by pg.player_id, pg.season_year order by pg.game_date) / 82.0) * 100 as pct_of_season_completed
    from player_games pg
),

-- Calculate streak with non-nested window functions
game_context as (
    select
        *,
        -- Apply streak calculation separately to avoid nested window functions
        sum(streak_continue_flag) over(partition by player_id, season_year order by game_date) as consecutive_games_played,
        
        -- Number of games in last 7 days (excluding current game)
        count(*) over(
            partition by player_id, season_year 
            order by game_date 
            range between interval '6 days' preceding and current row
        ) - 1 as games_last_7_days,
        
        -- Number of games in last 14 days (excluding current game)
        count(*) over(
            partition by player_id, season_year
            order by game_date 
            range between interval '13 days' preceding and current row
        ) - 1 as games_last_14_days,
        
        -- Number of games in last 30 days (excluding current game)
        count(*) over(
            partition by player_id, season_year
            order by game_date 
            range between interval '29 days' preceding and current row
        ) - 1 as games_last_30_days,
        
        -- Home/away game indicator - requires joining to game data
        -- For now using placeholder until we add the proper game detail data
        0 as is_home_game,
        
        -- Win/loss streak (placeholder for now)
        0 as win_loss_streak
    from game_context_base
),

-- Calculate traditional box score rolling averages
traditional_rolling_features as (
    select
        pg.player_game_key,
        pg.player_id,
        pg.game_id,
        pg.game_date,
        pg.season_year,
        pg.team_tricode,
        pg.team_id,
        
        -- Game context features
        gc.is_first_game_of_season,
        gc.days_since_last_game,
        gc.rest_day_range,
        gc.is_back_to_back,
        gc.is_second_game_of_back_to_back,
        gc.consecutive_games_played,
        gc.games_last_7_days,
        gc.games_last_14_days,
        gc.games_last_30_days,
        gc.is_home_game,
        gc.win_loss_streak,
        gc.game_number_in_season,
        gc.total_games_in_season_so_far,
        gc.pct_of_season_completed,
        
        -- Team context features from int_game_context
        -- Is this player on the home team or away team?
        case 
            when tgc.home_team_id = pg.team_id then 'home'
            when tgc.away_team_id = pg.team_id then 'away'
            else null
        end as home_or_away,
        
        -- Team rest days 
        case
            when tgc.home_team_id = pg.team_id then tgc.home_rest_days
            when tgc.away_team_id = pg.team_id then tgc.away_rest_days
            else null
        end as team_rest_days,
        
        -- Team back-to-back indicator
        case
            when tgc.home_team_id = pg.team_id then tgc.home_back_to_back
            when tgc.away_team_id = pg.team_id then tgc.away_back_to_back
            else null
        end as team_back_to_back,
        
        -- Team short rest indicator
        case
            when tgc.home_team_id = pg.team_id then tgc.home_short_rest
            when tgc.away_team_id = pg.team_id then tgc.away_short_rest
            else null
        end as team_short_rest,
        
        -- Team long rest indicator
        case
            when tgc.home_team_id = pg.team_id then tgc.home_long_rest
            when tgc.away_team_id = pg.team_id then tgc.away_long_rest
            else null
        end as team_long_rest,
        
        -- Team game number in season
        case
            when tgc.home_team_id = pg.team_id then tgc.home_team_game_num
            when tgc.away_team_id = pg.team_id then tgc.away_team_game_num
            else null
        end as team_game_num_season,
        
        -- Team recent performance
        case
            when tgc.home_team_id = pg.team_id then tgc.home_wins_last_10
            when tgc.away_team_id = pg.team_id then tgc.away_wins_last_10
            else null
        end as team_wins_last_10,
        
        case
            when tgc.home_team_id = pg.team_id then tgc.home_losses_last_10
            when tgc.away_team_id = pg.team_id then tgc.away_losses_last_10
            else null
        end as team_losses_last_10,
        
        case
            when tgc.home_team_id = pg.team_id then tgc.home_win_pct_last_10
            when tgc.away_team_id = pg.team_id then tgc.away_win_pct_last_10
            else null
        end as team_win_pct_last_10,
        
        -- Opponent team information
        case
            when tgc.home_team_id = pg.team_id then tgc.away_team_id
            when tgc.away_team_id = pg.team_id then tgc.home_team_id
            else null
        end as opponent_team_id,
        
        -- Opponent rest days
        case
            when tgc.home_team_id = pg.team_id then tgc.away_rest_days
            when tgc.away_team_id = pg.team_id then tgc.home_rest_days
            else null
        end as opponent_rest_days,
        
        -- Opponent back-to-back indicator
        case
            when tgc.home_team_id = pg.team_id then tgc.away_back_to_back
            when tgc.away_team_id = pg.team_id then tgc.home_back_to_back
            else null
        end as opponent_back_to_back,
        
        -- Opponent recent performance
        case
            when tgc.home_team_id = pg.team_id then tgc.away_win_pct_last_10
            when tgc.away_team_id = pg.team_id then tgc.home_win_pct_last_10
            else null
        end as opponent_win_pct_last_10,
        
        -- Game significance features
        tgc.is_playoff_game,
        tgc.is_late_season,
        
        -- Current game stats (target variables for predictive modeling)
        tbs.pts,
        tbs.min,
        tbs.fgm,
        tbs.fga,
        tbs.fg_pct,
        tbs.fg3m,
        tbs.fg3a,
        tbs.fg3_pct,
        tbs.ftm,
        tbs.fta,
        tbs.ft_pct,
        tbs.off_reb,
        tbs.def_reb,
        tbs.reb,
        tbs.ast,
        tbs.stl,
        tbs.blk,
        tbs.tov,
        tbs.pf,
        tbs.plus_minus,
        
        -- Advanced stats
        abs.est_off_rating,
        abs.off_rating,
        abs.est_def_rating,
        abs.def_rating,
        abs.est_net_rating,
        abs.net_rating,
        abs.ast_pct,
        abs.ast_to_tov_ratio,
        abs.ast_ratio,
        abs.off_reb_pct,
        abs.def_reb_pct,
        abs.reb_pct,
        abs.tov_ratio,
        abs.eff_fg_pct,
        abs.ts_pct,
        abs.usage_pct,
        abs.est_usage_pct,
        abs.est_pace,
        abs.pace,
        abs.pace_per_40,
        abs.possessions,
        abs.pie,
        
        -- Hustle stats
        hbs.cont_shots,
        hbs.cont_2pt,
        hbs.cont_3pt,
        hbs.deflections,
        hbs.charges_drawn,
        hbs.screen_ast,
        hbs.screen_ast_pts,
        hbs.off_loose_balls_rec,
        hbs.def_loose_balls_rec,
        hbs.tot_loose_balls_rec,
        hbs.off_box_outs,
        hbs.def_box_outs,
        hbs.box_out_team_reb,
        hbs.box_out_player_reb,
        hbs.tot_box_outs,
        
        -- Usage stats
        ubs.pct_of_team_fgm,
        ubs.pct_of_team_fga,
        ubs.pct_of_team_fg3m,
        ubs.pct_of_team_fg3a,
        ubs.pct_of_team_ftm,
        ubs.pct_of_team_fta,
        ubs.pct_of_team_oreb,
        ubs.pct_of_team_dreb,
        ubs.pct_of_team_reb,
        ubs.pct_of_team_ast,
        ubs.pct_of_team_tov,
        ubs.pct_of_team_stl,
        ubs.pct_of_team_blk,
        ubs.pct_of_team_blk_allowed,
        ubs.pct_of_team_pf,
        ubs.pct_of_team_pfd,
        ubs.pct_of_team_pts,
        
        -- Scoring stats
        sbs.pct_fga_2pt,
        sbs.pct_fga_3pt,
        sbs.pct_pts_2pt,
        sbs.pct_pts_midrange_2pt,
        sbs.pct_pts_3pt,
        sbs.pct_pts_fastbreak,
        sbs.pct_pts_ft,
        sbs.pct_pts_off_tov,
        sbs.pct_pts_in_paint,
        sbs.pct_assisted_2pt,
        sbs.pct_unassisted_2pt,
        sbs.pct_assisted_3pt,
        sbs.pct_unassisted_3pt,
        sbs.pct_assisted_fgm,
        sbs.pct_unassisted_fgm,
        
        -- Rolling 5-game averages
        {{ advanced_rolling_average('tbs.pts', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as pts_rolling_avg_5,
        {{ rolling_std_dev('tbs.pts', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as pts_rolling_std_5,
        {{ advanced_rolling_average('tbs.min', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as min_rolling_avg_5,
        {{ rolling_std_dev('tbs.min', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as min_rolling_std_5,
        {{ advanced_rolling_average('tbs.fgm', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as fgm_rolling_avg_5,
        {{ advanced_rolling_average('tbs.fga', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as fga_rolling_avg_5,
        {{ rolling_percentage('tbs.fgm', 'tbs.fga', ['pg.player_id'], 'pg.game_date', 5, false) }} as fg_pct_rolling_avg_5,
        {{ rolling_std_dev('tbs.fg_pct', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as fg_pct_rolling_std_5,
        {{ advanced_rolling_average('tbs.fg3m', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as fg3m_rolling_avg_5,
        {{ advanced_rolling_average('tbs.fg3a', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as fg3a_rolling_avg_5,
        {{ rolling_percentage('tbs.fg3m', 'tbs.fg3a', ['pg.player_id'], 'pg.game_date', 5, false) }} as fg3_pct_rolling_avg_5,
        {{ rolling_std_dev('tbs.fg3_pct', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as fg3_pct_rolling_std_5,
        {{ advanced_rolling_average('tbs.ftm', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as ftm_rolling_avg_5,
        {{ advanced_rolling_average('tbs.fta', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as fta_rolling_avg_5,
        {{ rolling_percentage('tbs.ftm', 'tbs.fta', ['pg.player_id'], 'pg.game_date', 5, false) }} as ft_pct_rolling_avg_5,
        {{ rolling_std_dev('tbs.ft_pct', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as ft_pct_rolling_std_5,
        {{ advanced_rolling_average('tbs.off_reb', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as off_reb_rolling_avg_5,
        {{ advanced_rolling_average('tbs.def_reb', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as def_reb_rolling_avg_5,
        {{ advanced_rolling_average('tbs.reb', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as reb_rolling_avg_5,
        {{ rolling_std_dev('tbs.reb', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as reb_rolling_std_5,
        {{ advanced_rolling_average('tbs.ast', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as ast_rolling_avg_5,
        {{ rolling_std_dev('tbs.ast', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as ast_rolling_std_5,
        {{ advanced_rolling_average('tbs.stl', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as stl_rolling_avg_5,
        {{ advanced_rolling_average('tbs.blk', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as blk_rolling_avg_5,
        {{ advanced_rolling_average('tbs.tov', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as tov_rolling_avg_5,
        {{ advanced_rolling_average('tbs.pf', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as pf_rolling_avg_5,
        {{ advanced_rolling_average('tbs.plus_minus', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as plus_minus_rolling_avg_5,
        {{ rolling_std_dev('tbs.plus_minus', ['pg.player_id'], 'pg.game_date', 5, 1, true, false) }} as plus_minus_rolling_std_5,
        
        -- Advanced stats 5-game rolling averages
        {{ advanced_rolling_average('abs.est_off_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as est_off_rating_rolling_avg_5,
        {{ advanced_rolling_average('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as off_rating_rolling_avg_5,
        {{ rolling_std_dev('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as off_rating_rolling_std_5,
        {{ advanced_rolling_average('abs.est_def_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as est_def_rating_rolling_avg_5,
        {{ advanced_rolling_average('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as def_rating_rolling_avg_5,
        {{ rolling_std_dev('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as def_rating_rolling_std_5,
        {{ advanced_rolling_average('abs.est_net_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as est_net_rating_rolling_avg_5,
        {{ advanced_rolling_average('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as net_rating_rolling_avg_5,
        {{ rolling_std_dev('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as net_rating_rolling_std_5,
        {{ advanced_rolling_average('abs.ast_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as ast_pct_rolling_avg_5,
        {{ advanced_rolling_average('abs.ast_to_tov_ratio', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as ast_to_tov_ratio_rolling_avg_5,
        {{ advanced_rolling_average('abs.ast_ratio', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as ast_ratio_rolling_avg_5,
        {{ advanced_rolling_average('abs.off_reb_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as off_reb_pct_rolling_avg_5,
        {{ advanced_rolling_average('abs.def_reb_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as def_reb_pct_rolling_avg_5,
        {{ advanced_rolling_average('abs.reb_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as reb_pct_rolling_avg_5,
        {{ advanced_rolling_average('abs.tov_ratio', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as tov_ratio_rolling_avg_5,
        {{ advanced_rolling_average('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as eff_fg_pct_rolling_avg_5,
        {{ rolling_std_dev('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as eff_fg_pct_rolling_std_5,
        {{ advanced_rolling_average('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as ts_pct_rolling_avg_5,
        {{ rolling_std_dev('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as ts_pct_rolling_std_5,
        {{ advanced_rolling_average('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as usage_pct_rolling_avg_5,
        {{ rolling_std_dev('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as usage_pct_rolling_std_5,
        {{ advanced_rolling_average('abs.est_usage_pct', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as est_usage_pct_rolling_avg_5,
        {{ advanced_rolling_average('abs.est_pace', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as est_pace_rolling_avg_5,
        {{ advanced_rolling_average('abs.pace', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pace_rolling_avg_5,
        {{ rolling_std_dev('abs.pace', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pace_rolling_std_5,
        {{ advanced_rolling_average('abs.pace_per_40', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pace_per_40_rolling_avg_5,
        {{ advanced_rolling_average('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as possessions_rolling_avg_5,
        {{ rolling_std_dev('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as possessions_rolling_std_5,
        {{ advanced_rolling_average('abs.pie', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pie_rolling_avg_5,

        -- Hustle stats 5-game rolling averages
        {{ advanced_rolling_average('hbs.cont_shots', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as cont_shots_rolling_avg_5,
        {{ advanced_rolling_average('hbs.cont_2pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as cont_2pt_rolling_avg_5,
        {{ advanced_rolling_average('hbs.cont_3pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as cont_3pt_rolling_avg_5,
        {{ advanced_rolling_average('hbs.deflections', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as deflections_rolling_avg_5,
        {{ advanced_rolling_average('hbs.charges_drawn', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as charges_drawn_rolling_avg_5,
        {{ advanced_rolling_average('hbs.screen_ast', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as screen_ast_rolling_avg_5,
        {{ advanced_rolling_average('hbs.screen_ast_pts', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as screen_ast_pts_rolling_avg_5,
        {{ advanced_rolling_average('hbs.off_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as off_loose_balls_rec_rolling_avg_5,
        {{ advanced_rolling_average('hbs.def_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as def_loose_balls_rec_rolling_avg_5,
        {{ advanced_rolling_average('hbs.tot_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as tot_loose_balls_rec_rolling_avg_5,
        {{ advanced_rolling_average('hbs.off_box_outs', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as off_box_outs_rolling_avg_5,
        {{ advanced_rolling_average('hbs.def_box_outs', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as def_box_outs_rolling_avg_5,
        {{ advanced_rolling_average('hbs.box_out_team_reb', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as box_out_team_reb_rolling_avg_5,
        {{ advanced_rolling_average('hbs.box_out_player_reb', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as box_out_player_reb_rolling_avg_5,
        {{ advanced_rolling_average('hbs.tot_box_outs', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as tot_box_outs_rolling_avg_5,

        -- Usage stats 5-game rolling averages
        {{ advanced_rolling_average('ubs.pct_of_team_fgm', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_fgm_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_fga', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_fga_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3m', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_fg3m_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3a', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_fg3a_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_ftm', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_ftm_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_fta', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_fta_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_oreb', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_oreb_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_dreb', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_dreb_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_reb', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_reb_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_ast', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_ast_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_tov', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_tov_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_stl', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_stl_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_blk', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_blk_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_blk_allowed', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_blk_allowed_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_pf', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_pf_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_pfd', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_pfd_rolling_avg_5,
        {{ advanced_rolling_average('ubs.pct_of_team_pts', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_of_team_pts_rolling_avg_5,
        
        -- Scoring stats 5-game rolling averages
        {{ advanced_rolling_average('sbs.pct_fga_2pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_fga_2pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_fga_3pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_fga_3pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_pts_2pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_pts_2pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_pts_midrange_2pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_pts_midrange_2pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_pts_3pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_pts_3pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_pts_fastbreak', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_pts_fastbreak_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_pts_ft', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_pts_ft_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_pts_off_tov', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_pts_off_tov_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_pts_in_paint', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_pts_in_paint_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_assisted_2pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_assisted_2pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_unassisted_2pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_unassisted_2pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_assisted_3pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_assisted_3pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_unassisted_3pt', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_unassisted_3pt_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_assisted_fgm', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_assisted_fgm_rolling_avg_5,
        {{ advanced_rolling_average('sbs.pct_unassisted_fgm', ['tbs.player_id'], 'tbs.game_date', 5, 1, true, false) }} as pct_unassisted_fgm_rolling_avg_5,
        
        -- Rolling 10-game averages
        {{ advanced_rolling_average('tbs.pts', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pts_rolling_avg_10,
        {{ rolling_std_dev('tbs.pts', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pts_rolling_std_10,
        {{ coefficient_of_variation('tbs.pts', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as pts_rolling_cv_10,
        {{ advanced_rolling_average('tbs.min', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as min_rolling_avg_10,
        {{ rolling_std_dev('tbs.min', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as min_rolling_std_10,
        {{ coefficient_of_variation('tbs.min', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as min_rolling_cv_10,
        {{ advanced_rolling_average('tbs.fgm', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as fgm_rolling_avg_10,
        {{ advanced_rolling_average('tbs.fga', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as fga_rolling_avg_10,
        {{ rolling_percentage('tbs.fgm', 'tbs.fga', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as fg_pct_rolling_avg_10,
        {{ rolling_std_dev('tbs.fg_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as fg_pct_rolling_std_10,
        {{ advanced_rolling_average('tbs.fg3m', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as fg3m_rolling_avg_10,
        {{ advanced_rolling_average('tbs.fg3a', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as fg3a_rolling_avg_10,
        {{ rolling_percentage('tbs.fg3m', 'tbs.fg3a', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as fg3_pct_rolling_avg_10,
        {{ rolling_std_dev('tbs.fg3_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as fg3_pct_rolling_std_10,
        {{ advanced_rolling_average('tbs.ftm', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ftm_rolling_avg_10,
        {{ advanced_rolling_average('tbs.fta', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as fta_rolling_avg_10,
        {{ rolling_percentage('tbs.ftm', 'tbs.fta', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as ft_pct_rolling_avg_10,
        {{ rolling_std_dev('tbs.ft_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ft_pct_rolling_std_10,
        {{ advanced_rolling_average('tbs.off_reb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as off_reb_rolling_avg_10,
        {{ advanced_rolling_average('tbs.def_reb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as def_reb_rolling_avg_10,
        {{ advanced_rolling_average('tbs.reb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as reb_rolling_avg_10,
        {{ rolling_std_dev('tbs.reb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as reb_rolling_std_10,
        {{ coefficient_of_variation('tbs.reb', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as reb_rolling_cv_10,
        {{ advanced_rolling_average('tbs.ast', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ast_rolling_avg_10,
        {{ rolling_std_dev('tbs.ast', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ast_rolling_std_10,
        {{ advanced_rolling_average('tbs.stl', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as stl_rolling_avg_10,
        {{ advanced_rolling_average('tbs.blk', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as blk_rolling_avg_10,
        {{ advanced_rolling_average('tbs.tov', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as tov_rolling_avg_10,
        {{ advanced_rolling_average('tbs.pf', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pf_rolling_avg_10,
        {{ advanced_rolling_average('tbs.plus_minus', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as plus_minus_rolling_avg_10,
        {{ rolling_std_dev('tbs.plus_minus', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as plus_minus_rolling_std_10,
        {{ coefficient_of_variation('tbs.plus_minus', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as plus_minus_rolling_cv_10,
        
        -- Advanced stats 10-game rolling averages  
        {{ advanced_rolling_average('abs.est_off_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as est_off_rating_rolling_avg_10,
        {{ advanced_rolling_average('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as off_rating_rolling_avg_10,
        {{ rolling_std_dev('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as off_rating_rolling_std_10,
        {{ coefficient_of_variation('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as off_rating_rolling_cv_10,
        {{ advanced_rolling_average('abs.est_def_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as est_def_rating_rolling_avg_10,
        {{ advanced_rolling_average('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as def_rating_rolling_avg_10,
        {{ rolling_std_dev('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as def_rating_rolling_std_10,
        {{ coefficient_of_variation('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as def_rating_rolling_cv_10,
        {{ advanced_rolling_average('abs.est_net_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as est_net_rating_rolling_avg_10,
        {{ advanced_rolling_average('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as net_rating_rolling_avg_10,
        {{ rolling_std_dev('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as net_rating_rolling_std_10,
        {{ advanced_rolling_average('abs.ast_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ast_pct_rolling_avg_10,
        {{ advanced_rolling_average('abs.ast_to_tov_ratio', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ast_to_tov_ratio_rolling_avg_10,
        {{ advanced_rolling_average('abs.ast_ratio', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ast_ratio_rolling_avg_10,
        {{ advanced_rolling_average('abs.off_reb_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as off_reb_pct_rolling_avg_10,
        {{ advanced_rolling_average('abs.def_reb_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as def_reb_pct_rolling_avg_10,
        {{ advanced_rolling_average('abs.reb_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as reb_pct_rolling_avg_10,
        {{ rolling_std_dev('abs.reb_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as reb_pct_rolling_std_10, 
        {{ coefficient_of_variation('abs.reb_pct', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as reb_pct_rolling_cv_10,
        {{ advanced_rolling_average('abs.tov_ratio', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as tov_ratio_rolling_avg_10,
        {{ advanced_rolling_average('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as eff_fg_pct_rolling_avg_10,
        {{ rolling_std_dev('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as eff_fg_pct_rolling_std_10,
        {{ coefficient_of_variation('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as eff_fg_pct_rolling_cv_10,
        {{ advanced_rolling_average('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ts_pct_rolling_avg_10,
        {{ rolling_std_dev('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as ts_pct_rolling_std_10,
        {{ advanced_rolling_average('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as usage_pct_rolling_avg_10,
        {{ rolling_std_dev('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as usage_pct_rolling_std_10,
        {{ coefficient_of_variation('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as usage_pct_rolling_cv_10,
        {{ advanced_rolling_average('abs.est_usage_pct', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as est_usage_pct_rolling_avg_10,
        {{ advanced_rolling_average('abs.est_pace', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as est_pace_rolling_avg_10,
        {{ advanced_rolling_average('abs.pace', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pace_rolling_avg_10,
        {{ rolling_std_dev('abs.pace', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pace_rolling_std_10,
        {{ coefficient_of_variation('abs.pace', ['tbs.player_id'], 'tbs.game_date', 10, false) }} as pace_rolling_cv_10,
        {{ advanced_rolling_average('abs.pace_per_40', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pace_per_40_rolling_avg_10,
        {{ advanced_rolling_average('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as possessions_rolling_avg_10,
        {{ rolling_std_dev('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as possessions_rolling_std_10,
        {{ advanced_rolling_average('abs.pie', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pie_rolling_avg_10,

        -- Hustle stats 10-game rolling averages
        {{ advanced_rolling_average('hbs.cont_shots', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as cont_shots_rolling_avg_10,
        {{ advanced_rolling_average('hbs.cont_2pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as cont_2pt_rolling_avg_10,
        {{ advanced_rolling_average('hbs.cont_3pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as cont_3pt_rolling_avg_10,
        {{ advanced_rolling_average('hbs.deflections', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as deflections_rolling_avg_10,
        {{ advanced_rolling_average('hbs.charges_drawn', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as charges_drawn_rolling_avg_10,
        {{ advanced_rolling_average('hbs.screen_ast', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as screen_ast_rolling_avg_10,
        {{ advanced_rolling_average('hbs.screen_ast_pts', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as screen_ast_pts_rolling_avg_10,
        {{ advanced_rolling_average('hbs.off_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as off_loose_balls_rec_rolling_avg_10,
        {{ advanced_rolling_average('hbs.def_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as def_loose_balls_rec_rolling_avg_10,
        {{ advanced_rolling_average('hbs.tot_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as tot_loose_balls_rec_rolling_avg_10,
        {{ advanced_rolling_average('hbs.off_box_outs', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as off_box_outs_rolling_avg_10,
        {{ advanced_rolling_average('hbs.def_box_outs', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as def_box_outs_rolling_avg_10,
        {{ advanced_rolling_average('hbs.box_out_team_reb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as box_out_team_reb_rolling_avg_10,
        {{ advanced_rolling_average('hbs.box_out_player_reb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as box_out_player_reb_rolling_avg_10,
        {{ advanced_rolling_average('hbs.tot_box_outs', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as tot_box_outs_rolling_avg_10,

        -- Usage stats 10-game rolling averages
        {{ advanced_rolling_average('ubs.pct_of_team_fgm', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_fgm_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_fga', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_fga_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3m', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_fg3m_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3a', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_fg3a_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_ftm', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_ftm_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_fta', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_fta_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_oreb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_oreb_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_dreb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_dreb_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_reb', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_reb_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_ast', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_ast_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_tov', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_tov_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_stl', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_stl_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_blk', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_blk_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_blk_allowed', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_blk_allowed_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_pf', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_pf_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_pfd', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_pfd_rolling_avg_10,
        {{ advanced_rolling_average('ubs.pct_of_team_pts', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_of_team_pts_rolling_avg_10,
        
        -- Scoring stats 10-game rolling averages
        {{ advanced_rolling_average('sbs.pct_fga_2pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_fga_2pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_fga_3pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_fga_3pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_pts_2pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_pts_2pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_pts_midrange_2pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_pts_midrange_2pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_pts_3pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_pts_3pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_pts_fastbreak', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_pts_fastbreak_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_pts_ft', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_pts_ft_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_pts_off_tov', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_pts_off_tov_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_pts_in_paint', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_pts_in_paint_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_assisted_2pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_assisted_2pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_unassisted_2pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_unassisted_2pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_assisted_3pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_assisted_3pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_unassisted_3pt', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_unassisted_3pt_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_assisted_fgm', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_assisted_fgm_rolling_avg_10,
        {{ advanced_rolling_average('sbs.pct_unassisted_fgm', ['tbs.player_id'], 'tbs.game_date', 10, 1, true, false) }} as pct_unassisted_fgm_rolling_avg_10,
        
        -- Rolling 15-game averages
        {{ advanced_rolling_average('tbs.pts', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pts_rolling_avg_15,
        {{ rolling_std_dev('tbs.pts', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pts_rolling_std_15,
        {{ advanced_rolling_average('tbs.min', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as min_rolling_avg_15,
        {{ rolling_std_dev('tbs.min', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as min_rolling_std_15,
        {{ advanced_rolling_average('tbs.fgm', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as fgm_rolling_avg_15,
        {{ advanced_rolling_average('tbs.fga', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as fga_rolling_avg_15,
        {{ rolling_percentage('tbs.fgm', 'tbs.fga', ['tbs.player_id'], 'tbs.game_date', 15, false) }} as fg_pct_rolling_avg_15,
        {{ rolling_std_dev('tbs.fg_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as fg_pct_rolling_std_15,
        {{ advanced_rolling_average('tbs.fg3m', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as fg3m_rolling_avg_15,
        {{ advanced_rolling_average('tbs.fg3a', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as fg3a_rolling_avg_15,
        {{ rolling_percentage('tbs.fg3m', 'tbs.fg3a', ['tbs.player_id'], 'tbs.game_date', 15, false) }} as fg3_pct_rolling_avg_15,
        {{ rolling_std_dev('tbs.fg3_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as fg3_pct_rolling_std_15,
        {{ advanced_rolling_average('tbs.ftm', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ftm_rolling_avg_15,
        {{ advanced_rolling_average('tbs.fta', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as fta_rolling_avg_15,
        {{ rolling_percentage('tbs.ftm', 'tbs.fta', ['tbs.player_id'], 'tbs.game_date', 15, false) }} as ft_pct_rolling_avg_15,
        {{ rolling_std_dev('tbs.ft_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ft_pct_rolling_std_15,
        {{ advanced_rolling_average('tbs.off_reb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as off_reb_rolling_avg_15,
        {{ advanced_rolling_average('tbs.def_reb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as def_reb_rolling_avg_15,
        {{ advanced_rolling_average('tbs.reb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as reb_rolling_avg_15,
        {{ rolling_std_dev('tbs.reb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as reb_rolling_std_15,
        {{ advanced_rolling_average('tbs.ast', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ast_rolling_avg_15,
        {{ rolling_std_dev('tbs.ast', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ast_rolling_std_15,
        {{ advanced_rolling_average('tbs.stl', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as stl_rolling_avg_15,
        {{ advanced_rolling_average('tbs.blk', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as blk_rolling_avg_15,
        {{ advanced_rolling_average('tbs.tov', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as tov_rolling_avg_15,
        {{ advanced_rolling_average('tbs.pf', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pf_rolling_avg_15,
        {{ advanced_rolling_average('tbs.plus_minus', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as plus_minus_rolling_avg_15,
        {{ rolling_std_dev('tbs.plus_minus', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as plus_minus_rolling_std_15,
        
        -- Advanced stats 15-game rolling averages
        {{ advanced_rolling_average('abs.est_off_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as est_off_rating_rolling_avg_15,
        {{ advanced_rolling_average('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as off_rating_rolling_avg_15,
        {{ rolling_std_dev('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as off_rating_rolling_std_15,
        {{ advanced_rolling_average('abs.est_def_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as est_def_rating_rolling_avg_15,
        {{ advanced_rolling_average('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as def_rating_rolling_avg_15,
        {{ rolling_std_dev('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as def_rating_rolling_std_15,
        {{ advanced_rolling_average('abs.est_net_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as est_net_rating_rolling_avg_15,
        {{ advanced_rolling_average('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as net_rating_rolling_avg_15,
        {{ rolling_std_dev('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as net_rating_rolling_std_15,
        {{ advanced_rolling_average('abs.ast_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ast_pct_rolling_avg_15,
        {{ advanced_rolling_average('abs.ast_to_tov_ratio', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ast_to_tov_ratio_rolling_avg_15,
        {{ advanced_rolling_average('abs.ast_ratio', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ast_ratio_rolling_avg_15,
        {{ advanced_rolling_average('abs.off_reb_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as off_reb_pct_rolling_avg_15,
        {{ advanced_rolling_average('abs.def_reb_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as def_reb_pct_rolling_avg_15,
        {{ advanced_rolling_average('abs.reb_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as reb_pct_rolling_avg_15,
        {{ advanced_rolling_average('abs.tov_ratio', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as tov_ratio_rolling_avg_15,
        {{ advanced_rolling_average('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as eff_fg_pct_rolling_avg_15,
        {{ rolling_std_dev('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as eff_fg_pct_rolling_std_15,
        {{ coefficient_of_variation('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 15, false) }} as eff_fg_pct_rolling_cv_15,
        {{ advanced_rolling_average('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ts_pct_rolling_avg_15,
        {{ rolling_std_dev('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as ts_pct_rolling_std_15,
        {{ advanced_rolling_average('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as usage_pct_rolling_avg_15,
        {{ rolling_std_dev('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as usage_pct_rolling_std_15,
        {{ coefficient_of_variation('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 15, false) }} as usage_pct_rolling_cv_15,
        {{ advanced_rolling_average('abs.est_usage_pct', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as est_usage_pct_rolling_avg_15,
        {{ advanced_rolling_average('abs.est_pace', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as est_pace_rolling_avg_15,
        {{ advanced_rolling_average('abs.pace', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pace_rolling_avg_15,
        {{ rolling_std_dev('abs.pace', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pace_rolling_std_15,
        {{ advanced_rolling_average('abs.pace_per_40', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pace_per_40_rolling_avg_15,
        {{ advanced_rolling_average('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as possessions_rolling_avg_15,
        {{ rolling_std_dev('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as possessions_rolling_std_15,
        {{ advanced_rolling_average('abs.pie', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pie_rolling_avg_15,

        -- Hustle stats 15-game rolling averages
        {{ advanced_rolling_average('hbs.cont_shots', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as cont_shots_rolling_avg_15,
        {{ advanced_rolling_average('hbs.cont_2pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as cont_2pt_rolling_avg_15,
        {{ advanced_rolling_average('hbs.cont_3pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as cont_3pt_rolling_avg_15,
        {{ advanced_rolling_average('hbs.deflections', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as deflections_rolling_avg_15,
        {{ advanced_rolling_average('hbs.charges_drawn', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as charges_drawn_rolling_avg_15,
        {{ advanced_rolling_average('hbs.screen_ast', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as screen_ast_rolling_avg_15,
        {{ advanced_rolling_average('hbs.screen_ast_pts', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as screen_ast_pts_rolling_avg_15,
        {{ advanced_rolling_average('hbs.off_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as off_loose_balls_rec_rolling_avg_15,
        {{ advanced_rolling_average('hbs.def_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as def_loose_balls_rec_rolling_avg_15,
        {{ advanced_rolling_average('hbs.tot_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as tot_loose_balls_rec_rolling_avg_15,
        {{ advanced_rolling_average('hbs.off_box_outs', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as off_box_outs_rolling_avg_15,
        {{ advanced_rolling_average('hbs.def_box_outs', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as def_box_outs_rolling_avg_15,
        {{ advanced_rolling_average('hbs.box_out_team_reb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as box_out_team_reb_rolling_avg_15,
        {{ advanced_rolling_average('hbs.box_out_player_reb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as box_out_player_reb_rolling_avg_15,
        {{ advanced_rolling_average('hbs.tot_box_outs', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as tot_box_outs_rolling_avg_15,
        
        -- Usage stats 15-game rolling averages
        {{ advanced_rolling_average('ubs.pct_of_team_fgm', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_fgm_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_fga', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_fga_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3m', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_fg3m_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3a', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_fg3a_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_ftm', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_ftm_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_fta', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_fta_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_oreb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_oreb_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_dreb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_dreb_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_reb', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_reb_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_ast', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_ast_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_tov', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_tov_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_stl', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_stl_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_blk', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_blk_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_blk_allowed', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_blk_allowed_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_pf', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_pf_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_pfd', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_pfd_rolling_avg_15,
        {{ advanced_rolling_average('ubs.pct_of_team_pts', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_of_team_pts_rolling_avg_15,
        
        -- Scoring stats 15-game rolling averages
        {{ advanced_rolling_average('sbs.pct_fga_2pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_fga_2pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_fga_3pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_fga_3pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_pts_2pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_pts_2pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_pts_midrange_2pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_pts_midrange_2pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_pts_3pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_pts_3pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_pts_fastbreak', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_pts_fastbreak_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_pts_ft', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_pts_ft_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_pts_off_tov', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_pts_off_tov_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_pts_in_paint', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_pts_in_paint_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_assisted_2pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_assisted_2pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_unassisted_2pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_unassisted_2pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_assisted_3pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_assisted_3pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_unassisted_3pt', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_unassisted_3pt_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_assisted_fgm', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_assisted_fgm_rolling_avg_15,
        {{ advanced_rolling_average('sbs.pct_unassisted_fgm', ['tbs.player_id'], 'tbs.game_date', 15, 1, true, false) }} as pct_unassisted_fgm_rolling_avg_15,
        
        -- Rolling 20-game averages
        {{ advanced_rolling_average('tbs.pts', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pts_rolling_avg_20,
        {{ rolling_std_dev('tbs.pts', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pts_rolling_std_20,
        {{ advanced_rolling_average('tbs.min', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as min_rolling_avg_20,
        {{ rolling_std_dev('tbs.min', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as min_rolling_std_20,
        {{ advanced_rolling_average('tbs.fgm', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as fgm_rolling_avg_20,
        {{ advanced_rolling_average('tbs.fga', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as fga_rolling_avg_20,
        {{ rolling_percentage('tbs.fgm', 'tbs.fga', ['tbs.player_id'], 'tbs.game_date', 20, false) }} as fg_pct_rolling_avg_20,
        {{ rolling_std_dev('tbs.fg_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as fg_pct_rolling_std_20,
        {{ advanced_rolling_average('tbs.fg3m', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as fg3m_rolling_avg_20,
        {{ advanced_rolling_average('tbs.fg3a', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as fg3a_rolling_avg_20,
        {{ rolling_percentage('tbs.fg3m', 'tbs.fg3a', ['tbs.player_id'], 'tbs.game_date', 20, false) }} as fg3_pct_rolling_avg_20,
        {{ rolling_std_dev('tbs.fg3_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as fg3_pct_rolling_std_20,
        {{ advanced_rolling_average('tbs.ftm', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ftm_rolling_avg_20,
        {{ advanced_rolling_average('tbs.fta', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as fta_rolling_avg_20,
        {{ rolling_percentage('tbs.ftm', 'tbs.fta', ['tbs.player_id'], 'tbs.game_date', 20, false) }} as ft_pct_rolling_avg_20,
        {{ rolling_std_dev('tbs.ft_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ft_pct_rolling_std_20,
        {{ advanced_rolling_average('tbs.off_reb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as off_reb_rolling_avg_20,
        {{ advanced_rolling_average('tbs.def_reb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as def_reb_rolling_avg_20,
        {{ advanced_rolling_average('tbs.reb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as reb_rolling_avg_20,
        {{ rolling_std_dev('tbs.reb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as reb_rolling_std_20,
        {{ advanced_rolling_average('tbs.ast', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ast_rolling_avg_20,
        {{ rolling_std_dev('tbs.ast', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ast_rolling_std_20,
        {{ advanced_rolling_average('tbs.stl', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as stl_rolling_avg_20,
        {{ advanced_rolling_average('tbs.blk', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as blk_rolling_avg_20,
        {{ advanced_rolling_average('tbs.tov', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as tov_rolling_avg_20,
        {{ advanced_rolling_average('tbs.pf', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pf_rolling_avg_20,
        {{ advanced_rolling_average('tbs.plus_minus', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as plus_minus_rolling_avg_20,
        {{ rolling_std_dev('tbs.plus_minus', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as plus_minus_rolling_std_20,
        
        -- Advanced stats 20-game rolling averages
        {{ advanced_rolling_average('abs.est_off_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as est_off_rating_rolling_avg_20,
        {{ advanced_rolling_average('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as off_rating_rolling_avg_20,
        {{ rolling_std_dev('abs.off_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as off_rating_rolling_std_20,
        {{ advanced_rolling_average('abs.est_def_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as est_def_rating_rolling_avg_20,
        {{ advanced_rolling_average('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as def_rating_rolling_avg_20,
        {{ rolling_std_dev('abs.def_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as def_rating_rolling_std_20,
        {{ advanced_rolling_average('abs.est_net_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as est_net_rating_rolling_avg_20,
        {{ advanced_rolling_average('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as net_rating_rolling_avg_20,
        {{ rolling_std_dev('abs.net_rating', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as net_rating_rolling_std_20,
        {{ advanced_rolling_average('abs.ast_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ast_pct_rolling_avg_20,
        {{ advanced_rolling_average('abs.ast_to_tov_ratio', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ast_to_tov_ratio_rolling_avg_20,
        {{ advanced_rolling_average('abs.ast_ratio', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ast_ratio_rolling_avg_20,
        {{ advanced_rolling_average('abs.off_reb_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as off_reb_pct_rolling_avg_20,
        {{ advanced_rolling_average('abs.def_reb_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as def_reb_pct_rolling_avg_20,
        {{ advanced_rolling_average('abs.reb_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as reb_pct_rolling_avg_20,
        {{ advanced_rolling_average('abs.tov_ratio', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as tov_ratio_rolling_avg_20,
        {{ advanced_rolling_average('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as eff_fg_pct_rolling_avg_20,
        {{ rolling_std_dev('abs.eff_fg_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as eff_fg_pct_rolling_std_20,
        {{ advanced_rolling_average('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ts_pct_rolling_avg_20,
        {{ rolling_std_dev('abs.ts_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as ts_pct_rolling_std_20,
        {{ advanced_rolling_average('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as usage_pct_rolling_avg_20,
        {{ rolling_std_dev('abs.usage_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as usage_pct_rolling_std_20,
        {{ advanced_rolling_average('abs.est_usage_pct', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as est_usage_pct_rolling_avg_20,
        {{ advanced_rolling_average('abs.est_pace', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as est_pace_rolling_avg_20,
        {{ advanced_rolling_average('abs.pace', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pace_rolling_avg_20,
        {{ rolling_std_dev('abs.pace', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pace_rolling_std_20,
        {{ advanced_rolling_average('abs.pace_per_40', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pace_per_40_rolling_avg_20,
        {{ advanced_rolling_average('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as possessions_rolling_avg_20,
        {{ rolling_std_dev('abs.possessions', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as possessions_rolling_std_20,
        {{ advanced_rolling_average('abs.pie', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pie_rolling_avg_20,
        
        -- Hustle stats 20-game rolling averages
        {{ advanced_rolling_average('hbs.cont_shots', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as cont_shots_rolling_avg_20,
        {{ advanced_rolling_average('hbs.cont_2pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as cont_2pt_rolling_avg_20,
        {{ advanced_rolling_average('hbs.cont_3pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as cont_3pt_rolling_avg_20,
        {{ advanced_rolling_average('hbs.deflections', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as deflections_rolling_avg_20,
        {{ advanced_rolling_average('hbs.charges_drawn', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as charges_drawn_rolling_avg_20,
        {{ advanced_rolling_average('hbs.screen_ast', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as screen_ast_rolling_avg_20,
        {{ advanced_rolling_average('hbs.screen_ast_pts', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as screen_ast_pts_rolling_avg_20,
        {{ advanced_rolling_average('hbs.off_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as off_loose_balls_rec_rolling_avg_20,
        {{ advanced_rolling_average('hbs.def_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as def_loose_balls_rec_rolling_avg_20,
        {{ advanced_rolling_average('hbs.tot_loose_balls_rec', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as tot_loose_balls_rec_rolling_avg_20,
        {{ advanced_rolling_average('hbs.off_box_outs', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as off_box_outs_rolling_avg_20,
        {{ advanced_rolling_average('hbs.def_box_outs', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as def_box_outs_rolling_avg_20,
        {{ advanced_rolling_average('hbs.box_out_team_reb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as box_out_team_reb_rolling_avg_20,
        {{ advanced_rolling_average('hbs.box_out_player_reb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as box_out_player_reb_rolling_avg_20,
        {{ advanced_rolling_average('hbs.tot_box_outs', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as tot_box_outs_rolling_avg_20,
        
        -- Usage stats 20-game rolling averages
        {{ advanced_rolling_average('ubs.pct_of_team_fgm', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_fgm_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_fga', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_fga_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3m', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_fg3m_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3a', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_fg3a_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_ftm', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_ftm_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_fta', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_fta_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_oreb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_oreb_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_dreb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_dreb_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_reb', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_reb_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_ast', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_ast_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_tov', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_tov_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_stl', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_stl_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_blk', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_blk_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_blk_allowed', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_blk_allowed_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_pf', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_pf_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_pfd', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_pfd_rolling_avg_20,
        {{ advanced_rolling_average('ubs.pct_of_team_pts', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_of_team_pts_rolling_avg_20,
        
        -- Season-to-date averages (partition by player_id and season_year)
        {{ advanced_rolling_average('tbs.pts', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pts_season_avg,
        {{ advanced_rolling_average('tbs.min', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as min_season_avg,
        {{ advanced_rolling_average('tbs.fgm', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as fgm_season_avg,
        {{ advanced_rolling_average('tbs.fga', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as fga_season_avg,
        {{ rolling_percentage('tbs.fgm', 'tbs.fga', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, false) }} as fg_pct_season_avg,
        {{ advanced_rolling_average('tbs.fg3m', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as fg3m_season_avg,
        {{ advanced_rolling_average('tbs.fg3a', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as fg3a_season_avg,
        {{ rolling_percentage('tbs.fg3m', 'tbs.fg3a', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, false) }} as fg3_pct_season_avg,
        {{ advanced_rolling_average('tbs.ftm', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as ftm_season_avg,
        {{ advanced_rolling_average('tbs.fta', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as fta_season_avg,
        {{ rolling_percentage('tbs.ftm', 'tbs.fta', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, false) }} as ft_pct_season_avg,
        {{ advanced_rolling_average('tbs.off_reb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as off_reb_season_avg,
        {{ advanced_rolling_average('tbs.def_reb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as def_reb_season_avg,
        {{ advanced_rolling_average('tbs.reb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as reb_season_avg,
        {{ advanced_rolling_average('tbs.ast', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as ast_season_avg,
        {{ advanced_rolling_average('tbs.stl', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as stl_season_avg,
        {{ advanced_rolling_average('tbs.blk', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as blk_season_avg,
        {{ advanced_rolling_average('tbs.tov', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as tov_season_avg,
        {{ advanced_rolling_average('tbs.pf', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pf_season_avg,
        {{ advanced_rolling_average('tbs.plus_minus', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as plus_minus_season_avg,
        
        -- Advanced stats season-to-date averages
        {{ advanced_rolling_average('abs.est_off_rating', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as est_off_rating_season_avg,
        {{ advanced_rolling_average('abs.off_rating', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as off_rating_season_avg,
        {{ advanced_rolling_average('abs.est_def_rating', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as est_def_rating_season_avg,
        {{ advanced_rolling_average('abs.def_rating', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as def_rating_season_avg,
        {{ advanced_rolling_average('abs.est_net_rating', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as est_net_rating_season_avg,
        {{ advanced_rolling_average('abs.net_rating', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as net_rating_season_avg,
        {{ advanced_rolling_average('abs.ast_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as ast_pct_season_avg,
        {{ advanced_rolling_average('abs.ast_to_tov_ratio', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as ast_to_tov_ratio_season_avg,
        {{ advanced_rolling_average('abs.ast_ratio', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as ast_ratio_season_avg,
        {{ advanced_rolling_average('abs.off_reb_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as off_reb_pct_season_avg,
        {{ advanced_rolling_average('abs.def_reb_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as def_reb_pct_season_avg,
        {{ advanced_rolling_average('abs.reb_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as reb_pct_season_avg,
        {{ advanced_rolling_average('abs.tov_ratio', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as tov_ratio_season_avg,
        {{ advanced_rolling_average('abs.eff_fg_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as eff_fg_pct_season_avg,
        {{ advanced_rolling_average('abs.ts_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as ts_pct_season_avg,
        {{ advanced_rolling_average('abs.usage_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as usage_pct_season_avg,
        {{ advanced_rolling_average('abs.est_usage_pct', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as est_usage_pct_season_avg,
        {{ advanced_rolling_average('abs.est_pace', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as est_pace_season_avg,
        {{ advanced_rolling_average('abs.pace', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pace_season_avg,
        {{ advanced_rolling_average('abs.pace_per_40', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pace_per_40_season_avg,
        {{ advanced_rolling_average('abs.possessions', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as possessions_season_avg,
        {{ advanced_rolling_average('abs.pie', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pie_season_avg,
        
        -- Hustle stats season-to-date averages
        {{ advanced_rolling_average('hbs.cont_shots', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as cont_shots_season_avg,
        {{ advanced_rolling_average('hbs.cont_2pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as cont_2pt_season_avg,
        {{ advanced_rolling_average('hbs.cont_3pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as cont_3pt_season_avg,
        {{ advanced_rolling_average('hbs.deflections', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as deflections_season_avg,
        {{ advanced_rolling_average('hbs.charges_drawn', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as charges_drawn_season_avg,
        {{ advanced_rolling_average('hbs.screen_ast', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as screen_ast_season_avg,
        {{ advanced_rolling_average('hbs.screen_ast_pts', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as screen_ast_pts_season_avg,
        {{ advanced_rolling_average('hbs.off_loose_balls_rec', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as off_loose_balls_rec_season_avg,
        {{ advanced_rolling_average('hbs.def_loose_balls_rec', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as def_loose_balls_rec_season_avg,
        {{ advanced_rolling_average('hbs.tot_loose_balls_rec', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as tot_loose_balls_rec_season_avg,
        {{ advanced_rolling_average('hbs.off_box_outs', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as off_box_outs_season_avg,
        {{ advanced_rolling_average('hbs.def_box_outs', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as def_box_outs_season_avg,
        {{ advanced_rolling_average('hbs.box_out_team_reb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as box_out_team_reb_season_avg,
        {{ advanced_rolling_average('hbs.box_out_player_reb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as box_out_player_reb_season_avg,
        {{ advanced_rolling_average('hbs.tot_box_outs', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as tot_box_outs_season_avg,

        -- Usage stats season-to-date averages
        {{ advanced_rolling_average('ubs.pct_of_team_fgm', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_fgm_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_fga', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_fga_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3m', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_fg3m_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_fg3a', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_fg3a_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_ftm', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_ftm_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_fta', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_fta_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_oreb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_oreb_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_dreb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_dreb_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_reb', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_reb_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_ast', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_ast_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_tov', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_tov_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_stl', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_stl_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_blk', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_blk_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_blk_allowed', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_blk_allowed_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_pf', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_pf_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_pfd', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_pfd_season_avg,
        {{ advanced_rolling_average('ubs.pct_of_team_pts', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_of_team_pts_season_avg,
        
        -- Scoring stats 20-game rolling averages
        {{ advanced_rolling_average('sbs.pct_fga_2pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_fga_2pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_fga_3pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_fga_3pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_pts_2pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_pts_2pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_pts_midrange_2pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_pts_midrange_2pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_pts_3pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_pts_3pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_pts_fastbreak', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_pts_fastbreak_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_pts_ft', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_pts_ft_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_pts_off_tov', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_pts_off_tov_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_pts_in_paint', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_pts_in_paint_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_assisted_2pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_assisted_2pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_unassisted_2pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_unassisted_2pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_assisted_3pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_assisted_3pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_unassisted_3pt', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_unassisted_3pt_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_assisted_fgm', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_assisted_fgm_rolling_avg_20,
        {{ advanced_rolling_average('sbs.pct_unassisted_fgm', ['tbs.player_id'], 'tbs.game_date', 20, 1, true, false) }} as pct_unassisted_fgm_rolling_avg_20,
        
        -- Scoring stats season-to-date averages
        {{ advanced_rolling_average('sbs.pct_fga_2pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_fga_2pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_fga_3pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_fga_3pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_pts_2pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_pts_2pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_pts_midrange_2pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_pts_midrange_2pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_pts_3pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_pts_3pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_pts_fastbreak', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_pts_fastbreak_season_avg,
        {{ advanced_rolling_average('sbs.pct_pts_ft', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_pts_ft_season_avg,
        {{ advanced_rolling_average('sbs.pct_pts_off_tov', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_pts_off_tov_season_avg,
        {{ advanced_rolling_average('sbs.pct_pts_in_paint', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_pts_in_paint_season_avg,
        {{ advanced_rolling_average('sbs.pct_assisted_2pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_assisted_2pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_unassisted_2pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_unassisted_2pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_assisted_3pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_assisted_3pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_unassisted_3pt', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_unassisted_3pt_season_avg,
        {{ advanced_rolling_average('sbs.pct_assisted_fgm', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_assisted_fgm_season_avg,
        {{ advanced_rolling_average('sbs.pct_unassisted_fgm', ['tbs.player_id', 'tbs.season_year'], 'tbs.game_date', 9999, 1, true, false) }} as pct_unassisted_fgm_season_avg
    from player_games as pg
    left join traditional_boxscores as tbs on pg.player_game_key = tbs.player_game_key
    left join advanced_boxscores as abs on pg.player_game_key = abs.player_game_key
    left join hustle_boxscores as hbs on pg.player_game_key = hbs.player_game_key
    left join usage_boxscores as ubs on pg.player_game_key = ubs.player_game_key
    left join scoring_boxscores as sbs on pg.player_game_key = sbs.player_game_key
    left join game_context as gc on pg.player_game_key = gc.player_game_key
    left join team_game_context as tgc on pg.game_id = tgc.game_id
)

select * from traditional_rolling_features
order by player_id, game_date
