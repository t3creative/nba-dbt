{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'team', 'performance', 'pregame', 'ml_safe'],
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['team_id', 'season_year'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']},
        {'columns': ['season_year', 'game_date']}
    ]
) }}

/*
ML-SAFE Team Performance Metrics - PRE-GAME ONLY
Uses historical data exclusively for prediction models.
No current game results or performance metrics included.
*/

with base_games as (
    select
        team_game_key,
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        home_away
    from {{ ref('feat_opp__game_opponents_v2') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Historical win/loss records (PRIOR games only)
historical_outcomes as (
    select
        tb.team_id,
        tb.game_id,
        tb.game_date,
        tb.season_year,
        tb.pts,
        -- Determine historical win/loss from completed games
        case when tb.pts > opp.pts then 1 else 0 end as was_win,
        tb.pts - opp.pts as point_margin,
        
        row_number() over (
            partition by tb.team_id, tb.season_year 
            order by tb.game_date
        ) as game_num_in_season
        
    from {{ ref('int_team__combined_boxscore') }} tb
    join {{ ref('int_team__combined_boxscore') }} opp 
        on tb.game_id = opp.game_id 
        and tb.opponent_id = opp.team_id
),

-- Calculate team records and streaks (PRIOR to current game)
team_records as (
    select
        bg.team_game_key,
        bg.game_id,
        bg.team_id,
        bg.opponent_id,
        bg.season_year,
        bg.game_date,
        bg.home_away,
        
        -- Season record PRIOR to current game
        coalesce((
            select sum(ho.was_win) 
            from historical_outcomes ho
            where ho.team_id = bg.team_id 
              and ho.season_year = bg.season_year
              and ho.game_date < bg.game_date
        ), 0) as prior_season_wins,
        
        coalesce((
            select count(*) 
            from historical_outcomes ho
            where ho.team_id = bg.team_id 
              and ho.season_year = bg.season_year
              and ho.game_date < bg.game_date
        ), 0) as prior_games_played,
        
        -- Last 5 games record PRIOR to current game
        coalesce((
            select sum(recent_5.was_win)
            from (
                select ho2.was_win
                from historical_outcomes ho2
                where ho2.team_id = bg.team_id 
                  and ho2.game_date < bg.game_date
                order by ho2.game_date desc
                limit 5
            ) recent_5
        ), 0) as prior_last5_wins,
        
        -- Last 10 games record PRIOR to current game
        coalesce((
            select sum(recent_10.was_win)
            from (
                select ho3.was_win
                from historical_outcomes ho3
                where ho3.team_id = bg.team_id 
                  and ho3.game_date < bg.game_date
                order by ho3.game_date desc
                limit 10
            ) recent_10
        ), 0) as prior_last10_wins,
        
        -- Average point margin PRIOR to current game
        coalesce((
            select avg(recent_10_margin.point_margin)
            from (
                select ho4.point_margin
                from historical_outcomes ho4
                where ho4.team_id = bg.team_id 
                  and ho4.game_date < bg.game_date
                order by ho4.game_date desc
                limit 10
            ) recent_10_margin
        ), 0) as prior_avg_point_margin
        
    from base_games bg
),

-- Add computed fields
team_records_computed as (
    select
        *,
        prior_games_played - prior_season_wins as prior_season_losses,
        5 - prior_last5_wins as prior_last5_losses,
        10 - prior_last10_wins as prior_last10_losses
    from team_records
),

-- Get team rolling performance metrics (PRE-GAME)
team_rolling_metrics as (
    select 
        team_game_key,
        max(pts_avg_l5) as pts_avg_l5,
        max(pts_avg_l10) as pts_avg_l10,
        max(pts_stddev_l5) as pts_stddev_l5,
        max(pts_max_l5) as pts_max_l5,
        max(pts_min_l5) as pts_min_l5,
        max(updated_at) as updated_at
    from {{ ref('feat_team__traditional_rolling_v2') }}
    group by team_game_key
),

team_rolling_advanced as (
    select 
        team_game_key,
        max(off_rating_avg_l5) as off_rating_avg_l5,
        max(def_rating_avg_l5) as def_rating_avg_l5,
        max(net_rating_avg_l5) as net_rating_avg_l5,
        max(pace_avg_l5) as pace_avg_l5,
        max(ts_pct_avg_l5) as ts_pct_avg_l5,
        max(eff_fg_pct_avg_l5) as eff_fg_pct_avg_l5,
        max(ast_to_tov_ratio_avg_l5) as ast_to_tov_ratio_avg_l5,
        max(reb_pct_avg_l5) as reb_pct_avg_l5,
        max(off_rating_avg_l10) as off_rating_avg_l10,
        max(def_rating_avg_l10) as def_rating_avg_l10,
        max(net_rating_avg_l10) as net_rating_avg_l10,
        max(pace_avg_l10) as pace_avg_l10,
        max(off_rating_stddev_l5) as off_rating_stddev_l5,
        max(def_rating_stddev_l5) as def_rating_stddev_l5,
        max(updated_at) as updated_at
    from {{ ref('feat_team__advanced_rolling_v2') }}
    group by team_game_key
),

team_rolling_scoring as (
    select 
        team_game_key,
        max(pct_pts_in_paint_avg_l5) as pct_pts_in_paint_avg_l5,
        max(pct_pts_fastbreak_avg_l5) as pct_pts_fastbreak_avg_l5,
        max(pct_pts_3pt_avg_l5) as pct_pts_3pt_avg_l5,
        max(pct_assisted_fgm_avg_l5) as pct_assisted_fgm_avg_l5,
        max(updated_at) as updated_at
    from {{ ref('feat_team__scoring_rolling_v2') }}
    group by team_game_key
),

final_performance_profile as (
    select
        tr.team_game_key,
        tr.game_id,
        tr.team_id,
        tr.opponent_id,
        tr.season_year,
        tr.game_date,
        tr.home_away,
        
        -- =================================================================
        -- TEAM RECORD & FORM (HISTORICAL ONLY)
        -- =================================================================
        tr.prior_season_wins,
        tr.prior_season_losses,
        tr.prior_games_played,
        tr.prior_last5_wins,
        5 - tr.prior_last5_wins as prior_last5_losses,
        tr.prior_last10_wins,
        10 - tr.prior_last10_wins as prior_last10_losses,
        tr.prior_avg_point_margin,
        
        -- Win Percentage (with early season handling)
        case 
            when tr.prior_games_played > 0 
            then tr.prior_season_wins::decimal / tr.prior_games_played
            else 0.5 -- Neutral for teams with no games
        end as prior_win_percentage,
        
        -- =================================================================
        -- RECENT PERFORMANCE METRICS (5-GAME ROLLING)
        -- =================================================================
        coalesce(trm.pts_avg_l5, 0) as recent_scoring_avg,
        coalesce(tra.off_rating_avg_l5, 0) as recent_off_rating,
        coalesce(tra.def_rating_avg_l5, 0) as recent_def_rating,
        coalesce(tra.net_rating_avg_l5, 0) as recent_net_rating,
        coalesce(tra.pace_avg_l5, 0) as recent_pace,
        coalesce(tra.ts_pct_avg_l5, 0) as recent_ts_pct,
        coalesce(tra.eff_fg_pct_avg_l5, 0) as recent_eff_fg_pct,
        coalesce(tra.ast_to_tov_ratio_avg_l5, 0) as recent_ast_tov_ratio,
        coalesce(tra.reb_pct_avg_l5, 0) as recent_reb_pct,
        
        -- =================================================================
        -- LONGER-TERM BASELINE (10-GAME ROLLING)
        -- =================================================================
        coalesce(trm.pts_avg_l10, 0) as baseline_scoring_avg,
        coalesce(tra.off_rating_avg_l10, 0) as baseline_off_rating,
        coalesce(tra.def_rating_avg_l10, 0) as baseline_def_rating,
        coalesce(tra.net_rating_avg_l10, 0) as baseline_net_rating,
        coalesce(tra.pace_avg_l10, 0) as baseline_pace,
        
        -- =================================================================
        -- TEAM PLAYSTYLE METRICS (HISTORICAL ROLLING)
        -- =================================================================
        coalesce(trs.pct_pts_in_paint_avg_l5, 0) as recent_paint_scoring_pct,
        coalesce(trs.pct_pts_fastbreak_avg_l5, 0) as recent_fastbreak_pct,
        coalesce(trs.pct_pts_3pt_avg_l5, 0) as recent_three_point_pct,
        coalesce(trs.pct_assisted_fgm_avg_l5, 0) as recent_assisted_fgm_pct,
        
        -- =================================================================
        -- TEAM STRENGTH & FORM INDICATORS
        -- =================================================================
        
        -- Team Quality Tier (based on prior record)
        case 
            when tr.prior_games_played < 5 then 'UNKNOWN'
            when (tr.prior_season_wins::decimal / tr.prior_games_played) > 0.65 then 'ELITE'
            when (tr.prior_season_wins::decimal / tr.prior_games_played) > 0.55 then 'GOOD'
            when (tr.prior_season_wins::decimal / tr.prior_games_played) > 0.45 then 'AVERAGE'
            when (tr.prior_season_wins::decimal / tr.prior_games_played) > 0.35 then 'POOR'
            else 'STRUGGLING'
        end as team_strength_tier,
        
        -- Current Form Assessment (based on last 10 games)
        case 
            when tr.prior_games_played < 10 then 'INSUFFICIENT_DATA'
            when tr.prior_last10_wins >= 8 then 'VERY_HOT'
            when tr.prior_last10_wins >= 6 then 'HOT'
            when tr.prior_last10_wins = 5 then 'NEUTRAL'
            when tr.prior_last10_wins >= 3 then 'COLD'
            else 'VERY_COLD'
        end as team_current_form,
        
        -- Momentum Indicators (recent vs baseline performance)
        coalesce(tra.off_rating_avg_l5, 0) - coalesce(tra.off_rating_avg_l10, 0) as offensive_momentum,
        coalesce(tra.def_rating_avg_l5, 0) - coalesce(tra.def_rating_avg_l10, 0) as defensive_momentum,
        coalesce(trm.pts_avg_l5, 0) - coalesce(trm.pts_avg_l10, 0) as scoring_momentum,
        
        -- =================================================================
        -- CONSISTENCY & VOLATILITY METRICS
        -- =================================================================
        coalesce(trm.pts_stddev_l5, 0) as recent_scoring_volatility,
        coalesce(tra.off_rating_stddev_l5, 0) as recent_offensive_volatility,
        coalesce(tra.def_rating_stddev_l5, 0) as recent_defensive_volatility,
        
        -- Performance Range (max - min over recent games)
        coalesce(trm.pts_max_l5, 0) - coalesce(trm.pts_min_l5, 0) as recent_scoring_range,
        
        -- =================================================================
        -- COMPOSITE PERFORMANCE INDICES
        -- =================================================================
        
        -- Overall Team Strength Score (0-100 scale)
        least(100, greatest(0,
            case when tr.prior_games_played > 0 
                 then (tr.prior_season_wins::decimal / tr.prior_games_played * 50) +
                      (coalesce(tra.net_rating_avg_l10, 0) / 20 * 30) +
                      (least(tr.prior_last10_wins, 10) * 2)
                 else 50 
            end
        )) as overall_team_strength_score,
        
        -- Recent Form Score (0-100 scale)
        case when tr.prior_games_played >= 5 
             then least(100, greatest(0, tr.prior_last5_wins * 20))
             else 50 
        end as recent_form_score,
        
        -- =================================================================
        -- EARLY SEASON FLAGS & CONTEXT
        -- =================================================================
        case when tr.prior_games_played < 10 then true else false end as early_season_flag,
        case when tr.prior_games_played < 5 then true else false end as very_early_season_flag,
        
        greatest(
            coalesce(trm.updated_at, '1900-01-01'::timestamp),
            coalesce(tra.updated_at, '1900-01-01'::timestamp),
            coalesce(trs.updated_at, '1900-01-01'::timestamp)
        ) as updated_at
        
    from team_records_computed tr
    left join team_rolling_metrics trm on tr.team_game_key = trm.team_game_key
    left join team_rolling_advanced tra on tr.team_game_key = tra.team_game_key
    left join team_rolling_scoring trs on tr.team_game_key = trs.team_game_key
)

select * from final_performance_profile