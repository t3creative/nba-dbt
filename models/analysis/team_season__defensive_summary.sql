{{
    config(
        schema='features',
        alias='feat_team__season_defensive_summary',
        materialized='incremental',
        unique_key='team_season_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['team_season_key'], 'unique': true},
            {'columns': ['team_id', 'season_year']}
        ]
    )
}}

/*
Calculates a comprehensive set of team-level defensive summary statistics aggregated over a season.
Uses metrics from advanced, traditional, hustle, miscellaneous, and scoring team boxscores
to provide a holistic view of defensive performance, including opponent shooting, scoring sources,
rebounding, turnovers forced, and shot contesting.
*/

with
team_advanced_bxsc as (
    select
        team_game_key,
        team_id,
        season_year,
        game_date,
        def_rating,
        eff_fg_pct,
        ts_pct,
        def_reb_pct
    from {{ ref('int_team__combined_boxscore') }}
),

team_misc_bxsc as (
    select
        team_game_key,
        opp_pts_off_tov,
        opp_second_chance_pts,
        opp_fastbreak_pts,
        opp_pts_in_paint
    from {{ ref('int_team__combined_boxscore') }}
),

team_hustle_bxsc as (
    select
        team_game_key,
        cont_2pt,
        cont_3pt,
        deflections,
        charges_drawn,
        def_loose_balls_rec
    from {{ ref('int_team__combined_boxscore') }}
),

team_traditional_bxsc as (
    select
        team_game_key,
        fgm,
        fga,
        fg3m,
        fg3a,
        ftm,
        fta,
        tov,
        pf
    from {{ ref('int_team__combined_boxscore') }}
),

team_scoring_bxsc as (
    select
        team_game_key,
        pct_fga_2pt,
        pct_fga_3pt,
        pct_pts_2pt,
        pct_pts_midrange_2pt,
        pct_pts_3pt,
        pct_pts_fastbreak,
        pct_pts_ft,
        pct_pts_off_tov,
        pct_pts_in_paint
    from {{ ref('int_team__combined_boxscore') }}
),

-- Combine all relevant team defense metrics per game
team_game_defense_metrics as (
    select
        adv.team_game_key,
        adv.team_id,
        adv.season_year,
        adv.game_date,
        -- Team Advanced
        adv.def_rating,
        adv.eff_fg_pct    as opp_eff_fg_pct,
        adv.ts_pct        as opp_ts_pct,
        adv.def_reb_pct,
        -- Misc
        misc.opp_pts_off_tov,
        misc.opp_second_chance_pts,
        misc.opp_fastbreak_pts,
        misc.opp_pts_in_paint,
        -- Hustle
        hustle.cont_2pt,
        hustle.cont_3pt,
        hustle.deflections,
        hustle.charges_drawn,
        hustle.def_loose_balls_rec,
        -- Opponent Traditional
        trad.fgm as opp_fgm,
        trad.fga as opp_fga,
        trad.fg3m as opp_fg3m,
        trad.fg3a as opp_fg3a,
        trad.ftm as opp_ftm,
        trad.fta as opp_fta,
        trad.tov as opp_tov,
        trad.pf  as opp_pf,
        -- Team Scoring Distribution
        score.pct_fga_2pt,
        score.pct_fga_3pt,
        score.pct_pts_2pt,
        score.pct_pts_midrange_2pt,
        score.pct_pts_3pt,
        score.pct_pts_fastbreak,
        score.pct_pts_ft,
        score.pct_pts_off_tov,
        score.pct_pts_in_paint,
        -- Opponent Scoring Distribution
        score.pct_fga_2pt     as opp_pct_fga_2pt,
        score.pct_fga_3pt     as opp_pct_fga_3pt,
        score.pct_pts_2pt     as opp_pct_pts_2pt,
        score.pct_pts_midrange_2pt as opp_pct_pts_midrange_2pt,
        score.pct_pts_3pt     as opp_pct_pts_3pt,
        score.pct_pts_fastbreak   as opp_pct_pts_fastbreak,
        score.pct_pts_ft      as opp_pct_pts_ft,
        score.pct_pts_off_tov as opp_pct_pts_off_tov,
        score.pct_pts_in_paint   as opp_pct_pts_in_paint
    from team_advanced_bxsc adv
    left join team_misc_bxsc misc                   using (team_game_key)
    left join team_hustle_bxsc hustle               using (team_game_key)
    left join team_traditional_bxsc trad            using (team_game_key)
    left join team_scoring_bxsc score               using (team_game_key)
    {% if is_incremental() %}
    where adv.game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Aggregate metrics per team per season
team_season_defense_agg as (
    select
        team_id,
        season_year,
        max(game_date) as game_date, -- Keep track of latest game date for incremental logic
        -- Construct a unique key for the team-season combination
        concat(team_id, '_', season_year) as team_season_key,

        -- Average defensive stats over the season
        avg(def_rating) as avg_def_rating,
        avg(opp_eff_fg_pct) as avg_opp_eff_fg_pct,
        avg(opp_ts_pct) as avg_opp_ts_pct,
        avg(def_reb_pct) as avg_def_reb_pct,
        avg(opp_pts_off_tov) as avg_opp_pts_off_tov,
        avg(opp_second_chance_pts) as avg_opp_second_chance_pts,
        avg(opp_fastbreak_pts) as avg_opp_fastbreak_pts,
        avg(opp_pts_in_paint) as avg_opp_pts_in_paint,
        avg(cont_2pt) as avg_cont_2pt_per_game,
        avg(cont_3pt) as avg_cont_3pt_per_game,
        avg(deflections) as avg_deflections_per_game,
        avg(charges_drawn) as avg_charges_drawn_per_game,
        avg(def_loose_balls_rec) as avg_def_loose_balls_rec_per_game,
        avg(opp_pct_fga_2pt) as avg_opp_pct_fga_2pt,
        avg(opp_pct_fga_3pt) as avg_opp_pct_fga_3pt,
        avg(opp_pct_pts_2pt) as avg_opp_pct_pts_2pt,
        avg(opp_pct_pts_3pt) as avg_opp_pct_pts_3pt,
        avg(opp_pct_pts_fastbreak) as avg_opp_pct_pts_fastbreak,
        avg(opp_pct_pts_ft) as avg_opp_pct_pts_ft,
        avg(opp_pct_pts_off_tov) as avg_opp_pct_pts_off_tov,
        avg(opp_pct_pts_in_paint) as avg_opp_pct_pts_in_paint

    from team_game_defense_metrics
    where team_id is not null and season_year is not null
    group by 1, 2
),

final as (
    select
        team_season_key,
        team_id,
        season_year,
        game_date, -- Renamed from season_last_game_date for consistency
        -- Round for cleaner presentation, adjust precision as needed
        round(avg_def_rating::numeric, 2) as avg_def_rating,
        round(avg_opp_eff_fg_pct::numeric, 4) as avg_opp_eff_fg_pct,
        round(avg_opp_ts_pct::numeric, 4) as avg_opp_ts_pct,
        round(avg_def_reb_pct::numeric, 4) as avg_def_reb_pct,
        round(avg_opp_pts_off_tov::numeric, 2) as avg_opp_pts_off_tov,
        round(avg_opp_second_chance_pts::numeric, 2) as avg_opp_second_chance_pts,
        round(avg_opp_fastbreak_pts::numeric, 2) as avg_opp_fastbreak_pts,
        round(avg_opp_pts_in_paint::numeric, 2) as avg_opp_pts_in_paint,
        round(avg_cont_2pt_per_game::numeric, 2) as avg_cont_2pt_per_game,
        round(avg_cont_3pt_per_game::numeric, 2) as avg_cont_3pt_per_game,
        round(avg_deflections_per_game::numeric, 2) as avg_deflections_per_game,
        round(avg_charges_drawn_per_game::numeric, 2) as avg_charges_drawn_per_game,
        round(avg_def_loose_balls_rec_per_game::numeric, 2) as avg_def_loose_balls_rec_per_game,
        -- Opponent Scoring Distribution
        round(avg_opp_pct_fga_2pt::numeric, 4) as avg_opp_pct_fga_2pt,
        round(avg_opp_pct_fga_3pt::numeric, 4) as avg_opp_pct_fga_3pt,
        round(avg_opp_pct_pts_2pt::numeric, 4) as avg_opp_pct_pts_2pt,
        round(avg_opp_pct_pts_3pt::numeric, 4) as avg_opp_pct_pts_3pt,
        round(avg_opp_pct_pts_fastbreak::numeric, 4) as avg_opp_pct_pts_fastbreak,
        round(avg_opp_pct_pts_ft::numeric, 4) as avg_opp_pct_pts_ft,
        round(avg_opp_pct_pts_off_tov::numeric, 4) as avg_opp_pct_pts_off_tov,
        round(avg_opp_pct_pts_in_paint::numeric, 4) as avg_opp_pct_pts_in_paint

    from team_season_defense_agg
)

select * 
from final 