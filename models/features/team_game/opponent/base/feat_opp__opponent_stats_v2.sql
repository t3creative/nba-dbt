{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'opponent', 'rolling', 'comprehensive'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
) }}

with base_opponents as (
    select * from {{ ref('feat_opp__game_opponents_v2') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Opponent Team Rolling Models
opp_traditional as (
    select * from {{ ref('feat_team__traditional_rolling_v2') }}
),

opp_advanced as (
    select * from {{ ref('feat_team__advanced_rolling_v2') }}
),

opp_scoring as (
    select * from {{ ref('feat_team__scoring_rolling_v2') }}
),

opp_hustle as (
    select * from {{ ref('feat_team__hustle_rolling_v2') }}
),

opp_misc as (
    select * from {{ ref('feat_team__misc_rolling_v2') }}
),

-- Base team boxscore for current game context
base_team_stats as (
    select 
        team_id,
        game_id,
        game_date,
        season_year,
        -- Current game stats for immediate context
        pts as current_pts,
        def_rating as current_def_rating,
        off_rating as current_off_rating,
        pace as current_pace,
        updated_at
    from {{ ref('int_team__combined_boxscore') }}
),

opponent_features as (
    select
        -- Base Information
        bo.team_game_key,
        bo.game_id,
        bo.team_id,
        bo.opponent_id,
        bo.season_year,
        bo.game_date,
        bo.home_away,

        -- =================================================================
        -- 3-GAME OPPONENT ROLLING AVERAGES
        -- =================================================================
        
        -- Traditional Opponent Stats (3-game)
        coalesce(ot.pts_avg_l3, 0) as opp_pts_avg_l3,
        coalesce(ot.fga_avg_l3, 0) as opp_fga_avg_l3,
        coalesce(ot.fgm_avg_l3, 0) as opp_fgm_avg_l3,
        coalesce(ot.fg_pct_avg_l3, 0) as opp_fg_pct_avg_l3,
        coalesce(ot.fg3a_avg_l3, 0) as opp_fg3a_avg_l3,
        coalesce(ot.fg3m_avg_l3, 0) as opp_fg3m_avg_l3,
        coalesce(ot.fg3_pct_avg_l3, 0) as opp_fg3_pct_avg_l3,
        coalesce(ot.fta_avg_l3, 0) as opp_fta_avg_l3,
        coalesce(ot.ftm_avg_l3, 0) as opp_ftm_avg_l3,
        coalesce(ot.ft_pct_avg_l3, 0) as opp_ft_pct_avg_l3,
        coalesce(ot.reb_avg_l3, 0) as opp_reb_avg_l3,
        coalesce(ot.off_reb_avg_l3, 0) as opp_off_reb_avg_l3,
        coalesce(ot.def_reb_avg_l3, 0) as opp_def_reb_avg_l3,
        coalesce(ot.ast_avg_l3, 0) as opp_ast_avg_l3,
        coalesce(ot.stl_avg_l3, 0) as opp_stl_avg_l3,
        coalesce(ot.blk_avg_l3, 0) as opp_blk_avg_l3,
        coalesce(ot.tov_avg_l3, 0) as opp_tov_avg_l3,
        coalesce(ot.pf_avg_l3, 0) as opp_pf_avg_l3,
        coalesce(ot.plus_minus_avg_l3, 0) as opp_plus_minus_avg_l3,

        -- Advanced Opponent Stats (3-game)
        coalesce(oa.off_rating_avg_l3, 0) as opp_off_rating_avg_l3,
        coalesce(oa.def_rating_avg_l3, 0) as opp_def_rating_avg_l3,
        coalesce(oa.net_rating_avg_l3, 0) as opp_net_rating_avg_l3,
        coalesce(oa.pace_avg_l3, 0) as opp_pace_avg_l3,
        coalesce(oa.ts_pct_avg_l3, 0) as opp_ts_pct_avg_l3,
        coalesce(oa.eff_fg_pct_avg_l3, 0) as opp_eff_fg_pct_avg_l3,
        coalesce(oa.ast_pct_avg_l3, 0) as opp_ast_pct_avg_l3,
        coalesce(oa.ast_to_tov_ratio_avg_l3, 0) as opp_ast_to_tov_ratio_avg_l3,
        coalesce(oa.off_reb_pct_avg_l3, 0) as opp_off_reb_pct_avg_l3,
        coalesce(oa.def_reb_pct_avg_l3, 0) as opp_def_reb_pct_avg_l3,
        coalesce(oa.reb_pct_avg_l3, 0) as opp_reb_pct_avg_l3,
        coalesce(oa.tov_ratio_avg_l3, 0) as opp_tov_ratio_avg_l3,
        coalesce(oa.possessions_avg_l3, 0) as opp_possessions_avg_l3,
        coalesce(oa.pie_avg_l3, 0) as opp_pie_avg_l3,

        -- Scoring Profile Opponent Stats (3-game)
        coalesce(os.pct_fga_2pt_avg_l3, 0) as opp_pct_fga_2pt_avg_l3,
        coalesce(os.pct_fga_3pt_avg_l3, 0) as opp_pct_fga_3pt_avg_l3,
        coalesce(os.pct_pts_2pt_avg_l3, 0) as opp_pct_pts_2pt_avg_l3,
        coalesce(os.pct_pts_3pt_avg_l3, 0) as opp_pct_pts_3pt_avg_l3,
        coalesce(os.pct_pts_in_paint_avg_l3, 0) as opp_pct_pts_in_paint_avg_l3,
        coalesce(os.pct_pts_fastbreak_avg_l3, 0) as opp_pct_pts_fastbreak_avg_l3,
        coalesce(os.pct_pts_ft_avg_l3, 0) as opp_pct_pts_ft_avg_l3,
        coalesce(os.pct_assisted_fgm_avg_l3, 0) as opp_pct_assisted_fgm_avg_l3,
        coalesce(os.pct_unassisted_fgm_avg_l3, 0) as opp_pct_unassisted_fgm_avg_l3,

        -- Hustle/Activity Opponent Stats (3-game)
        coalesce(oh.deflections_avg_l3, 0) as opp_deflections_avg_l3,
        coalesce(oh.charges_drawn_avg_l3, 0) as opp_charges_drawn_avg_l3,
        coalesce(oh.tot_loose_balls_rec_avg_l3, 0) as opp_loose_balls_avg_l3,
        coalesce(oh.screen_ast_avg_l3, 0) as opp_screen_ast_avg_l3,

        -- Miscellaneous Opponent Stats (3-game)
        coalesce(om.pts_off_tov_avg_l3, 0) as opp_pts_off_tov_avg_l3,
        coalesce(om.second_chance_pts_avg_l3, 0) as opp_second_chance_pts_avg_l3,
        coalesce(om.fastbreak_pts_avg_l3, 0) as opp_fastbreak_pts_avg_l3,
        coalesce(om.pts_in_paint_avg_l3, 0) as opp_pts_in_paint_avg_l3,

        -- =================================================================
        -- 5-GAME OPPONENT ROLLING AVERAGES  
        -- =================================================================
        
        -- Traditional Opponent Stats (5-game)
        coalesce(ot.pts_avg_l5, 0) as opp_pts_avg_l5,
        coalesce(ot.fga_avg_l5, 0) as opp_fga_avg_l5,
        coalesce(ot.fgm_avg_l5, 0) as opp_fgm_avg_l5,
        coalesce(ot.fg_pct_avg_l5, 0) as opp_fg_pct_avg_l5,
        coalesce(ot.fg3a_avg_l5, 0) as opp_fg3a_avg_l5,
        coalesce(ot.fg3m_avg_l5, 0) as opp_fg3m_avg_l5,
        coalesce(ot.fg3_pct_avg_l5, 0) as opp_fg3_pct_avg_l5,
        coalesce(ot.reb_avg_l5, 0) as opp_reb_avg_l5,
        coalesce(ot.off_reb_avg_l5, 0) as opp_off_reb_avg_l5,
        coalesce(ot.def_reb_avg_l5, 0) as opp_def_reb_avg_l5,
        coalesce(ot.ast_avg_l5, 0) as opp_ast_avg_l5,
        coalesce(ot.stl_avg_l5, 0) as opp_stl_avg_l5,
        coalesce(ot.blk_avg_l5, 0) as opp_blk_avg_l5,
        coalesce(ot.tov_avg_l5, 0) as opp_tov_avg_l5,

        -- Advanced Opponent Stats (5-game)
        coalesce(oa.off_rating_avg_l5, 0) as opp_off_rating_avg_l5,
        coalesce(oa.def_rating_avg_l5, 0) as opp_def_rating_avg_l5,
        coalesce(oa.net_rating_avg_l5, 0) as opp_net_rating_avg_l5,
        coalesce(oa.pace_avg_l5, 0) as opp_pace_avg_l5,
        coalesce(oa.ts_pct_avg_l5, 0) as opp_ts_pct_avg_l5,
        coalesce(oa.eff_fg_pct_avg_l5, 0) as opp_eff_fg_pct_avg_l5,
        coalesce(oa.ast_to_tov_ratio_avg_l5, 0) as opp_ast_to_tov_ratio_avg_l5,
        coalesce(oa.off_reb_pct_avg_l5, 0) as opp_off_reb_pct_avg_l5,
        coalesce(oa.def_reb_pct_avg_l5, 0) as opp_def_reb_pct_avg_l5,
        coalesce(oa.tov_ratio_avg_l5, 0) as opp_tov_ratio_avg_l5,

        -- Scoring Profile Opponent Stats (5-game)
        coalesce(os.pct_fga_3pt_avg_l5, 0) as opp_pct_fga_3pt_avg_l5,
        coalesce(os.pct_pts_in_paint_avg_l5, 0) as opp_pct_pts_in_paint_avg_l5,
        coalesce(os.pct_pts_fastbreak_avg_l5, 0) as opp_pct_pts_fastbreak_avg_l5,

        -- =================================================================
        -- 10-GAME OPPONENT ROLLING AVERAGES
        -- =================================================================
        
        -- Traditional Opponent Stats (10-game)
        coalesce(ot.pts_avg_l10, 0) as opp_pts_avg_l10,
        coalesce(ot.fga_avg_l10, 0) as opp_fga_avg_l10,
        coalesce(ot.fg_pct_avg_l10, 0) as opp_fg_pct_avg_l10,
        coalesce(ot.fg3_pct_avg_l10, 0) as opp_fg3_pct_avg_l10,
        coalesce(ot.reb_avg_l10, 0) as opp_reb_avg_l10,
        coalesce(ot.ast_avg_l10, 0) as opp_ast_avg_l10,
        coalesce(ot.tov_avg_l10, 0) as opp_tov_avg_l10,

        -- Advanced Opponent Stats (10-game)
        coalesce(oa.off_rating_avg_l10, 0) as opp_off_rating_avg_l10,
        coalesce(oa.def_rating_avg_l10, 0) as opp_def_rating_avg_l10,
        coalesce(oa.net_rating_avg_l10, 0) as opp_net_rating_avg_l10,
        coalesce(oa.pace_avg_l10, 0) as opp_pace_avg_l10,
        coalesce(oa.ts_pct_avg_l10, 0) as opp_ts_pct_avg_l10,
        coalesce(oa.eff_fg_pct_avg_l10, 0) as opp_eff_fg_pct_avg_l10,

        -- =================================================================
        -- OPPONENT VOLATILITY & CONSISTENCY METRICS
        -- =================================================================
        
        -- Scoring Volatility (3-game window)
        coalesce(ot.pts_stddev_l3, 0) as opp_pts_volatility_l3,
        coalesce(ot.fg_pct_stddev_l3, 0) as opp_fg_pct_volatility_l3,
        coalesce(oa.off_rating_stddev_l3, 0) as opp_off_rating_volatility_l3,
        coalesce(oa.def_rating_stddev_l3, 0) as opp_def_rating_volatility_l3,

        -- Performance Ranges (max - min over 5 games)
        coalesce(ot.pts_max_l5, 0) - coalesce(ot.pts_min_l5, 0) as opp_pts_range_l5,
        coalesce(oa.off_rating_max_l5, 0) - coalesce(oa.off_rating_min_l5, 0) as opp_off_rating_range_l5,
        coalesce(oa.def_rating_max_l5, 0) - coalesce(oa.def_rating_min_l5, 0) as opp_def_rating_range_l5,

        -- =================================================================
        -- DERIVED OPPONENT MATCHUP FEATURES
        -- =================================================================
        
        -- Opponent Offensive Efficiency Composite
        case
            when coalesce(oa.ts_pct_avg_l5, 0) > 0 and coalesce(oa.eff_fg_pct_avg_l5, 0) > 0
            then (coalesce(oa.ts_pct_avg_l5, 0) * 0.6) + (coalesce(oa.eff_fg_pct_avg_l5, 0) * 0.4)
            else 0
        end as opp_offensive_efficiency_composite_l5,

        -- Opponent Pace Impact Factor
        case
            when coalesce(oa.pace_avg_l5, 0) > 100
            then coalesce(oa.pace_avg_l5, 0) / 100.0
            else 1.0
        end as opp_pace_factor_l5,

        -- Opponent Three-Point Threat Level
        (coalesce(os.pct_fga_3pt_avg_l5, 0) / 100.0) * (coalesce(ot.fg3_pct_avg_l5, 0) / 100.0) as opp_three_point_threat_l5,

        -- Opponent Interior Scoring Threat
        (coalesce(os.pct_pts_in_paint_avg_l5, 0) / 100.0) * (coalesce(ot.pts_avg_l5, 0) / 100.0) as opp_interior_threat_l5,

        -- Opponent Transition Threat
        coalesce(os.pct_pts_fastbreak_avg_l5, 0) / 100.0 * coalesce(ot.pts_avg_l5, 0) as opp_transition_threat_l5,

        -- Opponent Rebounding Dominance
        (coalesce(oa.off_reb_pct_avg_l5, 0) + coalesce(oa.def_reb_pct_avg_l5, 0)) / 2.0 as opp_rebounding_dominance_l5,

        -- Opponent Ball Security
        case
            when coalesce(oa.ast_to_tov_ratio_avg_l5, 0) > 0
            then coalesce(oa.ast_to_tov_ratio_avg_l5, 0)
            else 0
        end as opp_ball_security_l5,

        -- Opponent Defensive Pressure
        (coalesce(ot.stl_avg_l5, 0) + coalesce(oh.deflections_avg_l5, 0)) / 2.0 as opp_defensive_pressure_l5,

        -- =================================================================
        -- OPPONENT TREND INDICATORS (SHORT vs LONG WINDOW)
        -- =================================================================
        
        -- Scoring Trend (3-game vs 10-game)
        coalesce(ot.pts_avg_l3, 0) - coalesce(ot.pts_avg_l10, 0) as opp_scoring_trend_3v10,

        -- Efficiency Trend
        coalesce(oa.off_rating_avg_l3, 0) - coalesce(oa.off_rating_avg_l10, 0) as opp_off_rating_trend_3v10,
        coalesce(oa.def_rating_avg_l3, 0) - coalesce(oa.def_rating_avg_l10, 0) as opp_def_rating_trend_3v10,

        -- Three-Point Shooting Trend
        coalesce(ot.fg3_pct_avg_l3, 0) - coalesce(ot.fg3_pct_avg_l10, 0) as opp_fg3_pct_trend_3v10,

        -- Pace Trend
        coalesce(oa.pace_avg_l3, 0) - coalesce(oa.pace_avg_l10, 0) as opp_pace_trend_3v10,

        -- =================================================================
        -- CURRENT GAME CONTEXT (NON-LEAKING)
        -- =================================================================
        
        -- Recent opponent performance for context (from previous games only)
        coalesce(bts.current_pts, 0) as opp_recent_scoring_context,
        coalesce(bts.current_off_rating, 0) as opp_recent_off_rating_context,
        coalesce(bts.current_def_rating, 0) as opp_recent_def_rating_context,
        coalesce(bts.current_pace, 0) as opp_recent_pace_context,

        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        greatest(
            coalesce(bo.updated_at, '1900-01-01'::timestamp),
            coalesce(ot.updated_at, '1900-01-01'::timestamp),
            coalesce(oa.updated_at, '1900-01-01'::timestamp),
            coalesce(os.updated_at, '1900-01-01'::timestamp),
            coalesce(oh.updated_at, '1900-01-01'::timestamp),
            coalesce(om.updated_at, '1900-01-01'::timestamp),
            coalesce(bts.updated_at, '1900-01-01'::timestamp)
        ) as updated_at

    from base_opponents bo
    left join opp_traditional ot 
        on bo.opponent_id = ot.team_id 
        and bo.game_id = ot.game_id
    left join opp_advanced oa 
        on bo.opponent_id = oa.team_id 
        and bo.game_id = oa.game_id
    left join opp_scoring os 
        on bo.opponent_id = os.team_id 
        and bo.game_id = os.game_id
    left join opp_hustle oh 
        on bo.opponent_id = oh.team_id 
        and bo.game_id = oh.game_id
    left join opp_misc om 
        on bo.opponent_id = om.team_id 
        and bo.game_id = om.game_id
    left join base_team_stats bts 
        on bo.opponent_id = bts.team_id 
        and bo.game_id = bts.game_id
)

select * from opponent_features