{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'opponent', 'pregame', 'composite'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
) }}

with base_opponent_features as (
    select
        *,
        -- Game sequencing for opponent
        row_number() over (
            partition by opponent_id, season_year 
            order by game_date
        ) as opponent_game_num_in_season
    from {{ ref('feat_opp__opponent_stats_v2') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- CTE for renamed fields and aliasing
aliased_opponent_features as (
    select
        *,
        opp_pts_off_tov_avg_l3 as opp_recent_pts_off_turnovers,
        opp_pts_volatility_l3 as opp_scoring_volatility,
        opp_fg_pct_volatility_l3 as opp_shooting_volatility,
        opp_off_rating_volatility_l3 as opp_offensive_volatility,
        opp_def_rating_volatility_l3 as opp_defensive_volatility
    from base_opponent_features
),

-- Calculate league-wide seasonal baselines for z-score normalization
seasonal_baselines as (
    select
        season_year,
        game_date,
        -- Rolling seasonal averages for z-score denominators
        avg(opp_off_rating_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_avg_off_rating_l10,
        
        stddev(opp_off_rating_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_stddev_off_rating_l10,
        
        avg(opp_def_rating_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_avg_def_rating_l10,
        
        stddev(opp_def_rating_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_stddev_def_rating_l10,
        
        avg(opp_pace_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_avg_pace_l10,
        
        stddev(opp_pace_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_stddev_pace_l10,
        
        avg(opp_pts_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_avg_pts_l10,
        
        stddev(opp_pts_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_stddev_pts_l10,
        
        avg(opp_ts_pct_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_avg_ts_pct_l10,
        
        stddev(opp_ts_pct_avg_l10) over (
            partition by season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_stddev_ts_pct_l10
    from base_opponent_features
),

pregame_opponent_profile as (
    select
        -- =================================================================
        -- BASE IDENTIFIERS & METADATA
        -- =================================================================
        bof.team_game_key,
        bof.game_id,
        bof.team_id,
        bof.opponent_id,
        bof.season_year,
        bof.game_date,
        bof.home_away,
        bof.opponent_game_num_in_season,

        -- =================================================================
        -- PRIMARY OPPONENT PROFILE (5-GAME RECENT FORM)
        -- =================================================================
        
        -- Core Performance Metrics
        bof.opp_pts_avg_l5 as opp_recent_scoring,
        bof.opp_off_rating_avg_l5 as opp_recent_off_rating,
        bof.opp_def_rating_avg_l5 as opp_recent_def_rating,
        bof.opp_net_rating_avg_l5 as opp_recent_net_rating,
        bof.opp_pace_avg_l5 as opp_recent_pace,
        
        -- Shooting Profile
        bof.opp_fg_pct_avg_l5 as opp_recent_fg_pct,
        bof.opp_fg3_pct_avg_l5 as opp_recent_fg3_pct,
        bof.opp_ts_pct_avg_l5 as opp_recent_ts_pct,
        bof.opp_eff_fg_pct_avg_l5 as opp_recent_eff_fg_pct,
        
        -- Rebounding & Defense
        bof.opp_reb_avg_l5 as opp_recent_rebounding,
        bof.opp_def_reb_pct_avg_l5 as opp_recent_def_reb_pct,
        bof.opp_off_reb_pct_avg_l5 as opp_recent_off_reb_pct,
        bof.opp_blk_avg_l5 as opp_recent_blocks,
        bof.opp_stl_avg_l5 as opp_recent_steals,
        
        -- Ball Movement & Turnovers
        bof.opp_ast_avg_l5 as opp_recent_assists,
        bof.opp_tov_avg_l5 as opp_recent_turnovers,
        bof.opp_ast_to_tov_ratio_avg_l5 as opp_recent_ast_tov_ratio,
        
        -- Scoring Breakdown
        bof.opp_pct_pts_in_paint_avg_l5 as opp_recent_paint_scoring_pct,
        bof.opp_pct_pts_fastbreak_avg_l5 as opp_recent_fastbreak_pct,
        bof.opp_pts_off_tov_avg_l3 as opp_recent_pts_off_turnovers,

        -- =================================================================
        -- LONGER-TERM BASELINE (10-GAME AVERAGES)
        -- =================================================================
        
        bof.opp_pts_avg_l10 as opp_baseline_scoring,
        bof.opp_off_rating_avg_l10 as opp_baseline_off_rating,
        bof.opp_def_rating_avg_l10 as opp_baseline_def_rating,
        bof.opp_pace_avg_l10 as opp_baseline_pace,
        bof.opp_fg_pct_avg_l10 as opp_baseline_fg_pct,
        bof.opp_fg3_pct_avg_l10 as opp_baseline_fg3_pct,
        bof.opp_ts_pct_avg_l10 as opp_baseline_ts_pct,

        -- =================================================================
        -- OPPONENT CONSISTENCY & VOLATILITY ANALYSIS
        -- =================================================================
        
        -- Performance Volatility (higher = less predictable)
        bof.opp_pts_volatility_l3 as opp_scoring_volatility,
        bof.opp_fg_pct_volatility_l3 as opp_shooting_volatility,
        bof.opp_off_rating_volatility_l3 as opp_offensive_volatility,
        bof.opp_def_rating_volatility_l3 as opp_defensive_volatility,
        
        -- Performance Ranges (max - min)
        bof.opp_pts_range_l5 as opp_scoring_range,
        bof.opp_off_rating_range_l5 as opp_offensive_range,
        bof.opp_def_rating_range_l5 as opp_defensive_range,

        -- =================================================================
        -- DERIVED OPPONENT THREAT ASSESSMENT
        -- =================================================================
        
        -- Composite Efficiency Metrics
        bof.opp_offensive_efficiency_composite_l5 as opp_offensive_threat_level,
        bof.opp_pace_factor_l5 as opp_pace_impact_factor,
        
        -- Specific Threat Categories
        bof.opp_three_point_threat_l5 as opp_perimeter_threat,
        bof.opp_interior_threat_l5 as opp_paint_threat,
        bof.opp_transition_threat_l5 as opp_fastbreak_threat,
        bof.opp_rebounding_dominance_l5 as opp_rebounding_threat,
        
        -- Defensive Capabilities
        bof.opp_ball_security_l5 as opp_ball_security_level,
        bof.opp_defensive_pressure_l5 as opp_defensive_pressure_level,

        -- =================================================================
        -- MOMENTUM & TREND INDICATORS
        -- =================================================================
        
        -- Performance Trends (recent vs baseline)
        bof.opp_scoring_trend_3v10 as opp_scoring_momentum,
        bof.opp_off_rating_trend_3v10 as opp_offensive_momentum,
        bof.opp_def_rating_trend_3v10 as opp_defensive_momentum,
        bof.opp_fg3_pct_trend_3v10 as opp_shooting_momentum,
        bof.opp_pace_trend_3v10 as opp_pace_momentum,
        
        -- Hot/Cold Streak Indicators
        case
            when bof.opp_scoring_trend_3v10 > 5 then 'HOT'
            when bof.opp_scoring_trend_3v10 < -5 then 'COLD'
            else 'NEUTRAL' 
        end as opp_scoring_streak_status,
        
        case
            when bof.opp_off_rating_trend_3v10 > 3 then 'HOT'
            when bof.opp_off_rating_trend_3v10 < -3 then 'COLD'
            else 'NEUTRAL'
        end as opp_offensive_streak_status,

        -- =================================================================
        -- LEAGUE-RELATIVE Z-SCORES (STANDARDIZED PERFORMANCE)
        -- =================================================================
        
        -- Offensive Rating Z-Score
        case
            when sb.season_stddev_off_rating_l10 > 0
            then (bof.opp_off_rating_avg_l10 - sb.season_avg_off_rating_l10) / 
                 nullif(sb.season_stddev_off_rating_l10, 0)
            else 0
        end as opp_off_rating_z_score,
        
        -- Defensive Rating Z-Score (lower is better, so we flip the sign)
        case
            when sb.season_stddev_def_rating_l10 > 0
            then -1 * (bof.opp_def_rating_avg_l10 - sb.season_avg_def_rating_l10) / 
                 nullif(sb.season_stddev_def_rating_l10, 0)
            else 0
        end as opp_def_rating_z_score,
        
        -- Pace Z-Score
        case
            when sb.season_stddev_pace_l10 > 0
            then (bof.opp_pace_avg_l10 - sb.season_avg_pace_l10) / 
                 nullif(sb.season_stddev_pace_l10, 0)
            else 0
        end as opp_pace_z_score,
        
        -- Scoring Z-Score
        case
            when sb.season_stddev_pts_l10 > 0
            then (bof.opp_pts_avg_l10 - sb.season_avg_pts_l10) / 
                 nullif(sb.season_stddev_pts_l10, 0)
            else 0
        end as opp_scoring_z_score,
        
        -- True Shooting Z-Score
        case
            when sb.season_stddev_ts_pct_l10 > 0
            then (bof.opp_ts_pct_avg_l10 - sb.season_avg_ts_pct_l10) / 
                 nullif(sb.season_stddev_ts_pct_l10, 0)
            else 0
        end as opp_shooting_efficiency_z_score,

        -- =================================================================
        -- ADAPTIVE FEATURES (EARLY SEASON HANDLING)
        -- =================================================================
        
        -- Use recent form if sufficient games, otherwise fall back to longer baseline
        coalesce(
            case when bof.opponent_game_num_in_season >= 5 
                 then bof.opp_off_rating_avg_l5 
                 else null end,
            bof.opp_off_rating_avg_l10,
            bof.opp_recent_off_rating_context
        ) as opp_adaptive_off_rating,
        
        coalesce(
            case when bof.opponent_game_num_in_season >= 5 
                 then bof.opp_def_rating_avg_l5 
                 else null end,
            bof.opp_def_rating_avg_l10,
            bof.opp_recent_def_rating_context
        ) as opp_adaptive_def_rating,
        
        coalesce(
            case when bof.opponent_game_num_in_season >= 5 
                 then bof.opp_pace_avg_l5 
                 else null end,
            bof.opp_pace_avg_l10,
            bof.opp_recent_pace_context
        ) as opp_adaptive_pace,
        
        coalesce(
            case when bof.opponent_game_num_in_season >= 5 
                 then bof.opp_pts_avg_l5 
                 else null end,
            bof.opp_pts_avg_l10,
            bof.opp_recent_scoring_context
        ) as opp_adaptive_scoring,

        -- =================================================================
        -- MATCHUP-SPECIFIC FEATURES
        -- =================================================================
        
        -- Opponent Strength Assessment (composite score 0-100)
        least(100, greatest(0, 
            (coalesce(bof.opp_offensive_efficiency_composite_l5, 50) * 0.4) +
            (coalesce(bof.opp_ball_security_l5, 50) * 0.2) +  
            (coalesce(bof.opp_rebounding_dominance_l5, 50) * 0.2) +
            (coalesce(bof.opp_defensive_pressure_l5, 50) * 0.2)
        )) as opp_overall_strength_score,
        
        -- Opponent Predictability Score (lower volatility = more predictable)
        case
            when (coalesce(bof.opp_scoring_volatility, 0) + 
                  coalesce(bof.opp_shooting_volatility, 0) + 
                  coalesce(bof.opp_offensive_volatility, 0)) > 0
            then 100 - least(100, (coalesce(bof.opp_scoring_volatility, 0) + 
                                   coalesce(bof.opp_shooting_volatility, 0) + 
                                   coalesce(bof.opp_offensive_volatility, 0)) * 10)
            else 50
        end as opp_predictability_score,

        -- =================================================================
        -- GAME CONTEXT FLAGS
        -- =================================================================
        
        -- Early Season Flag (less reliable data)
        case when bof.opponent_game_num_in_season < 10 then true else false end as opp_early_season_flag,
        
        -- High Variance Flag (unpredictable opponent)
        case when coalesce(bof.opp_scoring_volatility, 0) > 10 then true else false end as opp_high_variance_flag,
        
        -- Elite Opponent Flag (top tier performance)
        case when (coalesce(bof.opp_off_rating_avg_l10, 0) > 115 and 
                   coalesce(bof.opp_def_rating_avg_l10, 0) < 105) then true else false end as opp_elite_flag,

        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        bof.updated_at

    from aliased_opponent_features bof
    left join seasonal_baselines sb 
        on bof.season_year = sb.season_year 
        and bof.game_date = sb.game_date
),

deduped_pregame_opponent_profile AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team_game_key ORDER BY game_date DESC, updated_at DESC) AS row_num
        FROM pregame_opponent_profile
    ) t
    WHERE row_num = 1
)
select * from deduped_pregame_opponent_profile