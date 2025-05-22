{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['player_id', 'team_id', 'season_year'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['season_year', 'game_date']},
        {'columns': ['player_id', 'season_year']}
    ],
    tags=['features', 'player', 'scoring', 'ml_ready', 'composite'],
    on_schema_change='sync_all_columns'
) }}

/*
====================================================================================================
FEAT_PLAYER__SCORING_FEATURES - Comprehensive Player Scoring Feature Engineering Pipeline
====================================================================================================

PURPOSE:
    Unified scoring feature layer that combines derived metrics, contextual projections, 
    interior scoring patterns, and team interaction effects into ML-ready features.

TEMPORAL SAFETY:
    - All features use point-in-time correct data (no future leakage)
    - Team context uses lagged metrics from previous games
    - Rolling averages exclude current game performance
    - Projections based on historical patterns only

FIXED: Join logic restructured to prevent data loss from upstream models

====================================================================================================
*/

WITH 
-- ====================================================================================================
-- RAW DATA EXTRACTION: Get all data without restrictive joins
-- ====================================================================================================

derived_metrics_raw AS (
    SELECT *
    FROM {{ ref('feat_player__derived_scoring_metrics') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }})
    {% endif %}
),

contextual_projections_raw AS (
    SELECT *
    FROM {{ ref('feat_player__contextual_projections') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }})
    {% endif %}
),

interior_deviations_raw AS (
    SELECT *
    FROM {{ ref('feat_player__interior_scoring_deviations') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }})
    {% endif %}
),

game_context_raw AS (
    SELECT *
    FROM {{ ref('feat_player__game_context_attributes') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }})
    {% endif %}
),

-- ====================================================================================================
-- ESTABLISH GRAIN: Use primary model as base, ensure unique keys
-- ====================================================================================================

all_player_games AS (
    -- Use derived_metrics as the primary grain since it should have the most complete coverage
    SELECT DISTINCT
        player_game_key,
        player_id,
        player_name,
        game_id,
        team_id,
        season_year,
        game_date
    FROM derived_metrics_raw
    
    UNION
    
    -- Add any additional keys from contextual_projections that might not be in derived_metrics
    SELECT DISTINCT
        player_game_key,
        player_id,
        player_name,
        game_id,
        team_id,
        season_year,
        game_date
    FROM contextual_projections_raw
    WHERE player_game_key NOT IN (SELECT player_game_key FROM derived_metrics_raw)
),

-- ====================================================================================================
-- CLEAN DATA PREPARATION: Select relevant columns without joins
-- ====================================================================================================

derived_metrics AS (
    SELECT
        player_game_key,
        
        -- === FOUNDATIONAL ROLLING METRICS ===
        pts_roll_10g_avg,
        ts_pct_roll_10g_avg,
        usage_pct_roll_10g_avg,
        eff_fg_pct_roll_10g_avg,
        
        -- === ADVANCED EFFICIENCY COMPOSITES ===
        scoring_efficiency_composite_roll_10g,
        usage_weighted_ts_roll_10g,
        points_opportunity_ratio_roll_10g,
        
        -- === SHOT CREATION AND VOLUME METRICS ===
        shot_creation_index_roll_10g,
        self_created_scoring_rate_per_min_roll_10g,
        shooting_volume_per_min_roll_10g,
        free_throw_generation_aggressiveness_roll_10g,
        
        -- === SITUATIONAL SCORING PATTERNS ===
        contested_vs_uncontested_fg_pct_diff_roll_10g,
        contested_fg_makes_per_minute_roll_10g,
        opportunistic_scoring_rate_per_min_roll_10g,
        
        -- === SCORING VERSATILITY AND BALANCE ===
        scoring_versatility_ratio_roll_10g,
        three_pt_value_efficiency_index_roll_10g,
        paint_reliance_index_roll_10g,
        scoring_profile_balance_3pt_vs_paint_roll_10g,
        
        -- === ROLE AND CONTEXT METRICS ===
        offensive_role_factor_roll_10g,
        scoring_focus_ratio_roll_10g,
        pace_adjusted_points_per_minute_roll_10g
        
    FROM derived_metrics_raw
),

contextual_projections AS (
    SELECT
        player_game_key,
        
        -- === TEAM CONTEXT ATTRIBUTES ===
        team_l5_pace,
        team_l5_off_rating,
        team_form,
        team_playstyle,
        team_offensive_structure,
        
        -- === PLAYER-TEAM FIT METRICS ===
        player_offensive_role,
        player_team_style_fit_score,
        pace_impact_on_player,
        usage_opportunity,
        team_form_player_impact,
        team_playstyle_stat_impact,
        
        -- === CONTEXT MULTIPLIERS ===
        pace_multiplier,
        usage_multiplier,
        team_form_multiplier,
        playstyle_stat_multiplier,
        style_fit_multiplier,
        
        -- === PROJECTED PERFORMANCE ===
        team_adjusted_pts_projection,
        team_adjusted_reb_projection,
        team_adjusted_ast_projection,
        team_context_impact,
        
        -- === HISTORICAL CONTEXT PERFORMANCE ===
        player_l5_pts,
        player_l5_reb,
        player_l5_ast,
        pts_in_team_hot_streaks,
        pts_in_team_cold_streaks,
        pts_in_star_dominant_system,
        pts_in_balanced_system
        
    FROM contextual_projections_raw
),

interior_deviations AS (
    SELECT
        player_game_key,
        opponent_id,
        
        -- === BASELINE INTERIOR METRICS ===
        pct_pts_in_paint,
        def_at_rim_fg_pct,
        def_at_rim_fga_rate,
        
        -- === DEVIATION FROM SEASON PATTERNS ===
        paint_scoring_reliance_deviation,
        rim_finishing_efficiency_deviation,
        rim_attempt_rate_deviation
        
    FROM interior_deviations_raw
),

game_context AS (
    SELECT
        player_game_key,
        home_away,
        position,
        
        -- === TEAM SHARE METRICS ===
        pct_of_team_pts,
        pct_of_team_reb,
        pct_of_team_ast,
        pct_of_team_fga,
        
        -- === EFFICIENCY VS TEAM CONTEXT ===
        efficiency_vs_team_avg
        
    FROM game_context_raw
),

-- ====================================================================================================
-- CLEAN APPROACH: Use stable base model with direct joins
-- ====================================================================================================

base_features AS (
    SELECT
        dm.player_game_key,
        dm.player_id,
        dm.player_name,
        dm.game_id,
        dm.team_id,
        dm.season_year,
        dm.game_date,
        
        -- === FOUNDATIONAL PERFORMANCE METRICS FROM DERIVED_METRICS ===
        dm.pts_roll_10g_avg,
        dm.ts_pct_roll_10g_avg,
        dm.usage_pct_roll_10g_avg,
        dm.eff_fg_pct_roll_10g_avg,
        dm.scoring_efficiency_composite_roll_10g,
        dm.usage_weighted_ts_roll_10g,
        dm.points_opportunity_ratio_roll_10g,
        dm.shot_creation_index_roll_10g,
        dm.self_created_scoring_rate_per_min_roll_10g,
        dm.shooting_volume_per_min_roll_10g,
        dm.free_throw_generation_aggressiveness_roll_10g,
        dm.contested_vs_uncontested_fg_pct_diff_roll_10g,
        dm.contested_fg_makes_per_minute_roll_10g,
        dm.scoring_versatility_ratio_roll_10g,
        dm.three_pt_value_efficiency_index_roll_10g,
        dm.paint_reliance_index_roll_10g,
        dm.scoring_profile_balance_3pt_vs_paint_roll_10g,
        dm.offensive_role_factor_roll_10g,
        dm.scoring_focus_ratio_roll_10g,
        dm.pace_adjusted_points_per_minute_roll_10g,
        dm.opportunistic_scoring_rate_per_min_roll_10g,
        
        -- === CONTEXTUAL PROJECTIONS ===
        cp.team_l5_pace,
        cp.team_l5_off_rating,
        cp.team_form,
        cp.team_playstyle,
        cp.team_offensive_structure,
        cp.player_offensive_role,
        cp.player_team_style_fit_score,
        cp.pace_impact_on_player,
        cp.usage_opportunity,
        cp.team_form_player_impact,
        cp.team_playstyle_stat_impact,
        cp.pace_multiplier,
        cp.usage_multiplier,
        cp.team_form_multiplier,
        cp.playstyle_stat_multiplier,
        cp.style_fit_multiplier,
        cp.team_adjusted_pts_projection,
        cp.team_adjusted_reb_projection,
        cp.team_adjusted_ast_projection,
        cp.team_context_impact,
        cp.player_l5_pts,
        cp.player_l5_reb,
        cp.player_l5_ast,
        cp.pts_in_team_hot_streaks,
        cp.pts_in_team_cold_streaks,
        cp.pts_in_star_dominant_system,
        cp.pts_in_balanced_system,
        
        -- === INTERIOR DEVIATIONS ===
        id.opponent_id,
        id.pct_pts_in_paint,
        id.def_at_rim_fg_pct,
        id.def_at_rim_fga_rate,
        id.paint_scoring_reliance_deviation,
        id.rim_finishing_efficiency_deviation,
        id.rim_attempt_rate_deviation,
        
        -- === GAME CONTEXT ===
        gc.home_away,
        gc.position,
        gc.pct_of_team_pts,
        gc.pct_of_team_reb,
        gc.pct_of_team_ast,
        gc.pct_of_team_fga,
        gc.efficiency_vs_team_avg
        
    FROM {{ ref('feat_player__derived_scoring_metrics') }} dm
    {% if is_incremental() %}
    WHERE dm.game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }})
    {% endif %}
    LEFT JOIN {{ ref('feat_player__contextual_projections') }} cp 
        ON dm.player_game_key = cp.player_game_key
    LEFT JOIN {{ ref('feat_player__interior_scoring_deviations') }} id 
        ON dm.player_game_key = id.player_game_key
    LEFT JOIN {{ ref('feat_player__game_context_attributes') }} gc 
        ON dm.player_game_key = gc.player_game_key
)

SELECT
    -- === IDENTIFIERS ===
    player_game_key,
    player_id,
    player_name,
    game_id,
    team_id,
    season_year,
    game_date,
    COALESCE(home_away, 'UNKNOWN') as home_away,
    COALESCE(position, 'UNKNOWN') as position,
    COALESCE(opponent_id, 0) as opponent_id,
    
    -- === FOUNDATIONAL METRICS ===
    pts_roll_10g_avg,
    ts_pct_roll_10g_avg,
    usage_pct_roll_10g_avg,
    eff_fg_pct_roll_10g_avg,
    
    -- === ADVANCED EFFICIENCY ===
    scoring_efficiency_composite_roll_10g,
    usage_weighted_ts_roll_10g,
    points_opportunity_ratio_roll_10g,
    
    -- Composite efficiency reliability
    COALESCE(
        (scoring_efficiency_composite_roll_10g * 0.4 + 
         usage_weighted_ts_roll_10g * 0.3 + 
         points_opportunity_ratio_roll_10g * 0.3), 0
    ) AS composite_efficiency_reliability,
    
    -- === SHOT CREATION AND VOLUME ===
    shot_creation_index_roll_10g,
    self_created_scoring_rate_per_min_roll_10g,
    shooting_volume_per_min_roll_10g,
    free_throw_generation_aggressiveness_roll_10g,
    
    -- === DEFENSIVE PRESSURE METRICS ===
    contested_vs_uncontested_fg_pct_diff_roll_10g,
    contested_fg_makes_per_minute_roll_10g,
    
    -- Pressure performance tier
    CASE
        WHEN contested_fg_makes_per_minute_roll_10g > 0.5 AND usage_pct_roll_10g_avg > 0.25 THEN 'HIGH_PRESSURE_PERFORMER'
        WHEN contested_vs_uncontested_fg_pct_diff_roll_10g > -0.05 THEN 'PRESSURE_RESILIENT'
        WHEN contested_vs_uncontested_fg_pct_diff_roll_10g < -0.15 THEN 'PRESSURE_SENSITIVE'
        ELSE 'PRESSURE_NEUTRAL'
    END AS pressure_performance_tier,
    
    -- === SCORING VERSATILITY ===
    scoring_versatility_ratio_roll_10g,
    three_pt_value_efficiency_index_roll_10g,
    paint_reliance_index_roll_10g,
    scoring_profile_balance_3pt_vs_paint_roll_10g,
    
    -- Scoring archetype
    CASE
        WHEN COALESCE(pct_pts_in_paint, 0) > 0.6 AND paint_reliance_index_roll_10g > 0.4 THEN 'INTERIOR_DOMINANT'
        WHEN three_pt_value_efficiency_index_roll_10g > 0.15 AND COALESCE(pct_pts_in_paint, 0) < 0.4 THEN 'PERIMETER_FOCUSED'
        WHEN ABS(COALESCE(scoring_profile_balance_3pt_vs_paint_roll_10g, 0)) < 0.1 THEN 'BALANCED_SCORER'
        ELSE 'SITUATIONAL_SCORER'
    END AS scoring_archetype,
    
    -- === ROLE AND OPPORTUNITY ===
    offensive_role_factor_roll_10g,
    scoring_focus_ratio_roll_10g,
    pace_adjusted_points_per_minute_roll_10g,
    opportunistic_scoring_rate_per_min_roll_10g,
    
    -- Role consistency indicator
    CASE
        WHEN player_offensive_role = 'PRIMARY_OPTION' AND offensive_role_factor_roll_10g > 0.25 THEN 'CONSISTENT_PRIMARY'
        WHEN player_offensive_role = 'SECONDARY_OPTION' AND offensive_role_factor_roll_10g BETWEEN 0.15 AND 0.30 THEN 'CONSISTENT_SECONDARY'
        WHEN player_offensive_role = 'SUPPORTING_ROLE' AND offensive_role_factor_roll_10g < 0.20 THEN 'CONSISTENT_SUPPORT'
        ELSE 'ROLE_MISMATCH'
    END AS role_consistency_indicator,
    
    -- === TEAM CONTEXT ===
    team_l5_pace,
    team_l5_off_rating,
    team_form,
    team_playstyle,
    team_offensive_structure,
    player_offensive_role,
    player_team_style_fit_score,
    pace_impact_on_player,
    usage_opportunity,
    team_form_player_impact,
    team_playstyle_stat_impact,
    
    -- === CONTEXT MULTIPLIERS ===
    pace_multiplier,
    usage_multiplier,
    team_form_multiplier,
    playstyle_stat_multiplier,
    style_fit_multiplier,
    
    -- Combined context multiplier
    COALESCE(
        (pace_multiplier * usage_multiplier * team_form_multiplier * 
         playstyle_stat_multiplier * style_fit_multiplier), 1.0
    ) AS combined_context_multiplier,
    
    -- Context sensitivity tier
    CASE
        WHEN COALESCE((pace_multiplier * usage_multiplier * team_form_multiplier * 
                      playstyle_stat_multiplier * style_fit_multiplier), 1.0) > 1.15 THEN 'HIGH_CONTEXT_BOOST'
        WHEN COALESCE((pace_multiplier * usage_multiplier * team_form_multiplier * 
                      playstyle_stat_multiplier * style_fit_multiplier), 1.0) > 1.05 THEN 'MODERATE_CONTEXT_BOOST'
        WHEN COALESCE((pace_multiplier * usage_multiplier * team_form_multiplier * 
                      playstyle_stat_multiplier * style_fit_multiplier), 1.0) < 0.85 THEN 'CONTEXT_LIMITATION'
        WHEN COALESCE((pace_multiplier * usage_multiplier * team_form_multiplier * 
                      playstyle_stat_multiplier * style_fit_multiplier), 1.0) < 0.95 THEN 'MINOR_CONTEXT_LIMITATION'
        ELSE 'CONTEXT_NEUTRAL'
    END AS context_sensitivity_tier,
    
    -- === PROJECTIONS ===
    team_adjusted_pts_projection,
    team_adjusted_reb_projection,
    team_adjusted_ast_projection,
    team_context_impact,
    
    -- Projection vs baseline difference
    COALESCE(ABS(team_adjusted_pts_projection - pts_roll_10g_avg), 0) AS projection_vs_baseline_pts_diff,
    
    -- Projection accuracy ratio
    CASE
        WHEN pts_roll_10g_avg > 0 
        THEN COALESCE(ABS(team_adjusted_pts_projection - pts_roll_10g_avg) / pts_roll_10g_avg, 0)
        ELSE 0
    END AS projection_accuracy_ratio,
    
    -- === SITUATIONAL PERFORMANCE ===
    player_l5_pts,
    player_l5_reb,
    player_l5_ast,
    pts_in_team_hot_streaks,
    pts_in_team_cold_streaks,
    pts_in_star_dominant_system,
    pts_in_balanced_system,
    
    -- === INTERIOR SCORING ===
    COALESCE(pct_pts_in_paint, 0) AS pct_pts_in_paint,
    COALESCE(def_at_rim_fg_pct, 0) AS def_at_rim_fg_pct,
    COALESCE(def_at_rim_fga_rate, 0) AS def_at_rim_fga_rate,
    COALESCE(paint_scoring_reliance_deviation, 0) AS paint_scoring_reliance_deviation,
    COALESCE(rim_finishing_efficiency_deviation, 0) AS rim_finishing_efficiency_deviation,
    COALESCE(rim_attempt_rate_deviation, 0) AS rim_attempt_rate_deviation,
    
    -- Matchup adaptability index
    COALESCE(
        (ABS(COALESCE(paint_scoring_reliance_deviation, 0)) + 
         ABS(COALESCE(rim_finishing_efficiency_deviation, 0)) + 
         ABS(COALESCE(rim_attempt_rate_deviation, 0))) * 
         COALESCE(scoring_versatility_ratio_roll_10g, 0), 0
    ) AS matchup_adaptability_index,
    
    -- === TEAM CONTRIBUTION ===
    COALESCE(pct_of_team_pts, 0) AS pct_of_team_pts,
    COALESCE(pct_of_team_reb, 0) AS pct_of_team_reb,
    COALESCE(pct_of_team_ast, 0) AS pct_of_team_ast,
    COALESCE(pct_of_team_fga, 0) AS pct_of_team_fga,
    COALESCE(efficiency_vs_team_avg, 'UNKNOWN') AS efficiency_vs_team_avg,
    
    -- Team offensive impact magnitude
    COALESCE(
        (COALESCE(pct_of_team_pts, 0) * COALESCE(offensive_role_factor_roll_10g, 0) * 
         COALESCE(player_team_style_fit_score, 3) / 5.0), 0
    ) AS team_offensive_impact_magnitude,
    
    -- === METADATA ===
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM base_features
WHERE player_id IS NOT NULL 
  AND game_date IS NOT NULL
  AND pts_roll_10g_avg IS NOT NULL