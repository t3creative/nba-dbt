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

FEATURE CATEGORIES:
    1. Historical Performance Metrics (10-game rolling averages)
    2. Team Context Adjustments (pace, usage, form multipliers)
    3. Interior Scoring Specialization (paint vs perimeter patterns)
    4. Contextual Adaptability (deviation from baseline patterns)
    5. Composite Scoring Indicators (multi-dimensional performance)

GRAIN: One record per player per game
KEY: player_game_key (MD5 hash of game_id + player_id)

UPSTREAM DEPENDENCIES:
    - feat_player__derived_scoring_metrics: Rolling performance analytics
    - feat_player__contextual_projections: Team-adjusted projections  
    - feat_player__interior_scoring_deviations: Matchup-specific adaptations
    - feat_player__game_context_attributes: Player-team interaction features

====================================================================================================
*/

WITH base_identifiers AS (
    -- Establish the complete set of player-game combinations from our primary derived metrics
    -- This ensures we have a consistent grain across all upstream sources
    SELECT DISTINCT
        player_game_key,
        player_id,
        player_name,
        game_id,
        team_id,
        season_year,
        game_date
    FROM {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    -- Incremental processing: capture recent games plus lookback window for rolling calculations
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }})
    {% endif %}
),

derived_metrics AS (
    -- Core derived scoring metrics using 10-game rolling averages
    -- These represent the player's established scoring patterns and efficiency
    SELECT
        dsm.player_game_key,
        
        -- === FOUNDATIONAL ROLLING METRICS ===
        dsm.pts_roll_10g_avg,
        dsm.ts_pct_roll_10g_avg,
        dsm.usage_pct_roll_10g_avg,
        dsm.eff_fg_pct_roll_10g_avg,
        
        -- === ADVANCED EFFICIENCY COMPOSITES ===
        dsm.scoring_efficiency_composite_roll_10g,
        dsm.usage_weighted_ts_roll_10g,
        dsm.points_opportunity_ratio_roll_10g,
        
        -- === SHOT CREATION AND VOLUME METRICS ===
        dsm.shot_creation_index_roll_10g,
        dsm.self_created_scoring_rate_per_min_roll_10g,
        dsm.shooting_volume_per_min_roll_10g,
        dsm.free_throw_generation_aggressiveness_roll_10g,
        
        -- === SITUATIONAL SCORING PATTERNS ===
        dsm.contested_vs_uncontested_fg_pct_diff_roll_10g,
        dsm.contested_fg_makes_per_minute_roll_10g,
        dsm.opportunistic_scoring_rate_per_min_roll_10g,
        
        -- === SCORING VERSATILITY AND BALANCE ===
        dsm.scoring_versatility_ratio_roll_10g,
        dsm.three_pt_value_efficiency_index_roll_10g,
        dsm.paint_reliance_index_roll_10g,
        dsm.scoring_profile_balance_3pt_vs_paint_roll_10g,
        
        -- === ROLE AND CONTEXT METRICS ===
        dsm.offensive_role_factor_roll_10g,
        dsm.scoring_focus_ratio_roll_10g,
        dsm.pace_adjusted_points_per_minute_roll_10g
        
    FROM {{ ref('feat_player__derived_scoring_metrics') }} dsm
    INNER JOIN base_identifiers bi ON dsm.player_game_key = bi.player_game_key
),

contextual_projections AS (
    -- Team-context adjusted projections and multipliers
    -- These features capture how team dynamics affect individual scoring
    SELECT
        cp.player_game_key,
        
        -- === TEAM CONTEXT ATTRIBUTES ===
        cp.team_l5_pace,
        cp.team_l5_off_rating,
        cp.team_form,
        cp.team_playstyle,
        cp.team_offensive_structure,
        
        -- === PLAYER-TEAM FIT METRICS ===
        cp.player_offensive_role,
        cp.player_team_style_fit_score,
        cp.pace_impact_on_player,
        cp.usage_opportunity,
        cp.team_form_player_impact,
        cp.team_playstyle_stat_impact,
        
        -- === CONTEXT MULTIPLIERS ===
        cp.pace_multiplier,
        cp.usage_multiplier,
        cp.team_form_multiplier,
        cp.playstyle_stat_multiplier,
        cp.style_fit_multiplier,
        
        -- === PROJECTED PERFORMANCE ===
        cp.team_adjusted_pts_projection,
        cp.team_adjusted_reb_projection,
        cp.team_adjusted_ast_projection,
        cp.team_context_impact,
        
        -- === HISTORICAL CONTEXT PERFORMANCE ===
        cp.player_l5_pts,
        cp.player_l5_reb,
        cp.player_l5_ast,
        cp.pts_in_team_hot_streaks,
        cp.pts_in_team_cold_streaks,
        cp.pts_in_star_dominant_system,
        cp.pts_in_balanced_system
        
    FROM {{ ref('feat_player__contextual_projections') }} cp
    INNER JOIN base_identifiers bi ON cp.player_game_key = bi.player_game_key
),

interior_deviations AS (
    -- Matchup-specific interior scoring adaptations
    -- These capture how players adjust their scoring approach vs different opponents
    SELECT
        id.player_game_key,
        id.opponent_id,
        
        -- === BASELINE INTERIOR METRICS ===
        id.pct_pts_in_paint,
        id.def_at_rim_fg_pct,
        id.def_at_rim_fga_rate,
        
        -- === DEVIATION FROM SEASON PATTERNS ===
        id.paint_scoring_reliance_deviation,
        id.rim_finishing_efficiency_deviation,
        id.rim_attempt_rate_deviation
        
    FROM {{ ref('feat_player__interior_scoring_deviations') }} id
    INNER JOIN base_identifiers bi ON id.player_game_key = bi.player_game_key
),

game_context AS (
    -- Player-team interaction attributes and efficiency metrics
    -- These provide additional context for the player's role and performance environment
    SELECT
        gca.player_game_key,
        gca.home_away,
        gca.position,
        
        -- === TEAM SHARE METRICS ===
        gca.pct_of_team_pts,
        gca.pct_of_team_reb,
        gca.pct_of_team_ast,
        gca.pct_of_team_fga,
        
        -- === EFFICIENCY VS TEAM CONTEXT ===
        gca.efficiency_vs_team_avg
        
    FROM {{ ref('feat_player__game_context_attributes') }} gca
    INNER JOIN base_identifiers bi ON gca.player_game_key = bi.player_game_key
),

-- ====================================================================================================
-- FEATURE ENGINEERING: COMPOSITE INDICATORS AND CROSS-MODEL FEATURES
-- ====================================================================================================

enhanced_features AS (
    SELECT
        bi.player_game_key,
        bi.player_id,
        bi.player_name,
        bi.game_id,
        bi.team_id,
        bi.season_year,
        bi.game_date,
        
        -- === CORE IDENTIFIERS AND CONTEXT ===
        gc.home_away,
        gc.position,
        id.opponent_id,
        
        -- === FOUNDATIONAL PERFORMANCE METRICS ===
        dm.pts_roll_10g_avg,
        dm.ts_pct_roll_10g_avg,
        dm.usage_pct_roll_10g_avg,
        dm.eff_fg_pct_roll_10g_avg,
        
        -- === ADVANCED EFFICIENCY INDICATORS ===
        dm.scoring_efficiency_composite_roll_10g,
        dm.usage_weighted_ts_roll_10g,
        dm.points_opportunity_ratio_roll_10g,
        
        -- === SHOT CREATION AND SELF-RELIANCE ===
        dm.shot_creation_index_roll_10g,
        dm.self_created_scoring_rate_per_min_roll_10g,
        dm.shooting_volume_per_min_roll_10g,
        dm.free_throw_generation_aggressiveness_roll_10g,
        
        -- === DEFENSIVE PRESSURE PERFORMANCE ===
        dm.contested_vs_uncontested_fg_pct_diff_roll_10g,
        dm.contested_fg_makes_per_minute_roll_10g,
        
        -- === SCORING VERSATILITY AND BALANCE ===
        dm.scoring_versatility_ratio_roll_10g,
        dm.three_pt_value_efficiency_index_roll_10g,
        dm.paint_reliance_index_roll_10g,
        dm.scoring_profile_balance_3pt_vs_paint_roll_10g,
        
        -- === ROLE AND OPPORTUNITY METRICS ===
        dm.offensive_role_factor_roll_10g,
        dm.scoring_focus_ratio_roll_10g,
        dm.pace_adjusted_points_per_minute_roll_10g,
        dm.opportunistic_scoring_rate_per_min_roll_10g,
        
        -- === TEAM CONTEXT AND ENVIRONMENT ===
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
        
        -- === CONTEXT MULTIPLIERS ===
        cp.pace_multiplier,
        cp.usage_multiplier,
        cp.team_form_multiplier,
        cp.playstyle_stat_multiplier,
        cp.style_fit_multiplier,
        
        -- === TEAM-ADJUSTED PROJECTIONS ===
        cp.team_adjusted_pts_projection,
        cp.team_adjusted_reb_projection,
        cp.team_adjusted_ast_projection,
        cp.team_context_impact,
        
        -- === SITUATIONAL PERFORMANCE HISTORY ===
        cp.player_l5_pts,
        cp.player_l5_reb,
        cp.player_l5_ast,
        cp.pts_in_team_hot_streaks,
        cp.pts_in_team_cold_streaks,
        cp.pts_in_star_dominant_system,
        cp.pts_in_balanced_system,
        
        -- === INTERIOR SCORING PATTERNS ===
        COALESCE(id.pct_pts_in_paint, 0) AS pct_pts_in_paint,
        COALESCE(id.def_at_rim_fg_pct, 0) AS def_at_rim_fg_pct,
        COALESCE(id.def_at_rim_fga_rate, 0) AS def_at_rim_fga_rate,
        
        -- === MATCHUP ADAPTABILITY ===
        COALESCE(id.paint_scoring_reliance_deviation, 0) AS paint_scoring_reliance_deviation,
        COALESCE(id.rim_finishing_efficiency_deviation, 0) AS rim_finishing_efficiency_deviation,
        COALESCE(id.rim_attempt_rate_deviation, 0) AS rim_attempt_rate_deviation,
        
        -- === TEAM CONTRIBUTION METRICS ===
        COALESCE(gc.pct_of_team_pts, 0) AS pct_of_team_pts,
        COALESCE(gc.pct_of_team_reb, 0) AS pct_of_team_reb,
        COALESCE(gc.pct_of_team_ast, 0) AS pct_of_team_ast,
        COALESCE(gc.pct_of_team_fga, 0) AS pct_of_team_fga,
        COALESCE(gc.efficiency_vs_team_avg, 'UNKNOWN') AS efficiency_vs_team_avg,
        
        -- ====================================================================================================
        -- COMPOSITE FEATURES: CROSS-MODEL FEATURE ENGINEERING
        -- ====================================================================================================
        
        -- === PROJECTION ACCURACY INDICATORS ===
        -- Measures how well team context projections align with rolling performance
        ABS(cp.team_adjusted_pts_projection - dm.pts_roll_10g_avg) AS projection_vs_baseline_pts_diff,
        
        CASE
            WHEN dm.pts_roll_10g_avg > 0 
            THEN ABS(cp.team_adjusted_pts_projection - dm.pts_roll_10g_avg) / dm.pts_roll_10g_avg
            ELSE 0
        END AS projection_accuracy_ratio,
        
        -- === CONTEXT SENSITIVITY SCORE ===
        -- Measures how much team context affects player performance
        (cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
         cp.playstyle_stat_multiplier * cp.style_fit_multiplier) AS combined_context_multiplier,
        
        CASE
            WHEN (cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
                  cp.playstyle_stat_multiplier * cp.style_fit_multiplier) > 1.15 THEN 'HIGH_CONTEXT_BOOST'
            WHEN (cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
                  cp.playstyle_stat_multiplier * cp.style_fit_multiplier) > 1.05 THEN 'MODERATE_CONTEXT_BOOST'
            WHEN (cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
                  cp.playstyle_stat_multiplier * cp.style_fit_multiplier) < 0.85 THEN 'CONTEXT_LIMITATION'
            WHEN (cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
                  cp.playstyle_stat_multiplier * cp.style_fit_multiplier) < 0.95 THEN 'MINOR_CONTEXT_LIMITATION'
            ELSE 'CONTEXT_NEUTRAL'
        END AS context_sensitivity_tier,
        
        -- === ADAPTABILITY INDEX ===
        -- Combines interior scoring adaptability with overall versatility
        (ABS(COALESCE(id.paint_scoring_reliance_deviation, 0)) + 
         ABS(COALESCE(id.rim_finishing_efficiency_deviation, 0)) + 
         ABS(COALESCE(id.rim_attempt_rate_deviation, 0))) * 
         dm.scoring_versatility_ratio_roll_10g AS matchup_adaptability_index,
        
        -- === ROLE CONSISTENCY INDICATOR ===
        -- Measures alignment between derived role metrics and team context role
        CASE
            WHEN cp.player_offensive_role = 'PRIMARY_OPTION' AND dm.offensive_role_factor_roll_10g > 0.25 THEN 'CONSISTENT_PRIMARY'
            WHEN cp.player_offensive_role = 'SECONDARY_OPTION' AND dm.offensive_role_factor_roll_10g BETWEEN 0.15 AND 0.30 THEN 'CONSISTENT_SECONDARY'
            WHEN cp.player_offensive_role = 'SUPPORTING_ROLE' AND dm.offensive_role_factor_roll_10g < 0.20 THEN 'CONSISTENT_SUPPORT'
            ELSE 'ROLE_MISMATCH'
        END AS role_consistency_indicator,
        
        -- === EFFICIENCY RELIABILITY SCORE ===
        -- Combines multiple efficiency metrics with consistency indicators
        (dm.scoring_efficiency_composite_roll_10g * 0.4 + 
         dm.usage_weighted_ts_roll_10g * 0.3 + 
         dm.points_opportunity_ratio_roll_10g * 0.3) AS composite_efficiency_reliability,
        
        -- === PRESSURE PERFORMANCE INDICATOR ===
        -- Measures performance under defensive pressure and high-usage situations
        CASE
            WHEN dm.contested_fg_makes_per_minute_roll_10g > 0.5 AND dm.usage_pct_roll_10g_avg > 0.25 THEN 'HIGH_PRESSURE_PERFORMER'
            WHEN dm.contested_vs_uncontested_fg_pct_diff_roll_10g > -0.05 THEN 'PRESSURE_RESILIENT'
            WHEN dm.contested_vs_uncontested_fg_pct_diff_roll_10g < -0.15 THEN 'PRESSURE_SENSITIVE'
            ELSE 'PRESSURE_NEUTRAL'
        END AS pressure_performance_tier,
        
        -- === INTERIOR VS PERIMETER BALANCE ===
        -- Enhanced scoring balance considering both derived metrics and actual interior patterns
        CASE
            WHEN COALESCE(id.pct_pts_in_paint, 0) > 0.6 AND dm.paint_reliance_index_roll_10g > 0.4 THEN 'INTERIOR_DOMINANT'
            WHEN dm.three_pt_value_efficiency_index_roll_10g > 0.15 AND COALESCE(id.pct_pts_in_paint, 0) < 0.4 THEN 'PERIMETER_FOCUSED'
            WHEN ABS(dm.scoring_profile_balance_3pt_vs_paint_roll_10g) < 0.1 THEN 'BALANCED_SCORER'
            ELSE 'SITUATIONAL_SCORER'
        END AS scoring_archetype,
        
        -- === TEAM IMPACT MAGNITUDE ===
        -- Measures how much the player's presence affects team offensive dynamics
        (COALESCE(gc.pct_of_team_pts, 0) * dm.offensive_role_factor_roll_10g * 
         cp.player_team_style_fit_score / 5.0) AS team_offensive_impact_magnitude
        
    FROM base_identifiers bi
    LEFT JOIN derived_metrics dm ON bi.player_game_key = dm.player_game_key
    LEFT JOIN contextual_projections cp ON bi.player_game_key = cp.player_game_key
    LEFT JOIN interior_deviations id ON bi.player_game_key = id.player_game_key
    LEFT JOIN game_context gc ON bi.player_game_key = gc.player_game_key
)

-- ====================================================================================================
-- FINAL FEATURE SELECTION AND OUTPUT
-- ====================================================================================================

SELECT
    -- === IDENTIFIERS ===
    player_game_key,
    player_id,
    player_name,
    game_id,
    team_id,
    season_year,
    game_date,
    home_away,
    position,
    opponent_id,
    
    -- === FOUNDATIONAL METRICS ===
    pts_roll_10g_avg,
    ts_pct_roll_10g_avg,
    usage_pct_roll_10g_avg,
    eff_fg_pct_roll_10g_avg,
    
    -- === ADVANCED EFFICIENCY ===
    scoring_efficiency_composite_roll_10g,
    usage_weighted_ts_roll_10g,
    points_opportunity_ratio_roll_10g,
    composite_efficiency_reliability,
    
    -- === SHOT CREATION AND VOLUME ===
    shot_creation_index_roll_10g,
    self_created_scoring_rate_per_min_roll_10g,
    shooting_volume_per_min_roll_10g,
    free_throw_generation_aggressiveness_roll_10g,
    
    -- === DEFENSIVE PRESSURE METRICS ===
    contested_vs_uncontested_fg_pct_diff_roll_10g,
    contested_fg_makes_per_minute_roll_10g,
    pressure_performance_tier,
    
    -- === SCORING VERSATILITY ===
    scoring_versatility_ratio_roll_10g,
    three_pt_value_efficiency_index_roll_10g,
    paint_reliance_index_roll_10g,
    scoring_profile_balance_3pt_vs_paint_roll_10g,
    scoring_archetype,
    
    -- === ROLE AND OPPORTUNITY ===
    offensive_role_factor_roll_10g,
    scoring_focus_ratio_roll_10g,
    pace_adjusted_points_per_minute_roll_10g,
    opportunistic_scoring_rate_per_min_roll_10g,
    role_consistency_indicator,
    
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
    combined_context_multiplier,
    context_sensitivity_tier,
    
    -- === PROJECTIONS ===
    team_adjusted_pts_projection,
    team_adjusted_reb_projection,
    team_adjusted_ast_projection,
    team_context_impact,
    projection_vs_baseline_pts_diff,
    projection_accuracy_ratio,
    
    -- === SITUATIONAL PERFORMANCE ===
    player_l5_pts,
    player_l5_reb,
    player_l5_ast,
    pts_in_team_hot_streaks,
    pts_in_team_cold_streaks,
    pts_in_star_dominant_system,
    pts_in_balanced_system,
    
    -- === INTERIOR SCORING ===
    pct_pts_in_paint,
    def_at_rim_fg_pct,
    def_at_rim_fga_rate,
    paint_scoring_reliance_deviation,
    rim_finishing_efficiency_deviation,
    rim_attempt_rate_deviation,
    matchup_adaptability_index,
    
    -- === TEAM CONTRIBUTION ===
    pct_of_team_pts,
    pct_of_team_reb,
    pct_of_team_ast,
    pct_of_team_fga,
    efficiency_vs_team_avg,
    team_offensive_impact_magnitude,
    
    -- === METADATA ===
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM enhanced_features

-- Data quality filters
WHERE pts_roll_10g_avg IS NOT NULL
  AND player_id IS NOT NULL
  AND game_date IS NOT NULL