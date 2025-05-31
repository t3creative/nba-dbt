{{ config(
    materialized='table',
    schema='training',
    tags=['ml', 'training', 'points_prediction', 'regression'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['season_year']},
        {'columns': ['is_training_data']},
        {'columns': ['target_pts']}
    ]
) }}

/*
ML PLAYER POINTS TRAINING MODEL v2 - MEMORY OPTIMIZED
Comprehensive training dataset for player points prediction (regression).
First layer of ensemble model for player prop betting predictions.

FIXES: Corrected join keys to prevent cartesian products and memory explosion.

Core Value: Consolidates all feature store models into ML-ready format with
strict temporal safety and optimal feature selection for points prediction.

Grain: One row per player + game_date
Target: Current game points (regression)
Data Split: 2017-18 to 2022-23 = training, 2023-24 to 2024-25 = test
Temporal Safety: All features use PRIOR game data only (L3, L5, L10 lags)
*/

WITH base_target_data AS (
    SELECT
        player_game_key,
        player_id,
        player_name,
        team_id,
        game_id,
        game_date,
        season_year,
        opponent_id,
        position,
        home_away,
        
        -- TARGET VARIABLE (what we're predicting)
        pts AS target_pts,
        
        -- Additional context for analysis (not features)
        min AS target_min,
        reb AS target_reb,
        ast AS target_ast,
        pts + reb + ast AS target_pra,
        
        -- Training/test split logic
        CASE 
            WHEN season_year IN ('2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23') 
                THEN TRUE
            ELSE FALSE
        END AS is_training_data,
        
        CASE 
            WHEN season_year IN ('2023-24', '2024-25') 
                THEN TRUE
            ELSE FALSE
        END AS is_test_data

    FROM {{ ref('int_player__combined_boxscore') }}
    WHERE 
        min >= 5  -- Minimum playing time filter
        AND pts IS NOT NULL
        AND season_year BETWEEN '2017-18' AND '2024-25'
        -- MEMORY OPTIMIZATION: Limit to recent seasons for initial testing
        -- Remove this filter once joins are confirmed working
        AND season_year >= '2022-23'
),

-- =================================================================
-- CORE PERFORMANCE FEATURES (from player_scoring_features_v2)
-- =================================================================
core_scoring_features AS (
    SELECT
        player_game_key,  -- FIXED: Use proper unique key
        
        -- Recent Performance (L3, L5, L10)
        pts_avg_l3,
        pts_avg_l5,
        pts_avg_l10,
        fg_pct_avg_l5,
        fg3_pct_avg_l5,
        fga_avg_l5,
        min_avg_l5,
        
        -- Derived Scoring Metrics
        scoring_efficiency_composite_l5,
        usage_weighted_ts_l5,
        points_per_touch_l5,
        shot_creation_index_l5,
        three_point_value_index_l5,
        paint_dominance_index_l5,
        pace_adjusted_scoring_rate_l5,
        
        -- Team Context
        player_offensive_role,
        team_pts_share_l5,
        team_fga_share_l5,
        playstyle_compatibility_score,
        total_context_multiplier,
        
        -- Performance Classification
        pressure_performance_tier,
        scoring_archetype,
        context_adjusted_pts_projection_l5,
        
        -- Trends
        pts_avg_l3 - pts_avg_l10 AS recent_scoring_trend
        
    FROM {{ ref('player_scoring_features_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

-- =================================================================
-- SITUATIONAL CONTEXT FEATURES (from game_situational_context_v2)
-- =================================================================
situational_features AS (
    SELECT
        player_game_key,  -- FIXED: Use proper unique key
        
        -- Rest & Recovery
        rest_advantage_score,
        days_rest,
        is_back_to_back,
        is_well_rested,
        fatigue_risk_assessment,
        
        -- Workload Context
        avg_min_l5_prior,
        workload_classification,
        
        -- Opportunity Context
        opportunity_context,
        roster_health_status,
        rotation_players_available,
        
        -- Schedule Context
        season_progress_pct,
        season_phase,
        
        -- Composite Scores
        situational_favorability_score,
        
        -- Binary Flags
        is_home_game,
        has_opportunity_boost,
        has_fatigue_risk
        
    FROM {{ ref('game_situational_context_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

-- =================================================================
-- OPPONENT MATCHUP FEATURES
-- =================================================================
opponent_team_features AS (
    SELECT
        game_id,
        team_id,  -- FIXED: Include team_id for proper joining
        opponent_id,  -- FIXED: Include opponent_id for proper joining
        
        -- Opponent Strength
        opp_overall_strength_score,
        opp_predictability_score,
        opp_recent_def_rating,
        opp_recent_pace,
        
        -- Opponent Form
        opp_scoring_momentum,
        opp_defensive_momentum,
        
        -- Opponent Z-Scores
        opp_def_rating_z_score,
        opp_pace_z_score,
        
        -- Opponent Context
        opp_early_season_flag,
        opp_high_variance_flag,
        opp_elite_flag
        
    FROM {{ ref('opponent_pregame_profile_features_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

opponent_position_features AS (
    SELECT
        opponent_id,
        position,
        game_date,
        
        -- Position-Specific Matchup Quality
        position_overall_matchup_quality,
        position_scoring_matchup_quality,
        position_defense_consistency_score,
        
        -- Matchup Classifications
        is_exploitable_matchup,
        is_avoid_matchup,
        primary_matchup_classification,
        
        -- Position Defense Metrics
        avg_pts_allowed_to_position,
        position_defense_trend_direction
        
    FROM {{ ref('opponent_position_defense_features_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

-- =================================================================
-- HISTORICAL MATCHUP FEATURES
-- =================================================================
historical_features AS (
    SELECT
        player_id,
        opponent_id,
        game_id,
        
        -- Historical Performance vs Opponent
        avg_pts_vs_opponent_prior,
        recency_weighted_pts_vs_opponent,
        
        -- Historical Consistency
        pts_volatility_vs_opponent,
        best_scoring_game_vs_opponent,
        worst_scoring_game_vs_opponent,
        
        -- Confidence & Sample Size
        historical_confidence_score,
        prior_games_vs_opponent,
        
        -- Historical Trends
        pts_trend_vs_opponent,
        historical_matchup_favorability_score,
        
        -- Home/Away Context
        home_away_pts_differential_vs_opponent,
        venue_preference_vs_opponent,
        
        -- Historical Classification
        historical_matchup_classification,
        is_reliable_historical_predictor
        
    FROM {{ ref('player_historical_matchup_features_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

-- =================================================================
-- CONTEXT INTERACTION FEATURES
-- =================================================================
context_interaction_features AS (
    SELECT
        player_game_key,  -- FIXED: Use proper unique key
        
        -- Master Interaction Multiplier
        master_context_interaction_multiplier,
        
        -- Specific Interactions
        rest_opponent_multiplier,
        archetype_position_multiplier,
        pace_interaction_multiplier,
        
        -- Interaction Classifications
        rest_opponent_interaction_category,
        historical_situational_alignment,
        workload_sustainability_category,
        
        -- Signal Clarity
        context_signal_clarity,
        
        -- Binary Amplifiers
        is_high_confidence_favorable,
        is_high_confidence_difficult,
        has_amplified_opportunity
        
    FROM {{ ref('context_interaction_features_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

-- =================================================================
-- PERFORMANCE BOUNDS & VARIANCE FEATURES
-- =================================================================
performance_bounds_features AS (
    SELECT
        player_game_key,  -- FIXED: Use proper unique key
        
        -- Context-Amplified Projections
        context_amplified_baseline,
        context_amplified_ceiling,
        context_amplified_floor,
        amplified_performance_range,
        
        -- Scenario Projections
        most_likely_scenario_projection,
        best_case_scenario_projection,
        worst_case_scenario_projection,
        
        -- Amplification Analysis
        master_amplification_multiplier,
        amplification_category,
        
        -- Reliability Metrics
        projection_confidence_score,
        projection_reliability_tier,
        
        -- Risk-Reward Classification
        risk_reward_classification,
        primary_amplification_driver
        
    FROM {{ ref('player_context_amplified_bounds_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

variance_analysis_features AS (
    SELECT
        player_game_key,  -- FIXED: Use proper unique key
        
        -- Variance Metrics
        baseline_pts_volatility,
        confidence_adjusted_volatility,
        context_adjusted_volatility,
        
        -- Ceiling/Floor Estimates
        estimated_pts_ceiling,
        estimated_pts_floor,
        pts_projection_range,
        
        -- Probability Assessments
        ceiling_game_probability_tier,
        floor_game_risk_tier,
        
        -- Composite Classifications
        boom_bust_classification,
        dfs_variance_tier,
        
        -- Reliability Score
        projection_reliability_score AS variance_projection_reliability_score
        
    FROM {{ ref('player_performance_variance_analysis_features_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

-- =================================================================
-- USAGE OPPORTUNITY FEATURES
-- =================================================================
usage_opportunity_features AS (
    SELECT
        player_game_key,  -- FIXED: Use proper unique key
        
        -- Usage Context
        current_usage_rate,
        projected_expanded_usage_rate,
        usage_expansion_gap,
        
        -- Performance Elasticity
        performance_elasticity_score,
        usage_expansion_sustainability,
        
        -- Ceiling Projections
        usage_amplified_ceiling,
        opportunity_context_ceiling,
        theoretical_max_ceiling,
        
        -- Ceiling Probability
        ceiling_game_probability_score,
        ceiling_game_probability_tier AS usage_ceiling_probability_tier,
        
        -- Role Expansion
        projected_role_expansion,
        role_expansion_confidence,
        
        -- Composite Ratings
        overall_ceiling_game_rating,
        ceiling_candidate_classification
        
    FROM {{ ref('player_usage_opportunity_ceiling_v2') }}
    WHERE season_year >= '2022-23'  -- Match base data filter
),

-- =================================================================
-- FEATURE ENGINEERING & SELECTION - MEMORY OPTIMIZED JOINS
-- =================================================================
consolidated_features AS (
    SELECT
        btd.player_game_key,
        btd.player_id,
        btd.player_name,
        btd.team_id,
        btd.game_id,
        btd.game_date,
        btd.season_year,
        btd.opponent_id,
        btd.position,
        btd.home_away,
        
        -- TARGET VARIABLE
        btd.target_pts,
        
        -- TRAINING/TEST SPLIT
        btd.is_training_data,
        btd.is_test_data,
        
        -- =================================================================
        -- TIER 1: CORE PERFORMANCE FEATURES (Highest Predictive Value)
        -- =================================================================
        
        -- Recent Performance Baseline
        COALESCE(csf.pts_avg_l3, 0) AS pts_avg_l3,
        COALESCE(csf.pts_avg_l5, 0) AS pts_avg_l5,
        COALESCE(csf.pts_avg_l10, 0) AS pts_avg_l10,
        COALESCE(csf.recent_scoring_trend, 0) AS recent_scoring_trend,
        
        -- Efficiency & Role Context
        COALESCE(csf.scoring_efficiency_composite_l5, 50) AS scoring_efficiency_composite_l5,
        COALESCE(csf.usage_weighted_ts_l5, 0.5) AS usage_weighted_ts_l5,
        COALESCE(csf.team_pts_share_l5, 15) AS team_pts_share_l5,
        COALESCE(csf.total_context_multiplier, 1.0) AS total_context_multiplier,
        
        -- Playing Time Context
        COALESCE(csf.min_avg_l5, 20) AS min_avg_l5,
        
        -- =================================================================
        -- TIER 2: CONTEXTUAL AMPLIFIERS (High Impact on Variance)
        -- =================================================================
        
        -- Rest & Fatigue  
        COALESCE(sf.rest_advantage_score, 50) AS rest_advantage_score,
        COALESCE(sf.situational_favorability_score, 50) AS situational_favorability_score,
        CASE WHEN sf.is_back_to_back THEN 1 ELSE 0 END AS is_back_to_back_flag,
        CASE WHEN sf.has_fatigue_risk THEN 1 ELSE 0 END AS has_fatigue_risk_flag,
        
        -- Opportunity Context
        CASE WHEN sf.opportunity_context = 'HIGH_OPPORTUNITY' THEN 1 ELSE 0 END AS high_opportunity_flag,
        COALESCE(sf.rotation_players_available, 10) AS rotation_players_available,
        
        -- Home Court Advantage
        CASE WHEN sf.is_home_game THEN 1 ELSE 0 END AS is_home_game_flag,
        
        -- =================================================================
        -- TIER 3: OPPONENT MATCHUP FEATURES
        -- =================================================================
        
        -- Opponent Strength
        COALESCE(otf.opp_overall_strength_score, 50) AS opp_overall_strength_score,
        COALESCE(otf.opp_recent_def_rating, 110) AS opp_recent_def_rating,
        COALESCE(otf.opp_predictability_score, 50) AS opp_predictability_score,
        
        -- Position-Specific Matchup
        COALESCE(opf.position_overall_matchup_quality, 50) AS position_overall_matchup_quality,
        COALESCE(opf.position_scoring_matchup_quality, 50) AS position_scoring_matchup_quality,
        CASE WHEN opf.is_exploitable_matchup THEN 1 ELSE 0 END AS is_exploitable_matchup_flag,
        CASE WHEN opf.is_avoid_matchup THEN 1 ELSE 0 END AS is_avoid_matchup_flag,
        
        -- Opponent Form
        COALESCE(otf.opp_defensive_momentum, 0) AS opp_defensive_momentum,
        
        -- =================================================================
        -- TIER 4: HISTORICAL PERFORMANCE PATTERNS
        -- =================================================================
        
        -- Historical vs Opponent
        COALESCE(hf.recency_weighted_pts_vs_opponent, csf.pts_avg_l5, 15) AS recency_weighted_pts_vs_opponent,
        COALESCE(hf.historical_confidence_score, 30) AS historical_confidence_score,
        COALESCE(hf.pts_volatility_vs_opponent, 5) AS pts_volatility_vs_opponent,
        
        -- Historical Bounds
        COALESCE(hf.best_scoring_game_vs_opponent, csf.pts_avg_l5 * 1.8, 25) AS best_scoring_game_vs_opponent,
        COALESCE(hf.worst_scoring_game_vs_opponent, csf.pts_avg_l5 * 0.4, 5) AS worst_scoring_game_vs_opponent,
        
        -- Historical Favorability
        COALESCE(hf.historical_matchup_favorability_score, 50) AS historical_matchup_favorability_score,
        
        -- =================================================================
        -- TIER 5: ADVANCED CONTEXT INTERACTIONS
        -- =================================================================
        
        -- Master Context Multiplier
        COALESCE(cif.master_context_interaction_multiplier, 1.0) AS master_context_interaction_multiplier,
        
        -- Specific Interaction Effects
        COALESCE(cif.rest_opponent_multiplier, 1.0) AS rest_opponent_multiplier,
        COALESCE(cif.pace_interaction_multiplier, 1.0) AS pace_interaction_multiplier,
        
        -- Context Signal Quality
        CASE 
            WHEN cif.context_signal_clarity = 'HIGH_SIGNAL_ALIGNMENT' THEN 1
            WHEN cif.context_signal_clarity = 'MIXED_SIGNAL_CONTEXT' THEN -1
            ELSE 0
        END AS context_signal_clarity_score,
        
        -- High-Confidence Scenarios
        CASE WHEN cif.is_high_confidence_favorable THEN 1 ELSE 0 END AS is_high_confidence_favorable_flag,
        CASE WHEN cif.is_high_confidence_difficult THEN 1 ELSE 0 END AS is_high_confidence_difficult_flag,
        
        -- =================================================================
        -- TIER 6: PERFORMANCE BOUNDS & PROJECTIONS
        -- =================================================================
        
        -- Context-Amplified Projections
        COALESCE(pbf.context_amplified_baseline, csf.pts_avg_l5, 15) AS context_amplified_baseline,
        COALESCE(pbf.most_likely_scenario_projection, csf.pts_avg_l5, 15) AS most_likely_scenario_projection,
        COALESCE(pbf.projection_confidence_score, 50) AS projection_confidence_score,
        
        -- Performance Range
        COALESCE(pbf.amplified_performance_range, 15) AS amplified_performance_range,
        COALESCE(pbf.master_amplification_multiplier, 1.0) AS master_amplification_multiplier,
        
        -- =================================================================
        -- TIER 7: VARIANCE & CEILING ANALYSIS
        -- =================================================================
        
        -- Variance Metrics
        COALESCE(vaf.confidence_adjusted_volatility, 5) AS confidence_adjusted_volatility,
        COALESCE(vaf.estimated_pts_ceiling, csf.pts_avg_l5 + 8, 25) AS estimated_pts_ceiling,
        COALESCE(vaf.estimated_pts_floor, GREATEST(0, csf.pts_avg_l5 - 8), 5) AS estimated_pts_floor,
        
        -- Ceiling Probability
        CASE 
            WHEN vaf.ceiling_game_probability_tier = 'HIGH_CEILING_PROBABILITY' THEN 1
            WHEN vaf.ceiling_game_probability_tier = 'MODERATE_CEILING_PROBABILITY' THEN 0.5
            ELSE 0
        END AS ceiling_probability_score,
        
        -- Usage Opportunity
        COALESCE(uof.usage_expansion_gap, 0) AS usage_expansion_gap,
        COALESCE(uof.performance_elasticity_score, 1.0) AS performance_elasticity_score,
        COALESCE(uof.overall_ceiling_game_rating, 30) AS overall_ceiling_game_rating,
        
        -- =================================================================
        -- CATEGORICAL FEATURES (Encoded)
        -- =================================================================
        
        -- Player Role
        CASE 
            WHEN csf.player_offensive_role = 'PRIMARY_OPTION' THEN 3
            WHEN csf.player_offensive_role = 'SECONDARY_OPTION' THEN 2
            WHEN csf.player_offensive_role = 'SUPPORTING_ROLE' THEN 1
            ELSE 0
        END AS player_role_encoded,
        
        -- Position Encoding
        CASE 
            WHEN btd.position = 'PG' THEN 1
            WHEN btd.position = 'SG' THEN 2
            WHEN btd.position = 'SF' THEN 3
            WHEN btd.position = 'PF' THEN 4
            WHEN btd.position = 'C' THEN 5
            ELSE 0
        END AS position_encoded,
        
        -- Season Phase
        CASE 
            WHEN sf.season_phase = 'EARLY_SEASON' THEN 1
            WHEN sf.season_phase = 'SEASON_ADJUSTMENT' THEN 2
            WHEN sf.season_phase = 'MID_SEASON' THEN 3
            WHEN sf.season_phase = 'LATE_SEASON' THEN 4
            WHEN sf.season_phase = 'SEASON_END' THEN 5
            ELSE 3
        END AS season_phase_encoded,
        
        -- Workload Classification
        CASE 
            WHEN sf.workload_classification = 'HIGH_WORKLOAD' THEN 4
            WHEN sf.workload_classification = 'MODERATE_WORKLOAD' THEN 3
            WHEN sf.workload_classification = 'NORMAL_WORKLOAD' THEN 2
            WHEN sf.workload_classification = 'LIMITED_ROLE' THEN 1
            ELSE 0
        END AS workload_encoded,
        
        -- =================================================================
        -- DERIVED COMPOSITE FEATURES
        -- =================================================================
        
        -- Performance Momentum Index
        ROUND(
            (COALESCE(csf.recent_scoring_trend, 0) * 0.4) +
            (COALESCE(otf.opp_defensive_momentum, 0) * -0.3) +
            (COALESCE(sf.rest_advantage_score, 50) - 50) * 0.003,
            3
        ) AS performance_momentum_index,
        
        -- Matchup Advantage Score
        ROUND(
            (COALESCE(opf.position_scoring_matchup_quality, 50) * 0.4) +
            ((100 - COALESCE(otf.opp_overall_strength_score, 50)) * 0.3) +
            (COALESCE(hf.historical_matchup_favorability_score, 50) * 0.3),
            1
        ) AS matchup_advantage_score,
        
        -- Context Reliability Index
        ROUND(
            (COALESCE(pbf.projection_confidence_score, 50) * 0.5) +
            (COALESCE(hf.historical_confidence_score, 30) * 0.3) +
            (CASE WHEN cif.context_signal_clarity = 'HIGH_SIGNAL_ALIGNMENT' THEN 20 ELSE 0 END),
            1
        ) AS context_reliability_index,
        
        -- Ceiling Upside Potential
        ROUND(
            GREATEST(0,
                (COALESCE(vaf.estimated_pts_ceiling, 20) - COALESCE(csf.pts_avg_l5, 15)) *
                COALESCE(uof.performance_elasticity_score, 1.0) *
                CASE WHEN sf.opportunity_context = 'HIGH_OPPORTUNITY' THEN 1.2 ELSE 1.0 END
            ),
            2
        ) AS ceiling_upside_potential,
        
        -- Floor Risk Assessment
        ROUND(
            GREATEST(0,
                (COALESCE(csf.pts_avg_l5, 15) - COALESCE(vaf.estimated_pts_floor, 8)) *
                CASE WHEN sf.has_fatigue_risk THEN 1.3
                     WHEN cif.is_high_confidence_difficult THEN 1.2
                     ELSE 1.0 END
            ),
            2
        ) AS floor_risk_magnitude,
        
        -- =================================================================
        -- DATA QUALITY INDICATORS
        -- =================================================================
        
        -- Feature Completeness Score (percentage of non-null features)
        ROUND(
            (CASE WHEN csf.pts_avg_l5 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN sf.rest_advantage_score IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN otf.opp_overall_strength_score IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN hf.historical_confidence_score IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN pbf.projection_confidence_score IS NOT NULL THEN 1 ELSE 0 END) * 20.0,
            1
        ) AS feature_completeness_score,
        
        -- Historical Data Availability
        CASE 
            WHEN COALESCE(hf.prior_games_vs_opponent, 0) >= 5 THEN 'HIGH'
            WHEN COALESCE(hf.prior_games_vs_opponent, 0) >= 2 THEN 'MEDIUM'
            WHEN COALESCE(hf.prior_games_vs_opponent, 0) >= 1 THEN 'LOW'
            ELSE 'NONE'
        END AS historical_data_availability,
        
        -- =================================================================
        -- METADATA
        -- =================================================================
        CURRENT_TIMESTAMP AS created_at

    FROM base_target_data btd
    -- FIXED JOINS: Use proper keys to prevent cartesian products and duplicates
    LEFT JOIN core_scoring_features csf ON btd.player_game_key = csf.player_game_key
    LEFT JOIN situational_features sf ON btd.player_game_key = sf.player_game_key
    LEFT JOIN opponent_team_features otf ON btd.game_id = otf.game_id 
        AND btd.team_id = otf.team_id 
        AND btd.opponent_id = otf.opponent_id  -- FIXED: Ensure 1:1 join
    LEFT JOIN opponent_position_features opf ON btd.opponent_id = opf.opponent_id 
        AND btd.position = opf.position 
        AND btd.game_date = opf.game_date
    LEFT JOIN historical_features hf ON btd.player_id = hf.player_id 
        AND btd.opponent_id = hf.opponent_id 
        AND btd.game_id = hf.game_id
    LEFT JOIN context_interaction_features cif ON btd.player_game_key = cif.player_game_key
    LEFT JOIN performance_bounds_features pbf ON btd.player_game_key = pbf.player_game_key
    LEFT JOIN variance_analysis_features vaf ON btd.player_game_key = vaf.player_game_key
    LEFT JOIN usage_opportunity_features uof ON btd.player_game_key = uof.player_game_key
    
    WHERE btd.target_pts IS NOT NULL  -- Ensure we have target variable
),

-- =================================================================
-- DEDUPLICATION SAFETY NET
-- =================================================================
deduplicated_features AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY player_game_key ORDER BY game_date DESC, created_at DESC) AS row_num
        FROM consolidated_features
    ) t
    WHERE row_num = 1
)

SELECT * FROM deduplicated_features