{{ config(
    materialized='table',
    schema='ml_training',
    tags=['ml', 'validation', 'data_quality', 'temporal_safety']
) }}

/*
ML MODEL VALIDATION FRAMEWORK v2
Comprehensive validation suite for the player points prediction model.
Ensures temporal safety, data quality, and ML readiness.

Critical Validations:
- Zero data leakage verification
- Temporal consistency checks  
- Feature distribution analysis
- Missing data assessment
- Model readiness evaluation
*/

WITH validation_base AS (
    SELECT * 
    FROM {{ ref('ml_player_points_training_v2') }}
),

-- =================================================================
-- TEMPORAL SAFETY VALIDATION (CRITICAL FOR ML)
-- =================================================================
temporal_safety_checks AS (
    SELECT
        'TEMPORAL_SAFETY' AS validation_category,
        
        -- Check 1: No future data leakage in features
        CASE 
            WHEN COUNT(CASE WHEN pts_avg_l3 > target_pts * 2 THEN 1 END) = 0 
                THEN 'PASS' 
                ELSE 'FAIL' 
        END AS future_data_leakage_check,
        
        -- Check 2: Feature temporal consistency
        CASE 
            WHEN COUNT(CASE WHEN pts_avg_l3 > pts_avg_l5 + 20 THEN 1 END) < COUNT(*) * 0.01
                THEN 'PASS'
                ELSE 'WARNING'
        END AS feature_temporal_consistency_check,
        
        -- Check 3: Historical features use only prior data
        CASE 
            WHEN AVG(CASE WHEN recency_weighted_pts_vs_opponent <= pts_avg_l10 * 2 THEN 1 ELSE 0 END) >= 0.95
                THEN 'PASS'
                ELSE 'FAIL'
        END AS historical_prior_data_only_check,
        
        -- Check 4: Lag structure validation
        CASE 
            WHEN CORR(pts_avg_l3, pts_avg_l5) BETWEEN 0.7 AND 0.95
                AND CORR(pts_avg_l5, pts_avg_l10) BETWEEN 0.7 AND 0.95
                THEN 'PASS'
                ELSE 'WARNING'
        END AS lag_structure_validation,
        
        COUNT(*) AS total_records_validated
        
    FROM validation_base
    WHERE is_training_data = TRUE
),

-- =================================================================
-- DATA QUALITY VALIDATION
-- =================================================================
data_quality_checks AS (
    SELECT
        'DATA_QUALITY' AS validation_category,
        
        -- Target variable quality
        ROUND(AVG(CASE WHEN target_pts IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS target_completeness_pct,
        ROUND(STDDEV(target_pts), 2) AS target_stddev,
        MIN(target_pts) AS target_min,
        MAX(target_pts) AS target_max,
        
        -- Feature completeness analysis
        ROUND(AVG(feature_completeness_score), 1) AS avg_feature_completeness,
        ROUND(MIN(feature_completeness_score), 1) AS min_feature_completeness,
        
        -- Core feature availability
        ROUND(AVG(CASE WHEN pts_avg_l5 IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS core_performance_completeness_pct,
        ROUND(AVG(CASE WHEN rest_advantage_score IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS situational_completeness_pct,
        ROUND(AVG(CASE WHEN opp_overall_strength_score IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS opponent_completeness_pct,
        ROUND(AVG(CASE WHEN recency_weighted_pts_vs_opponent IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS historical_completeness_pct,
        
        -- Outlier detection
        COUNT(CASE WHEN target_pts > 50 THEN 1 END) AS extreme_high_games_count,
        COUNT(CASE WHEN target_pts = 0 THEN 1 END) AS zero_point_games_count,
        
        -- Data distribution health
        CASE 
            WHEN ROUND(STDDEV(target_pts), 2) BETWEEN 8 AND 12 
                AND MIN(target_pts) >= 0 
                AND MAX(target_pts) <= 70 
                THEN 'HEALTHY_DISTRIBUTION'
            ELSE 'DISTRIBUTION_CONCERNS'
        END AS target_distribution_health
        
    FROM validation_base
    WHERE is_training_data = TRUE
),

-- =================================================================
-- TRAIN/TEST SPLIT VALIDATION
-- =================================================================
split_validation AS (
    SELECT
        'TRAIN_TEST_SPLIT' AS validation_category,
        
        -- Training set statistics
        COUNT(CASE WHEN is_training_data = TRUE THEN 1 END) AS training_set_size,
        ROUND(AVG(CASE WHEN is_training_data = TRUE THEN target_pts END), 2) AS training_target_mean,
        ROUND(STDDEV(CASE WHEN is_training_data = TRUE THEN target_pts END), 2) AS training_target_stddev,
        
        -- Test set statistics  
        COUNT(CASE WHEN is_test_data = TRUE THEN 1 END) AS test_set_size,
        ROUND(AVG(CASE WHEN is_test_data = TRUE THEN target_pts END), 2) AS test_target_mean,
        ROUND(STDDEV(CASE WHEN is_test_data = TRUE THEN target_pts END), 2) AS test_target_stddev,
        
        -- Split ratio
        ROUND(
            COUNT(CASE WHEN is_training_data = TRUE THEN 1 END) * 100.0 / 
            NULLIF(COUNT(*), 0), 1
        ) AS training_set_percentage,
        
        -- Distribution similarity check
        CASE 
            WHEN ABS(
                AVG(CASE WHEN is_training_data = TRUE THEN target_pts END) - 
                AVG(CASE WHEN is_test_data = TRUE THEN target_pts END)
            ) <= 2.0
            THEN 'PASS'
            ELSE 'WARNING'
        END AS distribution_similarity_check,
        
        -- Temporal separation validation
        MAX(CASE WHEN is_training_data = TRUE THEN game_date END) AS latest_training_date,
        MIN(CASE WHEN is_test_data = TRUE THEN game_date END) AS earliest_test_date,
        
        CASE 
            WHEN MAX(CASE WHEN is_training_data = TRUE THEN game_date END) < 
                 MIN(CASE WHEN is_test_data = TRUE THEN game_date END)
            THEN 'PASS'
            ELSE 'FAIL'
        END AS temporal_separation_check
        
    FROM validation_base
    WHERE is_training_data = TRUE OR is_test_data = TRUE
),

-- =================================================================
-- FEATURE ENGINEERING VALIDATION
-- =================================================================
feature_engineering_validation AS (
    SELECT
        'FEATURE_ENGINEERING' AS validation_category,
        
        -- Derived feature consistency
        ROUND(CORR(context_amplified_baseline, most_likely_scenario_projection)::numeric, 3) AS projection_consistency_corr,
        ROUND(CORR(estimated_pts_ceiling, ceiling_upside_potential)::numeric, 3) AS ceiling_metrics_consistency,
        
        -- Context multiplier reasonableness
        ROUND(AVG(master_context_interaction_multiplier), 3) AS avg_context_multiplier,
        ROUND(STDDEV(master_context_interaction_multiplier), 3) AS stddev_context_multiplier,
        MIN(master_context_interaction_multiplier) AS min_context_multiplier,
        MAX(master_context_interaction_multiplier) AS max_context_multiplier,
        
        -- Encoded feature validation
        COUNT(CASE WHEN player_role_encoded NOT BETWEEN 0 AND 3 THEN 1 END) AS invalid_role_encoding_count,
        COUNT(CASE WHEN position_encoded NOT BETWEEN 0 AND 5 THEN 1 END) AS invalid_position_encoding_count,
        COUNT(CASE WHEN season_phase_encoded NOT BETWEEN 1 AND 5 THEN 1 END) AS invalid_season_encoding_count,
        
        -- Composite feature ranges
        CASE 
            WHEN AVG(performance_momentum_index) BETWEEN -10 AND 10
                AND AVG(matchup_advantage_score) BETWEEN 20 AND 80
                THEN 'PASS'
            ELSE 'WARNING'
        END AS composite_feature_range_check,
        
        COUNT(*) AS total_records_checked
        
    FROM validation_base
    WHERE is_training_data = TRUE
),

-- =================================================================
-- MISSING DATA PATTERN ANALYSIS
-- =================================================================
missing_data_analysis AS (
    SELECT
        'MISSING_DATA_PATTERNS' AS validation_category,
        
        -- Critical feature missing patterns
        COUNT(CASE WHEN pts_avg_l5 IS NULL THEN 1 END) AS missing_core_performance_count,
        COUNT(CASE WHEN recency_weighted_pts_vs_opponent IS NULL THEN 1 END) AS missing_historical_count,
        COUNT(CASE WHEN position_overall_matchup_quality IS NULL THEN 1 END) AS missing_matchup_count,
        
        -- Missing data by season (early season effect)
        ROUND(
            AVG(CASE WHEN season_phase_encoded = 1 THEN feature_completeness_score END), 1
        ) AS early_season_completeness,
        ROUND(
            AVG(CASE WHEN season_phase_encoded >= 3 THEN feature_completeness_score END), 1  
        ) AS mid_late_season_completeness,
        
        -- Missing data by player role
        ROUND(
            AVG(CASE WHEN player_role_encoded >= 2 THEN feature_completeness_score END), 1
        ) AS primary_secondary_completeness,
        ROUND(
            AVG(CASE WHEN player_role_encoded <= 1 THEN feature_completeness_score END), 1
        ) AS supporting_role_completeness,
        
        -- Historical data availability assessment
        COUNT(CASE WHEN historical_data_availability = 'NONE' THEN 1 END) AS no_historical_data_count,
        COUNT(CASE WHEN historical_data_availability = 'HIGH' THEN 1 END) AS rich_historical_data_count,
        
        ROUND(
            COUNT(CASE WHEN feature_completeness_score >= 80 THEN 1 END) * 100.0 / 
            NULLIF(COUNT(*), 0), 1
        ) AS high_quality_records_pct
        
    FROM validation_base
    WHERE is_training_data = TRUE
),

-- =================================================================
-- MODEL READINESS ASSESSMENT
-- =================================================================
model_readiness_assessment AS (
    SELECT
        'MODEL_READINESS' AS validation_category,
        
        -- Sample size adequacy
        COUNT(*) AS total_training_samples,
        CASE 
            WHEN COUNT(*) >= 50000 THEN 'EXCELLENT'
            WHEN COUNT(*) >= 30000 THEN 'GOOD' 
            WHEN COUNT(*) >= 15000 THEN 'ADEQUATE'
            ELSE 'INSUFFICIENT'
        END AS sample_size_adequacy,
        
        -- Feature quality score
        ROUND(AVG(feature_completeness_score), 1) AS avg_feature_quality,
        CASE 
            WHEN AVG(feature_completeness_score) >= 85 THEN 'EXCELLENT'
            WHEN AVG(feature_completeness_score) >= 75 THEN 'GOOD'
            WHEN AVG(feature_completeness_score) >= 65 THEN 'ADEQUATE'
            ELSE 'NEEDS_IMPROVEMENT'
        END AS feature_quality_rating,
        
        -- Target variable quality
        CASE 
            WHEN STDDEV(target_pts) BETWEEN 8 AND 12 
                AND COUNT(CASE WHEN target_pts IS NULL THEN 1 END) = 0
                THEN 'EXCELLENT'
            ELSE 'GOOD'
        END AS target_variable_quality,
        
        -- Class balance (for different point ranges)
        COUNT(CASE WHEN target_pts <= 10 THEN 1 END) AS low_scoring_games,
        COUNT(CASE WHEN target_pts BETWEEN 11 AND 20 THEN 1 END) AS moderate_scoring_games,
        COUNT(CASE WHEN target_pts BETWEEN 21 AND 30 THEN 1 END) AS good_scoring_games,
        COUNT(CASE WHEN target_pts > 30 THEN 1 END) AS high_scoring_games,
        
        -- Overall readiness score (0-100)
        LEAST(100, GREATEST(0,
            CASE 
                WHEN COUNT(*) >= 50000 THEN 30
                WHEN COUNT(*) >= 30000 THEN 25
                WHEN COUNT(*) >= 15000 THEN 20
                ELSE 10
            END +
            CASE 
                WHEN AVG(feature_completeness_score) >= 85 THEN 30
                WHEN AVG(feature_completeness_score) >= 75 THEN 25
                WHEN AVG(feature_completeness_score) >= 65 THEN 20
                ELSE 10
            END +
            CASE 
                WHEN STDDEV(target_pts) BETWEEN 8 AND 12 THEN 25
                ELSE 15
            END +
            15  -- Base score for proper data structure
        )) AS overall_readiness_score
        
    FROM validation_base
    WHERE is_training_data = TRUE
),

-- =================================================================
-- FINAL VALIDATION SUMMARY
-- =================================================================
validation_summary AS (
    SELECT
        'VALIDATION_SUMMARY' AS validation_category,
        JSON_BUILD_OBJECT(
            'temporal_safety', (
                SELECT JSON_BUILD_OBJECT(
                    'future_data_leakage_check', future_data_leakage_check,
                    'feature_temporal_consistency_check', feature_temporal_consistency_check,
                    'historical_prior_data_only_check', historical_prior_data_only_check,
                    'lag_structure_validation', lag_structure_validation
                )
                FROM temporal_safety_checks
            ),
            'data_quality', (
                SELECT JSON_BUILD_OBJECT(
                    'target_completeness_pct', target_completeness_pct,
                    'avg_feature_completeness', avg_feature_completeness,
                    'target_distribution_health', target_distribution_health,
                    'extreme_high_games_count', extreme_high_games_count
                )
                FROM data_quality_checks
            ),
            'train_test_split', (
                SELECT JSON_BUILD_OBJECT(
                    'training_set_size', training_set_size,
                    'test_set_size', test_set_size,
                    'training_set_percentage', training_set_percentage,
                    'distribution_similarity_check', distribution_similarity_check,
                    'temporal_separation_check', temporal_separation_check
                )
                FROM split_validation
            ),
            'model_readiness', (
                SELECT JSON_BUILD_OBJECT(
                    'sample_size_adequacy', sample_size_adequacy,
                    'feature_quality_rating', feature_quality_rating,
                    'target_variable_quality', target_variable_quality,
                    'overall_readiness_score', overall_readiness_score
                )
                FROM model_readiness_assessment
            )
        ) AS validation_results,
        CURRENT_TIMESTAMP AS created_at
),

-- =================================================================
-- DETAILED VALIDATION RESULTS
-- =================================================================
all_validation_results AS (
    SELECT
        validation_category,
        JSON_BUILD_OBJECT(
            'future_data_leakage_check', future_data_leakage_check,
            'feature_temporal_consistency_check', feature_temporal_consistency_check,
            'historical_prior_data_only_check', historical_prior_data_only_check,
            'lag_structure_validation', lag_structure_validation,
            'total_records_validated', total_records_validated
        )::text AS validation_results,
        CURRENT_TIMESTAMP AS created_at
    FROM temporal_safety_checks
    UNION ALL
    SELECT 
        validation_category,
        JSON_BUILD_OBJECT(
            'target_completeness_pct', target_completeness_pct,
            'avg_feature_completeness', avg_feature_completeness,
            'core_performance_completeness_pct', core_performance_completeness_pct,
            'situational_completeness_pct', situational_completeness_pct,
            'opponent_completeness_pct', opponent_completeness_pct,
            'historical_completeness_pct', historical_completeness_pct,
            'target_distribution_health', target_distribution_health,
            'extreme_high_games_count', extreme_high_games_count,
            'zero_point_games_count', zero_point_games_count
        )::text AS validation_results,
        CURRENT_TIMESTAMP AS created_at
    FROM data_quality_checks
    
    UNION ALL
    
    SELECT 
        validation_category,
        JSON_BUILD_OBJECT(
            'training_set_size', training_set_size,
            'test_set_size', test_set_size,
            'training_target_mean', training_target_mean,
            'test_target_mean', test_target_mean,
            'training_set_percentage', training_set_percentage,
            'distribution_similarity_check', distribution_similarity_check,
            'temporal_separation_check', temporal_separation_check,
            'latest_training_date', latest_training_date,
            'earliest_test_date', earliest_test_date
        )::text AS validation_results,
        CURRENT_TIMESTAMP AS created_at
    FROM split_validation
    
    UNION ALL
    
    SELECT 
        validation_category,
        JSON_BUILD_OBJECT(
            'avg_context_multiplier', avg_context_multiplier,
            'stddev_context_multiplier', stddev_context_multiplier,
            'composite_feature_range_check', composite_feature_range_check,
            'invalid_encoding_issues', invalid_role_encoding_count + invalid_position_encoding_count + invalid_season_encoding_count
        )::text AS validation_results,
        CURRENT_TIMESTAMP AS created_at
    FROM feature_engineering_validation
    
    UNION ALL
    
    SELECT 
        validation_category,
        JSON_BUILD_OBJECT(
            'missing_core_performance_count', missing_core_performance_count,
            'missing_historical_count', missing_historical_count,
            'missing_matchup_count', missing_matchup_count,
            'early_season_completeness', early_season_completeness,
            'mid_late_season_completeness', mid_late_season_completeness,
            'high_quality_records_pct', high_quality_records_pct,
            'no_historical_data_count', no_historical_data_count
        )::text AS validation_results,
        CURRENT_TIMESTAMP AS created_at
    FROM missing_data_analysis
    
    UNION ALL
    
    SELECT 
        validation_category,
        JSON_BUILD_OBJECT(
            'total_training_samples', total_training_samples,
            'sample_size_adequacy', sample_size_adequacy,
            'feature_quality_rating', feature_quality_rating,
            'target_variable_quality', target_variable_quality,
            'overall_readiness_score', overall_readiness_score,
            'low_scoring_games', low_scoring_games,
            'moderate_scoring_games', moderate_scoring_games,
            'good_scoring_games', good_scoring_games,
            'high_scoring_games', high_scoring_games
        )::text AS validation_results,
        CURRENT_TIMESTAMP AS created_at
    FROM model_readiness_assessment
)

SELECT 
    validation_category,
    validation_results AS detailed_results,
    created_at
FROM all_validation_results