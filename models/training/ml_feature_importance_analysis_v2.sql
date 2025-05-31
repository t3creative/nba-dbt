{{ config(
    materialized='table',
    schema='training',
    tags=['ml', 'feature_analysis', 'importance', 'correlation']
) }}

/*
ML FEATURE IMPORTANCE ANALYSIS v2
Systematic analysis of feature predictive power, correlations, and selection
for the player points prediction model.

Purpose: 
- Identify highest-impact features for model performance
- Detect multicollinearity and redundant features
- Provide feature selection recommendations
- Generate feature engineering insights
*/

WITH training_data AS (
    SELECT * 
    FROM {{ ref('ml_player_points_training_v2') }}
    WHERE is_training_data = TRUE
        AND target_pts IS NOT NULL
        AND feature_completeness_score >= 60  -- Ensure sufficient data quality
),

-- =================================================================
-- FEATURE CORRELATION ANALYSIS
-- =================================================================
feature_correlations AS (
    SELECT
        -- Core Performance Correlations
        CORR(target_pts, pts_avg_l3) AS corr_target_pts_avg_l3,
        CORR(target_pts, pts_avg_l5) AS corr_target_pts_avg_l5,
        CORR(target_pts, pts_avg_l10) AS corr_target_pts_avg_l10,
        CORR(target_pts, recent_scoring_trend) AS corr_target_recent_trend,
        
        -- Efficiency & Context Correlations
        CORR(target_pts, scoring_efficiency_composite_l5) AS corr_target_efficiency,
        CORR(target_pts, usage_weighted_ts_l5) AS corr_target_usage_ts,
        CORR(target_pts, team_pts_share_l5) AS corr_target_team_share,
        CORR(target_pts, total_context_multiplier) AS corr_target_context_mult,
        CORR(target_pts, min_avg_l5) AS corr_target_minutes,
        
        -- Situational Context Correlations
        CORR(target_pts, rest_advantage_score) AS corr_target_rest,
        CORR(target_pts, situational_favorability_score) AS corr_target_situation,
        
        -- Opponent Matchup Correlations
        CORR(target_pts, opp_overall_strength_score) AS corr_target_opp_strength,
        CORR(target_pts, position_overall_matchup_quality) AS corr_target_position_matchup,
        CORR(target_pts, opp_recent_def_rating) AS corr_target_opp_defense,
        
        -- Historical Performance Correlations
        CORR(target_pts, recency_weighted_pts_vs_opponent) AS corr_target_historical_pts,
        CORR(target_pts, historical_confidence_score) AS corr_target_historical_conf,
        CORR(target_pts, pts_volatility_vs_opponent) AS corr_target_volatility,
        
        -- Advanced Context Correlations
        CORR(target_pts, master_context_interaction_multiplier) AS corr_target_master_mult,
        CORR(target_pts, context_amplified_baseline) AS corr_target_amplified_baseline,
        CORR(target_pts, most_likely_scenario_projection) AS corr_target_projection,
        
        -- Derived Feature Correlations
        CORR(target_pts, performance_momentum_index) AS corr_target_momentum,
        CORR(target_pts, matchup_advantage_score) AS corr_target_matchup_adv,
        CORR(target_pts, ceiling_upside_potential) AS corr_target_ceiling_upside,
        
        -- Sample Size
        COUNT(*) AS total_observations
        
    FROM training_data
),

-- =================================================================
-- FEATURE UNIVARIATE STATISTICS
-- =================================================================
feature_statistics AS (
    SELECT
        -- Target Variable Stats
        AVG(target_pts) AS target_mean,
        STDDEV(target_pts) AS target_stddev,
        MIN(target_pts) AS target_min,
        MAX(target_pts) AS target_max,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY target_pts) AS target_q25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY target_pts) AS target_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY target_pts) AS target_q75,
        
        -- Core Performance Feature Stats
        AVG(pts_avg_l5) AS pts_avg_l5_mean,
        STDDEV(pts_avg_l5) AS pts_avg_l5_stddev,
        CORR(pts_avg_l5, target_pts) AS pts_avg_l5_target_corr,
        
        AVG(scoring_efficiency_composite_l5) AS efficiency_mean,
        STDDEV(scoring_efficiency_composite_l5) AS efficiency_stddev,
        
        AVG(team_pts_share_l5) AS usage_mean,
        STDDEV(team_pts_share_l5) AS usage_stddev,
        
        -- Contextual Feature Stats
        AVG(rest_advantage_score) AS rest_mean,
        STDDEV(rest_advantage_score) AS rest_stddev,
        
        AVG(opp_overall_strength_score) AS opp_strength_mean,
        STDDEV(opp_overall_strength_score) AS opp_strength_stddev,
        
        -- Missing Data Analysis
        COUNT(CASE WHEN pts_avg_l5 IS NULL THEN 1 END) * 100.0 / COUNT(*) AS pts_avg_l5_null_pct,
        COUNT(CASE WHEN recency_weighted_pts_vs_opponent IS NULL THEN 1 END) * 100.0 / COUNT(*) AS historical_null_pct,
        COUNT(CASE WHEN position_overall_matchup_quality IS NULL THEN 1 END) * 100.0 / COUNT(*) AS matchup_null_pct
        
    FROM training_data
),

-- =================================================================
-- FEATURE IMPORTANCE TIERS
-- =================================================================
feature_importance_ranking AS (
    SELECT
        features.feature_name,
        features.correlation_with_target,
        abs(features.correlation_with_target) AS abs_correlation,
        CASE 
            WHEN abs(features.correlation_with_target) >= 0.60 THEN 'TIER_1_CRITICAL'
            WHEN abs(features.correlation_with_target) >= 0.40 THEN 'TIER_2_HIGH_IMPACT'
            WHEN abs(features.correlation_with_target) >= 0.25 THEN 'TIER_3_MODERATE_IMPACT'
            WHEN abs(features.correlation_with_target) >= 0.15 THEN 'TIER_4_LOW_IMPACT'
            ELSE 'TIER_5_MINIMAL_IMPACT'
        END AS importance_tier,
        row_number() OVER (ORDER BY abs(features.correlation_with_target) DESC) AS importance_rank
    FROM feature_correlations fc
    CROSS JOIN LATERAL (
        VALUES
            ('pts_avg_l3', fc.corr_target_pts_avg_l3),
            ('pts_avg_l5', fc.corr_target_pts_avg_l5),
            ('pts_avg_l10', fc.corr_target_pts_avg_l10),
            ('recent_scoring_trend', fc.corr_target_recent_trend),
            ('scoring_efficiency_composite_l5', fc.corr_target_efficiency),
            ('usage_weighted_ts_l5', fc.corr_target_usage_ts),
            ('team_pts_share_l5', fc.corr_target_team_share),
            ('total_context_multiplier', fc.corr_target_context_mult),
            ('min_avg_l5', fc.corr_target_minutes),
            ('rest_advantage_score', fc.corr_target_rest),
            ('situational_favorability_score', fc.corr_target_situation),
            ('opp_overall_strength_score', fc.corr_target_opp_strength),
            ('position_overall_matchup_quality', fc.corr_target_position_matchup),
            ('opp_recent_def_rating', fc.corr_target_opp_defense),
            ('recency_weighted_pts_vs_opponent', fc.corr_target_historical_pts),
            ('historical_confidence_score', fc.corr_target_historical_conf),
            ('pts_volatility_vs_opponent', fc.corr_target_volatility),
            ('master_context_interaction_multiplier', fc.corr_target_master_mult),
            ('context_amplified_baseline', fc.corr_target_amplified_baseline),
            ('most_likely_scenario_projection', fc.corr_target_projection),
            ('performance_momentum_index', fc.corr_target_momentum),
            ('matchup_advantage_score', fc.corr_target_matchup_adv),
            ('ceiling_upside_potential', fc.corr_target_ceiling_upside)
    ) AS features(feature_name, correlation_with_target)
    WHERE features.correlation_with_target IS NOT NULL
),

-- =================================================================
-- MULTICOLLINEARITY DETECTION
-- =================================================================
multicollinearity_analysis AS (
    SELECT
        -- High correlation pairs that may cause multicollinearity
        'pts_avg_l3 vs pts_avg_l5' AS feature_pair,
        CORR(pts_avg_l3, pts_avg_l5) AS correlation,
        CASE WHEN ABS(CORR(pts_avg_l3, pts_avg_l5)) > 0.85 THEN TRUE ELSE FALSE END AS high_correlation_flag
    FROM training_data
    
    UNION ALL
    
    SELECT
        'pts_avg_l5 vs pts_avg_l10',
        CORR(pts_avg_l5, pts_avg_l10),
        CASE WHEN ABS(CORR(pts_avg_l5, pts_avg_l10)) > 0.85 THEN TRUE ELSE FALSE END
    FROM training_data
    
    UNION ALL
    
    SELECT
        'context_amplified_baseline vs most_likely_scenario_projection',
        CORR(context_amplified_baseline, most_likely_scenario_projection),
        CASE WHEN ABS(CORR(context_amplified_baseline, most_likely_scenario_projection)) > 0.85 THEN TRUE ELSE FALSE END
    FROM training_data
    
    UNION ALL
    
    SELECT
        'recency_weighted_pts_vs_opponent vs pts_avg_l5',
        CORR(recency_weighted_pts_vs_opponent, pts_avg_l5),
        CASE WHEN ABS(CORR(recency_weighted_pts_vs_opponent, pts_avg_l5)) > 0.85 THEN TRUE ELSE FALSE END
    FROM training_data
),

-- =================================================================
-- FEATURE SELECTION RECOMMENDATIONS
-- =================================================================
feature_selection_recommendations AS (
    SELECT
        importance_tier,
        COUNT(*) AS feature_count,
        STRING_AGG(feature_name, ', ' ORDER BY importance_rank) AS recommended_features,
        AVG(abs_correlation) AS avg_correlation_in_tier,
        MIN(abs_correlation) AS min_correlation_in_tier,
        MAX(abs_correlation) AS max_correlation_in_tier
    FROM feature_importance_ranking
    GROUP BY importance_tier
),

-- =================================================================
-- PERFORMANCE ANALYSIS BY FEATURE SEGMENTS
-- =================================================================
feature_segment_analysis AS (
    SELECT
        'Recent Performance (L5)' AS segment_type,
        CASE WHEN td.pts_avg_l5 >= pl.p75_l5 THEN 'HIGH' ELSE 'LOW' END AS segment,
        AVG(td.target_pts) AS avg_target_pts,
        STDDEV(td.target_pts) AS stddev_target_pts,
        COUNT(*) AS sample_size
    FROM training_data td
    CROSS JOIN (
        SELECT
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pts_avg_l5) AS p75_l5
        FROM training_data
    ) pl
    GROUP BY 1, 2
    
    UNION ALL
    
    SELECT
        'Usage Rate (Team Share)' AS segment_type,
        CASE WHEN team_pts_share_l5 >= 25 THEN 'HIGH_USAGE' ELSE 'LOW_USAGE' END AS segment,
        AVG(target_pts) AS avg_target_pts,
        STDDEV(target_pts) AS stddev_target_pts,
        COUNT(*) AS sample_size
    FROM training_data
    GROUP BY 1, 2
    
    UNION ALL
    
    SELECT
        'Rest Advantage' AS segment_type,
        CASE WHEN rest_advantage_score >= 75 THEN 'WELL_RESTED' ELSE 'NORMAL_REST' END AS segment,
        AVG(target_pts) AS avg_target_pts,
        STDDEV(target_pts) AS stddev_target_pts,
        COUNT(*) AS sample_size
    FROM training_data
    GROUP BY 1, 2
    
    UNION ALL
    
    SELECT
        'Opponent Strength' AS segment_type,
        CASE WHEN opp_overall_strength_score <= 40 THEN 'WEAK_OPPONENT' ELSE 'STRONG_OPPONENT' END AS segment,
        AVG(target_pts) AS avg_target_pts,
        STDDEV(target_pts) AS stddev_target_pts,
        COUNT(*) AS sample_size
    FROM training_data
    GROUP BY 1, 2
),

-- =================================================================
-- FINAL CONSOLIDATED ANALYSIS
-- =================================================================
consolidated_analysis AS (
    SELECT
        'FEATURE_CORRELATIONS' AS analysis_type,
        JSON_BUILD_OBJECT(
            'highest_correlation_features', (
                SELECT JSON_AGG(JSON_BUILD_OBJECT(
                    'feature', feature_name,
                    'correlation', ROUND(correlation_with_target::numeric, 4),
                    'importance_tier', importance_tier
                ))
                FROM feature_importance_ranking
                WHERE importance_rank <= 10
            ),
            'tier_1_critical_features', (
                SELECT STRING_AGG(feature_name, ', ')
                FROM feature_importance_ranking
                WHERE importance_tier = 'TIER_1_CRITICAL'
            ),
            'tier_2_high_impact_features', (
                SELECT STRING_AGG(feature_name, ', ')
                FROM feature_importance_ranking
                WHERE importance_tier = 'TIER_2_HIGH_IMPACT'
            )
        ) AS analysis_results
    
    UNION ALL
    
    SELECT
        'MULTICOLLINEARITY_ISSUES',
        JSON_BUILD_OBJECT(
            'high_correlation_pairs', (
                SELECT JSON_AGG(JSON_BUILD_OBJECT(
                    'pair', feature_pair,
                    'correlation', ROUND(correlation::numeric, 4)
                ))
                FROM multicollinearity_analysis
                WHERE high_correlation_flag = TRUE
            )
        )
    
    UNION ALL
    
    SELECT
        'DATA_QUALITY_SUMMARY',
        JSON_BUILD_OBJECT(
            'total_training_samples', (SELECT COUNT(*) FROM training_data),
            'target_statistics', JSON_BUILD_OBJECT(
                'mean', (SELECT ROUND(target_mean::numeric, 2) FROM feature_statistics),
                'stddev', (SELECT ROUND(target_stddev::numeric, 2) FROM feature_statistics),
                'median', (SELECT ROUND(target_median::numeric, 2) FROM feature_statistics)
            ),
            'missing_data_issues', JSON_BUILD_OBJECT(
                'historical_data_missing_pct', (SELECT ROUND(historical_null_pct, 1) FROM feature_statistics),
                'matchup_data_missing_pct', (SELECT ROUND(matchup_null_pct, 1) FROM feature_statistics)
            )
        )
    
    UNION ALL
    
    SELECT
        'FEATURE_SELECTION_RECOMMENDATION',
        JSON_BUILD_OBJECT(
            'recommended_minimal_feature_set', ARRAY[
                'pts_avg_l5',
                'team_pts_share_l5', 
                'min_avg_l5',
                'scoring_efficiency_composite_l5',
                'recency_weighted_pts_vs_opponent',
                'rest_advantage_score',
                'position_overall_matchup_quality',
                'master_context_interaction_multiplier'
            ],
            'recommended_full_feature_set', (
                SELECT ARRAY_AGG(feature_name)
                FROM feature_importance_ranking
                WHERE importance_tier IN ('TIER_1_CRITICAL', 'TIER_2_HIGH_IMPACT', 'TIER_3_MODERATE_IMPACT')
            ),
            'features_to_consider_dropping', (
                SELECT ARRAY_AGG(feature_name)
                FROM feature_importance_ranking
                WHERE importance_tier = 'TIER_5_MINIMAL_IMPACT'
            )
        )
)

SELECT 
    analysis_type,
    analysis_results,
    CURRENT_TIMESTAMP AS created_at
FROM consolidated_analysis

UNION ALL

-- Feature importance rankings table
SELECT
    'DETAILED_FEATURE_RANKINGS' AS analysis_type,
    JSON_BUILD_OBJECT(
        'feature_rankings', JSON_AGG(
            JSON_BUILD_OBJECT(
                'rank', importance_rank,
                'feature', feature_name,
                'correlation', ROUND(correlation_with_target::numeric, 4),
                'abs_correlation', ROUND(abs_correlation::numeric, 4),
                'tier', importance_tier
            ) ORDER BY importance_rank
        )
    ) AS analysis_results,
    CURRENT_TIMESTAMP AS created_at
FROM feature_importance_ranking