{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'player', 'context', 'bounds', 'ceiling_floor', 'amplified'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['season_year']}
    ]
) }}

/*
CONTEXT-AMPLIFIED PERFORMANCE BOUNDS v2
Integrates ALL contextual factors to create comprehensive performance ceiling/floor
projections that account for multiplicative effects between different contexts.

Core Value: Moves beyond isolated context analysis to model how rest, matchups, 
opportunities, historical patterns, and situational factors interact to amplify
or suppress performance bounds.

Grain: One row per player + game_date
Integration: Master model that leverages all context feature stores
*/

WITH base_data AS (
    SELECT 
        player_game_key, player_id, game_id, game_date, season_year,
        opponent_id, position, home_away
    FROM {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Core performance metrics and projections
performance_baseline AS (
    SELECT
        player_game_key,
        -- Current performance levels
        pts_avg_l3,
        pts_avg_l5, 
        pts_avg_l10,
        -- Efficiency context
        scoring_efficiency_composite_l5,
        usage_weighted_ts_l5,
        -- Team context
        team_pts_share_l5,
        player_offensive_role,
        total_context_multiplier,
        playstyle_compatibility_score,
        -- Pressure handling
        pressure_performance_tier,
        -- Performance trends
        pts_avg_l3 - pts_avg_l10 AS short_term_trend
    FROM {{ ref('player_scoring_features_v2') }}
),

-- Situational context factors
situational_amplifiers AS (
    SELECT
        player_game_key,
        -- Rest and fatigue
        rest_advantage_score,
        fatigue_risk_assessment,
        situational_favorability_score,
        -- Opportunity context
        opportunity_context,
        roster_health_status,
        workload_classification,
        -- Schedule factors
        is_back_to_back,
        is_well_rested,
        season_phase
    FROM {{ ref('game_situational_context_v2') }}
),

-- Opponent matchup factors
matchup_amplifiers AS (
    SELECT 
        om.team_game_key,
        -- Opponent strength
        om.opp_overall_strength_score,
        om.opp_predictability_score,
        om.opp_recent_def_rating,
        -- Position-specific matchup
        pm.position_overall_matchup_quality,
        pm.position_defense_consistency_score,
        pm.is_exploitable_matchup,
        pm.is_avoid_matchup
    FROM {{ ref('opponent_pregame_profile_features_v2') }} om
    LEFT JOIN {{ ref('opponent_position_defense_features_v2') }} pm
        ON om.opponent_id = pm.opponent_id 
        AND om.game_date = pm.game_date
),

-- Historical performance context
historical_bounds AS (
    SELECT
        player_matchup_key,
        player_id,
        opponent_id,
        game_id,
        game_date,
        season_year,
        -- Historical averages vs this opponent
        avg_pts_vs_opponent_prior,
        avg_pra_vs_opponent_prior,
        -- Historical bounds
        best_scoring_game_vs_opponent,
        worst_scoring_game_vs_opponent,
        -- Volatility and consistency
        pts_volatility_vs_opponent,
        pra_volatility_vs_opponent,
        -- Confidence and trends
        historical_confidence_score,
        historical_matchup_favorability_score,
        recency_weighted_pts_vs_opponent
    FROM {{ ref('player_historical_matchup_features_v2') }}
),

-- Master context interactions
context_interactions AS (
    SELECT
        player_game_key,
        -- Master multiplier
        master_context_interaction_multiplier,
        -- Specific interaction categories
        rest_opponent_interaction_category,
        historical_situational_alignment,
        workload_sustainability_category,
        -- Signal alignment
        context_signal_clarity,
        -- Binary flags
        is_high_confidence_favorable,
        is_high_confidence_difficult,
        has_contradictory_context_signals,
        has_amplified_opportunity
    FROM {{ ref('context_interaction_features_v2') }}
),

-- STEP 1: Calculate baseline performance assessment
baseline_assessment AS (
    SELECT
        bd.player_game_key,
        bd.player_id,
        bd.game_id,
        bd.game_date,
        bd.season_year,
        bd.opponent_id,
        bd.position,
        bd.home_away,
        
        -- =================================================================
        -- BASELINE PERFORMANCE ASSESSMENT
        -- =================================================================
        
        -- Choose most reliable baseline (recency-weighted when available)
        COALESCE(
            hb.recency_weighted_pts_vs_opponent,
            pb.pts_avg_l5,
            hb.avg_pts_vs_opponent_prior,
            pb.pts_avg_l10,
            15.0
        ) AS performance_baseline,
        
        -- Baseline confidence score
        CASE 
            WHEN hb.historical_confidence_score >= 70 THEN 'HIGH_BASELINE_CONFIDENCE'
            WHEN hb.historical_confidence_score >= 50 THEN 'MODERATE_BASELINE_CONFIDENCE'
            WHEN hb.historical_confidence_score >= 30 THEN 'LOW_BASELINE_CONFIDENCE'
            ELSE 'MINIMAL_BASELINE_CONFIDENCE'
        END AS baseline_confidence_tier,
        
        -- Pass through other needed columns for next CTE
        sa.rest_advantage_score,
        sa.fatigue_risk_assessment,
        sa.opportunity_context,
        sa.roster_health_status,
        ma.position_overall_matchup_quality,
        ma.is_exploitable_matchup,
        ma.is_avoid_matchup,
        pb.short_term_trend,
        pb.pressure_performance_tier,
        ci.master_context_interaction_multiplier,
        ci.has_amplified_opportunity,
        ci.is_high_confidence_favorable,
        ci.is_high_confidence_difficult,
        ci.has_contradictory_context_signals,
        ci.context_signal_clarity,
        hb.pts_volatility_vs_opponent,
        hb.best_scoring_game_vs_opponent,
        hb.worst_scoring_game_vs_opponent,
        pb.team_pts_share_l5,
        pb.player_offensive_role

    FROM base_data bd
    LEFT JOIN performance_baseline pb ON bd.player_game_key = pb.player_game_key
    LEFT JOIN situational_amplifiers sa ON bd.player_game_key = sa.player_game_key
    LEFT JOIN matchup_amplifiers ma ON bd.game_id = ma.team_game_key
    LEFT JOIN historical_bounds hb ON bd.player_id = hb.player_id 
        AND bd.opponent_id = hb.opponent_id 
        AND bd.game_id = hb.game_id
    LEFT JOIN context_interactions ci ON bd.player_game_key = ci.player_game_key
),

-- STEP 2: Calculate individual amplification factors
amplification_factors AS (
    SELECT
        *,
        
        -- =================================================================
        -- CONTEXT AMPLIFICATION ANALYSIS
        -- =================================================================
        
        -- Rest amplification factor
        ROUND(
            CASE 
                WHEN rest_advantage_score >= 85 THEN 1.08
                WHEN rest_advantage_score >= 70 THEN 1.04
                WHEN rest_advantage_score <= 30 THEN 0.94
                WHEN rest_advantage_score <= 20 THEN 0.88
                ELSE 1.0
            END,
            3
        ) AS rest_amplification_factor,
        
        -- Matchup amplification factor
        ROUND(
            CASE 
                WHEN COALESCE(position_overall_matchup_quality, 50) >= 75 THEN 1.12
                WHEN COALESCE(position_overall_matchup_quality, 50) >= 65 THEN 1.06
                WHEN COALESCE(position_overall_matchup_quality, 50) <= 35 THEN 0.92
                WHEN COALESCE(position_overall_matchup_quality, 50) <= 25 THEN 0.85
                ELSE 1.0
            END *
            CASE 
                WHEN is_exploitable_matchup = TRUE THEN 1.08
                WHEN is_avoid_matchup = TRUE THEN 0.90
                ELSE 1.0
            END,
            3
        ) AS matchup_amplification_factor,
        
        -- Opportunity amplification factor
        ROUND(
            CASE 
                WHEN opportunity_context = 'HIGH_OPPORTUNITY' THEN 1.15
                WHEN opportunity_context = 'MODERATE_OPPORTUNITY' THEN 1.08
                ELSE 1.0
            END *
            CASE 
                WHEN roster_health_status = 'DEPLETED_ROSTER' THEN 1.10
                WHEN roster_health_status = 'THIN_ROSTER' THEN 1.05
                ELSE 1.0
            END,
            3
        ) AS opportunity_amplification_factor,
        
        -- Form amplification factor
        ROUND(
            CASE 
                WHEN COALESCE(short_term_trend, 0) > 4 THEN 1.10
                WHEN COALESCE(short_term_trend, 0) > 2 THEN 1.05
                WHEN COALESCE(short_term_trend, 0) < -4 THEN 0.92
                WHEN COALESCE(short_term_trend, 0) < -2 THEN 0.96
                ELSE 1.0
            END,
            3
        ) AS form_amplification_factor
        
    FROM baseline_assessment
),

-- STEP 3: Calculate master amplification and final bounds
context_amplified_bounds AS (
    SELECT
        *,
        
        -- =================================================================
        -- MASTER AMPLIFICATION CALCULATION
        -- =================================================================
        
        -- Combined amplification multiplier
        ROUND(
            rest_amplification_factor *
            matchup_amplification_factor * 
            opportunity_amplification_factor *
            form_amplification_factor *
            COALESCE(master_context_interaction_multiplier, 1.0),
            3
        ) AS master_amplification_multiplier
        
    FROM amplification_factors
),

-- STEP 4: Calculate performance bounds with all amplification factors available
performance_bounds AS (
    SELECT
        player_game_key,
        player_id,
        game_id,
        game_date,
        season_year,
        opponent_id,
        position,
        home_away,
        
        -- Baseline metrics
        performance_baseline,
        baseline_confidence_tier,
        
        -- Amplification factors
        rest_amplification_factor,
        matchup_amplification_factor,
        opportunity_amplification_factor,
        form_amplification_factor,
        master_amplification_multiplier,
        
        -- Amplification category
        CASE 
            WHEN master_amplification_multiplier >= 1.25 THEN 'EXTREME_POSITIVE_AMPLIFICATION'
            WHEN master_amplification_multiplier >= 1.15 THEN 'STRONG_POSITIVE_AMPLIFICATION'
            WHEN master_amplification_multiplier >= 1.08 THEN 'MODERATE_POSITIVE_AMPLIFICATION'
            WHEN master_amplification_multiplier >= 1.02 THEN 'SLIGHT_POSITIVE_AMPLIFICATION'
            WHEN master_amplification_multiplier <= 0.80 THEN 'EXTREME_NEGATIVE_AMPLIFICATION'
            WHEN master_amplification_multiplier <= 0.88 THEN 'STRONG_NEGATIVE_AMPLIFICATION'
            WHEN master_amplification_multiplier <= 0.94 THEN 'MODERATE_NEGATIVE_AMPLIFICATION'
            WHEN master_amplification_multiplier <= 0.98 THEN 'SLIGHT_NEGATIVE_AMPLIFICATION'
            ELSE 'NEUTRAL_AMPLIFICATION'
        END AS amplification_category,
        
        -- =================================================================
        -- CONTEXT-AMPLIFIED PERFORMANCE BOUNDS
        -- =================================================================
        
        -- Amplified baseline projection
        ROUND(
            performance_baseline * master_amplification_multiplier,
            2
        ) AS context_amplified_baseline,
        
        -- Context-amplified ceiling
        ROUND(
            GREATEST(
                -- Method 1: Baseline + volatility-based ceiling boost
                performance_baseline + (COALESCE(pts_volatility_vs_opponent, 6) * 1.3),
                -- Method 2: Historical best game adjusted for context
                COALESCE(best_scoring_game_vs_opponent, performance_baseline * 1.6),
                -- Method 3: Usage-opportunity adjusted ceiling
                performance_baseline * (1.0 + COALESCE(team_pts_share_l5, 20) / 100.0)
            ) * master_amplification_multiplier *
            -- Additional ceiling boosters
            CASE 
                WHEN has_amplified_opportunity = TRUE THEN 1.12
                WHEN opportunity_context = 'HIGH_OPPORTUNITY' AND player_offensive_role != 'PRIMARY_OPTION' THEN 1.08
                ELSE 1.0
            END *
            -- Pressure performance ceiling adjustment
            CASE 
                WHEN pressure_performance_tier = 'PRESSURE_ELITE' THEN 1.05
                WHEN pressure_performance_tier = 'PRESSURE_VULNERABLE' THEN 0.96
                ELSE 1.0
            END,
            2
        ) AS context_amplified_ceiling,
        
        -- Context-amplified floor
        ROUND(
            GREATEST(0,
                LEAST(
                    -- Method 1: Baseline - volatility-based floor reduction
                    performance_baseline - (COALESCE(pts_volatility_vs_opponent, 6) * 1.3),
                    -- Method 2: Historical worst game adjusted for context
                    COALESCE(worst_scoring_game_vs_opponent, performance_baseline * 0.4),
                    -- Method 3: Conservative efficiency-based floor
                    performance_baseline * 0.6
                ) * master_amplification_multiplier *
                -- Floor risk adjustments
                CASE 
                    WHEN fatigue_risk_assessment = 'HIGH_FATIGUE_RISK' THEN 0.90
                    WHEN is_high_confidence_difficult = TRUE THEN 0.88
                    WHEN has_contradictory_context_signals = TRUE THEN 0.92
                    ELSE 1.0
                END *
                -- Pressure performance floor adjustment
                CASE 
                    WHEN pressure_performance_tier = 'PRESSURE_ELITE' THEN 1.08
                    WHEN pressure_performance_tier = 'PRESSURE_VULNERABLE' THEN 0.88
                    ELSE 1.0
                END
            ),
            2
        ) AS context_amplified_floor,
        
        -- Pass through other needed columns
        has_amplified_opportunity,
        opportunity_context,
        player_offensive_role,
        pressure_performance_tier,
        context_signal_clarity,
        is_high_confidence_favorable,
        is_high_confidence_difficult,
        has_contradictory_context_signals,
        pts_volatility_vs_opponent,
        best_scoring_game_vs_opponent,
        worst_scoring_game_vs_opponent,
        team_pts_share_l5
        
    FROM context_amplified_bounds
),

-- STEP 5: Calculate scenario projections using the bounds
scenario_projections AS (
    SELECT
        *,
        
        -- Performance range (ceiling - floor)
        context_amplified_ceiling - context_amplified_floor AS amplified_performance_range,
        
        -- =================================================================
        -- SCENARIO-SPECIFIC PROJECTIONS
        -- =================================================================
        
        -- Best-case scenario (90th percentile)
        ROUND(
            context_amplified_ceiling * 
            CASE 
                WHEN context_signal_clarity = 'HIGH_SIGNAL_ALIGNMENT' AND master_amplification_multiplier >= 1.15 THEN 1.05
                ELSE 1.0
            END,
            2
        ) AS best_case_scenario_projection,
        
        -- Worst-case scenario (10th percentile) 
        ROUND(
            context_amplified_floor * 
            CASE 
                WHEN context_signal_clarity = 'HIGH_SIGNAL_ALIGNMENT' AND master_amplification_multiplier <= 0.90 THEN 0.95
                ELSE 1.0
            END,
            2
        ) AS worst_case_scenario_projection,
        
        -- Most likely scenario (median expectation)
        ROUND(
            (context_amplified_baseline * 0.5) + 
            (context_amplified_ceiling * 0.3) + 
            (context_amplified_floor * 0.2),
            2
        ) AS most_likely_scenario_projection,
        
        -- =================================================================
        -- CONFIDENCE AND RELIABILITY METRICS
        -- =================================================================
        
        -- Projection confidence score
        LEAST(100, GREATEST(0,
            40 * 0.4 + -- Use 40 as default if no historical confidence
            CASE 
                WHEN context_signal_clarity = 'HIGH_SIGNAL_ALIGNMENT' THEN 30
                WHEN context_signal_clarity = 'MIXED_SIGNAL_CONTEXT' THEN 10
                ELSE 20
            END +
            15 + -- Default consistency score
            CASE 
                WHEN ABS(master_amplification_multiplier - 1.0) <= 0.05 THEN 10 -- Neutral context
                WHEN ABS(master_amplification_multiplier - 1.0) >= 0.20 THEN 5  -- Extreme context
                ELSE 8
            END
        )) AS projection_confidence_score
        
    FROM performance_bounds
),

-- STEP 6: Final calculations with all scenario projections available
final_calculations AS (
    SELECT
        *,
        
        -- Projection reliability tier
        CASE 
            WHEN projection_confidence_score >= 80 THEN 'VERY_HIGH_RELIABILITY'
            WHEN projection_confidence_score >= 65 THEN 'HIGH_RELIABILITY'
            WHEN projection_confidence_score >= 50 THEN 'MODERATE_RELIABILITY'
            WHEN projection_confidence_score >= 35 THEN 'LOW_RELIABILITY'
            ELSE 'MINIMAL_RELIABILITY'
        END AS projection_reliability_tier
        
    FROM scenario_projections
)

SELECT
    player_game_key,
    player_id,
    game_id,
    game_date,
    season_year,
    opponent_id,
    position,
    home_away,
    
    -- =================================================================
    -- BASELINE PERFORMANCE ASSESSMENT
    -- =================================================================
    performance_baseline,
    baseline_confidence_tier,
    
    -- =================================================================
    -- CONTEXT AMPLIFICATION ANALYSIS
    -- =================================================================
    rest_amplification_factor,
    matchup_amplification_factor,
    opportunity_amplification_factor,
    form_amplification_factor,
    master_amplification_multiplier,
    amplification_category,
    
    -- =================================================================
    -- CONTEXT-AMPLIFIED PERFORMANCE BOUNDS
    -- =================================================================
    context_amplified_baseline,
    context_amplified_ceiling,
    context_amplified_floor,
    amplified_performance_range,
    
    -- =================================================================
    -- SCENARIO-SPECIFIC PROJECTIONS
    -- =================================================================
    best_case_scenario_projection,
    worst_case_scenario_projection,
    most_likely_scenario_projection,
    
    -- =================================================================
    -- CONFIDENCE AND RELIABILITY METRICS
    -- =================================================================
    projection_confidence_score,
    projection_reliability_tier,
    
    -- =================================================================
    -- PROP BETTING ASSESSMENTS
    -- =================================================================
    
    -- Over/Under common point totals
    CASE 
        WHEN context_amplified_floor >= 20.5 THEN 'STRONG_OVER_20_5'
        WHEN most_likely_scenario_projection >= 22 AND context_amplified_floor >= 16 THEN 'LEAN_OVER_20_5'
        WHEN context_amplified_ceiling <= 18 THEN 'STRONG_UNDER_20_5'
        WHEN most_likely_scenario_projection <= 19 AND context_amplified_ceiling <= 23 THEN 'LEAN_UNDER_20_5'
        ELSE 'NEUTRAL_20_5'
    END AS over_under_20_5_assessment,
    
    CASE 
        WHEN context_amplified_floor >= 15.5 THEN 'STRONG_OVER_15_5'
        WHEN most_likely_scenario_projection >= 17 AND context_amplified_floor >= 12 THEN 'LEAN_OVER_15_5'
        WHEN context_amplified_ceiling <= 13 THEN 'STRONG_UNDER_15_5'
        WHEN most_likely_scenario_projection <= 14 AND context_amplified_ceiling <= 18 THEN 'LEAN_UNDER_15_5'
        ELSE 'NEUTRAL_15_5'
    END AS over_under_15_5_assessment,
    
    -- =================================================================
    -- DFS AND TOURNAMENT PLAY ASSESSMENTS
    -- =================================================================
    
    -- Ceiling probability for tournaments
    CASE 
        WHEN best_case_scenario_projection >= 35 AND projection_confidence_score >= 60 THEN 'ELITE_CEILING_PLAY'
        WHEN best_case_scenario_projection >= 30 AND amplified_performance_range >= 15 THEN 'STRONG_CEILING_PLAY'
        WHEN context_amplified_ceiling >= 25 AND master_amplification_multiplier >= 1.12 THEN 'MODERATE_CEILING_PLAY'
        ELSE 'LOW_CEILING_PLAY'
    END AS tournament_ceiling_assessment,
    
    -- Cash game safety for DFS
    CASE 
        WHEN context_amplified_floor >= 18 AND amplified_performance_range <= 12 THEN 'PREMIUM_CASH_PLAY'
        WHEN context_amplified_floor >= 15 AND most_likely_scenario_projection >= 20 THEN 'SOLID_CASH_PLAY'
        WHEN context_amplified_floor >= 12 AND projection_confidence_score >= 70 THEN 'REASONABLE_CASH_PLAY'
        ELSE 'AVOID_CASH_GAMES'
    END AS cash_game_safety_assessment,
    
    -- =================================================================
    -- CONTEXTUAL INSIGHTS
    -- =================================================================
    
    -- Primary amplification driver
    CASE 
        WHEN opportunity_amplification_factor = GREATEST(rest_amplification_factor, matchup_amplification_factor, opportunity_amplification_factor, form_amplification_factor) 
            THEN 'OPPORTUNITY_DRIVEN'
        WHEN matchup_amplification_factor = GREATEST(rest_amplification_factor, matchup_amplification_factor, opportunity_amplification_factor, form_amplification_factor) 
            THEN 'MATCHUP_DRIVEN'
        WHEN rest_amplification_factor = GREATEST(rest_amplification_factor, matchup_amplification_factor, opportunity_amplification_factor, form_amplification_factor) 
            THEN 'REST_DRIVEN'
        WHEN form_amplification_factor = GREATEST(rest_amplification_factor, matchup_amplification_factor, opportunity_amplification_factor, form_amplification_factor) 
            THEN 'FORM_DRIVEN'
        ELSE 'BALANCED_AMPLIFICATION'
    END AS primary_amplification_driver,
    
    -- Risk-reward classification
    CASE 
        WHEN amplified_performance_range >= 18 AND best_case_scenario_projection >= 30 THEN 'HIGH_RISK_HIGH_REWARD'
        WHEN amplified_performance_range <= 10 AND context_amplified_floor >= 15 THEN 'LOW_RISK_MODERATE_REWARD'
        WHEN amplified_performance_range >= 15 AND context_amplified_floor <= 10 THEN 'HIGH_RISK_MODERATE_REWARD'
        WHEN amplified_performance_range <= 12 AND best_case_scenario_projection <= 25 THEN 'LOW_RISK_LOW_REWARD'
        ELSE 'BALANCED_RISK_REWARD'
    END AS risk_reward_classification,
    
    -- =================================================================
    -- TIMESTAMPS
    -- =================================================================
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM final_calculations