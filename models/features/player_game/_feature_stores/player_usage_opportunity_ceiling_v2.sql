{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'player', 'usage', 'opportunity', 'ceiling', 'role_expansion'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['season_year']}
    ]
) }}

/*
USAGE-OPPORTUNITY CEILING ANALYSIS v2
Models how usage expansion opportunities translate to performance ceiling increases.
Critical for identifying "ceiling game" candidates when roles expand due to circumstances.

Core Value: Captures performance elasticity when players get expanded roles due to
injuries, rest games, favorable matchups, or team dynamics changes.

Grain: One row per player + game_date
Integration: Combines usage patterns, opportunity context, and role elasticity modeling
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

-- Current scoring and usage context
player_scoring_context AS (
    SELECT
        player_game_key,
        -- Current role and usage
        player_offensive_role,
        team_pts_share_l3,
        team_pts_share_l5,
        team_pts_share_l10,
        team_fga_share_l5,
        -- Performance metrics
        pts_avg_l3,
        pts_avg_l5,
        pts_avg_l10,
        scoring_efficiency_composite_l5,
        usage_weighted_ts_l5,
        -- Context multipliers
        usage_opportunity_multiplier,
        total_context_multiplier,
        playstyle_compatibility_score,
        -- Pressure handling
        pressure_performance_tier,
        -- Current form vs baseline
        pts_avg_l3 - pts_avg_l10 AS recent_scoring_trend
    FROM {{ ref('player_scoring_features_v2') }}
),

-- Situational opportunity context
situational_opportunity AS (
    SELECT
        player_game_key,
        -- Opportunity indicators
        opportunity_context,
        roster_health_status,
        workload_classification,
        -- Rest and fatigue context
        rest_advantage_score,
        fatigue_risk_assessment,
        situational_favorability_score,
        -- Team context
        rotation_players_available,
        core_players_available
    FROM {{ ref('game_situational_context_v2') }}
),

-- Historical usage expansion performance
historical_usage_patterns AS (
    SELECT
        player_matchup_key,
        player_id,
        opponent_id,
        game_id,
        game_date,
        season_year,
        -- Historical performance when given opportunities
        avg_pts_vs_opponent_prior,
        avg_pra_vs_opponent_prior,
        best_scoring_game_vs_opponent,
        historical_confidence_score,
        -- Volatility (higher volatility = higher upside potential)
        pts_volatility_vs_opponent,
        -- Recent form vs historical
        recent_3g_pts_vs_opponent,
        recency_weighted_pts_vs_opponent
    FROM {{ ref('player_historical_matchup_features_v2') }}
),

-- Context interaction amplifiers
context_amplifiers AS (
    SELECT
        player_game_key,
        -- Master interaction multiplier
        master_context_interaction_multiplier,
        -- Specific opportunity amplifiers
        has_amplified_opportunity,
        workload_sustainability_category,
        -- Signal clarity (affects ceiling confidence)
        context_signal_clarity,
        is_high_confidence_favorable
    FROM {{ ref('context_interaction_features_v2') }}
),

-- STEP 1: Calculate usage expansion basics
usage_expansion_basics AS (
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
        -- USAGE EXPANSION OPPORTUNITY ASSESSMENT
        -- =================================================================
        
        -- Current vs Potential Usage
        COALESCE(psc.team_pts_share_l5, 15) AS current_usage_rate,
        
        -- Usage expansion potential based on opportunity context
        CASE 
            WHEN so.opportunity_context = 'HIGH_OPPORTUNITY' 
                AND so.rotation_players_available <= 7 THEN 1.4
            WHEN so.opportunity_context = 'HIGH_OPPORTUNITY' THEN 1.25
            WHEN so.opportunity_context = 'MODERATE_OPPORTUNITY' THEN 1.15
            ELSE 1.0
        END AS usage_expansion_multiplier,
        
        -- Pass through needed columns
        psc.player_offensive_role,
        psc.scoring_efficiency_composite_l5,
        psc.pressure_performance_tier,
        psc.playstyle_compatibility_score,
        psc.pts_avg_l5,
        psc.recent_scoring_trend,
        psc.usage_weighted_ts_l5,
        so.opportunity_context,
        so.roster_health_status,
        so.rotation_players_available,
        so.rest_advantage_score,
        so.fatigue_risk_assessment,
        hup.avg_pts_vs_opponent_prior,
        hup.best_scoring_game_vs_opponent,
        ca.master_context_interaction_multiplier,
        ca.has_amplified_opportunity,
        ca.is_high_confidence_favorable,
        ca.context_signal_clarity

    FROM base_data bd
    LEFT JOIN player_scoring_context psc ON bd.player_game_key = psc.player_game_key
    LEFT JOIN situational_opportunity so ON bd.player_game_key = so.player_game_key
    LEFT JOIN historical_usage_patterns hup ON bd.player_id = hup.player_id
        AND bd.opponent_id = hup.opponent_id
        AND bd.game_id = hup.game_id
    LEFT JOIN context_amplifiers ca ON bd.player_game_key = ca.player_game_key
),

-- STEP 2: Calculate usage projections and elasticity
usage_projections AS (
    SELECT
        *,
        
        -- Expanded usage projection
        ROUND(
            current_usage_rate * usage_expansion_multiplier,
            2
        ) AS projected_expanded_usage_rate,
        
        -- Usage gap (how much additional usage is available)
        ROUND(
            (current_usage_rate * usage_expansion_multiplier) - current_usage_rate,
            2
        ) AS usage_expansion_gap,
        
        -- =================================================================
        -- PERFORMANCE ELASTICITY MODELING
        -- =================================================================
        
        -- Player's historical response to increased usage opportunities
        CASE 
            WHEN player_offensive_role = 'PRIMARY_OPTION' THEN 0.85  -- Already high usage
            WHEN player_offensive_role = 'SECONDARY_OPTION' THEN 1.2  -- Good expansion potential
            WHEN player_offensive_role = 'SUPPORTING_ROLE' THEN 1.4   -- High expansion potential
            ELSE 1.0
        END AS role_elasticity_factor,
        
        -- Efficiency sustainability under expanded usage
        CASE 
            WHEN COALESCE(scoring_efficiency_composite_l5, 50) >= 65 
                AND pressure_performance_tier IN ('PRESSURE_ELITE', 'PRESSURE_RESILIENT') 
                THEN 'SUSTAINABLE_EXPANSION'
            WHEN COALESCE(scoring_efficiency_composite_l5, 50) >= 60 
                AND pressure_performance_tier NOT IN ('PRESSURE_VULNERABLE') 
                THEN 'MODERATE_EXPANSION_RISK'
            WHEN COALESCE(scoring_efficiency_composite_l5, 50) <= 50 
                OR pressure_performance_tier IN ('PRESSURE_SENSITIVE', 'PRESSURE_VULNERABLE') 
                THEN 'HIGH_EXPANSION_RISK'
            ELSE 'NEUTRAL_EXPANSION_PROFILE'
        END AS usage_expansion_sustainability
        
    FROM usage_expansion_basics
),

-- STEP 3: Calculate performance elasticity and ceiling projections
ceiling_projections AS (
    SELECT
        *,
        
        -- Performance elasticity score (how well player scales with opportunity)
        ROUND(
            role_elasticity_factor *
            CASE 
                WHEN usage_expansion_sustainability = 'SUSTAINABLE_EXPANSION' THEN 1.2
                WHEN usage_expansion_sustainability = 'MODERATE_EXPANSION_RISK' THEN 1.0
                WHEN usage_expansion_sustainability = 'HIGH_EXPANSION_RISK' THEN 0.8
                ELSE 1.0
            END *
            CASE 
                WHEN COALESCE(playstyle_compatibility_score, 50) >= 70 THEN 1.1
                WHEN COALESCE(playstyle_compatibility_score, 50) <= 40 THEN 0.9
                ELSE 1.0
            END,
            3
        ) AS performance_elasticity_score,
        
        -- Base ceiling (current performance level)
        COALESCE(pts_avg_l5, avg_pts_vs_opponent_prior, 12) AS baseline_scoring_projection
        
    FROM usage_projections
),

-- STEP 4: Calculate amplified ceilings and probability factors
amplified_ceilings AS (
    SELECT
        *,
        
        -- Usage-amplified ceiling
        ROUND(
            baseline_scoring_projection * 
            (1.0 + (usage_expansion_gap / 100.0 * performance_elasticity_score)),
            2
        ) AS usage_amplified_ceiling,
        
        -- Context-amplified ceiling (adds situational boost)
        ROUND(
            (baseline_scoring_projection * 
            (1.0 + (usage_expansion_gap / 100.0 * performance_elasticity_score))) * 
            COALESCE(master_context_interaction_multiplier, 1.0) *
            CASE 
                WHEN rest_advantage_score >= 75 THEN 1.05
                WHEN fatigue_risk_assessment = 'HIGH_FATIGUE_RISK' THEN 0.95
                ELSE 1.0
            END,
            2
        ) AS opportunity_context_ceiling,
        
        -- Factors supporting ceiling game
        (CASE WHEN opportunity_context = 'HIGH_OPPORTUNITY' THEN 1 ELSE 0 END +
         CASE WHEN usage_expansion_gap >= 5 THEN 1 ELSE 0 END +
         CASE WHEN performance_elasticity_score >= 1.2 THEN 1 ELSE 0 END +
         CASE WHEN rest_advantage_score >= 75 THEN 1 ELSE 0 END +
         CASE WHEN is_high_confidence_favorable = TRUE THEN 1 ELSE 0 END +
         CASE WHEN recent_scoring_trend > 2 THEN 1 ELSE 0 END
        ) AS ceiling_game_supporting_factors
        
    FROM ceiling_projections
),

-- STEP 5: Calculate final ceiling metrics and probability scores
final_ceiling_analysis AS (
    SELECT
        *,
        
        -- Maximum theoretical ceiling (best-case scenario)
        ROUND(
            GREATEST(
                opportunity_context_ceiling,
                COALESCE(best_scoring_game_vs_opponent, baseline_scoring_projection * 1.5)
            ) * 
            CASE 
                WHEN has_amplified_opportunity = TRUE THEN 1.1
                ELSE 1.0
            END,
            2
        ) AS theoretical_max_ceiling,
        
        -- Ceiling game probability tier
        CASE 
            WHEN ceiling_game_supporting_factors >= 4 THEN 'VERY_HIGH_CEILING_PROBABILITY'
            WHEN ceiling_game_supporting_factors >= 3 THEN 'HIGH_CEILING_PROBABILITY'
            WHEN ceiling_game_supporting_factors >= 2 THEN 'MODERATE_CEILING_PROBABILITY'
            WHEN ceiling_game_supporting_factors >= 1 THEN 'LOW_CEILING_PROBABILITY'
            ELSE 'MINIMAL_CEILING_PROBABILITY'
        END AS ceiling_game_probability_tier,
        
        -- Numerical ceiling probability (0-100)
        LEAST(100, GREATEST(0,
            (ceiling_game_supporting_factors * 15) +
            (usage_expansion_gap * 2) +
            (performance_elasticity_score * 10) +
            CASE 
                WHEN context_signal_clarity = 'HIGH_SIGNAL_ALIGNMENT' THEN 10
                WHEN context_signal_clarity = 'MIXED_SIGNAL_CONTEXT' THEN -5
                ELSE 0
            END
        )) AS ceiling_game_probability_score,
        
        -- Role expansion scenarios
        CASE 
            WHEN player_offensive_role = 'SUPPORTING_ROLE' 
                AND opportunity_context = 'HIGH_OPPORTUNITY'
                AND performance_elasticity_score >= 1.3
                THEN 'TEMPORARY_FEATURED_ROLE'
            WHEN player_offensive_role = 'SECONDARY_OPTION'
                AND opportunity_context = 'HIGH_OPPORTUNITY'
                AND usage_expansion_gap >= 8
                THEN 'TEMPORARY_PRIMARY_OPTION'
            WHEN usage_expansion_gap >= 5
                AND performance_elasticity_score >= 1.2
                THEN 'EXPANDED_CURRENT_ROLE'
            ELSE 'MAINTAIN_CURRENT_ROLE'
        END AS projected_role_expansion,
        
        -- Role expansion confidence
        CASE 
            WHEN roster_health_status IN ('THIN_ROSTER', 'DEPLETED_ROSTER')
                AND usage_expansion_sustainability = 'SUSTAINABLE_EXPANSION'
                THEN 'HIGH_CONFIDENCE'
            WHEN opportunity_context = 'HIGH_OPPORTUNITY'
                AND usage_expansion_sustainability != 'HIGH_EXPANSION_RISK'
                THEN 'MODERATE_CONFIDENCE'
            ELSE 'LOW_CONFIDENCE'
        END AS role_expansion_confidence,
        
        -- Usage efficiency projections
        ROUND(
            baseline_scoring_projection / NULLIF(current_usage_rate, 0) *
            performance_elasticity_score,
            3
        ) AS projected_points_per_usage_point,
        
        -- Usage efficiency sustainability score
        ROUND(
            COALESCE(usage_weighted_ts_l5, 0.5) * 
            performance_elasticity_score *
            CASE 
                WHEN usage_expansion_gap <= 3 THEN 1.0
                WHEN usage_expansion_gap <= 6 THEN 0.95
                WHEN usage_expansion_gap <= 10 THEN 0.90
                ELSE 0.85
            END,
            3
        ) AS usage_efficiency_under_expansion,
        
        -- Opportunity timing assessment
        CASE 
            WHEN fatigue_risk_assessment = 'LOW_FATIGUE_RISK' 
                AND rest_advantage_score >= 70
                THEN 'OPTIMAL_EXPANSION_TIMING'
            WHEN fatigue_risk_assessment = 'MODERATE_FATIGUE_RISK'
                AND opportunity_context = 'HIGH_OPPORTUNITY'
                THEN 'ACCEPTABLE_EXPANSION_TIMING'
            WHEN fatigue_risk_assessment = 'HIGH_FATIGUE_RISK'
                THEN 'POOR_EXPANSION_TIMING'
            ELSE 'NEUTRAL_EXPANSION_TIMING'
        END AS opportunity_timing_assessment
        
    FROM amplified_ceilings
),

-- STEP 6: Calculate final composite scores and classifications
usage_opportunity_analysis AS (
    SELECT
        *,
        
        -- Overall ceiling game rating (0-100)
        LEAST(100, GREATEST(0,
            (ceiling_game_probability_score * 0.4) +
            (usage_expansion_gap * 3) +
            (performance_elasticity_score * 15) +
            CASE 
                WHEN projected_role_expansion IN ('TEMPORARY_FEATURED_ROLE', 'TEMPORARY_PRIMARY_OPTION') THEN 20
                WHEN projected_role_expansion = 'EXPANDED_CURRENT_ROLE' THEN 10
                ELSE 0
            END
        )) AS overall_ceiling_game_rating,
        
        -- Ceiling game classification for easy filtering
        CASE 
            WHEN LEAST(100, GREATEST(0,
                (ceiling_game_probability_score * 0.4) +
                (usage_expansion_gap * 3) +
                (performance_elasticity_score * 15) +
                CASE 
                    WHEN projected_role_expansion IN ('TEMPORARY_FEATURED_ROLE', 'TEMPORARY_PRIMARY_OPTION') THEN 20
                    WHEN projected_role_expansion = 'EXPANDED_CURRENT_ROLE' THEN 10
                    ELSE 0
                END
            )) >= 80 THEN 'PREMIUM_CEILING_CANDIDATE'
            WHEN LEAST(100, GREATEST(0,
                (ceiling_game_probability_score * 0.4) +
                (usage_expansion_gap * 3) +
                (performance_elasticity_score * 15) +
                CASE 
                    WHEN projected_role_expansion IN ('TEMPORARY_FEATURED_ROLE', 'TEMPORARY_PRIMARY_OPTION') THEN 20
                    WHEN projected_role_expansion = 'EXPANDED_CURRENT_ROLE' THEN 10
                    ELSE 0
                END
            )) >= 65 THEN 'STRONG_CEILING_CANDIDATE'
            WHEN LEAST(100, GREATEST(0,
                (ceiling_game_probability_score * 0.4) +
                (usage_expansion_gap * 3) +
                (performance_elasticity_score * 15) +
                CASE 
                    WHEN projected_role_expansion IN ('TEMPORARY_FEATURED_ROLE', 'TEMPORARY_PRIMARY_OPTION') THEN 20
                    WHEN projected_role_expansion = 'EXPANDED_CURRENT_ROLE' THEN 10
                    ELSE 0
                END
            )) >= 50 THEN 'MODERATE_CEILING_CANDIDATE'
            WHEN LEAST(100, GREATEST(0,
                (ceiling_game_probability_score * 0.4) +
                (usage_expansion_gap * 3) +
                (performance_elasticity_score * 15) +
                CASE 
                    WHEN projected_role_expansion IN ('TEMPORARY_FEATURED_ROLE', 'TEMPORARY_PRIMARY_OPTION') THEN 20
                    WHEN projected_role_expansion = 'EXPANDED_CURRENT_ROLE' THEN 10
                    ELSE 0
                END
            )) >= 35 THEN 'SPECULATIVE_CEILING_PLAY'
            ELSE 'LOW_CEILING_PROBABILITY'
        END AS ceiling_candidate_classification,
        
        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM final_ceiling_analysis
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
    -- USAGE EXPANSION OPPORTUNITY ASSESSMENT
    -- =================================================================
    current_usage_rate,
    usage_expansion_multiplier,
    projected_expanded_usage_rate,
    usage_expansion_gap,
    
    -- =================================================================
    -- PERFORMANCE ELASTICITY MODELING
    -- =================================================================
    role_elasticity_factor,
    usage_expansion_sustainability,
    performance_elasticity_score,
    
    -- =================================================================
    -- OPPORTUNITY-AMPLIFIED CEILING PROJECTIONS
    -- =================================================================
    baseline_scoring_projection,
    usage_amplified_ceiling,
    opportunity_context_ceiling,
    theoretical_max_ceiling,
    
    -- =================================================================
    -- CEILING GAME PROBABILITY ASSESSMENT
    -- =================================================================
    ceiling_game_supporting_factors,
    ceiling_game_probability_tier,
    ceiling_game_probability_score,
    
    -- =================================================================
    -- ROLE EXPANSION SCENARIOS
    -- =================================================================
    projected_role_expansion,
    role_expansion_confidence,
    
    -- =================================================================
    -- USAGE EFFICIENCY PROJECTIONS
    -- =================================================================
    projected_points_per_usage_point,
    usage_efficiency_under_expansion,
    
    -- =================================================================
    -- OPPORTUNITY TIMING FACTORS
    -- =================================================================
    opportunity_timing_assessment,
    
    -- =================================================================
    -- COMPOSITE CEILING INDICATORS
    -- =================================================================
    overall_ceiling_game_rating,
    ceiling_candidate_classification,
    
    -- =================================================================
    -- TIMESTAMPS
    -- =================================================================
    created_at,
    updated_at

FROM usage_opportunity_analysis