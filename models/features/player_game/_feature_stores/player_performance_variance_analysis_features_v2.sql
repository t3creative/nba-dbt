{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'player', 'variance', 'ceiling_floor', 'immediate_ready'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['season_year']}
    ]
) }}

/*
PERFORMANCE VARIANCE & CEILING/FLOOR ANALYSIS v2
Uses existing data sources to model performance distributions and predict
ceiling/floor scenarios for prop betting and DFS optimization.

IMMEDIATE IMPLEMENTATION READY - Uses only existing feature store data
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

-- Gather all existing variance metrics
historical_variance AS (
    SELECT 
        player_matchup_key,
        player_id,
        opponent_id,
        game_id,
        game_date,
        season_year,
        -- Existing volatility metrics
        pts_volatility_vs_opponent,
        pra_volatility_vs_opponent,
        pts_range_vs_opponent,
        pra_range_vs_opponent,
        best_scoring_game_vs_opponent,
        worst_scoring_game_vs_opponent,
        -- Historical averages for baseline
        avg_pts_vs_opponent_prior,
        avg_pra_vs_opponent_prior,
        -- Confidence metrics
        historical_confidence_score,
        prior_games_vs_opponent
    FROM {{ ref('player_historical_matchup_features_v2') }}
),

-- Current form metrics  
current_performance AS (
    SELECT
        player_game_key,
        -- Recent averages
        pts_avg_l3, pts_avg_l5, pts_avg_l10,
        -- Efficiency context
        scoring_efficiency_composite_l3,
        scoring_efficiency_composite_l5,
        usage_weighted_ts_l5,
        -- Role context
        team_pts_share_l5,
        player_offensive_role,
        -- Pressure performance
        pressure_performance_tier,
        -- Context multipliers
        total_context_multiplier
    FROM {{ ref('player_scoring_features_v2') }}
),

-- Contextual amplifiers
context_factors AS (
    SELECT
        player_game_key,
        -- Situational context
        situational_favorability_score,
        rest_advantage_score,
        fatigue_risk_assessment,
        opportunity_context,
        -- Opponent context  
        roster_health_status
    FROM {{ ref('game_situational_context_v2') }}
),

-- Opponent defensive consistency
opponent_variance AS (
    SELECT
        opponent_id, position, game_date,
        -- Opponent defensive consistency (affects player variance)  
        position_defense_consistency_score,
        position_overall_matchup_quality,
        is_exploitable_matchup,
        is_avoid_matchup
    FROM {{ ref('opponent_position_defense_features_v2') }}
),

-- STEP 1: Calculate base variance metrics
base_variance_metrics AS (
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
        -- BASELINE PERFORMANCE VARIANCE ANALYSIS
        -- =================================================================
        
        -- Historical variance metrics (when available)
        COALESCE(hv.pts_volatility_vs_opponent, 5.0) AS baseline_pts_volatility,
        COALESCE(hv.pra_volatility_vs_opponent, 8.0) AS baseline_pra_volatility,
        COALESCE(hv.pts_range_vs_opponent, 20) AS historical_pts_range,
        
        -- Confidence-weighted variance (less history = higher uncertainty)
        ROUND(
            COALESCE(hv.pts_volatility_vs_opponent, 5.0) * 
            CASE 
                WHEN COALESCE(hv.prior_games_vs_opponent, 0) >= 10 THEN 1.0
                WHEN COALESCE(hv.prior_games_vs_opponent, 0) >= 5 THEN 1.2  
                WHEN COALESCE(hv.prior_games_vs_opponent, 0) >= 2 THEN 1.5
                ELSE 2.0  -- High uncertainty for new matchups
            END,
            2
        ) AS confidence_adjusted_volatility,
        
        -- =================================================================
        -- CONTEXT-AMPLIFIED VARIANCE MODELING
        -- =================================================================
        
        -- Opponent defensive consistency impact (using correct column name)
        CASE 
            WHEN COALESCE(ov.position_defense_consistency_score, 50) >= 70 THEN 'PREDICTABLE_MATCHUP'
            WHEN COALESCE(ov.position_defense_consistency_score, 50) >= 50 THEN 'MODERATE_VARIANCE'  
            WHEN COALESCE(ov.position_defense_consistency_score, 50) >= 30 THEN 'HIGH_VARIANCE'
            ELSE 'CHAOTIC_MATCHUP'
        END AS opponent_variance_impact,
        
        -- Situational variance amplification
        ROUND(
            COALESCE(hv.pts_volatility_vs_opponent, 5.0) *
            CASE 
                -- High rest + favorable matchup = more predictable ceiling
                WHEN COALESCE(cf.situational_favorability_score, 50) >= 80 
                     AND COALESCE(cf.rest_advantage_score, 50) >= 80 THEN 0.8
                -- Fatigue + difficult matchup = higher variance (more unpredictable)
                WHEN COALESCE(cf.situational_favorability_score, 50) <= 40 
                     AND cf.fatigue_risk_assessment = 'HIGH_FATIGUE_RISK' THEN 1.4
                -- High opportunity context = variance amplification  
                WHEN cf.opportunity_context = 'HIGH_OPPORTUNITY' THEN 1.2
                ELSE 1.0
            END,
            2
        ) AS context_adjusted_volatility,
        
        -- Pass through needed columns
        hv.avg_pts_vs_opponent_prior,
        cp.pts_avg_l5,
        cf.situational_favorability_score,
        cf.rest_advantage_score,
        cf.fatigue_risk_assessment,
        cf.opportunity_context,
        ov.position_overall_matchup_quality,
        cp.player_offensive_role,
        cp.team_pts_share_l5,
        cp.pressure_performance_tier,
        cp.pts_avg_l3,
        cp.pts_avg_l10,
        hv.historical_confidence_score,
        hv.prior_games_vs_opponent,
        ov.position_defense_consistency_score

    FROM base_data bd
    LEFT JOIN historical_variance hv ON bd.player_id = hv.player_id
        AND bd.opponent_id = hv.opponent_id
        AND bd.game_id = hv.game_id
    LEFT JOIN current_performance cp ON bd.player_game_key = cp.player_game_key  
    LEFT JOIN context_factors cf ON bd.player_game_key = cf.player_game_key
    LEFT JOIN opponent_variance ov ON bd.opponent_id = ov.opponent_id 
        AND bd.position = ov.position 
        AND bd.game_date = ov.game_date
),

-- STEP 2: Calculate ceiling/floor estimates
ceiling_floor_estimates AS (
    SELECT
        *,
        
        -- =================================================================
        -- CEILING/FLOOR ESTIMATION USING EXISTING DATA
        -- =================================================================
        
        -- Performance ceiling estimation (90th percentile equivalent)
        ROUND(
            COALESCE(pts_avg_l5, avg_pts_vs_opponent_prior, 15) + 
            (confidence_adjusted_volatility * 1.3) +  -- ~90th percentile
            -- Context boost for ceiling
            CASE 
                WHEN COALESCE(situational_favorability_score, 50) >= 75 THEN 3
                WHEN COALESCE(position_overall_matchup_quality, 50) >= 75 THEN 2  
                WHEN opportunity_context = 'HIGH_OPPORTUNITY' THEN 4
                ELSE 0
            END +
            -- Role-based ceiling adjustment
            CASE 
                WHEN player_offensive_role = 'PRIMARY_OPTION' THEN 2
                WHEN player_offensive_role = 'SECONDARY_OPTION' THEN 1
                ELSE 0
            END,
            1
        ) AS estimated_pts_ceiling,
        
        -- Performance floor estimation (10th percentile equivalent)
        ROUND(
            GREATEST(0,
                COALESCE(pts_avg_l5, avg_pts_vs_opponent_prior, 15) - 
                (confidence_adjusted_volatility * 1.3) -  -- ~10th percentile
                -- Context penalty for floor
                CASE 
                    WHEN COALESCE(situational_favorability_score, 50) <= 30 THEN 3
                    WHEN fatigue_risk_assessment = 'HIGH_FATIGUE_RISK' THEN 2
                    WHEN COALESCE(position_overall_matchup_quality, 50) <= 30 THEN 2
                    ELSE 0
                END
            ),
            1
        ) AS estimated_pts_floor
        
    FROM base_variance_metrics
),

-- STEP 3: Calculate usage and pressure adjustments
adjusted_projections AS (
    SELECT
        *,
        
        -- Ceiling-floor range
        estimated_pts_ceiling - estimated_pts_floor AS pts_projection_range,
        
        -- =================================================================
        -- PROBABILITY DISTRIBUTIONS FOR PROP BETTING
        -- =================================================================
        
        -- Usage-adjusted ceiling (higher usage = higher ceiling potential)
        ROUND(
            estimated_pts_ceiling * 
            CASE 
                WHEN COALESCE(team_pts_share_l5, 20) >= 30 THEN 1.1
                WHEN COALESCE(team_pts_share_l5, 20) >= 25 THEN 1.05
                WHEN COALESCE(team_pts_share_l5, 20) <= 15 THEN 0.9
                ELSE 1.0
            END,
            1
        ) AS usage_adjusted_ceiling,
        
        -- Pressure-adjusted floor (how player handles pressure affects floor)
        ROUND(
            estimated_pts_floor *
            CASE 
                WHEN pressure_performance_tier = 'PRESSURE_ELITE' THEN 1.1
                WHEN pressure_performance_tier = 'PRESSURE_RESILIENT' THEN 1.05
                WHEN pressure_performance_tier = 'PRESSURE_SENSITIVE' THEN 0.95
                WHEN pressure_performance_tier = 'PRESSURE_VULNERABLE' THEN 0.85
                ELSE 1.0
            END,
            1
        ) AS pressure_adjusted_floor,
        
        -- =================================================================
        -- CEILING/FLOOR PROBABILITY ASSESSMENTS
        -- =================================================================
        
        -- Ceiling game probability (based on recent form + context)
        CASE 
            WHEN COALESCE(pts_avg_l3, 0) >= (COALESCE(pts_avg_l10, 0) + 5)
                 AND COALESCE(situational_favorability_score, 50) >= 70
                 AND COALESCE(position_overall_matchup_quality, 50) >= 65
                 THEN 'HIGH_CEILING_PROBABILITY'
            WHEN COALESCE(pts_avg_l3, 0) >= COALESCE(pts_avg_l5, 0)
                 AND COALESCE(situational_favorability_score, 50) >= 60
                 THEN 'MODERATE_CEILING_PROBABILITY'
            ELSE 'LOW_CEILING_PROBABILITY'
        END AS ceiling_game_probability_tier,
        
        -- Floor game risk assessment
        CASE 
            WHEN fatigue_risk_assessment = 'HIGH_FATIGUE_RISK'
                 AND COALESCE(situational_favorability_score, 50) <= 40
                 AND COALESCE(position_overall_matchup_quality, 50) <= 40
                 THEN 'HIGH_FLOOR_RISK'
            WHEN COALESCE(pts_avg_l3, 15) < (COALESCE(pts_avg_l10, 15) - 3)
                 AND COALESCE(situational_favorability_score, 50) <= 50
                 THEN 'MODERATE_FLOOR_RISK'  
            ELSE 'LOW_FLOOR_RISK'
        END AS floor_game_risk_tier
        
    FROM ceiling_floor_estimates
),

-- STEP 4: Calculate prop betting features and reliability score
prop_betting_features AS (
    SELECT
        *,
        
        -- =================================================================
        -- PROP BETTING SPECIFIC FEATURES
        -- =================================================================
        
        -- Over/Under 20.5 pts assessment
        CASE 
            WHEN estimated_pts_ceiling >= 25 AND estimated_pts_floor >= 16 THEN 'STRONG_OVER_20_5'
            WHEN estimated_pts_ceiling >= 23 AND estimated_pts_floor >= 12 THEN 'LEAN_OVER_20_5'
            WHEN estimated_pts_ceiling <= 18 AND estimated_pts_floor <= 12 THEN 'STRONG_UNDER_20_5'
            WHEN estimated_pts_ceiling <= 22 AND estimated_pts_floor <= 10 THEN 'LEAN_UNDER_20_5'
            ELSE 'COIN_FLIP_20_5'
        END AS over_under_20_5_assessment,
        
        -- Variance tier for DFS (higher variance = higher upside but riskier)
        CASE 
            WHEN pts_projection_range >= 20 THEN 'HIGH_VARIANCE_DFS'
            WHEN pts_projection_range >= 15 THEN 'MODERATE_VARIANCE_DFS'  
            WHEN pts_projection_range >= 10 THEN 'LOW_VARIANCE_DFS'
            ELSE 'STABLE_PROJECTION_DFS'
        END AS dfs_variance_tier,
        
        -- =================================================================
        -- COMPOSITE SCORES
        -- =================================================================
        
        -- Ceiling/Floor Confidence Score (how reliable are these projections?)
        ROUND(
            (COALESCE(historical_confidence_score, 30) * 0.4) +
            (CASE 
                WHEN COALESCE(prior_games_vs_opponent, 0) >= 5 THEN 70
                WHEN COALESCE(prior_games_vs_opponent, 0) >= 2 THEN 50  
                ELSE 30
            END * 0.3) +
            (CASE 
                WHEN COALESCE(position_defense_consistency_score, 50) >= 70 THEN 70
                WHEN COALESCE(position_defense_consistency_score, 50) >= 50 THEN 50
                ELSE 30  
            END * 0.3),
            1
        ) AS projection_reliability_score
        
    FROM adjusted_projections
),

-- STEP 5: Final calculations with reliability score available
final_variance_analysis AS (
    SELECT
        *,
        
        -- Boom/Bust Classification
        CASE 
            WHEN pts_projection_range >= 18 AND ceiling_game_probability_tier = 'HIGH_CEILING_PROBABILITY' 
                THEN 'BOOM_CANDIDATE'
            WHEN pts_projection_range >= 15 AND floor_game_risk_tier = 'HIGH_FLOOR_RISK'
                THEN 'BUST_RISK'  
            WHEN pts_projection_range <= 10 AND projection_reliability_score >= 65
                THEN 'SAFE_PLAY'
            ELSE 'BALANCED_VARIANCE'
        END AS boom_bust_classification
        
    FROM prop_betting_features
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
    -- BASELINE PERFORMANCE VARIANCE ANALYSIS
    -- =================================================================
    baseline_pts_volatility,
    baseline_pra_volatility,
    historical_pts_range,
    confidence_adjusted_volatility,
    
    -- =================================================================
    -- CONTEXT-AMPLIFIED VARIANCE MODELING
    -- =================================================================
    opponent_variance_impact,
    context_adjusted_volatility,
    
    -- =================================================================
    -- CEILING/FLOOR ESTIMATION
    -- =================================================================
    estimated_pts_ceiling,
    estimated_pts_floor,
    pts_projection_range,
    
    -- =================================================================
    -- PROBABILITY DISTRIBUTIONS FOR PROP BETTING
    -- =================================================================
    usage_adjusted_ceiling,
    pressure_adjusted_floor,
    
    -- =================================================================
    -- CEILING/FLOOR PROBABILITY ASSESSMENTS
    -- =================================================================
    ceiling_game_probability_tier,
    floor_game_risk_tier,
    
    -- =================================================================
    -- PROP BETTING SPECIFIC FEATURES
    -- =================================================================
    over_under_20_5_assessment,
    dfs_variance_tier,
    
    -- =================================================================
    -- COMPOSITE SCORES
    -- =================================================================
    projection_reliability_score,
    boom_bust_classification,
    
    -- =================================================================
    -- TIMESTAMPS  
    -- =================================================================
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM final_variance_analysis