{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'player', 'context_interactions', 'ml_critical'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['season_year']}
    ]
) }}

/*
MULTI-CONTEXT INTERACTION FEATURES v2
Addresses critical gap: modeling how multiple contexts amplify/diminish each other
rather than treating contexts in isolation.

Core Value: Captures multiplicative effects between situational context, 
opponent matchups, team dynamics, and historical patterns.

Grain: One row per player + game_date
Integration: Joins all context feature stores for cross-pollination analysis
*/

WITH base_player_context AS (
    SELECT
        player_game_key,
        player_id,
        game_id,
        game_date,
        season_year,
        opponent_id,
        position,
        home_away
    FROM {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Bring in all context sources
situational_context AS (
    SELECT * FROM {{ ref('game_situational_context_v2') }}
),

opponent_matchup AS (
    SELECT * FROM {{ ref('opponent_pregame_profile_features_v2') }}
),

position_matchup AS (
    SELECT * FROM {{ ref('opponent_position_defense_features_v2') }}
),

historical_matchup AS (
    SELECT * FROM {{ ref('player_historical_matchup_features_v2') }}
),

scoring_context AS (
    SELECT * FROM {{ ref('player_scoring_features_v2') }}
),

context_interactions AS (
    SELECT
        bpc.player_game_key,
        bpc.player_id,
        bpc.game_id,
        bpc.game_date,
        bpc.season_year,
        bpc.opponent_id,
        bpc.position,
        bpc.home_away,
        
        -- =================================================================
        -- REST × OPPONENT STRENGTH INTERACTIONS
        -- =================================================================
        
        -- Rest advantage amplified by opponent weakness
        CASE 
            WHEN sc.rest_advantage_score >= 80 AND om.opp_overall_strength_score <= 40 
                THEN 'REST_ADVANTAGE_VS_WEAK_OPP'
            WHEN sc.rest_advantage_score <= 30 AND om.opp_overall_strength_score >= 70 
                THEN 'FATIGUE_RISK_VS_STRONG_OPP'
            WHEN sc.rest_advantage_score >= 80 AND om.opp_overall_strength_score >= 70 
                THEN 'WELL_RESTED_VS_ELITE'
            ELSE 'NEUTRAL_REST_MATCHUP'
        END AS rest_opponent_interaction_category,
        
        -- Numerical interaction multiplier
        ROUND(
            (sc.rest_advantage_score / 100.0) * 
            (1.0 + (50 - COALESCE(om.opp_overall_strength_score, 50)) / 100.0),
            3
        ) AS rest_opponent_multiplier,
        
        -- =================================================================
        -- HISTORICAL PERFORMANCE × CURRENT CONTEXT
        -- =================================================================
        
        -- Historical success amplified by favorable current context
        CASE 
            WHEN hm.historical_matchup_favorability_score >= 75 
                AND sc.situational_favorability_score >= 75 
                THEN 'HISTORICAL_AND_SITUATIONAL_ALIGNED'
            WHEN hm.historical_matchup_favorability_score <= 40 
                AND sc.situational_favorability_score <= 40 
                THEN 'DOUBLE_HEADWIND_SCENARIO'
            WHEN hm.historical_matchup_favorability_score >= 70 
                AND sc.situational_favorability_score <= 40 
                THEN 'HISTORICAL_EDGE_SITUATIONAL_DRAG'
            WHEN hm.historical_matchup_favorability_score <= 40 
                AND sc.situational_favorability_score >= 70 
                THEN 'POOR_HISTORY_GOOD_SETUP'
            ELSE 'MIXED_CONTEXT_SIGNALS'
        END AS historical_situational_alignment,
        
        -- Weighted composite of historical + situational
        ROUND(
            (COALESCE(hm.historical_matchup_favorability_score, 50) * 
             CASE WHEN hm.historical_confidence_score >= 60 THEN 0.6 ELSE 0.3 END) +
            (COALESCE(sc.situational_favorability_score, 50) * 
             CASE WHEN hm.historical_confidence_score >= 60 THEN 0.4 ELSE 0.7 END),
            1
        ) AS historical_situational_composite_score,
        
        -- =================================================================
        -- POSITION MATCHUP × PLAYER ARCHETYPE INTERACTIONS  
        -- =================================================================
        
        -- Position-specific vulnerability exploitability
        CASE 
            WHEN pm.is_exploitable_matchup = TRUE 
                AND sco.scoring_archetype IN ('INTERIOR_DOMINANT', 'PERIMETER_SPECIALIST')
                AND pm.position_overall_matchup_quality >= 70
                THEN 'ARCHETYPE_EXPLOIT_OPPORTUNITY'
            WHEN pm.is_avoid_matchup = TRUE 
                AND sco.scoring_archetype = 'SITUATIONAL_SCORER'
                THEN 'ARCHETYPE_MISMATCH_RISK'
            WHEN pm.position_overall_matchup_quality >= 70 
                AND sco.scoring_archetype = 'BALANCED_SCORER'
                THEN 'BALANCED_ARCHETYPE_ADVANTAGE'
            ELSE 'NEUTRAL_ARCHETYPE_MATCHUP'
        END AS archetype_position_matchup_category,
        
        -- Numerical archetype-position interaction
        ROUND(
            COALESCE(pm.position_overall_matchup_quality, 50) / 100.0 *
            CASE 
                WHEN sco.scoring_archetype IN ('INTERIOR_DOMINANT', 'PERIMETER_SPECIALIST') THEN 1.2
                WHEN sco.scoring_archetype = 'BALANCED_SCORER' THEN 1.1  
                WHEN sco.scoring_archetype = 'SHOT_CREATOR' THEN 1.15
                ELSE 1.0
            END,
            3
        ) AS archetype_position_multiplier,
        
        -- =================================================================
        -- WORKLOAD × PERFORMANCE SUSTAINABILITY INTERACTIONS
        -- =================================================================
        
        -- High usage sustainability assessment
        CASE 
            WHEN sc.workload_classification = 'HIGH_WORKLOAD' 
                AND sc.fatigue_risk_assessment = 'HIGH_FATIGUE_RISK'
                AND sco.pressure_performance_tier IN ('PRESSURE_SENSITIVE', 'PRESSURE_VULNERABLE')
                THEN 'UNSUSTAINABLE_WORKLOAD_SCENARIO'
            WHEN sc.workload_classification = 'HIGH_WORKLOAD' 
                AND sc.fatigue_risk_assessment = 'LOW_FATIGUE_RISK'
                AND sco.pressure_performance_tier IN ('PRESSURE_ELITE', 'PRESSURE_RESILIENT')
                THEN 'SUSTAINABLE_HIGH_USAGE'
            WHEN sc.opportunity_context = 'HIGH_OPPORTUNITY'
                AND sc.workload_classification IN ('NORMAL_WORKLOAD', 'LIMITED_ROLE')
                THEN 'OPPORTUNITY_EXPANSION_SCENARIO'
            ELSE 'NORMAL_WORKLOAD_CONTEXT'
        END AS workload_sustainability_category,
        
        -- =================================================================
        -- HOME/AWAY × HISTORICAL PERFORMANCE INTERACTIONS
        -- =================================================================
        
        -- Venue-specific historical edge
        CASE 
            WHEN bpc.home_away = 'HOME' 
                AND hm.home_away_pts_differential_vs_opponent > 5
                AND hm.venue_preference_vs_opponent = 'STRONG_HOME_ADVANTAGE'
                THEN 'STRONG_HOME_HISTORICAL_EDGE'
            WHEN bpc.home_away = 'AWAY' 
                AND hm.home_away_pts_differential_vs_opponent < -3
                AND hm.venue_preference_vs_opponent = 'ROAD_WARRIOR'
                THEN 'ROAD_WARRIOR_EDGE'
            WHEN bpc.home_away = 'HOME' 
                AND hm.venue_preference_vs_opponent = 'MODERATE_AWAY_ADVANTAGE'
                THEN 'HOME_GAME_HISTORICAL_DRAG'
            ELSE 'NEUTRAL_VENUE_HISTORY'
        END AS venue_historical_interaction,
        
        -- =================================================================
        -- TEAM CONTEXT × OPPONENT PACE INTERACTIONS
        -- =================================================================
        
        -- Pace mismatch exploitation
        CASE 
            WHEN ABS(sco.team_pace_avg_l5 - om.opp_recent_pace) >= 5
                AND sco.playstyle_compatibility_score >= 75
                THEN 'PACE_MISMATCH_ADVANTAGE'
            WHEN ABS(sco.team_pace_avg_l5 - om.opp_recent_pace) <= 2
                AND om.opp_predictability_score >= 80
                THEN 'PACE_SYNC_PREDICTABLE_GAME'
            WHEN sco.team_pace_avg_l5 > (om.opp_recent_pace + 3)
                AND sco.player_offensive_role = 'PRIMARY_OPTION'
                THEN 'PACE_UP_PRIMARY_USAGE'
            ELSE 'NEUTRAL_PACE_CONTEXT'
        END AS pace_exploitation_category,
        
        -- Numerical pace interaction factor
        ROUND(
            1.0 + (ABS(COALESCE(sco.team_pace_avg_l5, 100) - COALESCE(om.opp_recent_pace, 100)) / 100.0) *
            CASE 
                WHEN sco.player_offensive_role = 'PRIMARY_OPTION' THEN 0.15
                WHEN sco.player_offensive_role = 'SECONDARY_OPTION' THEN 0.10
                ELSE 0.05
            END,
            3
        ) AS pace_interaction_multiplier,
        
        -- =================================================================
        -- COMPOSITE INTERACTION SCORES
        -- =================================================================
        
    -- Master interaction multiplier (combines all context interactions)
    ROUND(
        (ROUND(
            (sc.rest_advantage_score / 100.0) * 
            (1.0 + (50 - COALESCE(om.opp_overall_strength_score, 50)) / 100.0),
            3
        )) * 
        (ROUND(
            COALESCE(pm.position_overall_matchup_quality, 50) / 100.0 *
            CASE 
                WHEN sco.scoring_archetype IN ('INTERIOR_DOMINANT', 'PERIMETER_SPECIALIST') THEN 1.2
                WHEN sco.scoring_archetype = 'BALANCED_SCORER' THEN 1.1  
                WHEN sco.scoring_archetype = 'SHOT_CREATOR' THEN 1.15
                ELSE 1.0
            END,
            3
        )) * 
        (ROUND(
            1.0 + (ABS(COALESCE(sco.team_pace_avg_l5, 100) - COALESCE(om.opp_recent_pace, 100)) / 100.0) *
            CASE 
                WHEN sco.player_offensive_role = 'PRIMARY_OPTION' THEN 0.15
                WHEN sco.player_offensive_role = 'SECONDARY_OPTION' THEN 0.10
                ELSE 0.05
            END,
            3
        )) * 
        ((ROUND(
            (COALESCE(hm.historical_matchup_favorability_score, 50) * 
            CASE WHEN hm.historical_confidence_score >= 60 THEN 0.6 ELSE 0.3 END) +
            (COALESCE(sc.situational_favorability_score, 50) * 
            CASE WHEN hm.historical_confidence_score >= 60 THEN 0.4 ELSE 0.7 END),
            1
        )) / 100.0),
        3
    ) AS master_context_interaction_multiplier,
        
        -- Context clarity score (how aligned are the signals?)
        CASE 
            WHEN (
                (sc.situational_favorability_score >= 70 AND hm.historical_matchup_favorability_score >= 70 AND pm.position_overall_matchup_quality >= 70) OR
                (sc.situational_favorability_score <= 40 AND hm.historical_matchup_favorability_score <= 40 AND pm.position_overall_matchup_quality <= 40)
            ) THEN 'HIGH_SIGNAL_ALIGNMENT'
            WHEN (
                (sc.situational_favorability_score >= 60 OR hm.historical_matchup_favorability_score >= 60 OR pm.position_overall_matchup_quality >= 60) AND
                (sc.situational_favorability_score <= 50 OR hm.historical_matchup_favorability_score <= 50 OR pm.position_overall_matchup_quality <= 50)
            ) THEN 'MIXED_SIGNAL_CONTEXT'
            ELSE 'NEUTRAL_SIGNAL_ALIGNMENT'
        END AS context_signal_clarity,
        
        -- =================================================================
        -- BINARY FLAGS FOR MODEL FEATURES
        -- =================================================================
        
-- High-confidence favorable scenario
((ROUND(
    (ROUND(
        (sc.rest_advantage_score / 100.0) * 
        (1.0 + (50 - COALESCE(om.opp_overall_strength_score, 50)) / 100.0),
        3
    )) * 
    (ROUND(
        COALESCE(pm.position_overall_matchup_quality, 50) / 100.0 *
        CASE 
            WHEN sco.scoring_archetype IN ('INTERIOR_DOMINANT', 'PERIMETER_SPECIALIST') THEN 1.2
            WHEN sco.scoring_archetype = 'BALANCED_SCORER' THEN 1.1  
            WHEN sco.scoring_archetype = 'SHOT_CREATOR' THEN 1.15
            ELSE 1.0
        END,
        3
    )) * 
    (ROUND(
        1.0 + (ABS(COALESCE(sco.team_pace_avg_l5, 100) - COALESCE(om.opp_recent_pace, 100)) / 100.0) *
        CASE 
            WHEN sco.player_offensive_role = 'PRIMARY_OPTION' THEN 0.15
            WHEN sco.player_offensive_role = 'SECONDARY_OPTION' THEN 0.10
            ELSE 0.05
        END,
        3
    )) * 
    ((ROUND(
        (COALESCE(hm.historical_matchup_favorability_score, 50) * 
         CASE WHEN hm.historical_confidence_score >= 60 THEN 0.6 ELSE 0.3 END) +
        (COALESCE(sc.situational_favorability_score, 50) * 
         CASE WHEN hm.historical_confidence_score >= 60 THEN 0.4 ELSE 0.7 END),
        1
    )) / 100.0),
    3
)) >= 1.20 AND (CASE 
    WHEN (
        (sc.situational_favorability_score >= 70 AND hm.historical_matchup_favorability_score >= 70 AND pm.position_overall_matchup_quality >= 70) OR
        (sc.situational_favorability_score <= 40 AND hm.historical_matchup_favorability_score <= 40 AND pm.position_overall_matchup_quality <= 40)
    ) THEN 'HIGH_SIGNAL_ALIGNMENT'
    WHEN (
        (sc.situational_favorability_score >= 60 OR hm.historical_matchup_favorability_score >= 60 OR pm.position_overall_matchup_quality >= 60) AND
        (sc.situational_favorability_score <= 50 OR hm.historical_matchup_favorability_score <= 50 OR pm.position_overall_matchup_quality <= 50)
    ) THEN 'MIXED_SIGNAL_CONTEXT'
    ELSE 'NEUTRAL_SIGNAL_ALIGNMENT'
END) = 'HIGH_SIGNAL_ALIGNMENT') AS is_high_confidence_favorable,

-- High-confidence difficult scenario  
((ROUND(
    (ROUND(
        (sc.rest_advantage_score / 100.0) * 
        (1.0 + (50 - COALESCE(om.opp_overall_strength_score, 50)) / 100.0),
        3
    )) * 
    (ROUND(
        COALESCE(pm.position_overall_matchup_quality, 50) / 100.0 *
        CASE 
            WHEN sco.scoring_archetype IN ('INTERIOR_DOMINANT', 'PERIMETER_SPECIALIST') THEN 1.2
            WHEN sco.scoring_archetype = 'BALANCED_SCORER' THEN 1.1  
            WHEN sco.scoring_archetype = 'SHOT_CREATOR' THEN 1.15
            ELSE 1.0
        END,
        3
    )) * 
    (ROUND(
        1.0 + (ABS(COALESCE(sco.team_pace_avg_l5, 100) - COALESCE(om.opp_recent_pace, 100)) / 100.0) *
        CASE 
            WHEN sco.player_offensive_role = 'PRIMARY_OPTION' THEN 0.15
            WHEN sco.player_offensive_role = 'SECONDARY_OPTION' THEN 0.10
            ELSE 0.05
        END,
        3
    )) * 
    ((ROUND(
        (COALESCE(hm.historical_matchup_favorability_score, 50) * 
         CASE WHEN hm.historical_confidence_score >= 60 THEN 0.6 ELSE 0.3 END) +
        (COALESCE(sc.situational_favorability_score, 50) * 
         CASE WHEN hm.historical_confidence_score >= 60 THEN 0.4 ELSE 0.7 END),
        1
    )) / 100.0),
    3
)) <= 0.85 AND (CASE 
    WHEN (
        (sc.situational_favorability_score >= 70 AND hm.historical_matchup_favorability_score >= 70 AND pm.position_overall_matchup_quality >= 70) OR
        (sc.situational_favorability_score <= 40 AND hm.historical_matchup_favorability_score <= 40 AND pm.position_overall_matchup_quality <= 40)
    ) THEN 'HIGH_SIGNAL_ALIGNMENT'
    WHEN (
        (sc.situational_favorability_score >= 60 OR hm.historical_matchup_favorability_score >= 60 OR pm.position_overall_matchup_quality >= 60) AND
        (sc.situational_favorability_score <= 50 OR hm.historical_matchup_favorability_score <= 50 OR pm.position_overall_matchup_quality <= 50)
    ) THEN 'MIXED_SIGNAL_CONTEXT'
    ELSE 'NEUTRAL_SIGNAL_ALIGNMENT'
END) = 'HIGH_SIGNAL_ALIGNMENT') AS is_high_confidence_difficult,

-- Contradictory context flag
((CASE 
    WHEN (
        (sc.situational_favorability_score >= 70 AND hm.historical_matchup_favorability_score >= 70 AND pm.position_overall_matchup_quality >= 70) OR
        (sc.situational_favorability_score <= 40 AND hm.historical_matchup_favorability_score <= 40 AND pm.position_overall_matchup_quality <= 40)
    ) THEN 'HIGH_SIGNAL_ALIGNMENT'
    WHEN (
        (sc.situational_favorability_score >= 60 OR hm.historical_matchup_favorability_score >= 60 OR pm.position_overall_matchup_quality >= 60) AND
        (sc.situational_favorability_score <= 50 OR hm.historical_matchup_favorability_score <= 50 OR pm.position_overall_matchup_quality <= 50)
    ) THEN 'MIXED_SIGNAL_CONTEXT'
    ELSE 'NEUTRAL_SIGNAL_ALIGNMENT'
END) = 'MIXED_SIGNAL_CONTEXT') AS has_contradictory_context_signals,

-- Opportunity amplification flag
(sc.opportunity_context = 'HIGH_OPPORTUNITY' AND (ROUND(
    (ROUND(
        (sc.rest_advantage_score / 100.0) * 
        (1.0 + (50 - COALESCE(om.opp_overall_strength_score, 50)) / 100.0),
        3
    )) * 
    (ROUND(
        COALESCE(pm.position_overall_matchup_quality, 50) / 100.0 *
        CASE 
            WHEN sco.scoring_archetype IN ('INTERIOR_DOMINANT', 'PERIMETER_SPECIALIST') THEN 1.2
            WHEN sco.scoring_archetype = 'BALANCED_SCORER' THEN 1.1  
            WHEN sco.scoring_archetype = 'SHOT_CREATOR' THEN 1.15
            ELSE 1.0
        END,
        3
    )) * 
    (ROUND(
        1.0 + (ABS(COALESCE(sco.team_pace_avg_l5, 100) - COALESCE(om.opp_recent_pace, 100)) / 100.0) *
        CASE 
            WHEN sco.player_offensive_role = 'PRIMARY_OPTION' THEN 0.15
            WHEN sco.player_offensive_role = 'SECONDARY_OPTION' THEN 0.10
            ELSE 0.05
        END,
        3
    )) * 
    ((ROUND(
        (COALESCE(hm.historical_matchup_favorability_score, 50) * 
         CASE WHEN hm.historical_confidence_score >= 60 THEN 0.6 ELSE 0.3 END) +
        (COALESCE(sc.situational_favorability_score, 50) * 
         CASE WHEN hm.historical_confidence_score >= 60 THEN 0.4 ELSE 0.7 END),
        1
    )) / 100.0),
    3
)) >= 1.10) AS has_amplified_opportunity,
        
        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM base_player_context bpc
    LEFT JOIN situational_context sc ON bpc.player_game_key = sc.player_game_key
    LEFT JOIN opponent_matchup om ON bpc.game_id = om.game_id AND bpc.opponent_id = om.team_id 
    LEFT JOIN position_matchup pm ON bpc.opponent_id = pm.opponent_id 
        AND bpc.position = pm.position 
        AND bpc.game_date = pm.game_date
    LEFT JOIN historical_matchup hm ON bpc.player_id = hm.player_id 
    AND bpc.opponent_id = hm.opponent_id 
    AND bpc.game_id = hm.game_id
    LEFT JOIN scoring_context sco ON bpc.player_game_key = sco.player_game_key
)

SELECT * FROM context_interactions