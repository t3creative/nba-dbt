{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'game', 'situational', 'context', 'scheduling'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']},
        {'columns': ['season_year']}
    ]
) }}

/*
GAME SITUATIONAL CONTEXT FEATURE STORE v2 - FIXED VERSION
Focused on game situation and scheduling context that affects player performance.

Core Value: Captures rest patterns, workload context, roster situations, and scheduling
factors that influence player performance independent of opponent strength.

Grain: One row per player + game_date
Temporal Safety: All context features are known before game starts (zero data leakage)
Integration: Designed to join with other player features on player_id + game_date

FIXED: Proper CTE sequencing to avoid self-referencing errors with is_back_to_back calculation
*/

WITH player_game_schedule AS (
    SELECT
        pb.player_game_key,
        pb.player_id,
        pb.player_name,
        pb.team_id,
        pb.game_id,
        pb.game_date,
        pb.season_year,
        pb.home_away,
        pb.position,
        pb.min AS current_game_min, -- For next game analysis
        
        -- Get opponent info for context
        go.opponent_id,
        
        -- Game sequencing for scheduling analysis
        ROW_NUMBER() OVER (
            PARTITION BY pb.player_id, pb.season_year
            ORDER BY pb.game_date, pb.game_id
        ) AS season_game_num,
        
        -- Previous game information for rest calculation
        LAG(pb.game_date, 1) OVER (
            PARTITION BY pb.player_id, pb.season_year
            ORDER BY pb.game_date, pb.game_id
        ) AS previous_game_date,
        
        LAG(pb.min, 1) OVER (
            PARTITION BY pb.player_id, pb.season_year
            ORDER BY pb.game_date, pb.game_id
        ) AS previous_game_min,
        
        LAG(pb.home_away, 1) OVER (
            PARTITION BY pb.player_id, pb.season_year
            ORDER BY pb.game_date, pb.game_id
        ) AS previous_game_location,
        
        -- Look ahead for next game (for back-to-back detection)
        LEAD(pb.game_date, 1) OVER (
            PARTITION BY pb.player_id, pb.season_year
            ORDER BY pb.game_date, pb.game_id
        ) AS next_game_date

    FROM {{ ref('int_player__combined_boxscore') }} pb
    JOIN {{ ref('feat_opp__game_opponents_v2') }} go
        ON pb.game_id = go.game_id 
        AND pb.team_id = go.team_id
    WHERE 
        pb.game_date IS NOT NULL
    {% if is_incremental() %}
        AND pb.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Calculate basic rest metrics FIRST (separate CTE to avoid self-referencing)
rest_schedule_metrics AS (
    SELECT
        player_game_key,
        player_id,
        team_id,
        game_id,
        game_date,
        season_year,
        home_away,
        position,
        opponent_id,
        season_game_num,
        previous_game_date,
        previous_game_min,
        previous_game_location,
        next_game_date,
        
        -- =================================================================
        -- REST & RECOVERY CALCULATIONS
        -- =================================================================
        
        -- Days of rest before this game
        CASE 
            WHEN previous_game_date IS NULL THEN NULL -- First game of season
            ELSE (game_date::date - previous_game_date::date)
        END AS days_rest,
        
        -- Rest classification
        CASE 
            WHEN previous_game_date IS NULL THEN 'SEASON_OPENER'
            WHEN (game_date::date - previous_game_date::date) = 0 THEN 'SAME_DAY' -- Rare but possible
            WHEN (game_date::date - previous_game_date::date) = 1 THEN 'BACK_TO_BACK'
            WHEN (game_date::date - previous_game_date::date) = 2 THEN 'ONE_DAY_REST'
            WHEN (game_date::date - previous_game_date::date) = 3 THEN 'TWO_DAY_REST'
            WHEN (game_date::date - previous_game_date::date) <= 5 THEN 'NORMAL_REST'
            WHEN (game_date::date - previous_game_date::date) <= 7 THEN 'EXTENDED_REST'
            ELSE 'LONG_LAYOFF'
        END AS rest_classification,
        
        -- Back-to-back game flags
        CASE 
            WHEN (game_date::date - previous_game_date::date) = 1 THEN TRUE
            ELSE FALSE
        END AS is_back_to_back,
        
        CASE 
            WHEN next_game_date IS NOT NULL 
                AND (next_game_date::date - game_date::date) = 1 THEN TRUE
            ELSE FALSE
        END AS is_front_of_back_to_back,
        
        -- Multiple game stretch detection
        CASE 
            WHEN (game_date::date - previous_game_date::date) = 1 
                AND next_game_date IS NOT NULL 
                AND (next_game_date::date - game_date::date) = 1 
            THEN TRUE
            ELSE FALSE
        END AS is_middle_of_back_to_back_stretch,
        
        -- Travel situation
        CASE 
            WHEN previous_game_location IS NULL THEN 'SEASON_OPENER'
            WHEN home_away = 'HOME' AND previous_game_location = 'AWAY' THEN 'RETURNING_HOME'
            WHEN home_away = 'AWAY' AND previous_game_location = 'HOME' THEN 'LEAVING_HOME'
            WHEN home_away = 'HOME' AND previous_game_location = 'HOME' THEN 'HOME_STAND'
            WHEN home_away = 'AWAY' AND previous_game_location = 'AWAY' THEN 'ROAD_TRIP'
            ELSE 'UNKNOWN'
        END AS travel_situation,
        
        -- Rest advantage score (0-100, higher = more rested)
        CASE 
            WHEN previous_game_date IS NULL THEN 85 -- Season opener
            WHEN (game_date::date - previous_game_date::date) = 1 THEN 20 -- Back-to-back
            WHEN (game_date::date - previous_game_date::date) = 2 THEN 40 -- One day rest
            WHEN (game_date::date - previous_game_date::date) = 3 THEN 70 -- Two day rest
            WHEN (game_date::date - previous_game_date::date) <= 5 THEN 85 -- Normal rest
            WHEN (game_date::date - previous_game_date::date) <= 7 THEN 95 -- Extended rest
            ELSE 75 -- Long layoff (could be rust factor)
        END AS rest_advantage_score

    FROM player_game_schedule
),

-- Calculate team-level context for roster/injury impacts
team_game_context AS (
    SELECT
        team_id,
        game_date,
        season_year,
        
        -- Count active players with significant minutes (proxy for roster health)
        COUNT(CASE WHEN min >= 15 THEN 1 END) AS rotation_players_available,
        COUNT(CASE WHEN min >= 25 THEN 1 END) AS core_players_available,
        
        -- Total team minutes distribution
        SUM(min) AS total_team_minutes,
        AVG(min) AS avg_player_minutes,
        MAX(min) AS max_player_minutes,
        
        -- Starter vs bench context
        COUNT(CASE WHEN min >= 30 THEN 1 END) AS likely_starters_count,
        
        -- Team travel context (home/away)
        MAX(home_away) AS team_game_location -- Should be consistent across team

    FROM {{ ref('int_player__combined_boxscore') }}
    WHERE 
        game_date IS NOT NULL
        AND min > 0
    {% if is_incremental() %}
        AND game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
    GROUP BY team_id, game_date, season_year
),

-- Calculate player workload and fatigue indicators
player_workload_context AS (
    SELECT 
        player_id,
        game_date,
        season_year,
        
        -- Recent workload (L3, L5, L7 games - PRIOR only)
        AVG(min) OVER w_l3_prior AS avg_min_l3_prior,
        AVG(min) OVER w_l5_prior AS avg_min_l5_prior,
        AVG(min) OVER w_l7_prior AS avg_min_l7_prior,
        
        -- Workload volatility (consistency of minutes)
        STDDEV(min) OVER w_l7_prior AS min_volatility_l7_prior,
        
        -- Heavy usage indicators
        COUNT(CASE WHEN min >= 35 THEN 1 END) OVER w_l7_prior AS heavy_usage_games_l7_prior,
        COUNT(CASE WHEN min >= 40 THEN 1 END) OVER w_l7_prior AS extreme_usage_games_l7_prior,
        
        -- Recent usage trend
        AVG(min) OVER w_l3_prior - AVG(min) OVER w_l7_prior AS min_trend_l3_vs_l7

    FROM {{ ref('int_player__combined_boxscore') }}
    WHERE game_date IS NOT NULL
    WINDOW 
        w_l3_prior AS (
            PARTITION BY player_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ),
        w_l5_prior AS (
            PARTITION BY player_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),
        w_l7_prior AS (
            PARTITION BY player_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        )
),

-- Generate comprehensive situational features using pre-calculated rest metrics
situational_context_features AS (
    SELECT
        -- =================================================================
        -- UNIQUE KEY & IDENTIFIERS
        -- =================================================================
        rsm.player_game_key,
        rsm.player_id,
        pgs.player_name,
        rsm.team_id,
        rsm.game_id,
        rsm.game_date,
        rsm.season_year,
        rsm.opponent_id,
        rsm.position,
        rsm.home_away,
        rsm.season_game_num,
        
        -- =================================================================
        -- REST & RECOVERY CONTEXT (from pre-calculated CTE)
        -- =================================================================
        rsm.days_rest,
        rsm.rest_classification,
        rsm.is_back_to_back,
        rsm.is_front_of_back_to_back,
        rsm.is_middle_of_back_to_back_stretch,
        rsm.rest_advantage_score,
        
        -- =================================================================
        -- TRAVEL & VENUE CONTEXT
        -- =================================================================
        rsm.home_away AS current_game_location,
        rsm.previous_game_location,
        rsm.travel_situation,
        
        -- =================================================================
        -- WORKLOAD & FATIGUE CONTEXT
        -- =================================================================
        
        -- Previous game context
        rsm.previous_game_min,
        
        -- Recent workload from workload CTE
        ROUND(pwc.avg_min_l3_prior, 1) AS avg_min_l3_prior,
        ROUND(pwc.avg_min_l5_prior, 1) AS avg_min_l5_prior,
        ROUND(pwc.avg_min_l7_prior, 1) AS avg_min_l7_prior,
        ROUND(pwc.min_volatility_l7_prior, 1) AS min_volatility_l7_prior,
        
        -- Heavy usage context
        pwc.heavy_usage_games_l7_prior,
        pwc.extreme_usage_games_l7_prior,
        ROUND(pwc.min_trend_l3_vs_l7, 1) AS min_trend_l3_vs_l7,
        
        -- Workload classification
        CASE 
            WHEN pwc.avg_min_l7_prior >= 35 THEN 'HIGH_WORKLOAD'
            WHEN pwc.avg_min_l7_prior >= 28 THEN 'MODERATE_WORKLOAD'
            WHEN pwc.avg_min_l7_prior >= 20 THEN 'NORMAL_WORKLOAD'
            WHEN pwc.avg_min_l7_prior >= 15 THEN 'LIMITED_ROLE'
            WHEN pwc.avg_min_l7_prior IS NOT NULL THEN 'MINIMAL_ROLE'
            ELSE 'NO_RECENT_DATA'
        END AS workload_classification,
        
        -- Fatigue risk assessment (NOW using pre-calculated is_back_to_back)
        CASE 
            WHEN rsm.is_back_to_back AND pwc.avg_min_l3_prior >= 32 THEN 'HIGH_FATIGUE_RISK'
            WHEN rsm.is_back_to_back AND pwc.avg_min_l3_prior >= 25 THEN 'MODERATE_FATIGUE_RISK'
            WHEN pwc.heavy_usage_games_l7_prior >= 4 THEN 'HIGH_FATIGUE_RISK'
            WHEN pwc.heavy_usage_games_l7_prior >= 2 THEN 'MODERATE_FATIGUE_RISK'
            ELSE 'LOW_FATIGUE_RISK'
        END AS fatigue_risk_assessment,
        
        -- =================================================================
        -- TEAM ROSTER CONTEXT
        -- =================================================================
        
        -- Team context from team CTE
        tgc.rotation_players_available,
        tgc.core_players_available,
        tgc.likely_starters_count,
        ROUND(tgc.avg_player_minutes, 1) AS team_avg_player_minutes,
        
        -- Roster health assessment
        CASE 
            WHEN tgc.rotation_players_available >= 9 THEN 'HEALTHY_ROSTER'
            WHEN tgc.rotation_players_available >= 7 THEN 'ADEQUATE_ROSTER'
            WHEN tgc.rotation_players_available >= 6 THEN 'THIN_ROSTER'
            ELSE 'DEPLETED_ROSTER'
        END AS roster_health_status,
        
        -- Opportunity context (more opportunity when roster is thin)
        CASE 
            WHEN tgc.rotation_players_available <= 7 THEN 'HIGH_OPPORTUNITY'
            WHEN tgc.rotation_players_available <= 8 THEN 'MODERATE_OPPORTUNITY'
            ELSE 'NORMAL_OPPORTUNITY'
        END AS opportunity_context,
        
        -- =================================================================
        -- SCHEDULE CONTEXT
        -- =================================================================
        
        -- Season progress
        ROUND(rsm.season_game_num / 82.0 * 100, 1) AS season_progress_pct,
        
        -- Season phase
        CASE 
            WHEN rsm.season_game_num <= 10 THEN 'EARLY_SEASON'
            WHEN rsm.season_game_num <= 20 THEN 'SEASON_ADJUSTMENT'
            WHEN rsm.season_game_num <= 50 THEN 'MID_SEASON'
            WHEN rsm.season_game_num <= 70 THEN 'LATE_SEASON'
            ELSE 'SEASON_END'
        END AS season_phase,
        
        -- =================================================================
        -- COMPOSITE SITUATIONAL INDICES
        -- =================================================================
        
        -- Situational favorability composite (0-100)
        LEAST(100, GREATEST(0,
            -- Rest component (40% weight)
            (rsm.rest_advantage_score * 0.4) +
            
            -- Home court component (25% weight)
            (CASE WHEN rsm.home_away = 'HOME' THEN 75 ELSE 50 END * 0.25) +
            
            -- Roster health component (20% weight)  
            (CASE 
                WHEN tgc.rotation_players_available <= 7 THEN 80 -- More opportunity
                WHEN tgc.rotation_players_available >= 9 THEN 60 -- Normal
                ELSE 70
            END * 0.20) +
            
            -- Workload sustainability component (15% weight)
            (CASE 
                WHEN pwc.avg_min_l7_prior <= 32 THEN 80
                WHEN pwc.avg_min_l7_prior <= 36 THEN 65
                ELSE 50
            END * 0.15)
        )) AS situational_favorability_score,
        
        -- =================================================================
        -- BINARY FLAGS FOR EASY FILTERING
        -- =================================================================
        
        -- Rest flags
        (rsm.days_rest <= 1) AS is_low_rest,
        (rsm.days_rest >= 4) AS is_well_rested,
        
        -- Workload flags
        (pwc.avg_min_l7_prior >= 35) AS is_high_workload_player,
        (pwc.heavy_usage_games_l7_prior >= 3) AS has_recent_heavy_usage,
        
        -- Opportunity flags
        (tgc.rotation_players_available <= 8) AS has_opportunity_boost,
        (rsm.home_away = 'HOME') AS is_home_game,
        
        -- Risk flags (NOW properly using pre-calculated is_back_to_back)
        (rsm.is_back_to_back AND pwc.avg_min_l7_prior >= 30) AS has_fatigue_risk,
        
        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM rest_schedule_metrics rsm
    LEFT JOIN player_game_schedule pgs
        ON rsm.player_game_key = pgs.player_game_key
    LEFT JOIN team_game_context tgc
        ON rsm.team_id = tgc.team_id 
        AND rsm.game_date = tgc.game_date
        AND rsm.season_year = tgc.season_year
    LEFT JOIN player_workload_context pwc
        ON rsm.player_id = pwc.player_id 
        AND rsm.game_date = pwc.game_date
        AND rsm.season_year = pwc.season_year
)

SELECT * FROM situational_context_features