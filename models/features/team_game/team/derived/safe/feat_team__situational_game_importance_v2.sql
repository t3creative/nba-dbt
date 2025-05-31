{{ config(
    materialized='incremental',
    schema='features',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'team', 'situational', 'game_importance', 'ml_safe'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']},
        {'columns': ['season_year']}
    ]
) }}

/*
TEAM SITUATIONAL GAME IMPORTANCE v2 - ML-SAFE VERSION
Calculates situational factors for teams leading up to games, including historical records, 
streaks, form, and rest patterns - ALL using PRIOR games only (zero data leakage).

Core Value: Provides team-level contextual factors that influence player performance,
including momentum, rest situations, and competitive context.

Grain: One row per team + game_date
Temporal Safety: ALL metrics calculated using games PRIOR to current game only
Integration: Designed for joining with player features via team_id + game_date
*/

WITH base_team_games AS (
    SELECT
        tb.team_game_key,
        tb.team_id,
        tb.season_year,
        tb.game_id,
        tb.game_date,
        tb.pts AS team_pts,
        tb.opponent_id,
        
        -- Get opponent points for win/loss determination (from PRIOR completed games)
        opp_tb.pts AS opponent_pts,
        
        -- Win/loss determination (only for COMPLETED games, not current prediction target)
        CASE 
            WHEN tb.pts > opp_tb.pts THEN 1 
            WHEN tb.pts < opp_tb.pts THEN 0 
            ELSE NULL  -- Null for incomplete/future games
        END AS game_result,
        
        -- Game sequence for proper temporal ordering
        ROW_NUMBER() OVER (
            PARTITION BY tb.team_id, tb.season_year 
            ORDER BY tb.game_date, tb.game_id
        ) AS season_game_num,
        
        -- Previous/next game dates for rest calculations
        LAG(tb.game_date, 1) OVER (
            PARTITION BY tb.team_id, tb.season_year 
            ORDER BY tb.game_date, tb.game_id
        ) AS prev_game_date,
        
        LEAD(tb.game_date, 1) OVER (
            PARTITION BY tb.team_id, tb.season_year 
            ORDER BY tb.game_date, tb.game_id
        ) AS next_game_date

    FROM {{ ref('int_team__combined_boxscore') }} tb
    LEFT JOIN {{ ref('int_team__combined_boxscore') }} opp_tb
        ON tb.game_id = opp_tb.game_id 
        AND tb.opponent_id = opp_tb.team_id
    WHERE tb.game_date IS NOT NULL
    {% if is_incremental() %}
        AND tb.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Calculate season records using PRIOR games only (ML-safe)
season_records_prior AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        season_game_num,

        -- Season record BEFORE current game (temporal safety)
        SUM(COALESCE(game_result, 0)) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS wins_before_game,

        SUM(CASE WHEN game_result = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS losses_before_game,

        COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS games_played_before_game,

        -- Win percentage before current game
        CASE 
            WHEN COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
                PARTITION BY team_id, season_year
                ORDER BY game_date, game_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) > 0 THEN ROUND(
                (SUM(COALESCE(game_result, 0)) OVER (
                    PARTITION BY team_id, season_year
                    ORDER BY game_date, game_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ))::NUMERIC / NULLIF(COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
                    PARTITION BY team_id, season_year
                    ORDER BY game_date, game_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), 0),
                3
            )
            ELSE NULL
        END AS win_pct_before_game,

        -- Season phase classification
        CASE
            WHEN season_game_num <= 20 THEN 'EARLY_SEASON'
            WHEN season_game_num <= 60 THEN 'MID_SEASON'
            ELSE 'LATE_SEASON'
        END AS season_phase,

        -- Rest calculations
        prev_game_date,
        next_game_date

    FROM base_team_games
),

-- Step 1: Calculate previous game results (avoid nested window functions)
team_game_results_with_lag AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        game_result,
        
        -- Get previous game result
        LAG(game_result, 1) OVER (
            PARTITION BY team_id, season_year 
            ORDER BY game_date, game_id
        ) AS prev_game_result

    FROM base_team_games
    WHERE game_result IS NOT NULL  -- Only count completed games for streaks
),

-- Step 2: Identify streak changes
team_streak_changes AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        game_result,
        prev_game_result,
        
        -- Identify when streak changes occur
        CASE 
            WHEN game_result != prev_game_result OR prev_game_result IS NULL 
            THEN 1 
            ELSE 0 
        END AS is_streak_change

    FROM team_game_results_with_lag
),

-- Step 3: Create streak groups using cumulative sum
team_streaks_prior AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        game_result,
        prev_game_result,
        
        -- Create streak group IDs
        SUM(is_streak_change) OVER (
            PARTITION BY team_id, season_year 
            ORDER BY game_date, game_id 
            ROWS UNBOUNDED PRECEDING
        ) AS streak_group_id

    FROM team_streak_changes
),

-- Calculate current streak length BEFORE each game
current_streaks_prior AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        
        -- Current streak length BEFORE this game
        COUNT(*) OVER (
            PARTITION BY team_id, season_year, streak_group_id
            ORDER BY game_date, game_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS current_streak_length_prior,
        
        -- Streak type BEFORE this game
        FIRST_VALUE(game_result) OVER (
            PARTITION BY team_id, season_year, streak_group_id
            ORDER BY game_date, game_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS streak_type_result

    FROM team_streaks_prior
),

-- Calculate recent form (L10) using PRIOR games only
recent_form_prior AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,

        -- Last 10 games performance BEFORE current game
        SUM(COALESCE(game_result, 0)) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS last_10_wins_prior,

        COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS last_10_games_played_prior,

        -- Last 10 win percentage
        CASE 
            WHEN COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
                PARTITION BY team_id, season_year
                ORDER BY game_date, game_id
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ) > 0 THEN ROUND(
                (SUM(COALESCE(game_result, 0)) OVER (
                    PARTITION BY team_id, season_year
                    ORDER BY game_date, game_id
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ))::NUMERIC / NULLIF(COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
                    PARTITION BY team_id, season_year
                    ORDER BY game_date, game_id
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ), 0),
                3
            )
            ELSE NULL
        END AS last_10_win_pct_prior,

        -- Last 5 games performance BEFORE current game
        SUM(COALESCE(game_result, 0)) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last_5_wins_prior,

        COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last_5_games_played_prior,

        -- Last 5 win percentage
        CASE 
            WHEN COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
                PARTITION BY team_id, season_year
                ORDER BY game_date, game_id
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) > 0 THEN ROUND(
                (SUM(COALESCE(game_result, 0)) OVER (
                    PARTITION BY team_id, season_year
                    ORDER BY game_date, game_id
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ))::NUMERIC / NULLIF(COUNT(CASE WHEN game_result IS NOT NULL THEN 1 END) OVER (
                    PARTITION BY team_id, season_year
                    ORDER BY game_date, game_id
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ), 0),
                3
            )
            ELSE NULL
        END AS last_5_win_pct_prior

    FROM base_team_games
),

-- Generate comprehensive team situational features
team_situational_features AS (
    SELECT
        -- =================================================================
        -- UNIQUE KEY & IDENTIFIERS
        -- =================================================================
        btg.team_game_key,
        btg.team_id,
        btg.game_id,
        btg.game_date,
        btg.season_year,
        btg.opponent_id,
        btg.season_game_num,
        
        -- =================================================================
        -- SEASON RECORD CONTEXT (PRIOR GAMES ONLY)
        -- =================================================================
        COALESCE(srp.wins_before_game, 0) AS wins_before_game,
        COALESCE(srp.losses_before_game, 0) AS losses_before_game,
        COALESCE(srp.games_played_before_game, 0) AS games_played_before_game,
        
        srp.win_pct_before_game,
        
        -- Season phase
        srp.season_phase,
        
        -- =================================================================
        -- REST & SCHEDULE CONTEXT
        -- =================================================================
        
        -- Days since last game
        CASE 
            WHEN srp.prev_game_date IS NULL THEN NULL -- Season opener
            ELSE (btg.game_date::date - srp.prev_game_date::date)
        END AS days_since_last_game,
        
        -- Days until next game
        CASE 
            WHEN srp.next_game_date IS NULL THEN NULL -- Season ender
            ELSE (srp.next_game_date::date - btg.game_date::date)
        END AS days_until_next_game,
        
        -- Back-to-back flags
        CASE 
            WHEN (btg.game_date::date - srp.prev_game_date::date) = 1 THEN TRUE
            ELSE FALSE
        END AS is_back_to_back,
        
        CASE 
            WHEN srp.next_game_date IS NOT NULL 
                AND (srp.next_game_date::date - btg.game_date::date) = 1 THEN TRUE
            ELSE FALSE
        END AS is_front_of_back_to_back,
        
        -- Rest advantage flags
        CASE 
            WHEN (btg.game_date::date - srp.prev_game_date::date) >= 3 THEN TRUE
            ELSE FALSE
        END AS has_rest_advantage,
        
        -- Rest classification
        CASE 
            WHEN srp.prev_game_date IS NULL THEN 'SEASON_OPENER'
            WHEN (btg.game_date::date - srp.prev_game_date::date) = 1 THEN 'BACK_TO_BACK'
            WHEN (btg.game_date::date - srp.prev_game_date::date) = 2 THEN 'ONE_DAY_REST'
            WHEN (btg.game_date::date - srp.prev_game_date::date) = 3 THEN 'TWO_DAY_REST'
            WHEN (btg.game_date::date - srp.prev_game_date::date) <= 5 THEN 'NORMAL_REST'
            WHEN (btg.game_date::date - srp.prev_game_date::date) <= 7 THEN 'EXTENDED_REST'
            ELSE 'LONG_LAYOFF'
        END AS rest_classification,
        
        -- =================================================================
        -- STREAK CONTEXT (PRIOR GAMES ONLY)
        -- =================================================================
        COALESCE(csp.current_streak_length_prior, 0) AS current_streak_length_prior,
        
        -- Streak type classification
        CASE 
            WHEN csp.streak_type_result = 1 THEN 'WIN_STREAK'
            WHEN csp.streak_type_result = 0 THEN 'LOSS_STREAK'
            ELSE 'NO_STREAK'
        END AS current_streak_type,
        
        -- Streak significance
        CASE 
            WHEN csp.streak_type_result = 1 AND csp.current_streak_length_prior >= 5 THEN TRUE
            WHEN csp.streak_type_result = 0 AND csp.current_streak_length_prior >= 5 THEN TRUE
            ELSE FALSE
        END AS is_significant_streak,
        
        -- =================================================================
        -- RECENT FORM CONTEXT (PRIOR GAMES ONLY)
        -- =================================================================
        COALESCE(rfp.last_10_wins_prior, 0) AS last_10_wins_prior,
        COALESCE(rfp.last_10_games_played_prior, 0) AS last_10_games_played_prior,
        COALESCE(rfp.last_5_wins_prior, 0) AS last_5_wins_prior,
        COALESCE(rfp.last_5_games_played_prior, 0) AS last_5_games_played_prior,
        
        rfp.last_10_win_pct_prior,
        rfp.last_5_win_pct_prior,
        
        -- =================================================================
        -- MOMENTUM CLASSIFICATION
        -- =================================================================
        
        -- Momentum classification using rfp.last_5_win_pct_prior
        CASE 
            WHEN csp.streak_type_result = 1 AND csp.current_streak_length_prior >= 3 THEN 'STRONG_POSITIVE_MOMENTUM'
            WHEN csp.streak_type_result = 0 AND csp.current_streak_length_prior >= 3 THEN 'STRONG_NEGATIVE_MOMENTUM'
            WHEN COALESCE(rfp.last_5_win_pct_prior, 0) >= 0.8 THEN 'POSITIVE_MOMENTUM'
            WHEN COALESCE(rfp.last_5_win_pct_prior, 0) <= 0.2 THEN 'NEGATIVE_MOMENTUM'
            WHEN COALESCE(rfp.last_10_win_pct_prior, 0) >= 0.7 THEN 'MODERATE_POSITIVE_MOMENTUM'
            WHEN COALESCE(rfp.last_10_win_pct_prior, 0) <= 0.3 THEN 'MODERATE_NEGATIVE_MOMENTUM'
            ELSE 'NEUTRAL_MOMENTUM'
        END AS team_momentum_classification,
        
        -- =================================================================
        -- SCHEDULE SITUATION CLASSIFICATION
        -- =================================================================
        
        -- Schedule advantage/disadvantage
        CASE 
            WHEN (btg.game_date::date - srp.prev_game_date::date) = 1 THEN 'SCHEDULE_DISADVANTAGE'
            WHEN (btg.game_date::date - srp.prev_game_date::date) >= 4 THEN 'SCHEDULE_ADVANTAGE'
            ELSE 'NEUTRAL_SCHEDULE'
        END AS schedule_situation,
        
        -- =================================================================
        -- GAME IMPORTANCE CONTEXT
        -- =================================================================
        
        -- Season-based importance
        CASE 
            WHEN srp.season_phase = 'LATE_SEASON' THEN 'HIGH_IMPORTANCE'
            WHEN srp.season_phase = 'MID_SEASON' THEN 'MEDIUM_IMPORTANCE'
            ELSE 'LOW_IMPORTANCE'
        END AS base_game_importance,
        
        -- Competitive balance factor (close records increase importance)
        CASE 
            WHEN srp.season_phase = 'LATE_SEASON' 
                AND COALESCE(srp.games_played_before_game, 0) >= 40
                AND ABS(COALESCE(srp.win_pct_before_game, 0.5) - 0.5) <= 0.1 
                THEN 'PLAYOFF_IMPLICATIONS'
            WHEN csp.current_streak_length_prior >= 5 
                THEN 'STREAK_IMPLICATIONS'
            ELSE 'NORMAL_IMPLICATIONS'
        END AS game_implications,
        
        -- =================================================================
        -- COMPOSITE SITUATIONAL INDICES
        -- =================================================================
        
        -- Team situational favorability score (0-100)
        LEAST(100, GREATEST(0,
            -- Recent form component (40% weight)
            (COALESCE(last_5_win_pct_prior, 0.5) * 40) +
            
            -- Rest component (30% weight)
            (CASE 
                WHEN (btg.game_date::date - srp.prev_game_date::date) >= 3 THEN 30
                WHEN (btg.game_date::date - srp.prev_game_date::date) = 2 THEN 20
                WHEN (btg.game_date::date - srp.prev_game_date::date) = 1 THEN 5
                WHEN srp.prev_game_date IS NULL THEN 25 -- Season opener
                ELSE 15
            END) +
            
            -- Momentum component (20% weight)
            (CASE 
                WHEN csp.streak_type_result = 1 AND csp.current_streak_length_prior >= 3 THEN 20
                WHEN csp.streak_type_result = 0 AND csp.current_streak_length_prior >= 3 THEN 0
                WHEN COALESCE(last_10_win_pct_prior, 0.5) >= 0.7 THEN 15
                WHEN COALESCE(last_10_win_pct_prior, 0.5) <= 0.3 THEN 5
                ELSE 10
            END) +
            
            -- Season context component (10% weight)
            (CASE 
                WHEN srp.season_phase = 'EARLY_SEASON' THEN 8
                WHEN srp.season_phase = 'MID_SEASON' THEN 10
                ELSE 5 -- Late season is more pressure
            END)
        )) AS team_situational_favorability_score,
        
        -- =================================================================
        -- BINARY FLAGS FOR ML FEATURES
        -- =================================================================
        
        -- Binary performance flags
        (COALESCE(rfp.last_5_win_pct_prior, 0) >= 0.8) AS is_hot_team,
        (COALESCE(rfp.last_5_win_pct_prior, 0) <= 0.2) AS is_cold_team,
        (COALESCE(rfp.last_10_win_pct_prior, 0) >= 0.7) AS is_good_form_team,
        (COALESCE(rfp.last_10_win_pct_prior, 0) <= 0.3) AS is_poor_form_team,
        
        -- Schedule flags
        ((btg.game_date::date - srp.prev_game_date::date) = 1) AS is_back_to_back_flag,
        ((btg.game_date::date - srp.prev_game_date::date) >= 4) AS is_well_rested_flag,
        
        -- Streak flags
        (csp.streak_type_result = 1 AND csp.current_streak_length_prior >= 3) AS is_on_win_streak,
        (csp.streak_type_result = 0 AND csp.current_streak_length_prior >= 3) AS is_on_loss_streak,
        
        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM base_team_games btg
    LEFT JOIN season_records_prior srp
        ON btg.team_id = srp.team_id 
        AND btg.game_id = srp.game_id
    LEFT JOIN current_streaks_prior csp
        ON btg.team_id = csp.team_id 
        AND btg.game_id = csp.game_id
    LEFT JOIN recent_form_prior rfp
        ON btg.team_id = rfp.team_id 
        AND btg.game_id = rfp.game_id
)

SELECT * FROM team_situational_features