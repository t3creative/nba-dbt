{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'team', 'usage', 'pregame', 'ml_safe'],
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['team_id', 'season_year'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']}
    ]
) }}

/*
ML-SAFE Team Usage Distribution Profile - PRE-GAME ONLY
Uses historical player usage patterns exclusively for prediction models.
No current game usage distribution data included.

FIXED: Separated window functions from aggregate functions to avoid nesting errors
*/

WITH base_games AS (
    SELECT
        team_game_key,
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        home_away
    FROM {{ ref('feat_opp__game_opponents_v2') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Step 1: Get player stats with rankings (separate window functions)
player_usage_with_rankings AS (
    SELECT 
        pb.team_id,
        pb.game_id,
        pb.game_date,
        pb.season_year,
        pb.usage_pct,
        pb.pct_of_team_pts,
        pb.min,
        
        -- Calculate rankings separately (avoid nesting in aggregates)
        ROW_NUMBER() OVER (
            PARTITION BY pb.game_id, pb.team_id 
            ORDER BY pb.usage_pct DESC
        ) AS usage_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY pb.game_id, pb.team_id 
            ORDER BY pb.pct_of_team_pts DESC
        ) AS scoring_rank,
        
        -- For Gini coefficient calculation
        ROW_NUMBER() OVER (
            PARTITION BY pb.game_id, pb.team_id 
            ORDER BY pb.usage_pct ASC
        ) AS usage_order_rank,
        
        COUNT(*) OVER (
            PARTITION BY pb.game_id, pb.team_id
        ) AS team_player_count
        
    FROM {{ ref('int_player__combined_boxscore') }} pb
    WHERE pb.min >= 5  -- Filter out minimal playing time
),

-- Step 2: Calculate Gini coefficient components (separate from main aggregation)
gini_coefficient_components AS (
    SELECT
        team_id,
        game_id,
        game_date,
        season_year,
        team_player_count,
        
        -- Gini coefficient calculation (now properly separated)
        SUM(usage_pct) AS total_usage_pct,
        SUM((usage_order_rank - (team_player_count + 1.0) / 2.0) * usage_pct) AS gini_numerator
        
    FROM player_usage_with_rankings
    GROUP BY team_id, game_id, game_date, season_year, team_player_count
),

-- Step 3: Calculate historical team-level usage distributions (PRIOR games only)
historical_team_usage AS (
    SELECT
        pur.team_id,
        pur.game_id,
        pur.game_date,
        pur.season_year,
        
        -- Usage concentration metrics from completed games
        MAX(pur.usage_pct) AS max_usage_pct,
        MIN(pur.usage_pct) FILTER (WHERE pur.min >= 15) AS min_significant_usage_pct,
        STDDEV(pur.usage_pct) AS usage_distribution_stddev,
        
        -- Top player usage concentration
        SUM(CASE WHEN pur.usage_rank <= 1 THEN pur.usage_pct ELSE 0 END) AS top1_usage_pct,
        SUM(CASE WHEN pur.usage_rank <= 2 THEN pur.usage_pct ELSE 0 END) AS top2_usage_pct,
        SUM(CASE WHEN pur.usage_rank <= 3 THEN pur.usage_pct ELSE 0 END) AS top3_usage_pct,
        
        -- High usage player counts
        COUNT(CASE WHEN pur.usage_pct >= 30 THEN 1 END) AS num_star_usage_players,
        COUNT(CASE WHEN pur.usage_pct >= 25 THEN 1 END) AS num_high_usage_players,
        COUNT(CASE WHEN pur.usage_pct >= 20 THEN 1 END) AS num_featured_players,
        
        -- Hero ball indicator
        CASE WHEN MAX(pur.usage_pct) > 35 THEN 1 ELSE 0 END AS hero_ball_game,
        
        -- Scoring distribution
        MAX(pur.pct_of_team_pts) AS max_scoring_share_pct,
        SUM(CASE WHEN pur.scoring_rank <= 3 THEN pur.pct_of_team_pts ELSE 0 END) AS top3_scoring_share_pct,
        
        COUNT(*) FILTER (WHERE pur.min >= 5) AS rotation_player_count,
        
        -- Usage equality index (Gini coefficient) - now calculated from pre-computed components
        CASE 
            WHEN gcc.total_usage_pct > 0 AND gcc.team_player_count > 1 
            THEN 1 - (2.0 / gcc.team_player_count) * gcc.gini_numerator / gcc.total_usage_pct
            ELSE 0.5  -- Default to neutral if calculation not possible
        END AS usage_equality_index
        
    FROM player_usage_with_rankings pur
    LEFT JOIN gini_coefficient_components gcc
        ON pur.team_id = gcc.team_id 
        AND pur.game_id = gcc.game_id
    GROUP BY 
        pur.team_id, pur.game_id, pur.game_date, pur.season_year,
        gcc.total_usage_pct, gcc.gini_numerator, gcc.team_player_count
),

-- Calculate rolling team usage metrics (PRIOR to current game)
team_usage_patterns AS (
    SELECT
        bg.team_game_key,
        bg.game_id,
        bg.team_id,
        bg.opponent_id,
        bg.season_year,
        bg.game_date,
        bg.home_away,
        
        -- =================================================================
        -- RECENT USAGE PATTERNS (5-GAME ROLLING AVERAGES - PRIOR ONLY)
        -- =================================================================
        
        -- Usage Concentration (Recent)
        COALESCE(AVG(htu.max_usage_pct) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0) AS recent_max_usage_pct,
        
        COALESCE(AVG(htu.top3_usage_pct) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0) AS recent_top3_usage_pct,
        
        COALESCE(AVG(htu.usage_distribution_stddev) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0) AS recent_usage_stddev,
        
        -- Usage Equality (Recent)
        COALESCE(AVG(htu.usage_equality_index) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0.5) AS recent_usage_equality,
        
        -- Player Counts (Recent)
        COALESCE(AVG(htu.num_high_usage_players) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0) AS recent_high_usage_player_count,
        
        COALESCE(AVG(htu.rotation_player_count) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 8) AS recent_rotation_size,
        
        -- Hero Ball Frequency (Recent)
        COALESCE(AVG(htu.hero_ball_game::NUMERIC) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0) AS recent_hero_ball_frequency,
        
        -- Scoring Distribution (Recent)
        COALESCE(AVG(htu.max_scoring_share_pct) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0) AS recent_max_scoring_share,
        
        COALESCE(AVG(htu.top3_scoring_share_pct) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ), 0) AS recent_top3_scoring_share,
        
        -- =================================================================
        -- LONGER-TERM BASELINE (10-GAME ROLLING - PRIOR ONLY)
        -- =================================================================
        
        COALESCE(AVG(htu.max_usage_pct) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ), 0) AS baseline_max_usage_pct,
        
        COALESCE(AVG(htu.top3_usage_pct) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ), 0) AS baseline_top3_usage_pct,
        
        COALESCE(AVG(htu.usage_equality_index) OVER (
            PARTITION BY bg.team_id
            ORDER BY bg.game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ), 0.5) AS baseline_usage_equality,
        
        -- =================================================================
        -- SEASON-TO-DATE PATTERNS (PRIOR ONLY)
        -- =================================================================
        
        COALESCE(AVG(htu.max_usage_pct) OVER (
            PARTITION BY bg.team_id, bg.season_year
            ORDER BY bg.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS season_max_usage_pct,
        
        COALESCE(AVG(htu.usage_equality_index) OVER (
            PARTITION BY bg.team_id, bg.season_year
            ORDER BY bg.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0.5) AS season_usage_equality,
        
        -- Games played for context
        COALESCE(COUNT(htu.game_id) OVER (
            PARTITION BY bg.team_id, bg.season_year
            ORDER BY bg.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS prior_games_played
        
    FROM base_games bg
    LEFT JOIN historical_team_usage htu 
        ON bg.team_id = htu.team_id 
        AND htu.game_date < bg.game_date
        AND bg.season_year = htu.season_year
),

usage_distribution_profile AS (
    SELECT
        team_game_key,
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        home_away,
        prior_games_played,
        
        -- =================================================================
        -- RECENT USAGE DISTRIBUTION PROFILE (5-GAME)
        -- =================================================================
        recent_max_usage_pct,
        recent_top3_usage_pct,
        recent_usage_stddev,
        recent_usage_equality,
        recent_high_usage_player_count,
        recent_rotation_size,
        recent_hero_ball_frequency,
        recent_max_scoring_share,
        recent_top3_scoring_share,
        
        -- =================================================================
        -- BASELINE USAGE PATTERNS (10-GAME)
        -- =================================================================
        baseline_max_usage_pct,
        baseline_top3_usage_pct,
        baseline_usage_equality,
        
        -- =================================================================
        -- ADAPTIVE USAGE METRICS (EARLY SEASON HANDLING)
        -- =================================================================
        
        -- Use recent if enough games, otherwise fall back to baseline/season
        COALESCE(
            CASE WHEN prior_games_played >= 5 THEN recent_max_usage_pct ELSE NULL END,
            baseline_max_usage_pct,
            season_max_usage_pct,
            25.0  -- Reasonable default
        ) AS adaptive_max_usage_pct,
        
        COALESCE(
            CASE WHEN prior_games_played >= 5 THEN recent_usage_equality ELSE NULL END,
            baseline_usage_equality,
            season_usage_equality,
            0.5  -- Neutral default
        ) AS adaptive_usage_equality,
        
        -- =================================================================
        -- USAGE DISTRIBUTION TRENDS
        -- =================================================================
        recent_max_usage_pct - baseline_max_usage_pct AS max_usage_trend,
        recent_usage_equality - baseline_usage_equality AS usage_equality_trend,
        recent_top3_usage_pct - baseline_top3_usage_pct AS top3_usage_trend,
        
        -- =================================================================
        -- TEAM OFFENSIVE STRUCTURE CLASSIFICATION
        -- =================================================================
        
        -- Primary Structure
        CASE
            WHEN COALESCE(
                CASE WHEN prior_games_played >= 5 THEN recent_max_usage_pct ELSE NULL END,
                baseline_max_usage_pct, season_max_usage_pct, 25
            ) > 32 THEN 'STAR_DOMINANT'
            WHEN COALESCE(
                CASE WHEN prior_games_played >= 5 THEN recent_top3_usage_pct ELSE NULL END,
                baseline_top3_usage_pct, 75
            ) > 75 THEN 'TOP_HEAVY'
            WHEN COALESCE(
                CASE WHEN prior_games_played >= 5 THEN recent_usage_equality ELSE NULL END,
                baseline_usage_equality, season_usage_equality, 0.5
            ) > 0.7 THEN 'BALANCED'
            ELSE 'MODERATE_DISTRIBUTION'
        END AS team_offensive_structure,
        
        -- Secondary Characteristics
        CASE
            WHEN recent_hero_ball_frequency > 0.3 THEN 'HERO_BALL_PRONE'
            WHEN recent_rotation_size > 9 THEN 'DEEP_ROTATION'
            WHEN recent_high_usage_player_count > 2 THEN 'MULTI_STAR'
            ELSE 'STANDARD'
        END AS offensive_style_modifier,
        
        -- =================================================================
        -- USAGE CONCENTRATION INDICES
        -- =================================================================
        
        -- Star Dependence Index (0-100, higher = more dependent on star)
        LEAST(100, GREATEST(0,
            COALESCE(
                CASE WHEN prior_games_played >= 5 THEN recent_max_usage_pct ELSE NULL END,
                baseline_max_usage_pct, season_max_usage_pct, 25
            ) * 2.5
        )) AS star_dependence_index,
        
        -- Offensive Balance Index (0-100, higher = more balanced)
        LEAST(100, GREATEST(0,
            COALESCE(
                CASE WHEN prior_games_played >= 5 THEN recent_usage_equality ELSE NULL END,
                baseline_usage_equality, season_usage_equality, 0.5
            ) * 100
        )) AS offensive_balance_index,
        
        -- Depth Utilization Index (based on rotation size)
        LEAST(100, GREATEST(0, recent_rotation_size * 10)) AS depth_utilization_index,
        
        -- =================================================================
        -- CONSISTENCY & PREDICTABILITY METRICS
        -- =================================================================
        
        -- Usage Volatility (lower = more consistent usage patterns)
        COALESCE(recent_usage_stddev, 5) AS usage_volatility,
        
        -- Hero Ball Risk Score (0-100, higher = more risk of hero ball games)
        LEAST(100, recent_hero_ball_frequency * 100) AS hero_ball_risk_score,
        
        -- =================================================================
        -- CONTEXT FLAGS
        -- =================================================================
        CASE WHEN prior_games_played < 10 THEN TRUE ELSE FALSE END AS early_season_flag,
        CASE WHEN recent_hero_ball_frequency > 0.4 THEN TRUE ELSE FALSE END AS hero_ball_team_flag,
        CASE WHEN recent_rotation_size < 7 THEN TRUE ELSE FALSE END AS shallow_rotation_flag,
        
        CURRENT_TIMESTAMP AS updated_at
        
    FROM team_usage_patterns
),

deduped_usage_distribution_profile AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team_game_key ORDER BY game_date DESC, updated_at DESC) AS row_num
        FROM usage_distribution_profile
    ) t
    WHERE row_num = 1
)
SELECT * FROM deduped_usage_distribution_profile