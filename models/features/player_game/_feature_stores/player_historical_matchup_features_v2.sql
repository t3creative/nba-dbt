{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_matchup_key',
    incremental_strategy='delete+insert',
    tags=['features', 'player', 'historical', 'matchups', 'head_to_head'],
    indexes=[
        {'columns': ['player_matchup_key'], 'unique': True},
        {'columns': ['player_id', 'opponent_id', 'game_date']},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['opponent_id', 'game_date']},
        {'columns': ['season_year']}
    ]
) }}

/*
PLAYER HISTORICAL MATCHUPS FEATURE STORE v2
Focused on head-to-head player vs opponent historical performance analysis.

Core Value: Captures player-specific performance patterns against specific opponents,
with confidence weighting and recency adjustments for prediction modeling.

Grain: One row per player + opponent + game_date
Temporal Safety: All historical metrics use PRIOR games only (zero data leakage)
Integration: Designed to join with other player features on player_id + game_date
*/

WITH player_opponent_games AS (
    SELECT
        pb.player_id,
        pb.player_name,
        pb.team_id,
        pb.game_id,
        pb.game_date,
        pb.season_year,
        pb.min,
        pb.pts,
        pb.reb,
        pb.ast,
        pb.stl,
        pb.blk,
        pb.tov,
        pb.fg_pct,
        pb.fg3_pct,
        pb.ts_pct,
        pb.usage_pct,
        pb.plus_minus,
        pb.position,
        
        -- Get opponent information
        go.opponent_id,
        go.home_away,
        
        -- Combined stats for analysis
        pb.pts + pb.reb + pb.ast AS pra_total,
        pb.pts + pb.ast AS pa_total,
        pb.reb + pb.ast AS ra_total,
        
        -- Game sequence for proper temporal ordering
        ROW_NUMBER() OVER (
            PARTITION BY pb.player_id, go.opponent_id
            ORDER BY pb.game_date, pb.game_id
        ) AS head_to_head_game_num,
        
        -- Season-level sequencing for recency weighting
        ROW_NUMBER() OVER (
            PARTITION BY pb.player_id, pb.season_year
            ORDER BY pb.game_date, pb.game_id
        ) AS season_game_num

    FROM {{ ref('int_player__combined_boxscore') }} pb
    JOIN {{ ref('feat_opp__game_opponents_v2') }} go
      ON pb.game_id = go.game_id AND pb.team_id = go.team_id
    WHERE pb.min >= 5 AND pb.game_date IS NOT NULL
    {% if is_incremental() %}
        AND pb.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

player_opponent_seasons AS (
    SELECT 
        player_id,
        opponent_id,
        COUNT(DISTINCT season_year) AS seasons_played_vs_opponent
    FROM player_opponent_games
    GROUP BY player_id, opponent_id
),

-- Calculate head-to-head historical performance (PRIOR games only)
historical_head_to_head_stats AS (
    SELECT
        pog.player_id,
        pog.opponent_id,
        pog.game_id,
        pog.game_date,
        pog.season_year,
        pog.player_name,
        pog.position,
        pog.home_away,
        pog.head_to_head_game_num,
        s.seasons_played_vs_opponent,
        
        -- Count prior games for confidence scoring
        COUNT(*) OVER w_prior AS prior_games_vs_opponent,
        
        -- Confidence level classification
        CASE 
            WHEN COUNT(*) OVER w_prior >= 10 THEN 'VERY_HIGH'
            WHEN COUNT(*) OVER w_prior >= 6 THEN 'HIGH'
            WHEN COUNT(*) OVER w_prior >= 3 THEN 'MEDIUM'
            WHEN COUNT(*) OVER w_prior >= 1 THEN 'LOW'
            ELSE 'NO_HISTORY'
        END AS historical_confidence_level,
        
        -- Confidence scoring
        LEAST(100, GREATEST(0, 
            COUNT(*) OVER w_prior * 15 + 
            COALESCE(s.seasons_played_vs_opponent, 0) * 5
        )) AS historical_confidence_score,
        
        -- =================================================================
        -- CORE HISTORICAL PERFORMANCE METRICS
        -- =================================================================
        
        -- Basic stats vs this opponent (PRIOR games only)
        ROUND(AVG(pts) OVER w_prior, 2) AS avg_pts_vs_opponent_prior,
        ROUND(AVG(reb) OVER w_prior, 2) AS avg_reb_vs_opponent_prior,
        ROUND(AVG(ast) OVER w_prior, 2) AS avg_ast_vs_opponent_prior,
        ROUND(AVG(pra_total) OVER w_prior, 2) AS avg_pra_vs_opponent_prior,
        ROUND(AVG(stl) OVER w_prior, 2) AS avg_stl_vs_opponent_prior,
        ROUND(AVG(blk) OVER w_prior, 2) AS avg_blk_vs_opponent_prior,
        ROUND(AVG(tov) OVER w_prior, 2) AS avg_tov_vs_opponent_prior,
        
        -- Efficiency metrics vs this opponent
        ROUND(AVG(fg_pct) OVER w_prior, 3) AS avg_fg_pct_vs_opponent_prior,
        ROUND(AVG(fg3_pct) OVER w_prior, 3) AS avg_fg3_pct_vs_opponent_prior,
        ROUND(AVG(ts_pct) OVER w_prior, 3) AS avg_ts_pct_vs_opponent_prior,
        ROUND(AVG(usage_pct) OVER w_prior, 2) AS avg_usage_pct_vs_opponent_prior,
        ROUND(AVG(plus_minus) OVER w_prior, 2) AS avg_plus_minus_vs_opponent_prior,
        
        -- Playing time context
        ROUND(AVG(min) OVER w_prior, 1) AS avg_min_vs_opponent_prior,
        
        -- =================================================================
        -- PERFORMANCE CONSISTENCY & VOLATILITY
        -- =================================================================
        
        -- Performance volatility (lower = more consistent)
        ROUND(STDDEV(pts) OVER w_prior, 2) AS pts_volatility_vs_opponent,
        ROUND(STDDEV(pra_total) OVER w_prior, 2) AS pra_volatility_vs_opponent,
        ROUND(STDDEV(plus_minus) OVER w_prior, 2) AS plus_minus_volatility_vs_opponent,
        
        -- Performance ranges (max - min)
        (MAX(pts) OVER w_prior - MIN(pts) OVER w_prior) AS pts_range_vs_opponent,
        (MAX(pra_total) OVER w_prior - MIN(pra_total) OVER w_prior) AS pra_range_vs_opponent,
        
        -- Best/worst games vs this opponent
        MAX(pts) OVER w_prior AS best_scoring_game_vs_opponent,
        MIN(pts) OVER w_prior AS worst_scoring_game_vs_opponent,
        MAX(pra_total) OVER w_prior AS best_overall_game_vs_opponent,
        
        -- =================================================================
        -- RECENCY-WEIGHTED PERFORMANCE 
        -- =================================================================
        
        -- Last 3 games vs this opponent (if available)
        ROUND(AVG(pts) OVER w_recent_3, 2) AS recent_3g_pts_vs_opponent,
        ROUND(AVG(pra_total) OVER w_recent_3, 2) AS recent_3g_pra_vs_opponent,
        ROUND(AVG(fg_pct) OVER w_recent_3, 3) AS recent_3g_fg_pct_vs_opponent,
        
        -- =================================================================
        -- HOME/AWAY SPLITS VS THIS OPPONENT
        -- =================================================================
        
        -- Home performance vs this opponent
        ROUND(
            AVG(CASE WHEN home_away = 'HOME' THEN pts END) OVER w_prior,
            2
        ) AS avg_pts_vs_opponent_at_home,
        
        -- Away performance vs this opponent  
        ROUND(
            AVG(CASE WHEN home_away = 'AWAY' THEN pts END) OVER w_prior,
            2
        ) AS avg_pts_vs_opponent_away,
        
        -- =================================================================
        -- EXTREME PERFORMANCE FLAGS
        -- =================================================================
        
        -- Elite games vs this opponent
        COUNT(CASE WHEN pts >= 30 THEN 1 END) OVER w_prior AS elite_scoring_games_vs_opponent,
        COUNT(CASE WHEN pra_total >= 40 THEN 1 END) OVER w_prior AS elite_overall_games_vs_opponent,
        
        -- Poor games vs this opponent
        COUNT(CASE WHEN pts <= 8 THEN 1 END) OVER w_prior AS poor_scoring_games_vs_opponent,
        COUNT(CASE WHEN pra_total <= 15 THEN 1 END) OVER w_prior AS poor_overall_games_vs_opponent,
        
        -- Performance rate vs this opponent
        ROUND(
            COUNT(CASE WHEN pts >= 20 THEN 1 END) OVER w_prior * 100.0 / 
            NULLIF(COUNT(*) OVER w_prior, 0),
            1
        ) AS solid_scoring_rate_vs_opponent_pct

    FROM player_opponent_games pog
    LEFT JOIN player_opponent_seasons s 
        ON pog.player_id = s.player_id AND pog.opponent_id = s.opponent_id
    WINDOW 
        w_prior AS (
            PARTITION BY pog.player_id, pog.opponent_id 
            ORDER BY pog.game_date, pog.game_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        w_recent_3 AS (
            PARTITION BY pog.player_id, pog.opponent_id 
            ORDER BY pog.game_date, pog.game_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        )
),

-- Calculate derived metrics that require multiple window functions
derived_metrics AS (
    SELECT
        *,
        
        -- =================================================================
        -- SIMPLIFIED RECENCY WEIGHTING
        -- =================================================================
        
        -- Simple recency weighted average (last 3 games get 60% weight, rest get 40%)
        CASE 
            WHEN prior_games_vs_opponent >= 3 THEN
                ROUND(
                    (COALESCE(recent_3g_pts_vs_opponent, avg_pts_vs_opponent_prior) * 0.6) +
                    (avg_pts_vs_opponent_prior * 0.4),
                    2
                )
            ELSE avg_pts_vs_opponent_prior
        END AS recency_weighted_pts_vs_opponent,
        
        CASE 
            WHEN prior_games_vs_opponent >= 3 THEN
                ROUND(
                    (COALESCE(recent_3g_pra_vs_opponent, avg_pra_vs_opponent_prior) * 0.6) +
                    (avg_pra_vs_opponent_prior * 0.4),
                    2
                )
            ELSE avg_pra_vs_opponent_prior
        END AS recency_weighted_pra_vs_opponent,
        
        -- =================================================================
        -- TREND ANALYSIS (Simplified)
        -- =================================================================
        
        -- Recent vs overall performance trend
        CASE 
            WHEN prior_games_vs_opponent >= 4 AND recent_3g_pts_vs_opponent IS NOT NULL THEN
                ROUND(recent_3g_pts_vs_opponent - avg_pts_vs_opponent_prior, 2)
            ELSE NULL
        END AS pts_trend_vs_opponent,
        
        CASE 
            WHEN prior_games_vs_opponent >= 4 AND recent_3g_pra_vs_opponent IS NOT NULL THEN
                ROUND(recent_3g_pra_vs_opponent - avg_pra_vs_opponent_prior, 2)
            ELSE NULL
        END AS pra_trend_vs_opponent,
        
        -- Home vs Away differential
        CASE 
            WHEN avg_pts_vs_opponent_at_home IS NOT NULL 
                AND avg_pts_vs_opponent_away IS NOT NULL
            THEN ROUND(avg_pts_vs_opponent_at_home - avg_pts_vs_opponent_away, 2)
            ELSE NULL
        END AS home_away_pts_differential_vs_opponent
        
    FROM historical_head_to_head_stats
),

-- Final feature generation with composite metrics
final_historical_matchup_features AS (
    SELECT
        -- =================================================================
        -- UNIQUE KEY & IDENTIFIERS
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key(['player_id', 'opponent_id', 'game_id']) }} AS player_matchup_key,
        player_id,
        opponent_id,
        game_id,
        game_date,
        season_year,
        player_name,
        position,
        home_away,
        
        -- =================================================================
        -- CONFIDENCE & SAMPLE SIZE FEATURES
        -- =================================================================
        prior_games_vs_opponent,
        seasons_played_vs_opponent,
        historical_confidence_level,
        historical_confidence_score,
        
        -- =================================================================
        -- CORE HISTORICAL PERFORMANCE
        -- =================================================================
        avg_pts_vs_opponent_prior,
        avg_reb_vs_opponent_prior,
        avg_ast_vs_opponent_prior,
        avg_pra_vs_opponent_prior,
        avg_stl_vs_opponent_prior,
        avg_blk_vs_opponent_prior,
        avg_tov_vs_opponent_prior,
        avg_fg_pct_vs_opponent_prior,
        avg_fg3_pct_vs_opponent_prior,
        avg_ts_pct_vs_opponent_prior,
        avg_usage_pct_vs_opponent_prior,
        avg_plus_minus_vs_opponent_prior,
        avg_min_vs_opponent_prior,
        
        -- =================================================================
        -- PERFORMANCE CONSISTENCY METRICS
        -- =================================================================
        pts_volatility_vs_opponent,
        pra_volatility_vs_opponent,
        plus_minus_volatility_vs_opponent,
        pts_range_vs_opponent,
        pra_range_vs_opponent,
        best_scoring_game_vs_opponent,
        worst_scoring_game_vs_opponent,
        best_overall_game_vs_opponent,
        
        -- =================================================================
        -- RECENCY-WEIGHTED FEATURES
        -- =================================================================
        recent_3g_pts_vs_opponent,
        recent_3g_pra_vs_opponent,
        recent_3g_fg_pct_vs_opponent,
        recency_weighted_pts_vs_opponent,
        recency_weighted_pra_vs_opponent,
        
        -- =================================================================
        -- TREND ANALYSIS
        -- =================================================================
        pts_trend_vs_opponent,
        pra_trend_vs_opponent,
        
        -- Trend classification
        CASE 
            WHEN pts_trend_vs_opponent > 3 THEN 'IMPROVING'
            WHEN pts_trend_vs_opponent < -3 THEN 'DECLINING'
            WHEN pts_trend_vs_opponent IS NOT NULL THEN 'STABLE'
            ELSE 'INSUFFICIENT_DATA'
        END AS pts_trend_classification_vs_opponent,
        
        -- =================================================================
        -- HOME/AWAY CONTEXT
        -- =================================================================
        avg_pts_vs_opponent_at_home,
        avg_pts_vs_opponent_away,
        home_away_pts_differential_vs_opponent,
        
        -- Home/away preference classification
        CASE 
            WHEN home_away_pts_differential_vs_opponent > 5 THEN 'STRONG_HOME_ADVANTAGE'
            WHEN home_away_pts_differential_vs_opponent > 2 THEN 'MODERATE_HOME_ADVANTAGE'
            WHEN home_away_pts_differential_vs_opponent < -5 THEN 'ROAD_WARRIOR'
            WHEN home_away_pts_differential_vs_opponent < -2 THEN 'MODERATE_AWAY_ADVANTAGE'
            WHEN home_away_pts_differential_vs_opponent IS NOT NULL THEN 'NEUTRAL'
            ELSE 'INSUFFICIENT_DATA'
        END AS venue_preference_vs_opponent,
        
        -- =================================================================
        -- EXTREME PERFORMANCE ANALYSIS
        -- =================================================================
        elite_scoring_games_vs_opponent,
        elite_overall_games_vs_opponent,
        poor_scoring_games_vs_opponent,
        poor_overall_games_vs_opponent,
        solid_scoring_rate_vs_opponent_pct,
        
        -- =================================================================
        -- COMPOSITE MATCHUP ASSESSMENT
        -- =================================================================
        
        -- Overall matchup favorability (0-100 scale)
        LEAST(100, GREATEST(0,
            ROUND(
                -- Base performance (40% weight)
                (COALESCE(recency_weighted_pts_vs_opponent, avg_pts_vs_opponent_prior, 0) * 2) +
                -- Consistency bonus (30% weight) - less volatility is better
                (CASE WHEN COALESCE(pts_volatility_vs_opponent, 999) <= 5 THEN 15 
                      WHEN COALESCE(pts_volatility_vs_opponent, 999) <= 8 THEN 10
                      WHEN COALESCE(pts_volatility_vs_opponent, 999) <= 12 THEN 5
                      ELSE 0 END) +
                -- Trend bonus (20% weight)
                (CASE WHEN COALESCE(pts_trend_vs_opponent, 0) > 2 THEN 10
                      WHEN COALESCE(pts_trend_vs_opponent, 0) > 0 THEN 5
                      WHEN COALESCE(pts_trend_vs_opponent, 0) < -2 THEN -5
                      ELSE 0 END) +
                -- Sample size confidence (10% weight)
                (historical_confidence_score * 0.1),
                1
            )
        )) AS historical_matchup_favorability_score,
        
        -- Matchup classification for easy interpretation
        CASE 
            WHEN prior_games_vs_opponent = 0 THEN 'NO_HISTORY'
            WHEN prior_games_vs_opponent <= 2 THEN 'LIMITED_HISTORY'
            WHEN COALESCE(recency_weighted_pts_vs_opponent, 0) >= COALESCE(avg_pts_vs_opponent_prior, 0) + 3 THEN 'TRENDING_UP'
            WHEN COALESCE(recency_weighted_pts_vs_opponent, 0) <= COALESCE(avg_pts_vs_opponent_prior, 0) - 3 THEN 'TRENDING_DOWN'
            WHEN COALESCE(pts_volatility_vs_opponent, 999) <= 4 THEN 'CONSISTENT_PERFORMER'
            WHEN COALESCE(pts_volatility_vs_opponent, 0) >= 10 THEN 'VOLATILE_PERFORMER'
            WHEN COALESCE(avg_pts_vs_opponent_prior, 0) >= 22 THEN 'HISTORICALLY_FAVORABLE'
            WHEN COALESCE(avg_pts_vs_opponent_prior, 0) <= 12 THEN 'HISTORICALLY_DIFFICULT'
            ELSE 'NEUTRAL_HISTORY'
        END AS historical_matchup_classification,
        
        -- Prediction reliability flag
        CASE 
            WHEN prior_games_vs_opponent >= 6 AND COALESCE(pts_volatility_vs_opponent, 999) <= 6 THEN TRUE
            WHEN prior_games_vs_opponent >= 10 AND COALESCE(pts_volatility_vs_opponent, 999) <= 10 THEN TRUE
            ELSE FALSE
        END AS is_reliable_historical_predictor,
        
        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM derived_metrics
    WHERE prior_games_vs_opponent >= 0  -- Include all games, even those with no prior history
)

SELECT * FROM final_historical_matchup_features