{{ config(
    materialized='incremental',
    schema='features',
    unique_key=['opponent_id', 'position', 'game_date'],
    incremental_strategy='delete+insert',
    tags=['features', 'opponent', 'position', 'matchup', 'quality'],
    indexes=[
        {'columns': ['opponent_id', 'position', 'game_date'], 'unique': True},
        {'columns': ['opponent_id', 'game_date']},
        {'columns': ['position', 'game_date']},
        {'columns': ['season_year']}
    ]
) }}

/*
POSITION MATCHUP QUALITY ASSESSMENT v2
Focused feature store that complements opponent_pregame_profile_features_v2
by providing position-specific defensive vulnerability analysis.

Core Value: Identifies which positions this opponent struggles to defend,
creating targeted matchup quality signals for player performance prediction.

Grain: One row per opponent + position + game_date
Temporal Safety: All features use PRIOR games only (no data leakage)
Integration: Designed to join with player features on opponent_id + position + game_date
*/

WITH player_boxscores AS (
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
        pb.fg3m,
        pb.fg_pct,
        pb.fg3_pct,
        pb.ts_pct,
        pb.usage_pct,
        
        -- Get opponent info
        go.opponent_id,
        
        -- Get player position
        dp.position,
        
        -- Combined stats for efficiency
        pb.pts + pb.reb + pb.ast AS pra_total,
        
        -- Season game sequence for proper windowing
        ROW_NUMBER() OVER (
            PARTITION BY pb.player_id, pb.season_year
            ORDER BY pb.game_date, pb.game_id
        ) AS player_season_game_num

    FROM {{ ref('int_player__combined_boxscore') }} pb
    JOIN {{ ref('feat_opp__game_opponents_v2') }} go
        ON pb.game_id = go.game_id 
        AND pb.team_id = go.team_id
    LEFT JOIN {{ ref('dim__players') }} dp 
        ON pb.player_id = dp.player_id
    WHERE 
        pb.min >= 8  -- Focus on meaningful playing time
        AND dp.position IS NOT NULL
        AND pb.game_date IS NOT NULL
    {% if is_incremental() %}
        AND pb.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Calculate game-level position stats allowed by each opponent
game_position_stats_allowed AS (
    SELECT
        opponent_id,
        position,
        season_year,
        game_date,
        
        -- Count of position players faced (for weighting)
        COUNT(*) AS position_players_faced,
        
        -- Average stats allowed to this position in this game
        ROUND(AVG(pts), 2) AS game_pts_allowed,
        ROUND(AVG(reb), 2) AS game_reb_allowed,
        ROUND(AVG(ast), 2) AS game_ast_allowed,
        ROUND(AVG(pra_total), 2) AS game_pra_allowed,
        ROUND(AVG(fg_pct), 3) AS game_fg_pct_allowed,
        ROUND(AVG(ts_pct), 3) AS game_ts_pct_allowed,
        ROUND(AVG(usage_pct), 2) AS game_usage_pct_allowed,
        
        -- High-performance game indicators
        COUNT(CASE WHEN pts >= 25 THEN 1 END) AS big_scoring_games_allowed,
        COUNT(CASE WHEN pra_total >= 35 THEN 1 END) AS elite_games_allowed

    FROM player_boxscores
    GROUP BY opponent_id, position, season_year, game_date
),

-- Calculate league baselines by position and season for normalization
league_position_baselines AS (
    SELECT
        position,
        season_year,
        
        -- League averages by position (for z-score calculations)
        ROUND(AVG(game_pts_allowed), 2) AS league_avg_pts_by_position,
        ROUND(AVG(game_reb_allowed), 2) AS league_avg_reb_by_position,
        ROUND(AVG(game_ast_allowed), 2) AS league_avg_ast_by_position,
        ROUND(AVG(game_pra_allowed), 2) AS league_avg_pra_by_position,
        ROUND(AVG(game_fg_pct_allowed), 3) AS league_avg_fg_pct_by_position,
        ROUND(AVG(game_ts_pct_allowed), 3) AS league_avg_ts_pct_by_position,
        
        -- League standard deviations for z-scores
        ROUND(STDDEV(game_pts_allowed), 2) AS league_stddev_pts_by_position,
        ROUND(STDDEV(game_reb_allowed), 2) AS league_stddev_reb_by_position,
        ROUND(STDDEV(game_ast_allowed), 2) AS league_stddev_ast_by_position,
        ROUND(STDDEV(game_pra_allowed), 2) AS league_stddev_pra_by_position
        
    FROM game_position_stats_allowed
    GROUP BY position, season_year
),

-- Calculate rolling position-specific defensive metrics (PRIOR games only)
position_defensive_metrics AS (
    SELECT
        opponent_id,
        position,
        season_year,
        game_date,
        
        -- Sample size for confidence weighting
        COUNT(*) OVER w_prior AS games_defending_position,
        
        -- Rolling averages (PRIOR games only - no data leakage)
        ROUND(AVG(game_pts_allowed) OVER w_prior, 2) AS avg_pts_allowed_to_position,
        ROUND(AVG(game_reb_allowed) OVER w_prior, 2) AS avg_reb_allowed_to_position,
        ROUND(AVG(game_ast_allowed) OVER w_prior, 2) AS avg_ast_allowed_to_position,
        ROUND(AVG(game_pra_allowed) OVER w_prior, 2) AS avg_pra_allowed_to_position,
        ROUND(AVG(game_fg_pct_allowed) OVER w_prior, 3) AS avg_fg_pct_allowed_to_position,
        ROUND(AVG(game_ts_pct_allowed) OVER w_prior, 3) AS avg_ts_pct_allowed_to_position,
        
        -- Consistency metrics (lower = more consistent defense)
        ROUND(STDDEV(game_pts_allowed) OVER w_prior, 2) AS pts_defense_consistency,
        ROUND(STDDEV(game_pra_allowed) OVER w_prior, 2) AS pra_defense_consistency,
        
        -- Elite performance frequency allowed
        ROUND(
            SUM(big_scoring_games_allowed) OVER w_prior * 100.0 / 
            NULLIF(SUM(position_players_faced) OVER w_prior, 0), 
            1
        ) AS big_game_rate_allowed_pct,
        
        -- Recent trend (L5 vs L15 comparison)
        ROUND(AVG(game_pts_allowed) OVER w_recent, 2) AS recent_pts_allowed_to_position,
        ROUND(AVG(game_pra_allowed) OVER w_recent, 2) AS recent_pra_allowed_to_position

    FROM game_position_stats_allowed
    WINDOW 
        w_prior AS (
            PARTITION BY opponent_id, position, season_year 
            ORDER BY game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        w_recent AS (
            PARTITION BY opponent_id, position, season_year 
            ORDER BY game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        )
),

opponent_game_id_mapping AS (
    SELECT
        game_id,
        game_date,
        team_id AS participating_team_id
    FROM {{ ref('feat_opp__game_opponents_v2') }}
    UNION -- Ensures distinct (game_id, game_date, participating_team_id) tuples
    SELECT
        game_id,
        game_date,
        opponent_id AS participating_team_id
    FROM {{ ref('feat_opp__game_opponents_v2') }}
),

-- Generate final position matchup quality features
position_matchup_quality AS (
    SELECT
        pdm.opponent_id,
        ogm.game_id,
        pdm.position,
        pdm.season_year,
        pdm.game_date,
        
        -- =================================================================
        -- SAMPLE SIZE & CONFIDENCE METRICS
        -- =================================================================
        pdm.games_defending_position,
        
        -- Confidence score based on sample size
        CASE 
            WHEN pdm.games_defending_position >= 15 THEN 'HIGH'
            WHEN pdm.games_defending_position >= 8 THEN 'MEDIUM'
            WHEN pdm.games_defending_position >= 4 THEN 'LOW'
            ELSE 'VERY_LOW'
        END AS matchup_confidence_level,
        
        -- =================================================================
        -- CORE POSITION MATCHUP QUALITY INDICES (0-100 scale)
        -- =================================================================
        
        -- Primary Scoring Matchup Quality (higher = more favorable for offensive player)
        LEAST(100, GREATEST(0, 
            ROUND(50 + (pdm.avg_pts_allowed_to_position - lpb.league_avg_pts_by_position) * 3, 1)
        )) AS position_scoring_matchup_quality,
        
        -- Rebounding Matchup Quality
        LEAST(100, GREATEST(0, 
            ROUND(50 + (pdm.avg_reb_allowed_to_position - lpb.league_avg_reb_by_position) * 4, 1)
        )) AS position_rebounding_matchup_quality,
        
        -- Playmaking Matchup Quality  
        LEAST(100, GREATEST(0, 
            ROUND(50 + (pdm.avg_ast_allowed_to_position - lpb.league_avg_ast_by_position) * 5, 1)
        )) AS position_playmaking_matchup_quality,
        
        -- Overall PRA Matchup Quality (composite)
        LEAST(100, GREATEST(0, 
            ROUND(50 + (pdm.avg_pra_allowed_to_position - lpb.league_avg_pra_by_position) * 2, 1)
        )) AS position_overall_matchup_quality,
        
        -- Shooting Efficiency Matchup Quality
        LEAST(100, GREATEST(0, 
            ROUND(50 + (pdm.avg_fg_pct_allowed_to_position - lpb.league_avg_fg_pct_by_position) * 200, 1)
        )) AS position_shooting_matchup_quality,
        
        -- =================================================================
        -- DEFENSIVE CONSISTENCY & PREDICTABILITY
        -- =================================================================
        
        -- Defense Consistency Score (higher = more consistent, predictable)
        CASE 
            WHEN pdm.pts_defense_consistency <= 3 THEN 90
            WHEN pdm.pts_defense_consistency <= 5 THEN 75  
            WHEN pdm.pts_defense_consistency <= 8 THEN 60
            WHEN pdm.pts_defense_consistency <= 12 THEN 40
            ELSE 20
        END AS position_defense_consistency_score,
        
        -- Elite Game Prevention Rate (lower = better defense)
        ROUND(COALESCE(pdm.big_game_rate_allowed_pct, 0), 1) AS elite_game_prevention_rate,
        
        -- =================================================================
        -- MOMENTUM & TREND INDICATORS
        -- =================================================================
        
        -- Recent Form vs Season Average (positive = getting worse at defending position)
        ROUND(
            COALESCE(pdm.recent_pts_allowed_to_position, 0) - 
            COALESCE(pdm.avg_pts_allowed_to_position, 0), 
            2
        ) AS position_defense_trend,
        
        -- Trend Classification
        CASE 
            WHEN (COALESCE(pdm.recent_pts_allowed_to_position, 0) - COALESCE(pdm.avg_pts_allowed_to_position, 0)) > 3 
                THEN 'DECLINING'
            WHEN (COALESCE(pdm.recent_pts_allowed_to_position, 0) - COALESCE(pdm.avg_pts_allowed_to_position, 0)) < -3 
                THEN 'IMPROVING'
            ELSE 'STABLE'
        END AS position_defense_trend_direction,
        
        -- =================================================================
        -- LEAGUE-RELATIVE PERFORMANCE (Z-SCORES)
        -- =================================================================
        
        -- Scoring Defense Z-Score (negative = better than average defense)
        CASE 
            WHEN lpb.league_stddev_pts_by_position > 0 
            THEN ROUND(
                (pdm.avg_pts_allowed_to_position - lpb.league_avg_pts_by_position) / 
                NULLIF(lpb.league_stddev_pts_by_position, 0), 
                2
            )
            ELSE 0
        END AS position_scoring_defense_z_score,
        
        -- PRA Defense Z-Score  
        CASE 
            WHEN lpb.league_stddev_pra_by_position > 0 
            THEN ROUND(
                (pdm.avg_pra_allowed_to_position - lpb.league_avg_pra_by_position) / 
                NULLIF(lpb.league_stddev_pra_by_position, 0), 
                2
            )
            ELSE 0
        END AS position_overall_defense_z_score,
        
        -- =================================================================
        -- MATCHUP CLASSIFICATION FLAGS
        -- =================================================================
        
        -- Primary Matchup Classification
        CASE 
            WHEN pdm.avg_pts_allowed_to_position > (lpb.league_avg_pts_by_position + lpb.league_stddev_pts_by_position) 
                THEN 'FAVORABLE'
            WHEN pdm.avg_pts_allowed_to_position < (lpb.league_avg_pts_by_position - lpb.league_stddev_pts_by_position) 
                THEN 'DIFFICULT'  
            ELSE 'NEUTRAL'
        END AS primary_matchup_classification,
        
        -- Exploit Flag (weak position defense + high consistency = reliable target)
        CASE 
            WHEN pdm.avg_pts_allowed_to_position > (lpb.league_avg_pts_by_position + 0.5 * lpb.league_stddev_pts_by_position)
                AND pdm.pts_defense_consistency <= 6
                AND pdm.games_defending_position >= 8
                THEN TRUE
            ELSE FALSE
        END AS is_exploitable_matchup,
        
        -- Avoid Flag (strong position defense + consistent)
        CASE 
            WHEN pdm.avg_pts_allowed_to_position < (lpb.league_avg_pts_by_position - 0.5 * lpb.league_stddev_pts_by_position)
                AND pdm.pts_defense_consistency <= 6  
                AND pdm.games_defending_position >= 8
                THEN TRUE
            ELSE FALSE
        END AS is_avoid_matchup,
        
        -- =================================================================
        -- RAW METRICS (for debugging and advanced analysis)
        -- =================================================================
        pdm.avg_pts_allowed_to_position,
        pdm.avg_reb_allowed_to_position,
        pdm.avg_ast_allowed_to_position,
        pdm.avg_pra_allowed_to_position,
        pdm.avg_fg_pct_allowed_to_position,
        pdm.avg_ts_pct_allowed_to_position,
        
        -- League context for reference
        lpb.league_avg_pts_by_position,
        lpb.league_avg_pra_by_position,
        
        -- =================================================================
        -- METADATA
        -- =================================================================
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM position_defensive_metrics pdm
    LEFT JOIN league_position_baselines lpb
        ON pdm.position = lpb.position
        AND pdm.season_year = lpb.season_year
    LEFT JOIN opponent_game_id_mapping ogm
        ON pdm.opponent_id = ogm.participating_team_id
        AND pdm.game_date = ogm.game_date
    WHERE pdm.games_defending_position >= 2  -- Minimum sample size filter
)

SELECT * FROM position_matchup_quality