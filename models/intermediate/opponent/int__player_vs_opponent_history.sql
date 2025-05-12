{{ config(
    materialized='table',
    tags=['intermediate', 'history', 'matchup', 'prediction']
) }}

WITH player_games AS (
    SELECT
        pb.player_id,
        pb.player_name,
        pb.game_id,
        pb.team_id,
        go.opponent_id,
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
        pb.ft_pct,
        pb.plus_minus,
        pb.ts_pct
    FROM {{ ref('int__combined_player_boxscore') }} pb
    JOIN {{ ref('int__game_opponents') }} go
        ON pb.game_id = go.game_id 
        AND pb.team_id = go.team_id
    WHERE pb.min >= 5  -- Filter out minimal playing time
),

-- Calculate historical stats for each player against each opponent
player_vs_opponent AS (
    SELECT
        player_id,
        player_name,
        opponent_id,
        
        -- Game counts
        COUNT(*) AS games_vs_opponent,
        MAX(game_date) AS last_game_vs_opponent,
        
        -- Basic averages
        AVG(min) AS avg_min_vs_opponent,
        AVG(pts) AS avg_pts_vs_opponent,
        AVG(reb) AS avg_reb_vs_opponent,
        AVG(ast) AS avg_ast_vs_opponent,
        AVG(stl) AS avg_stl_vs_opponent,
        AVG(blk) AS avg_blk_vs_opponent,
        AVG(tov) AS avg_tov_vs_opponent,
        
        -- Efficiency metrics
        AVG(fg_pct) AS avg_fg_pct_vs_opponent,
        AVG(fg3_pct) AS avg_fg3_pct_vs_opponent,
        AVG(ft_pct) AS avg_ft_pct_vs_opponent,
        AVG(ts_pct) AS avg_ts_pct_vs_opponent,
        AVG(plus_minus) AS avg_plus_minus_vs_opponent,
        
        -- Game-by-game variance (consistency metric)
        STDDEV(pts) AS pts_stddev_vs_opponent,
        STDDEV(reb) AS reb_stddev_vs_opponent,
        STDDEV(ast) AS ast_stddev_vs_opponent,
        
        -- Recent performance (last 3 games only)
        ARRAY_AGG(pts ORDER BY game_date DESC LIMIT 3) AS last3_pts_vs_opponent,
        ARRAY_AGG(reb ORDER BY game_date DESC LIMIT 3) AS last3_reb_vs_opponent,
        ARRAY_AGG(ast ORDER BY game_date DESC LIMIT 3) AS last3_ast_vs_opponent,
        
        -- Performance trend
        CASE 
            WHEN COUNT(*) >= 3 THEN
                REGR_SLOPE(
                    pts, 
                    EXTRACT(EPOCH FROM game_date)
                )
            ELSE NULL
        END AS pts_trend_vs_opponent
        
    FROM player_games
    GROUP BY player_id, player_name, opponent_id
),

-- League average player stats as a reference point
league_avgs AS (
    SELECT
        season_year,
        position,
        AVG(pts) AS league_avg_pts_by_position,
        AVG(reb) AS league_avg_reb_by_position,
        AVG(ast) AS league_avg_ast_by_position
    FROM {{ ref('int__combined_player_boxscore') }}
    GROUP BY season_year, position
)

SELECT
    pvo.*,
    
    -- Calculate recency-weighted averages for key stats
    CASE 
        WHEN pvo.games_vs_opponent >= 3 THEN
            (
                -- Last 3 game average (more weight to recent games)
                (pvo.last3_pts_vs_opponent[1] * 0.5 + 
                 pvo.last3_pts_vs_opponent[2] * 0.3 + 
                 pvo.last3_pts_vs_opponent[3] * 0.2)
            )
        WHEN pvo.games_vs_opponent = 2 THEN
            (
                (pvo.last3_pts_vs_opponent[1] * 0.6 + 
                 pvo.last3_pts_vs_opponent[2] * 0.4)
            )
        WHEN pvo.games_vs_opponent = 1 THEN
            pvo.last3_pts_vs_opponent[1]
        ELSE NULL
    END AS recency_weighted_pts,
    
    -- Historical performance flags
    CASE
        WHEN pvo.games_vs_opponent >= 3 AND
             pvo.avg_pts_vs_opponent > 
             (SELECT la.league_avg_pts_by_position 
              FROM league_avgs la 
              WHERE la.position = (
                SELECT DISTINCT position 
                FROM {{ ref('int__combined_player_boxscore') }} 
                WHERE player_id = pvo.player_id 
                LIMIT 1
              ) 
              AND la.season_year = (SELECT MAX(season_year) FROM league_avgs)
              LIMIT 1) * 1.15
        THEN 'STRONG_HISTORY'
        WHEN pvo.games_vs_opponent >= 3 AND
             pvo.avg_pts_vs_opponent < 
             (SELECT la.league_avg_pts_by_position 
              FROM league_avgs la 
              WHERE la.position = (
                SELECT DISTINCT position 
                FROM {{ ref('int__combined_player_boxscore') }} 
                WHERE player_id = pvo.player_id 
                LIMIT 1
              ) 
              AND la.season_year = (SELECT MAX(season_year) FROM league_avgs)
              LIMIT 1) * 0.85
        THEN 'WEAK_HISTORY'
        ELSE 'NEUTRAL_HISTORY'
    END AS historical_performance_flag,
    
    -- Create a confidence score based on sample size and consistency
    CASE
        WHEN pvo.games_vs_opponent >= 10 THEN 
            5 -- High confidence with 10+ games
        WHEN pvo.games_vs_opponent >= 5 THEN 
            4 -- Good confidence with 5-9 games
        WHEN pvo.games_vs_opponent >= 3 THEN 
            3 -- Moderate confidence with 3-4 games
        WHEN pvo.games_vs_opponent >= 1 THEN 
            2 -- Low confidence with 1-2 games
        ELSE 
            1 -- No confidence with 0 games
    END AS sample_confidence,
    
    CURRENT_TIMESTAMP AS created_at
FROM player_vs_opponent pvo