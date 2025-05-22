{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_opponent_key',
    tags=['derived', 'features', 'opponent', 'history', 'point_in_time']
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
        pb.player_id || '-' || pb.game_id || '-' || go.opponent_id as player_game_opponent_key,
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
        pb.ts_pct,
        COALESCE(pb.position, 'Unknown') as player_position
    FROM {{ ref('int_player__combined_boxscore') }} pb
    JOIN {{ ref('feat_opp__game_opponents') }} go
        ON pb.game_id = go.game_id 
        AND pb.team_id = go.team_id
    WHERE pb.min >= 1
),

player_opponent_historical_stats AS (
    SELECT
        pg.player_game_opponent_key,
        pg.player_id,
        pg.player_name,
        pg.game_id,
        pg.team_id,
        pg.opponent_id,
        pg.game_date,
        pg.season_year,
        pg.player_position,
        COUNT(pg.game_id) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_games_vs_opponent,
        AVG(pg.min) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_min_vs_opponent_prior,
        AVG(pg.pts) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_pts_vs_opponent_prior,
        STDDEV(pg.pts) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS stddev_pts_vs_opponent_prior,
        AVG(pg.reb) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_reb_vs_opponent_prior,
        STDDEV(pg.reb) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS stddev_reb_vs_opponent_prior,
        AVG(pg.ast) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_ast_vs_opponent_prior,
        STDDEV(pg.ast) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS stddev_ast_vs_opponent_prior,
        AVG(pg.fg_pct) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_fg_pct_vs_opponent_prior,
        AVG(pg.fg3_pct) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_fg3_pct_vs_opponent_prior,
        AVG(pg.ts_pct) OVER (
            PARTITION BY pg.player_id, pg.opponent_id 
            ORDER BY pg.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_ts_pct_vs_opponent_prior,
        LAG(pg.pts, 1) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS pts_lag1_vs_opp,
        LAG(pg.pts, 2) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS pts_lag2_vs_opp,
        LAG(pg.pts, 3) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS pts_lag3_vs_opp,
        LAG(pg.reb, 1) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS reb_lag1_vs_opp,
        LAG(pg.reb, 2) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS reb_lag2_vs_opp,
        LAG(pg.reb, 3) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS reb_lag3_vs_opp,
        LAG(pg.ast, 1) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS ast_lag1_vs_opp,
        LAG(pg.ast, 2) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS ast_lag2_vs_opp,
        LAG(pg.ast, 3) OVER (PARTITION BY pg.player_id, pg.opponent_id ORDER BY pg.game_date) AS ast_lag3_vs_opp
    FROM player_games pg
),

league_avgs AS (
    SELECT
        season_year,
        COALESCE(position, 'Unknown') as player_position,
        AVG(pts) AS league_avg_pts_by_position,
        AVG(reb) AS league_avg_reb_by_position,
        AVG(ast) AS league_avg_ast_by_position
    FROM {{ ref('int_player__combined_boxscore') }}
    GROUP BY season_year, COALESCE(position, 'Unknown')
)

SELECT
    pohs.player_game_opponent_key,
    pohs.player_id,
    pohs.player_name,
    pohs.game_id,
    pohs.opponent_id,
    pohs.game_date,
    pohs.season_year,
    pohs.player_position,
    COALESCE(pohs.prior_games_vs_opponent, 0) AS prior_games_vs_opponent,
    pohs.avg_min_vs_opponent_prior,
    pohs.avg_pts_vs_opponent_prior,
    pohs.stddev_pts_vs_opponent_prior,
    pohs.avg_reb_vs_opponent_prior,
    pohs.stddev_reb_vs_opponent_prior,
    pohs.avg_ast_vs_opponent_prior,
    pohs.stddev_ast_vs_opponent_prior,
    pohs.avg_fg_pct_vs_opponent_prior,
    pohs.avg_fg3_pct_vs_opponent_prior,
    pohs.avg_ts_pct_vs_opponent_prior,
    pohs.pts_lag1_vs_opp,
    pohs.pts_lag2_vs_opp,
    pohs.pts_lag3_vs_opp,
    pohs.reb_lag1_vs_opp,
    pohs.reb_lag2_vs_opp,
    pohs.reb_lag3_vs_opp,
    pohs.ast_lag1_vs_opp,
    pohs.ast_lag2_vs_opp,
    pohs.ast_lag3_vs_opp,
    CASE 
        WHEN pohs.pts_lag1_vs_opp IS NOT NULL AND pohs.pts_lag2_vs_opp IS NOT NULL AND pohs.pts_lag3_vs_opp IS NOT NULL THEN
            (pohs.pts_lag1_vs_opp * 0.5 + pohs.pts_lag2_vs_opp * 0.3 + pohs.pts_lag3_vs_opp * 0.2)
        WHEN pohs.pts_lag1_vs_opp IS NOT NULL AND pohs.pts_lag2_vs_opp IS NOT NULL THEN
            (pohs.pts_lag1_vs_opp * 0.6 + pohs.pts_lag2_vs_opp * 0.4)
        WHEN pohs.pts_lag1_vs_opp IS NOT NULL THEN
            pohs.pts_lag1_vs_opp
        ELSE pohs.avg_pts_vs_opponent_prior
    END AS recency_weighted_pts_prior,
    CASE
        WHEN COALESCE(pohs.prior_games_vs_opponent, 0) >= 3 AND pohs.avg_pts_vs_opponent_prior IS NOT NULL AND la.league_avg_pts_by_position IS NOT NULL AND
             pohs.avg_pts_vs_opponent_prior > la.league_avg_pts_by_position * 1.15
        THEN 'STRONG_HISTORY_PRIOR'
        WHEN COALESCE(pohs.prior_games_vs_opponent, 0) >= 3 AND pohs.avg_pts_vs_opponent_prior IS NOT NULL AND la.league_avg_pts_by_position IS NOT NULL AND
             pohs.avg_pts_vs_opponent_prior < la.league_avg_pts_by_position * 0.85
        THEN 'WEAK_HISTORY_PRIOR'
        ELSE 'NEUTRAL_HISTORY_PRIOR'
    END AS historical_performance_flag_prior,
    CASE
        WHEN COALESCE(pohs.prior_games_vs_opponent, 0) >= 10 THEN 5
        WHEN COALESCE(pohs.prior_games_vs_opponent, 0) >= 5  THEN 4
        WHEN COALESCE(pohs.prior_games_vs_opponent, 0) >= 3  THEN 3
        WHEN COALESCE(pohs.prior_games_vs_opponent, 0) >= 1  THEN 2
        ELSE 1
    END AS sample_confidence_prior,
    CURRENT_TIMESTAMP AS created_at
FROM player_opponent_historical_stats pohs
LEFT JOIN league_avgs la
    ON pohs.season_year = la.season_year
    AND pohs.player_position = la.player_position