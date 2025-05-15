{{ config(
    materialized='incremental',
    unique_key=['player_id', 'game_date'],
    tags=['intermediate', 'player', 'performance'],
    indexes=[
        {'columns': ['player_id', 'game_date'], 'unique': true}
    ],
) }}

WITH player_games AS (
    SELECT *
    FROM {{ ref('int_player__combined_boxscore') }}
    WHERE min > 0
    {% if is_incremental() %}
    AND game_date > (SELECT MAX(game_date) - INTERVAL '60 days' FROM {{ this }})
    {% endif %}
),

player_with_ranks AS (
    SELECT
        player_id,
        player_name,
        game_id,
        game_date,
        season_year,
        pts,
        reb,
        ast,
        min,
        ts_pct,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS game_rank,
        (game_date - LAG(game_date) OVER(PARTITION BY player_id ORDER BY game_date)) AS days_rest
    FROM player_games
),

player_last5 AS (
    SELECT
        player_id,
        game_date,
        AVG(pts) AS last5_avg_pts,
        AVG(reb) AS last5_avg_reb,
        AVG(ast) AS last5_avg_ast,
        AVG(min) AS last5_avg_min,
        AVG(ts_pct) AS last5_avg_ts_pct
    FROM player_with_ranks
    WHERE game_rank <= 5
    GROUP BY player_id, game_date
),

player_season AS (
    SELECT
        player_id,
        season_year,
        AVG(pts) AS season_avg_pts,
        AVG(reb) AS season_avg_reb,
        AVG(ast) AS season_avg_ast
    FROM player_games
    GROUP BY player_id, season_year
),

player_last3 AS (
    SELECT
        player_id,
        game_date,
        AVG(pts) AS last3_avg_pts
    FROM player_with_ranks
    WHERE game_rank <= 3
    GROUP BY player_id, game_date
)

SELECT
    pg.player_id,
    pg.game_date,
    pg.season_year,
    -- Last 5 game stats
    COALESCE(pl5.last5_avg_pts, 0) AS last5_avg_pts,
    COALESCE(pl5.last5_avg_reb, 0) AS last5_avg_reb,
    COALESCE(pl5.last5_avg_ast, 0) AS last5_avg_ast,
    COALESCE(pl5.last5_avg_min, 0) AS last5_avg_min,
    COALESCE(pl5.last5_avg_ts_pct, 0) AS last5_avg_ts_pct,
    
    -- Season averages
    COALESCE(ps.season_avg_pts, 0) AS season_avg_pts,
    COALESCE(ps.season_avg_reb, 0) AS season_avg_reb,
    COALESCE(ps.season_avg_ast, 0) AS season_avg_ast,
    
    -- Form indicators
    CASE
        WHEN COALESCE(pl3.last3_avg_pts, 0) > COALESCE(ps.season_avg_pts, 0) * 1.15
        THEN 'HOT'
        WHEN COALESCE(pl3.last3_avg_pts, 0) < COALESCE(ps.season_avg_pts, 0) * 0.85
        THEN 'COLD'
        ELSE 'NEUTRAL'
    END AS pts_form_indicator,
    
    -- Rest days
    pg.days_rest,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM player_with_ranks pg
LEFT JOIN player_last5 pl5
    ON pg.player_id = pl5.player_id AND pg.game_date = pl5.game_date
LEFT JOIN player_season ps
    ON pg.player_id = ps.player_id AND pg.season_year = ps.season_year
LEFT JOIN player_last3 pl3
    ON pg.player_id = pl3.player_id AND pg.game_date = pl3.game_date
WHERE pg.game_rank = 1 -- Only keep the latest game data for each player
