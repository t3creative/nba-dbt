{{ config(
    materialized='table',
    tags=['intermediate', 'position', 'defense', 'prediction']
) }}

WITH player_boxscores AS (
    SELECT
        {{ dbt_utils.star(
            from=ref('int_player__combined_boxscore'),
            relation_alias='pb',
            except=['opponent_id', 'game_date', 'game_id', 'position', 'season_year']
        ) }},
        
        op.opponent_id,
        op.game_date::date AS game_date,
        
        dp.position AS position,

        CASE
            WHEN EXTRACT(MONTH FROM op.game_date::date) >= 10 THEN EXTRACT(YEAR FROM op.game_date::date)::TEXT
            ELSE (EXTRACT(YEAR FROM op.game_date::date) - 1)::TEXT
        END AS season_year
        
    FROM {{ ref('int_player__combined_boxscore') }} pb
    JOIN {{ ref('feat_opp__game_opponents') }} op
        ON pb.game_id = op.game_id 
        AND pb.team_id = op.team_id
    LEFT JOIN {{ ref('dim__players') }} dp 
        ON pb.player_id = dp.player_id
    WHERE pb.min >= 5 AND dp.position IS NOT NULL
),

game_level_position_stats_allowed AS (
    SELECT
        opponent_id,
        position,
        season_year,
        game_date,
        AVG(COALESCE(pts, 0)) AS avg_daily_pts_allowed,
        AVG(COALESCE(reb, 0)) AS avg_daily_reb_allowed,
        AVG(COALESCE(ast, 0)) AS avg_daily_ast_allowed,
        AVG(COALESCE(stl, 0)) AS avg_daily_stl_allowed,
        AVG(COALESCE(blk, 0)) AS avg_daily_blk_allowed,
        AVG(COALESCE(fg3m, 0)) AS avg_daily_fg3m_allowed,
        AVG(COALESCE(fg_pct, 0.0)) AS avg_daily_fg_pct_allowed,
        AVG(COALESCE(fg3_pct, 0.0)) AS avg_daily_fg3_pct_allowed,

        -- Combined daily stats
        (AVG(COALESCE(pts, 0)) + AVG(COALESCE(ast, 0))) AS avg_daily_pts_ast_allowed,
        (AVG(COALESCE(pts, 0)) + AVG(COALESCE(reb, 0))) AS avg_daily_pts_reb_allowed,
        (AVG(COALESCE(pts, 0)) + AVG(COALESCE(ast, 0)) + AVG(COALESCE(reb, 0))) AS avg_daily_pts_reb_ast_allowed,
        (AVG(COALESCE(ast, 0)) + AVG(COALESCE(reb, 0))) AS avg_daily_ast_reb_allowed
    FROM player_boxscores
    GROUP BY
        opponent_id,
        position,
        season_year,
        game_date
),

position_defense AS (
    SELECT
        opponent_id,
        position,
        season_year,
        game_date,
        
        -- Rolling averages for individual stats
        AVG(avg_daily_pts_allowed) OVER w AS avg_pts_allowed_to_position,
        AVG(avg_daily_reb_allowed) OVER w AS avg_reb_allowed_to_position,
        AVG(avg_daily_ast_allowed) OVER w AS avg_ast_allowed_to_position,
        AVG(avg_daily_stl_allowed) OVER w AS avg_stl_allowed_to_position,
        AVG(avg_daily_blk_allowed) OVER w AS avg_blk_allowed_to_position,
        AVG(avg_daily_fg3m_allowed) OVER w AS avg_fg3m_allowed_to_position,
        AVG(avg_daily_fg_pct_allowed) OVER w AS avg_fg_pct_allowed_to_position,
        AVG(avg_daily_fg3_pct_allowed) OVER w AS avg_fg3_pct_allowed_to_position,
        
        -- Rolling averages for combined stats
        AVG(avg_daily_pts_ast_allowed) OVER w AS avg_pts_ast_allowed_to_position,
        AVG(avg_daily_pts_reb_allowed) OVER w AS avg_pts_reb_allowed_to_position,
        AVG(avg_daily_pts_reb_ast_allowed) OVER w AS avg_pts_reb_ast_allowed_to_position,
        AVG(avg_daily_ast_reb_allowed) OVER w AS avg_ast_reb_allowed_to_position,

        -- Last 10 games for points (can be expanded for other stats if needed)
        AVG(avg_daily_pts_allowed) OVER (
            PARTITION BY opponent_id, position, season_year ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        ) AS l10_pts_allowed_to_position

    FROM game_level_position_stats_allowed
    WINDOW w AS (PARTITION BY opponent_id, position, season_year ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

league_position_avgs AS (
    SELECT
        position,
        season_year,
        AVG(avg_pts_allowed_to_position) AS league_avg_pts_by_position,
        AVG(avg_reb_allowed_to_position) AS league_avg_reb_by_position,
        AVG(avg_ast_allowed_to_position) AS league_avg_ast_by_position,
        AVG(avg_stl_allowed_to_position) AS league_avg_stl_by_position,
        AVG(avg_blk_allowed_to_position) AS league_avg_blk_by_position,
        AVG(avg_fg3m_allowed_to_position) AS league_avg_fg3m_by_position,
        AVG(avg_fg_pct_allowed_to_position) AS league_avg_fg_pct_by_position,
        AVG(avg_fg3_pct_allowed_to_position) AS league_avg_fg3_pct_by_position,

        -- League averages for combined stats
        AVG(avg_pts_ast_allowed_to_position) AS league_avg_pts_ast_by_position,
        AVG(avg_pts_reb_allowed_to_position) AS league_avg_pts_reb_by_position,
        AVG(avg_pts_reb_ast_allowed_to_position) AS league_avg_pts_reb_ast_by_position,
        AVG(avg_ast_reb_allowed_to_position) AS league_avg_ast_reb_by_position
    FROM position_defense
    GROUP BY position, season_year
)

SELECT
    pd.opponent_id,
    pd.position,
    pd.season_year,
    pd.game_date,
    
    -- Raw opponent averages allowed to position
    pd.avg_pts_allowed_to_position,
    pd.avg_reb_allowed_to_position,
    pd.avg_ast_allowed_to_position,
    pd.avg_stl_allowed_to_position,
    pd.avg_blk_allowed_to_position,
    pd.avg_fg3m_allowed_to_position,
    pd.avg_fg_pct_allowed_to_position,
    pd.avg_fg3_pct_allowed_to_position,
    pd.avg_pts_ast_allowed_to_position,
    pd.avg_pts_reb_allowed_to_position,
    pd.avg_pts_reb_ast_allowed_to_position,
    pd.avg_ast_reb_allowed_to_position,
    pd.l10_pts_allowed_to_position,
    
    -- League averages for position (can be used for context)
    lpa.league_avg_pts_by_position,
    lpa.league_avg_reb_by_position,
    lpa.league_avg_ast_by_position,
    lpa.league_avg_stl_by_position,
    lpa.league_avg_blk_by_position,
    lpa.league_avg_fg3m_by_position,
    lpa.league_avg_fg_pct_by_position,
    lpa.league_avg_fg3_pct_by_position,
    lpa.league_avg_pts_ast_by_position,
    lpa.league_avg_pts_reb_by_position,
    lpa.league_avg_pts_reb_ast_by_position,
    lpa.league_avg_ast_reb_by_position,

    -- Matchup quality indicators (vs league average)
    (pd.avg_pts_allowed_to_position - lpa.league_avg_pts_by_position) AS pts_vs_league_avg,
    (pd.avg_reb_allowed_to_position - lpa.league_avg_reb_by_position) AS reb_vs_league_avg,
    (pd.avg_ast_allowed_to_position - lpa.league_avg_ast_by_position) AS ast_vs_league_avg,
    (pd.avg_stl_allowed_to_position - lpa.league_avg_stl_by_position) AS stl_vs_league_avg,
    (pd.avg_blk_allowed_to_position - lpa.league_avg_blk_by_position) AS blk_vs_league_avg,
    (pd.avg_fg3m_allowed_to_position - lpa.league_avg_fg3m_by_position) AS fg3m_vs_league_avg,
    (pd.avg_fg_pct_allowed_to_position - lpa.league_avg_fg_pct_by_position) AS fg_pct_vs_league_avg,
    (pd.avg_fg3_pct_allowed_to_position - lpa.league_avg_fg3_pct_by_position) AS fg3_pct_vs_league_avg,
    (pd.avg_pts_ast_allowed_to_position - lpa.league_avg_pts_ast_by_position) AS pts_ast_vs_league_avg,
    (pd.avg_pts_reb_allowed_to_position - lpa.league_avg_pts_reb_by_position) AS pts_reb_vs_league_avg,
    (pd.avg_pts_reb_ast_allowed_to_position - lpa.league_avg_pts_reb_ast_by_position) AS pts_reb_ast_vs_league_avg,
    (pd.avg_ast_reb_allowed_to_position - lpa.league_avg_ast_reb_by_position) AS ast_reb_vs_league_avg
    
FROM position_defense pd
JOIN league_position_avgs lpa
    ON pd.position = lpa.position
    AND pd.season_year = lpa.season_year