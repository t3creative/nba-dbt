{{ config(
    materialized='table',
    tags=['intermediate', 'position', 'defense', 'prediction']
) }}

WITH player_boxscores AS (
    SELECT
        -- Select all columns from pb (int_player__combined_boxscore)
        -- EXCEPT opponent_id and game_date, to avoid conflict.
        {{ dbt_utils.star(from=ref('int_player__combined_boxscore'), relation_alias='pb', except=['opponent_id', 'game_date']) }},
        
        -- Explicitly select opponent_id and game_date from op (int_opp__game_opponents).
        -- These will be the definitive opponent_id and game_date for this CTE.
        op.opponent_id,
        op.game_date
        
        -- Columns like pb.position, pb.season_year, pb.pts, pb.reb, etc., 
        -- needed by the position_defense CTE, will be included by the dbt_utils.star call above,
        -- as long as they are not in the 'except' list.
    FROM {{ ref('int_player__combined_boxscore') }} pb
    JOIN {{ ref('int_opp__game_opponents') }} op
        ON pb.game_id = op.game_id 
        AND pb.team_id = op.team_id
    WHERE pb.min >= 5  -- Filter out players with minimal playing time
),

-- Aggregate stats by position allowed by each team
position_defense AS (
    SELECT
        opponent_id,
        position,
        season_year,
        game_date,
        
        -- Calculate a running average for each stat by position
        AVG(pts) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_pts_allowed_to_position,
        
        AVG(reb) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_reb_allowed_to_position,
        
        AVG(ast) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_ast_allowed_to_position,
        
        AVG(stl) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_stl_allowed_to_position,
        
        AVG(blk) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_blk_allowed_to_position,
        
        AVG(fg_pct) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_fg_pct_allowed_to_position,
        
        AVG(fg3_pct) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_fg3_pct_allowed_to_position,
        
        -- Last 10 games metrics
        AVG(pts) OVER(
            PARTITION BY opponent_id, position, season_year
            ORDER BY game_date
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        ) AS l10_pts_allowed_to_position
    FROM player_boxscores
),

-- Calculate league averages by position
league_position_avgs AS (
    SELECT
        position,
        season_year,
        AVG(avg_pts_allowed_to_position) AS league_avg_pts_by_position,
        AVG(avg_reb_allowed_to_position) AS league_avg_reb_by_position,
        AVG(avg_ast_allowed_to_position) AS league_avg_ast_by_position,
        AVG(avg_stl_allowed_to_position) AS league_avg_stl_by_position,
        AVG(avg_blk_allowed_to_position) AS league_avg_blk_by_position,
        AVG(avg_fg_pct_allowed_to_position) AS league_avg_fg_pct_by_position,
        AVG(avg_fg3_pct_allowed_to_position) AS league_avg_fg3_pct_by_position
    FROM position_defense
    GROUP BY position, season_year
)

SELECT
    pd.opponent_id,
    pd.position,
    pd.season_year,
    pd.game_date,
    
    -- Raw stats
    pd.avg_pts_allowed_to_position,
    pd.avg_reb_allowed_to_position,
    pd.avg_ast_allowed_to_position,
    pd.avg_stl_allowed_to_position,
    pd.avg_blk_allowed_to_position,
    pd.avg_fg_pct_allowed_to_position,
    pd.avg_fg3_pct_allowed_to_position,
    pd.l10_pts_allowed_to_position,
    
    -- League averages
    lpa.league_avg_pts_by_position,
    lpa.league_avg_reb_by_position,
    lpa.league_avg_ast_by_position,
    
    -- Derived metrics (vs league average)
    (pd.avg_pts_allowed_to_position - lpa.league_avg_pts_by_position) 
        AS pts_vs_league_avg,
    (pd.avg_reb_allowed_to_position - lpa.league_avg_reb_by_position) 
        AS reb_vs_league_avg,
    (pd.avg_ast_allowed_to_position - lpa.league_avg_ast_by_position) 
        AS ast_vs_league_avg,
        
    -- Fantasy point potential indicators
    CASE 
        WHEN pd.avg_pts_allowed_to_position > lpa.league_avg_pts_by_position THEN 'HIGH'
        WHEN pd.avg_pts_allowed_to_position < lpa.league_avg_pts_by_position THEN 'LOW'
        ELSE 'AVERAGE'
    END AS pts_matchup_quality,
    
    CASE 
        WHEN pd.avg_reb_allowed_to_position > lpa.league_avg_reb_by_position THEN 'HIGH'
        WHEN pd.avg_reb_allowed_to_position < lpa.league_avg_reb_by_position THEN 'LOW'
        ELSE 'AVERAGE'
    END AS reb_matchup_quality,
    
    CASE 
        WHEN pd.avg_ast_allowed_to_position > lpa.league_avg_ast_by_position THEN 'HIGH'
        WHEN pd.avg_ast_allowed_to_position < lpa.league_avg_ast_by_position THEN 'LOW'
        ELSE 'AVERAGE'
    END AS ast_matchup_quality
    
FROM position_defense pd
JOIN league_position_avgs lpa
    ON pd.position = lpa.position
    AND pd.season_year = lpa.season_year