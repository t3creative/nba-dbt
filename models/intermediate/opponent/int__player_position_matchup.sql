{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    tags=['intermediate', 'position', 'matchup', 'prediction']
) }}

WITH upcoming_games AS (
    SELECT
        go.game_id,
        go.team_id,
        go.opponent_id,
        go.game_date,
        go.season_year,
        go.home_away,
        go.game_team_key
    FROM {{ ref('int__game_opponents') }} go
    {% if is_incremental() %}
    WHERE go.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

player_data AS (
    SELECT DISTINCT
        player_id,
        team_id,
        position,
        player_name,
        first_name,
        family_name
    FROM {{ ref('int__combined_player_boxscore') }}
    WHERE season_year = (SELECT MAX(season_year) FROM {{ ref('int__combined_player_boxscore') }})
),

position_defense AS (
    SELECT
        pd.*
    FROM {{ ref('int__position_defense_profile') }} pd
),

final AS (
    SELECT
        -- Key identifiers
        ug.game_id,
        pd.player_id,
        ug.team_id,
        ug.opponent_id,
        ug.game_date,
        ug.season_year,
        ug.home_away,
        MD5(CONCAT(ug.game_team_key, '-', pd.player_id)) AS player_game_key,
        
        -- Player details
        pd.player_name,
        pd.position,
        
        -- Position matchup metrics
        pdef.avg_pts_allowed_to_position,
        pdef.avg_reb_allowed_to_position,
        pdef.avg_ast_allowed_to_position,
        pdef.avg_stl_allowed_to_position,
        pdef.avg_blk_allowed_to_position,
        pdef.avg_fg_pct_allowed_to_position,
        pdef.l10_pts_allowed_to_position,
        
        -- Comparative metrics
        pdef.pts_vs_league_avg,
        pdef.reb_vs_league_avg,
        pdef.ast_vs_league_avg,
        
        -- Matchup quality indicators
        pdef.pts_matchup_quality,
        pdef.reb_matchup_quality,
        pdef.ast_matchup_quality,
        
        -- Overall matchup score (simplified example - would customize based on position)
        CASE
            WHEN pd.position = 'G' THEN 
                (CASE WHEN pdef.pts_matchup_quality = 'HIGH' THEN 3 ELSE 
                  CASE WHEN pdef.pts_matchup_quality = 'AVERAGE' THEN 2 ELSE 1 END END) * 0.5 +
                (CASE WHEN pdef.ast_matchup_quality = 'HIGH' THEN 3 ELSE 
                  CASE WHEN pdef.ast_matchup_quality = 'AVERAGE' THEN 2 ELSE 1 END END) * 0.5
            WHEN pd.position = 'F' THEN 
                (CASE WHEN pdef.pts_matchup_quality = 'HIGH' THEN 3 ELSE 
                  CASE WHEN pdef.pts_matchup_quality = 'AVERAGE' THEN 2 ELSE 1 END END) * 0.4 +
                (CASE WHEN pdef.reb_matchup_quality = 'HIGH' THEN 3 ELSE 
                  CASE WHEN pdef.reb_matchup_quality = 'AVERAGE' THEN 2 ELSE 1 END END) * 0.6
            WHEN pd.position = 'C' THEN 
                (CASE WHEN pdef.pts_matchup_quality = 'HIGH' THEN 3 ELSE 
                  CASE WHEN pdef.pts_matchup_quality = 'AVERAGE' THEN 2 ELSE 1 END END) * 0.3 +
                (CASE WHEN pdef.reb_matchup_quality = 'HIGH' THEN 3 ELSE 
                  CASE WHEN pdef.reb_matchup_quality = 'AVERAGE' THEN 2 ELSE 1 END END) * 0.7
            ELSE 2
        END AS matchup_score,
        
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM upcoming_games ug
    JOIN player_data pd
        ON ug.team_id = pd.team_id
    LEFT JOIN position_defense pdef
        ON ug.opponent_id = pdef.opponent_id
        AND pd.position = pdef.position
        AND ug.season_year = pdef.season_year
)

SELECT * FROM final