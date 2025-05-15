-- Test to ensure no null values in critical prediction fields
SELECT
    player_game_key,
    player_id,
    game_id,
    blended_pts_projection,
    blended_reb_projection,
    blended_ast_projection
FROM {{ ref('feat_opp___player_vs_opponent') }}
WHERE 
    blended_pts_projection IS NULL OR
    blended_reb_projection IS NULL OR
    blended_ast_projection IS NULL