-- Test to ensure no null values in critical prediction fields
SELECT
    player_game_key,
    player_id,
    team_id,
    game_date,
    team_adjusted_pts_projection,
    team_adjusted_reb_projection,
    team_adjusted_ast_projection
FROM {{ ref('marts__team_context_features') }}
WHERE 
    team_adjusted_pts_projection IS NULL OR
    team_adjusted_reb_projection IS NULL OR
    team_adjusted_ast_projection IS NULL