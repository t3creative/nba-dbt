{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    tags=['features', 'opp', 'base', 'stats'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']}
    ],
    incremental_strategy='delete+insert'
) }}

WITH base_opponents AS (
    SELECT DISTINCT ON (team_game_key) *
    FROM {{ ref('feat_opp__game_opponents') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
    ORDER BY team_game_key, game_date DESC
),

opponent_boxscores AS (
    SELECT 
        tb.team_id,
        tb.game_id,
        tb.game_date,
        tb.season_year,
        tb.def_rating,
        tb.off_rating,
        tb.pace,
        tb.pts,
        tb.pts_in_paint,
        tb.fastbreak_pts,
        tb.second_chance_pts,
        tb.opp_pts_in_paint,
        tb.opp_pts_off_tov,
        tb.reb_pct,
        tb.off_reb_pct,
        tb.def_reb_pct,
        tb.blk,
        tb.stl,
        tb.fg_pct,
        tb.fg3_pct,
        tb.ft_pct,
        tb.tov_ratio
    FROM {{ ref('int_team__combined_boxscore') }} tb
)

SELECT
    -- Base fields
    bo.team_game_key,
    bo.game_id,
    bo.team_id,
    bo.opponent_id,
    bo.season_year,
    bo.game_date,
    bo.home_away,
    
    -- Join key for performance stats for the opponent team
    COALESCE(obs.def_rating, 0) AS opp_def_rating,
    COALESCE(obs.off_rating, 0) AS opp_off_rating,
    COALESCE(obs.pace, 0) AS opp_pace,
    COALESCE(obs.pts, 0) AS opp_pts_per_game,
    COALESCE(obs.pts_in_paint, 0) AS opp_pts_in_paint,
    COALESCE(obs.fastbreak_pts, 0) AS opp_fastbreak_pts,
    COALESCE(obs.opp_pts_in_paint, 0) AS opp_allowed_pts_in_paint,
    COALESCE(obs.opp_pts_off_tov, 0) AS opp_allowed_pts_off_tov,
    COALESCE(obs.reb_pct, 0) AS opp_reb_pct,
    COALESCE(obs.off_reb_pct, 0) AS opp_off_reb_pct,
    COALESCE(obs.def_reb_pct, 0) AS opp_def_reb_pct,
    COALESCE(obs.blk, 0) AS opp_blocks_per_game,
    COALESCE(obs.stl, 0) AS opp_steals_per_game,
    COALESCE(obs.fg_pct, 0) AS opp_fg_pct,
    COALESCE(obs.fg3_pct, 0) AS opp_fg3_pct,
    COALESCE(obs.ft_pct, 0) AS opp_ft_pct,
    COALESCE(obs.tov_ratio, 0) AS opp_tov_ratio,
    
    bo.created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM base_opponents bo
LEFT JOIN opponent_boxscores obs
    ON bo.opponent_id = obs.team_id 
    AND bo.game_id = obs.game_id