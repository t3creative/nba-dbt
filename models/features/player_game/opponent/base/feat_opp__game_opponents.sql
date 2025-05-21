{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'opp', 'base', 'game_structure'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
) }}

WITH source_schedules AS (
    /* Select all needed columns from the source and deduplicate at game level */
    SELECT DISTINCT ON (game_id)
        game_id,
        season_year,
        game_date,
        home_team_id,
        away_team_id
    FROM {{ ref('stg__schedules') }} AS stg__schedules
    WHERE
        /* Ensure necessary IDs are present */
        home_team_id IS NOT NULL 
        AND away_team_id IS NOT NULL
        AND home_team_id <> away_team_id
    {% if is_incremental() %}
        /* Process only new games based on game_date */
        AND game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
    ORDER BY 
        game_id,
        game_date DESC NULLS LAST,
        /* If multiple records exist for same game_id and date, 
           ensure deterministic selection */
        home_team_id,
        away_team_id
), schedules AS (
    /* Filter for relevant seasons */
    SELECT
        game_id,
        season_year,
        game_date,
        home_team_id,
        away_team_id
    FROM source_schedules
    WHERE season_year >= '2017-18'
), home_team_perspective AS (
    /* Create one row for the home team's perspective */
    SELECT
        game_id,
        season_year,
        game_date,
        home_team_id AS team_id,
        away_team_id AS opponent_id,
        'HOME' AS home_away,
        /* Use a more robust surrogate key generation */
        {{ dbt_utils.generate_surrogate_key(['game_id', 'home_team_id']) }} AS team_game_key
    FROM schedules
), away_team_perspective AS (
    /* Create one row for the away team's perspective */
    SELECT
        game_id,
        season_year,
        game_date,
        away_team_id AS team_id,
        home_team_id AS opponent_id,
        'AWAY' AS home_away,
        /* Use a more robust surrogate key generation */
        {{ dbt_utils.generate_surrogate_key(['game_id', 'away_team_id']) }} AS team_game_key
    FROM schedules
), final AS (
    /* Combine both perspectives */
    SELECT 
        game_id,
        season_year,
        game_date,
        team_id,
        opponent_id,
        home_away,
        team_game_key,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM home_team_perspective
    UNION ALL
    SELECT 
        game_id,
        season_year,
        game_date,
        team_id,
        opponent_id,
        home_away,
        team_game_key,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM away_team_perspective
)
SELECT *
FROM final