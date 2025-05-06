{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'boxscore_inactive_players') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        CAST(
            "PLAYER_ID" AS INTEGER
        ) AS player_id,
        CAST(
            "TEAM_ID" AS INTEGER
        ) AS team_id,
        "FIRST_NAME" AS first_name,
        "LAST_NAME" AS last_name,
        "JERSEY_NUM" AS jersey_num, -- Keep as text, can have non-numeric chars
        "TEAM_CITY" AS team_city,
        "TEAM_NAME" AS team_name,
        "TEAM_ABBREVIATION" AS team_abbreviation
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id', 'player_id']) }} AS game_inactive_player_key,
    -- Identifiers
    game_id,
    player_id,
    team_id,
    -- Player Info
    first_name,
    last_name,
    jersey_num,
    -- Team Info
    team_city,
    team_name,
    team_abbreviation
FROM
    renamed 