{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'boxscore_officials') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        CAST(
            "OFFICIAL_ID" AS INTEGER
        ) AS official_id,
        "FIRST_NAME" AS first_name,
        "LAST_NAME" AS last_name,
        "JERSEY_NUM" AS jersey_num -- Keep as text
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id', 'official_id']) }} AS game_official_key,
    -- Identifiers
    game_id,
    official_id,
    -- Official Info
    first_name,
    last_name,
    jersey_num
FROM
    renamed 