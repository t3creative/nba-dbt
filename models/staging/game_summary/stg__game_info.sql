{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw_boxscores', 'boxscore_game_info') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        -- Cast GAME_DATE from text to date using standard CAST
        CAST(
            "GAME_DATE" AS DATE
        ) AS game_date,
        -- Cast ATTENDANCE to integer
        CAST(
            "ATTENDANCE" AS INTEGER
        ) AS attendance,
        -- Keep GAME_TIME as text, potential for interval conversion later
        "GAME_TIME" AS game_time
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_info_key,
    -- Identifiers
    game_id,
    -- Game Info
    game_date,
    attendance,
    game_time
FROM
    renamed 