{{ config(materialized='table') }}

WITH arena_source AS (
    SELECT DISTINCT
        arena_name,
        arena_city,
        arena_state,
        arena_country
    FROM {{ ref('int_game__schedules') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['arena_name', 'arena_city', 'arena_state']) }} AS arena_key,
        arena_name,
        arena_city,
        arena_state,
        COALESCE(arena_country, 'USA') AS arena_country,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM arena_source
)

SELECT * FROM final
