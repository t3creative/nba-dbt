{{ config(materialized='table',
    tags=['core', 'games'],
    unique_key=['game_id'],
    partition_by={
        'field': 'game_id',
        'data_type': 'integer',
        'granularity': 'month'}
) }}

WITH game_source AS (
    SELECT * FROM {{ ref('int__game_summary') }}
),

arena_source AS (
    SELECT 
        game_id,
        arena_name,
        arena_city,
        arena_state,
        arena_country
    FROM {{ ref('int__game_context') }}
),

joined_data AS (
    SELECT
        g.*,
        a.arena_name,
        a.arena_city,
        a.arena_state,
        a.arena_country
    FROM game_source g
    LEFT JOIN arena_source a
        ON g.game_id = a.game_id
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_key,
        game_id,
        game_date,
        home_team_id,
        away_team_id,
        arena_name,
        arena_city,
        arena_state,
        attendance,
        game_time,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM joined_data
)

SELECT * FROM final
