{{ config(materialized='table',
    tags=['core', 'players'],
    unique_key=['player_id'],
    partition_by={
        'field': 'player_id',
        'data_type': 'integer',
        'granularity': 'month'}
) }}

WITH player_source AS (
    SELECT * FROM {{ ref('stg__player_index') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS player_key,
        player_id,
        player_first_name,
        player_last_name,
        player_slug,
        position,
        height,
        weight,
        college,
        country,
        draft_year,
        draft_round,
        draft_number,
        from_year,
        to_year,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM player_source
)

SELECT * FROM final
