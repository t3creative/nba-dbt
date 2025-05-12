{{ config(materialized='table',
    tags=['core', 'teams'],
    unique_key=['team_id'],
    partition_by={
        'field': 'team_id',
        'data_type': 'integer',
        'granularity': 'month'}
) }}

WITH team_source AS (
    SELECT * FROM {{ ref('stg__teams') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['team_id']) }} AS team_key,
        team_id,
    -- Team Info
        team_full_name,
        team_tricode,
        team_nickname,
        team_city,
        team_state,
        -- Additional Info
        year_founded,
        championship_years,
        league,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM team_source
)

SELECT * FROM final
