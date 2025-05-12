{{ config(materialized='table',
    tags=['core', 'officials'],
    unique_key=['official_id'],
    partition_by={
        'field': 'official_id',
        'data_type': 'integer',
        'granularity': 'month'}
) }}

WITH official_source AS (
    SELECT DISTINCT
        official_id,
        first_name,
        last_name,
        jersey_num
    FROM {{ ref('stg__game_officials') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['official_id']) }} AS official_key,
        official_id,
        first_name,
        last_name,
        jersey_num,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM official_source
)

SELECT * FROM final
