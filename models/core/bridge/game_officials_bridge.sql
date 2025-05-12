{{ config(
    materialized='table',
    tags=['core', 'bridge', 'games', 'officials'],
    unique_key=['game_id', 'official_id'],
    partition_by={
        'field': 'game_date',
        'data_type': 'date',
        'granularity': 'month'
    }
) }}

WITH officials_source AS (
    SELECT 
        *
    FROM {{ ref('stg__game_officials') }}
),

games_source AS (
    SELECT
        game_id,
        game_key,
        game_date,
        season,
        season_type
    FROM {{ ref('dim__games') }}
),

officials_dim AS (
    SELECT
        official_id,
        official_key,
        first_name,
        last_name
    FROM {{ ref('dim__officials') }}
    WHERE is_current = TRUE
),

joined_data AS (
    SELECT
        o.game_id,
        g.game_key,
        g.game_date,
        g.season_year,
        g.season_type,
        o.official_id,
        od.official_key,
        od.first_name,
        od.last_name,
        o.jersey_num,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM officials_source o
    INNER JOIN games_source g
        ON o.game_id = g.game_id
    INNER JOIN officials_dim od
        ON o.official_id = od.official_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['game_id', 'official_id']) }} AS game_official_key,
    game_id,
    game_key,
    game_date,
    season_year,
    season_type,
    official_id,
    official_key,
    first_name,
    last_name,
    jersey_num,
    valid_from,
    valid_to,
    is_current
FROM joined_data
