{{ config(
    materialized='table',
    tags=['core', 'bridge', 'teams', 'seasons'],
    unique_key=['team_id', 'season', 'season_type'],
    partition_by={
        'field': 'season',
        'data_type': 'string',
        'granularity': 'all'
    }
) }}

WITH team_source AS (
    SELECT * FROM {{ ref('dim__teams') }}
    WHERE is_current = TRUE
),

season_source AS (
    SELECT * FROM {{ ref('dim__seasons') }}
),

-- Get the distinct seasons from game data to ensure accurate team season records
game_seasons AS (
    SELECT DISTINCT
        season_year,
        season_type,
        home_team_id AS team_id
    FROM {{ ref('dim__games') }}
    
    UNION
    
    SELECT DISTINCT
        season_year,
        season_type,
        away_team_id AS team_id
    FROM {{ ref('dim__games') }}
),

-- Join teams with the seasons they participated in
team_seasons AS (
    SELECT
        ts.team_id,
        t.team_key,
        t.team_tricode,
        t.team_nickname,
        t.team_city,
        t.team_state,
        ts.season_year,
        ts.season_type,
        s.season_key,
        s.season_type_sort,
        s.start_date,
        s.end_date,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM game_seasons ts
    INNER JOIN team_source t
        ON ts.team_id = t.team_id
    INNER JOIN season_source s
        ON ts.season = s.season
        AND ts.season_type = s.season_type
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_id', 'season_year', 'season_type']) }} AS team_season_key,
    team_id,
    team_key,
    team_tricode,
    team_nickname,
    team_city,
    team_state,
    season_year,
    season_type,
    season_key,
    season_type_sort,
    conference,
    division,
    start_date,
    end_date,
    valid_from,
    valid_to,
    is_current
FROM team_seasons
