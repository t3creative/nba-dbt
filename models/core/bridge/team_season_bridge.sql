{{ config(
    materialized='table',
    tags=['core', 'bridge', 'teams', 'seasons'],
    unique_key=['team_id', 'season_year', 'season_type'],
    partition_by={
        'field': 'season_year',
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

-- Get all unique (team_id, season_year, season_type) pairs from games
game_team_seasons AS (
    SELECT DISTINCT
        home_team_id AS team_id,
        game_date,
        game_id
    FROM {{ ref('dim__games') }}
    UNION
    SELECT DISTINCT
        away_team_id AS team_id,
        game_date,
        game_id
    FROM {{ ref('dim__games') }}
),

team_seasons AS (
    SELECT
        gts.team_id,
        t.team_key,
        t.team_tricode,
        t.team_nickname,
        t.team_city,
        t.team_state,
        gts.game_date,
        gts.game_id,
        s.season_year,
        s.season_type,
        s.season_key,
        s.season_type_sort,
        s.start_date,
        s.end_date,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM game_team_seasons gts
    INNER JOIN team_source t
        ON gts.team_id = t.team_id
    INNER JOIN season_source s
        ON gts.game_date BETWEEN s.start_date AND s.end_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_id', 'season_year', 'season_type']) }} AS team_season_key,
    team_id,
    team_key,
    team_tricode,
    team_nickname,
    team_city,
    team_state,
    game_date,
    game_id,
    season_year,
    season_type,
    season_key,
    season_type_sort,
    start_date,
    end_date,
    valid_from,
    valid_to,
    is_current
FROM team_seasons
