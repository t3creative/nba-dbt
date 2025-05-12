{{ config(
    materialized='table',
    tags=['core', 'bridge', 'players', 'seasons'],
    unique_key=['player_id', 'season', 'season_type'],
    partition_by={
        'field': 'season',
        'data_type': 'string',
        'granularity': 'all'
    }
) }}

WITH player_source AS (
    SELECT * FROM {{ ref('dim__players') }}
    WHERE is_current = TRUE
),

season_source AS (
    SELECT * FROM {{ ref('dim__seasons') }}
    WHERE season_type = 'Regular Season' -- Focus on regular seasons for player career tracking
),

-- Get the distinct seasons from player boxscores data
player_seasons AS (
    SELECT DISTINCT
        player_id,
        season_year,
        'Regular Season' AS season_type
    FROM {{ ref('int__combined_player_boxscore') }}
),

-- Get player career range
player_career AS (
    SELECT
        player_id,
        from_year,
        to_year
    FROM player_source
),

-- Join players with the seasons they participated in
player_season_data AS (
    SELECT
        ps.player_id,
        p.player_key,
        p.player_first_name,
        p.player_last_name,
        ps.season_year,
        ps.season_type,
        s.season_key,
        s.start_date,
        s.end_date,
        -- Calculate player age for the season
        EXTRACT(YEAR FROM s.start_date) - p.draft_year + 22 AS estimated_age, -- Rough estimate based on draft year
        -- Determine player status
        CASE
            WHEN CAST(ps.season_year AS INTEGER) BETWEEN pc.from_year AND pc.to_year THEN 'Active'
            WHEN CAST(ps.season_year AS INTEGER) > pc.to_year THEN 'Retired'
            ELSE 'Unknown'
        END AS player_status,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM player_seasons ps
    INNER JOIN player_source p
        ON ps.player_id = p.player_id
    INNER JOIN season_source s
        ON ps.season_year = s.season_year
        AND ps.season_type = s.season_type
    LEFT JOIN player_career pc
        ON ps.player_id = pc.player_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['player_id', 'season_year', 'season_type']) }} AS player_season_key,
    player_id,
    player_key,
    season_year,
    season_type,
    season_key,
    player_first_name,
    player_last_name,
    estimated_age,
    player_status,
    start_date,
    end_date,
    valid_from,
    valid_to,
    is_current
FROM player_season_data
