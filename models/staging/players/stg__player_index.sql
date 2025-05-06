{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'player_index') }}
),

renamed AS (
    SELECT
        CAST("PERSON_ID" AS INTEGER) AS player_id,
        "PLAYER_LAST_NAME" AS player_last_name,
        "PLAYER_FIRST_NAME" AS player_first_name,
        "PLAYER_SLUG" AS player_slug,
        CAST("TEAM_ID" AS INTEGER) AS team_id,
        "TEAM_SLUG" AS team_slug,
        CAST("IS_DEFUNCT" AS BOOLEAN) AS is_defunct,
        "TEAM_CITY" AS team_city,
        "TEAM_NAME" AS team_name,
        "TEAM_ABBREVIATION" AS team_abbreviation,
        "JERSEY_NUMBER" AS jersey_number,
        "POSITION" AS position,
        "HEIGHT" AS height,
        "WEIGHT" AS weight,
        "COLLEGE" AS college,
        "COUNTRY" AS country,
        CAST("DRAFT_YEAR" AS INTEGER) AS draft_year,
        CAST("DRAFT_ROUND" AS INTEGER) AS draft_round,
        CAST("DRAFT_NUMBER" AS INTEGER) AS draft_number,
        "ROSTER_STATUS" AS roster_status,
        CAST("FROM_YEAR" AS INTEGER) AS from_year,
        CAST("TO_YEAR" AS INTEGER) AS to_year,
        CAST("PTS" AS NUMERIC) AS pts,
        CAST("REB" AS NUMERIC) AS reb,
        CAST("AST" AS NUMERIC) AS ast,
        "STATS_TIMEFRAME" AS stats_timeframe
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS player_key,
    -- Identifiers
    player_id,
    team_id,
    -- Player Info
    player_first_name,
    player_last_name,
    player_slug,
    -- Team Info
    team_slug,
    is_defunct,
    team_city,
    team_name,
    team_abbreviation,
    -- Player Details
    jersey_number,
    position,
    height,
    weight,
    college,
    country,
    -- Draft Info
    draft_year,
    draft_round,
    draft_number,
    -- Career Info
    roster_status,
    from_year,
    to_year,
    -- Stats
    pts,
    reb,
    ast,
    stats_timeframe
FROM
    renamed 