{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'boxscore_last_meeting') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        "LAST_GAME_ID" AS last_game_id,
        CAST(
            "LAST_GAME_DATE_EST" AS DATE
        ) AS last_game_date_est,
        CAST(
            "LAST_GAME_HOME_TEAM_ID" AS INTEGER
        ) AS last_game_home_team_id,
        "LAST_GAME_HOME_TEAM_CITY" AS last_game_home_team_city,
        "LAST_GAME_HOME_TEAM_NAME" AS last_game_home_team_name,
        "LAST_GAME_HOME_TEAM_ABBREVIATION" AS last_game_home_team_tricode,
        CAST(
            "LAST_GAME_HOME_TEAM_POINTS" AS INTEGER
        ) AS last_game_home_team_points,
        CAST(
            "LAST_GAME_VISITOR_TEAM_ID" AS INTEGER
        ) AS last_game_away_team_id,
        "LAST_GAME_VISITOR_TEAM_CITY" AS last_game_away_team_city,
        "LAST_GAME_VISITOR_TEAM_NAME" AS last_game_away_team_name,
        "LAST_GAME_VISITOR_TEAM_CITY1" AS last_game_away_team_tricode,
        CAST(
            "LAST_GAME_VISITOR_TEAM_POINTS" AS INTEGER
        ) AS last_game_away_team_points
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_last_meeting_key,
    -- Identifiers
    game_id,
    last_game_id,
    last_game_home_team_id,
    last_game_away_team_id,
    -- Last Game Details
    last_game_date_est,
    -- Home Team
    last_game_home_team_city,
    last_game_home_team_name,
    last_game_home_team_tricode,
    last_game_home_team_points,
    -- Away Team
    last_game_away_team_city,
    last_game_away_team_name,
    last_game_away_team_tricode,
    last_game_away_team_points
FROM
    renamed 