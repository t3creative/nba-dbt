{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'boxscore_season_series') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        CAST(
            "HOME_TEAM_ID" AS INTEGER
        ) AS home_team_id,
        CAST(
            "VISITOR_TEAM_ID" AS INTEGER
        ) AS visitor_team_id,
        CAST(
            "GAME_DATE_EST" AS DATE
        ) AS game_date_est,
        CAST(
            "HOME_TEAM_WINS" AS INTEGER
        ) AS home_team_wins,
        CAST(
            "HOME_TEAM_LOSSES" AS INTEGER
        ) AS home_team_losses,
        "SERIES_LEADER" AS series_leader -- Text, e.g., "Tied", "Warriors"
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_season_series_key,
    -- Identifiers
    game_id,
    home_team_id,
    visitor_team_id,
    -- Series Info
    game_date_est,
    home_team_wins,
    home_team_losses,
    series_leader
FROM
    renamed 