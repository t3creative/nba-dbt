{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'teams') }}
),

renamed AS (
    SELECT
        CAST("id" AS INTEGER) AS team_id,
        "full_name" AS team_full_name,
        "abbreviation" AS team_tricode,
        "nickname" AS team_nickname,
        "city" AS team_city,
        "state" AS team_state,
        CAST("year_founded" AS INTEGER) AS year_founded,
        "championship_years" AS championship_years,
        "league" AS league
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['team_id']) }} AS team_key,
    -- Identifiers
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
    league
FROM
    renamed 