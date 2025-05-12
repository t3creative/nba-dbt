{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'boxscore_other_stats') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        CAST(
            "TEAM_ID" AS INTEGER
        ) AS team_id,
        "LEAGUE_ID" AS league_id, -- Typically like '00'
        "TEAM_ABBREVIATION" AS team_tricode,
        "TEAM_CITY" AS team_city,
        CAST(
            "PTS_PAINT" AS INTEGER
        ) AS pts_in_paint,
        CAST(
            "PTS_2ND_CHANCE" AS INTEGER
        ) AS second_chance_pts,
        CAST(
            "PTS_FB" AS INTEGER
        ) AS fastbreak_pts, -- Fast break points
        CAST(
            "LARGEST_LEAD" AS INTEGER
        ) AS largest_lead,
        CAST(
            "LEAD_CHANGES" AS INTEGER
        ) AS lead_changes,
        CAST(
            "TIMES_TIED" AS INTEGER
        ) AS times_tied,
        CAST(
            "TEAM_TURNOVERS" AS INTEGER
        ) AS team_turnovers, -- Seems redundant with TOTAL_TURNOVERS
        CAST(
            "TOTAL_TURNOVERS" AS INTEGER
        ) AS total_turnovers,
        CAST(
            "TEAM_REBOUNDS" AS INTEGER
        ) AS team_rebounds,
        CAST(
            "PTS_OFF_TO" AS INTEGER
        ) AS pts_off_to -- Points off turnovers
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id', 'team_id']) }} AS game_team_other_stats_key,
    -- Identifiers
    game_id,
    team_id,
    league_id,
    -- Team Info
    team_tricode,
    team_city,
    -- Stats
    pts_in_paint,
    second_chance_pts,
    fastbreak_pts,
    largest_lead,
    lead_changes,
    times_tied,
    team_turnovers,
    total_turnovers,
    team_rebounds,
    pts_off_to
FROM
    renamed 