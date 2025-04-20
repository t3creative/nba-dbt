{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw_boxscores', 'boxscore_game_summary') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        CAST(
            "GAME_DATE_EST" AS DATE
        ) AS game_date_est,
        CAST(
            "GAME_SEQUENCE" AS INTEGER
        ) AS game_sequence,
        CAST(
            "GAME_STATUS_ID" AS INTEGER
        ) AS game_status_id,
        "GAME_STATUS_TEXT" AS game_status_text,
        "GAMECODE" AS gamecode,
        CAST(
            "HOME_TEAM_ID" AS INTEGER
        ) AS home_team_id,
        CAST(
            "VISITOR_TEAM_ID" AS INTEGER
        ) AS visitor_team_id,
        "SEASON" AS season,
        CAST(
            "LIVE_PERIOD" AS INTEGER
        ) AS live_period,
        "LIVE_PC_TIME" AS live_pc_time, -- Text, might represent time remaining
        "NATL_TV_BROADCASTER_ABBREVIATION" AS natl_tv_broadcaster_abbreviation,
        "LIVE_PERIOD_TIME_BCAST" AS live_period_time_bcast, -- Text
        CAST(
            "WH_STATUS" AS INTEGER
        ) AS wh_status -- Warehouse status? Unclear meaning
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_summary_key,
    -- Identifiers
    game_id,
    home_team_id,
    visitor_team_id,
    -- Game Info
    game_date_est,
    game_sequence,
    game_status_id,
    game_status_text,
    gamecode,
    season,
    -- Live Game Info
    live_period,
    live_pc_time,
    live_period_time_bcast,
    -- Other
    natl_tv_broadcaster_abbreviation,
    wh_status
FROM
    renamed 