{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('nba_api', 'boxscore_line_score') }}
),

renamed AS (
    SELECT
        "GAME_ID" AS game_id,
        CAST(
            "TEAM_ID" AS INTEGER
        ) AS team_id,
        CAST(
            "GAME_DATE_EST" AS DATE
        ) AS game_date_est,
        CAST(
            "GAME_SEQUENCE" AS INTEGER
        ) AS game_sequence,
        "TEAM_ABBREVIATION" AS team_tricode,
        "TEAM_CITY_NAME" AS team_city_name,
        "TEAM_NICKNAME" AS team_nickname,
        "TEAM_WINS_LOSSES" AS team_wins_losses, -- Text, like "10-5"
        CAST(
            "PTS_QTR1" AS INTEGER
        ) AS pts_qtr1,
        CAST(
            "PTS_QTR2" AS INTEGER
        ) AS pts_qtr2,
        CAST(
            "PTS_QTR3" AS INTEGER
        ) AS pts_qtr3,
        CAST(
            "PTS_QTR4" AS INTEGER
        ) AS pts_qtr4,
        CAST(
            "PTS_OT1" AS INTEGER
        ) AS pts_ot1,
        CAST(
            "PTS_OT2" AS INTEGER
        ) AS pts_ot2,
        CAST(
            "PTS_OT3" AS INTEGER
        ) AS pts_ot3,
        CAST(
            "PTS_OT4" AS INTEGER
        ) AS pts_ot4,
        CAST(
            "PTS_OT5" AS INTEGER
        ) AS pts_ot5,
        CAST(
            "PTS_OT6" AS INTEGER
        ) AS pts_ot6,
        CAST(
            "PTS_OT7" AS INTEGER
        ) AS pts_ot7,
        CAST(
            "PTS_OT8" AS INTEGER
        ) AS pts_ot8,
        CAST(
            "PTS_OT9" AS INTEGER
        ) AS pts_ot9,
        CAST(
            "PTS_OT10" AS INTEGER
        ) AS pts_ot10,
        CAST(
            "PTS" AS INTEGER
        ) AS pts
    FROM
        source
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['game_id', 'team_id']) }} AS game_team_line_score_key,
    -- Identifiers
    game_id,
    team_id,
    -- Game & Team Info
    game_date_est,
    game_sequence,
    team_tricode,
    team_city_name,
    team_nickname,
    team_wins_losses,
    -- Points per Period
    pts_qtr1,
    pts_qtr2,
    pts_qtr3,
    pts_qtr4,
    pts_ot1,
    pts_ot2,
    pts_ot3,
    pts_ot4,
    pts_ot5,
    pts_ot6,
    pts_ot7,
    pts_ot8,
    pts_ot9,
    pts_ot10,
    pts -- Total points
FROM
    renamed 