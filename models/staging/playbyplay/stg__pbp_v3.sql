{{
    config(
        schema='staging',
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('nba_api', 'playbyplay_v3_events') }}
),

cleaned AS (
    SELECT
        "gameId",
        TRIM("actionNumber"::text) AS action_number_clean,
        "clock",
        TRIM("period"::text) AS period_clean,
        TRIM("teamId"::text) AS team_id_clean,
        "teamTricode",
        TRIM("personId"::text) AS person_id_clean,
        "playerName",
        TRIM("xLegacy"::text) AS shot_x_clean,
        TRIM("yLegacy"::text) AS shot_y_clean,
        TRIM("shotDistance"::text) AS shot_distance_clean,
        "shotResult",
        TRIM("isFieldGoal"::text) AS is_field_goal_clean,
        TRIM("shotValue"::text) AS shot_value_clean,
        TRIM("scoreHome"::text) AS home_score_clean,
        TRIM("scoreAway"::text) AS away_score_clean,
        TRIM("pointsTotal"::text) AS points_clean,
        "location",
        "description",
        "actionType",
        "subType",
        "actionId"
    FROM source
),

renamed AS (
    SELECT
        -- Primary keys and identifiers
        "gameId"::varchar as game_id,
        CASE 
            WHEN action_number_clean = '' THEN NULL
            ELSE action_number_clean::integer
        END as event_number,
        "clock"::varchar as clock,
        CASE 
            WHEN period_clean = '' THEN NULL
            ELSE period_clean::integer
        END as period,
        CASE 
            WHEN team_id_clean = '' THEN NULL
            ELSE team_id_clean::integer
        END as team_id,
        "teamTricode"::varchar as team_tricode,
        CASE 
            WHEN person_id_clean = '' THEN NULL
            ELSE person_id_clean::integer
        END as player_id,
        "playerName"::varchar as player_name,
        
        -- Shot data
        CASE 
            WHEN shot_x_clean = '' THEN NULL
            ELSE shot_x_clean::decimal(10,2)
        END as shot_x,
        CASE 
            WHEN shot_y_clean = '' THEN NULL
            ELSE shot_y_clean::decimal(10,2)
        END as shot_y,
        CASE 
            WHEN shot_distance_clean = '' THEN NULL
            ELSE shot_distance_clean::decimal(10,2)
        END as shot_distance,
        "shotResult"::varchar as shot_result,
        CASE 
            WHEN is_field_goal_clean = '' THEN NULL
            WHEN is_field_goal_clean = '0' THEN false
            ELSE true
        END as is_field_goal,
        CASE 
            WHEN shot_value_clean = '' THEN NULL
            ELSE shot_value_clean::integer
        END as shot_value,
        
        -- Score data
        CASE 
            WHEN home_score_clean = '' THEN NULL
            ELSE home_score_clean::integer
        END as home_score,
        CASE 
            WHEN away_score_clean = '' THEN NULL
            ELSE away_score_clean::integer
        END as away_score,
        CASE 
            WHEN points_clean = '' THEN NULL
            ELSE points_clean::integer
        END as points,
        
        -- Descriptive data
        "location"::varchar as location,
        "description"::varchar as description,
        "actionType"::varchar as action_type,
        "subType"::varchar as sub_type,
        "actionId"::varchar as action_id
    FROM cleaned
),

final AS (
    SELECT
        *,
        -- Add derived columns
        CASE 
            WHEN shot_distance < 5 THEN 'At Rim'
            WHEN shot_distance < 15 THEN 'Mid Range'
            WHEN shot_distance < 23.75 THEN 'Long Mid Range'
            ELSE '3PT'
        END as shot_zone,
        EXTRACT(EPOCH FROM CAST(clock AS TIME)) as seconds_remaining_in_period,
        
        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    FROM renamed
)

SELECT * FROM final