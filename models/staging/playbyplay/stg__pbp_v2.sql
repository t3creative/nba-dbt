{{
    config(
        schema='staging',
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('nba_api', 'playbyplay_v2_events') }}
),

renamed AS (
    SELECT
        -- Primary keys and identifiers
        "GAME_ID"::varchar as game_id,
        "EVENTNUM"::integer as event_num,
        "EVENTMSGTYPE"::integer as event_type,
        "EVENTMSGACTIONTYPE"::integer as event_action_type,
        "PERIOD"::integer as period,
        
        -- Time data
        "WCTIMESTRING"::varchar as wall_clock_time,
        "PCTIMESTRING"::varchar as game_clock_time,
        
        -- Descriptions
        "HOMEDESCRIPTION"::varchar as home_description,
        "NEUTRALDESCRIPTION"::varchar as neutral_description,
        "VISITORDESCRIPTION"::varchar as away_description,
        
        -- Score data
        "SCORE"::varchar as score,
        "SCOREMARGIN"::varchar as score_margin,
        
        -- Person 1 data
        "PERSON1TYPE"::varchar as person1_type,
        "PLAYER1_ID"::integer as player1_id,
        "PLAYER1_NAME"::varchar as player1_name,
        "PLAYER1_TEAM_ID"::integer as player1_team_id,
        "PLAYER1_TEAM_CITY"::varchar as player1_team_city,
        "PLAYER1_TEAM_NICKNAME"::varchar as player1_team_nickname,
        "PLAYER1_TEAM_ABBREVIATION"::varchar as player1_team_tricode,
        
        -- Person 2 data
        "PERSON2TYPE"::varchar as person2_type,
        "PLAYER2_ID"::integer as player2_id,
        "PLAYER2_NAME"::varchar as player2_name,
        "PLAYER2_TEAM_ID"::integer as player2_team_id,
        "PLAYER2_TEAM_CITY"::varchar as player2_team_city,
        "PLAYER2_TEAM_NICKNAME"::varchar as player2_team_nickname,
        "PLAYER2_TEAM_ABBREVIATION"::varchar as player2_team_tricode,
        
        -- Person 3 data
        "PERSON3TYPE"::varchar as person3_type,
        "PLAYER3_ID"::integer as player3_id,
        "PLAYER3_NAME"::varchar as player3_name,
        "PLAYER3_TEAM_ID"::integer as player3_team_id,
        "PLAYER3_TEAM_CITY"::varchar as player3_team_city,
        "PLAYER3_TEAM_NICKNAME"::varchar as player3_team_nickname,
        "PLAYER3_TEAM_ABBREVIATION"::varchar as player3_team_tricode,
        
        -- Miscellaneous
        "VIDEO_AVAILABLE_FLAG"::boolean as video_available
    FROM source
),

parsed AS (
    SELECT
        *,
        -- Parse the score string into separate scores
        CASE 
            WHEN score IS NOT NULL THEN 
                SPLIT_PART(score, ' - ', 1)::INTEGER 
            ELSE NULL 
        END as home_score,
        CASE 
            WHEN score IS NOT NULL THEN 
                SPLIT_PART(score, ' - ', 2)::INTEGER 
            ELSE NULL 
        END as away_score,
        
        -- Translate event types to descriptive names
        CASE
            WHEN event_type = 1 THEN 'SHOT'
            WHEN event_type = 2 THEN 'SHOT_MISS'
            WHEN event_type = 3 THEN 'FREE_THROW'
            WHEN event_type = 4 THEN 'REBOUND'
            WHEN event_type = 5 THEN 'TURNOVER'
            WHEN event_type = 6 THEN 'FOUL'
            WHEN event_type = 7 THEN 'VIOLATION'
            WHEN event_type = 8 THEN 'SUBSTITUTION'
            WHEN event_type = 9 THEN 'TIMEOUT'
            WHEN event_type = 10 THEN 'JUMP_BALL'
            WHEN event_type = 11 THEN 'EJECTION'
            WHEN event_type = 12 THEN 'START_PERIOD'
            WHEN event_type = 13 THEN 'END_PERIOD'
            ELSE 'OTHER'
        END as event_type_name,
        
        -- Convert game clock to seconds
        EXTRACT(EPOCH FROM CAST(game_clock_time AS TIME)) as seconds_remaining_in_period,
        
        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    FROM renamed
)

SELECT * FROM parsed