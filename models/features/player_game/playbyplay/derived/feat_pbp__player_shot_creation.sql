-- feat_pbp__player_shot_creation.sql
WITH shot_events AS (
    SELECT
        game_id,
        player_id,
        event_number,
        action_type,
        shot_zone,
        shot_made_flag,
        points,
        -- Extract player who assisted the shot
        LAG(player_id) OVER (
            PARTITION BY game_id 
            ORDER BY seconds_elapsed_game, event_number
        ) AS potential_assisting_player,
        
        -- Check if previous event was an assist
        LAG(action_type) OVER (
            PARTITION BY game_id 
            ORDER BY seconds_elapsed_game, event_number
        ) AS previous_action_type,
        
        -- Time since previous event
        seconds_elapsed_game - LAG(seconds_elapsed_game) OVER (
            PARTITION BY game_id 
            ORDER BY seconds_elapsed_game, event_number
        ) AS seconds_since_last_event
    FROM {{ ref('int_pbp__player_game_events') }}
    WHERE action_type = 'SHOT'
),

shot_creation_data AS (
    SELECT
        game_id,
        player_id,
        shot_zone,
        shot_made_flag,
        points,
        CASE 
            WHEN previous_action_type = 'ASSIST' THEN 'Assisted'
            WHEN seconds_since_last_event < 3 THEN 'Quick Shot'
            ELSE 'Self Created'
        END AS shot_creation_type,
        CASE 
            WHEN previous_action_type = 'ASSIST' THEN potential_assisting_player
            ELSE NULL
        END AS assisting_player_id
    FROM shot_events
)

SELECT
    game_id,
    player_id,
    shot_zone,
    shot_creation_type,
    COUNT(*) AS shots,
    SUM(CASE WHEN shot_made_flag THEN 1 ELSE 0 END) AS makes,
    SUM(points) AS points
FROM shot_creation_data
GROUP BY 1, 2, 3, 4