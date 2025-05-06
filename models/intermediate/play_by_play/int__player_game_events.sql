{{
    config(
        schema='intermediate',
        materialized='incremental'
    )
}}
WITH events AS (
    SELECT 
        *,
        -- Calculate seconds_remaining_in_period from game_clock_time if not already available
        CASE
            WHEN game_clock_time IS NOT NULL THEN 
                EXTRACT(EPOCH FROM CAST(game_clock_time AS TIME))
            ELSE NULL
        END AS seconds_remaining_in_period
    FROM {{ ref('int__combined_events') }}
    {% if is_incremental() %}
    -- Filter events based on the incremental logic of the combined table
    WHERE game_id > (SELECT MAX(game_id) FROM {{ this }})
    {% endif %}
),

-- Calculate game timing context first
timed_events AS (
    SELECT
        *,
        -- Calculate total seconds elapsed at the start of the period
        -- Assuming standard 12-minute (720 seconds) periods for periods 1-4
        -- Assuming standard 5-minute (300 seconds) periods for OT (period > 4)
        CASE
            WHEN period <= 4 THEN (period - 1) * 720
            ELSE (4 * 720) + (period - 5) * 300
        END AS seconds_at_period_start,

        -- Calculate seconds elapsed in the period, with null handling
        CASE
            WHEN seconds_remaining_in_period IS NOT NULL THEN
                CASE
                    WHEN period <= 4 THEN 720 - seconds_remaining_in_period
                    ELSE 300 - seconds_remaining_in_period
                END
            ELSE NULL -- Handle null seconds_remaining_in_period
        END AS seconds_elapsed_in_period
    FROM events
),

-- Add derived time-based flags and segments
contextualized_events AS (
    SELECT
        *,
        (seconds_at_period_start + COALESCE(seconds_elapsed_in_period, 0)) AS seconds_elapsed_game,
        CASE
            WHEN period <= 4 THEN 'Regulation'
            ELSE 'Overtime ' || (period - 4)::TEXT
        END AS game_segment,
        (period IN (2, 4) AND COALESCE(seconds_remaining_in_period, 0) <= 180) AS is_clutch_time_end_half, -- Last 3 mins of 2nd/4th
        (period >= 4 AND ABS(COALESCE(score_margin, 0)) <= 5 AND COALESCE(seconds_remaining_in_period, 0) <= 300) AS is_clutch_time_end_game, -- Last 5 mins of 4th/OT if score within 5
        
        -- Add derived score context
        CASE WHEN home_score > away_score THEN home_score - away_score ELSE 0 END AS home_lead,
        CASE WHEN away_score > home_score THEN away_score - home_score ELSE 0 END AS away_lead,
        
        -- Add shot_made_flag for easier filtering
        CASE 
            WHEN shot_result = 'Made' THEN TRUE
            WHEN shot_result = 'Missed' THEN FALSE
            ELSE NULL
        END AS shot_made_flag
    FROM timed_events
),

player_events AS (
    -- Get all events with player1 involvement
    SELECT
        game_id,
        period,
        event_number, -- Added event_number
        seconds_remaining_in_period,
        player1_id AS player_id,
        player1_name AS player_name,
        player1_team_id AS team_id,
        player1_team_tricode AS team_code,
        event_type,
        event_action_type,
        action_type, -- Use action_type directly
        sub_type,
        shot_result,
        shot_value,
        points,
        shot_distance,
        shot_zone,
        shot_x,
        shot_y,
        home_score AS score_home,
        away_score AS score_away,
        score_margin,
        seconds_elapsed_game,
        game_segment,
        is_clutch_time_end_half,
        is_clutch_time_end_game,
        shot_made_flag,
        home_lead,
        away_lead
    FROM contextualized_events
    WHERE player1_id IS NOT NULL

    UNION ALL

    -- Get all events with player2 involvement (e.g., assists, rebounds, blocks)
    SELECT
        game_id,
        period,
        event_number, -- Added event_number
        seconds_remaining_in_period,
        player2_id AS player_id,
        player2_name AS player_name,
        player2_team_id AS team_id,
        player2_team_tricode AS team_code,
        event_type,
        event_action_type,
        action_type,
        sub_type,
        shot_result,
        shot_value,
        points,
        shot_distance,
        shot_zone,
        shot_x,
        shot_y,
        home_score AS score_home,
        away_score AS score_away,
        score_margin,
        seconds_elapsed_game,
        game_segment,
        is_clutch_time_end_half,
        is_clutch_time_end_game,
        shot_made_flag,
        home_lead,
        away_lead
    FROM contextualized_events
    -- Filter for events where player2 is relevant (adjust logic as needed)
    WHERE player2_id IS NOT NULL AND action_type IN ('REBOUND', 'ASSIST', 'BLOCK', 'STEAL', 'FOUL') -- Example filter

    UNION ALL

    -- Get all events with player3 involvement (e.g., fouled, blocked by)
    SELECT
        game_id,
        period,
        event_number, -- Added event_number
        seconds_remaining_in_period,
        player3_id AS player_id,
        player3_name AS player_name,
        player3_team_id AS team_id,
        player3_team_tricode AS team_code,
        event_type,
        event_action_type,
        action_type,
        sub_type,
        shot_result,
        shot_value,
        points,
        shot_distance,
        shot_zone,
        shot_x,
        shot_y,
        home_score AS score_home,
        away_score AS score_away,
        score_margin,
        seconds_elapsed_game,
        game_segment,
        is_clutch_time_end_half,
        is_clutch_time_end_game,
        shot_made_flag,
        home_lead,
        away_lead
    FROM contextualized_events
    -- Filter for events where player3 is relevant (adjust logic as needed)
    WHERE player3_id IS NOT NULL AND action_type IN ('FOUL', 'BLOCK', 'VIOLATION') -- Example filter
),

final AS (
    SELECT
        *,
        -- Create sequence number within game for each player
        ROW_NUMBER() OVER (
            PARTITION BY game_id, player_id
            ORDER BY seconds_elapsed_game, event_number -- Use event_number for consistent sorting
        ) AS player_game_event_seq,

        -- Calculate time elapsed since player's previous event
        seconds_elapsed_game - LAG(seconds_elapsed_game, 1, 0) OVER ( -- Default to 0 for first event
            PARTITION BY game_id, player_id
            ORDER BY seconds_elapsed_game, event_number
        ) AS time_since_last_event
    FROM player_events
)

SELECT * FROM final