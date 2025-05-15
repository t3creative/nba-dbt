{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='game_id',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_id",
            "data_type": "text",
            "granularity": "day"
        },
        indexes=[
            {'columns': ['game_id']}
        ]
    )
}}

WITH events AS (
    SELECT * FROM {{ ref('int_pbp__combined_events') }}
    {% if is_incremental() %}
    WHERE game_id > (SELECT max(game_id) FROM {{ this }})
    {% endif %}
),

-- Calculate time elapsed in the game based on period and game clock
game_time AS (
    SELECT
        game_id,
        event_number,
        period,
        game_clock_time,
        -- Convert game_clock_time to seconds elapsed
        (CASE 
            WHEN period = 1 THEN 0
            WHEN period = 2 THEN 12*60
            WHEN period = 3 THEN 24*60
            WHEN period = 4 THEN 36*60
            ELSE (4*12*60) + ((period-5)*5*60)
        END) + 
        (12*60 - (
            CASE 
                WHEN game_clock_time ~ '^[0-9]+:[0-9]+$' 
                THEN 
                    (SPLIT_PART(game_clock_time, ':', 1)::integer * 60) + 
                    SPLIT_PART(game_clock_time, ':', 2)::integer
                ELSE 0
            END
        )) AS seconds_elapsed_game
    FROM events
),

possessions AS (
    SELECT
        e.game_id,
        e.period,
        -- Identify possession changes using player team information instead of direct team_id
        COALESCE(e.player1_team_id, e.player2_team_id, e.player3_team_id) AS team_id,
        COALESCE(e.player1_team_tricode, e.player2_team_tricode, e.player3_team_tricode) AS team_code,
        e.event_number AS action_number,
        gt.seconds_elapsed_game,
        e.action_type AS event_type_name,
        e.event_action_type AS action_type,
        e.shot_result,
        
        -- Mark likely possession-ending events
        CASE
            WHEN e.action_type = 'SHOT' AND e.shot_result = 'Made' THEN 1
            WHEN e.action_type = 'SHOT' AND e.shot_result = 'Missed' AND 
                 LEAD(e.action_type, 1) OVER (PARTITION BY e.game_id ORDER BY e.event_number) = 'REBOUND' AND
                 COALESCE(e.player1_team_id, e.player2_team_id, e.player3_team_id) != 
                 LEAD(COALESCE(e.player1_team_id, e.player2_team_id, e.player3_team_id), 1) OVER (PARTITION BY e.game_id ORDER BY e.event_number) THEN 1
            WHEN e.action_type = 'TURNOVER' THEN 1
            ELSE 0
        END AS possession_end_flag,
        
        -- Calculate time spent on each event
        gt.seconds_elapsed_game - LAG(gt.seconds_elapsed_game, 1, gt.seconds_elapsed_game) OVER (
            PARTITION BY e.game_id
            ORDER BY e.event_number
        ) AS event_duration
        
    FROM events e
    JOIN game_time gt ON e.game_id = gt.game_id AND e.event_number = gt.event_number
),

possession_changes AS (
    SELECT
        game_id,
        period,
        team_id,
        team_code,
        action_number,
        seconds_elapsed_game,
        event_type_name,
        action_type,
        shot_result,
        possession_end_flag,
        
        -- Calculate possession sequences
        SUM(possession_end_flag) OVER (
            PARTITION BY game_id
            ORDER BY action_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS possession_sequence
    FROM possessions
),

-- Add an intermediate CTE to avoid nesting window functions
possession_teams AS (
    SELECT
        game_id,
        period,
        team_id,
        team_code,
        action_number,
        seconds_elapsed_game,
        event_type_name,
        action_type,
        shot_result,
        possession_end_flag,
        possession_sequence,
        -- Track team for each possession - now in a separate CTE
        FIRST_VALUE(team_id) OVER (
            PARTITION BY game_id, possession_sequence
            ORDER BY action_number
        ) AS possession_team_id,
        
        -- Calculate time spent on each event
        -- Get this from the original possessions CTE
        (SELECT p.event_duration FROM possessions p 
         WHERE p.game_id = pc.game_id AND p.action_number = pc.action_number) AS event_duration
        
    FROM possession_changes pc
),

possession_stats AS (
    SELECT
        pc.game_id,
        pc.period,
        pc.possession_sequence,
        pc.possession_team_id,
        MIN(pc.seconds_elapsed_game) AS possession_start_time,
        MAX(pc.seconds_elapsed_game) AS possession_end_time,
        MAX(pc.seconds_elapsed_game) - MIN(pc.seconds_elapsed_game) AS possession_duration,
        
        -- Possession outcome
        MAX(CASE 
            WHEN pc.event_type_name = 'SHOT' AND pc.shot_result = 'Made' AND pc.possession_end_flag = 1 THEN 'SCORE'
            WHEN pc.event_type_name = 'SHOT' AND pc.shot_result = 'Missed' AND pc.possession_end_flag = 1 THEN 'MISS'
            WHEN pc.event_type_name = 'TURNOVER' AND pc.possession_end_flag = 1 THEN 'TURNOVER'
            WHEN pc.possession_end_flag = 1 THEN 'OTHER'
            ELSE NULL
        END) AS possession_outcome
        
    FROM possession_teams pc
    GROUP BY pc.game_id, pc.period, pc.possession_sequence, pc.possession_team_id
),

game_pace AS (
    SELECT
        game_id,
        
        -- Basic possession counts
        COUNT(DISTINCT possession_sequence) AS total_possessions,
        COUNT(DISTINCT CASE WHEN period = 1 THEN possession_sequence ELSE NULL END) AS q1_possessions,
        COUNT(DISTINCT CASE WHEN period = 2 THEN possession_sequence ELSE NULL END) AS q2_possessions,
        COUNT(DISTINCT CASE WHEN period = 3 THEN possession_sequence ELSE NULL END) AS q3_possessions,
        COUNT(DISTINCT CASE WHEN period = 4 THEN possession_sequence ELSE NULL END) AS q4_possessions,
        COUNT(DISTINCT CASE WHEN period > 4 THEN possession_sequence ELSE NULL END) AS ot_possessions,
        
        -- Possession durations
        AVG(possession_duration) AS avg_possession_duration,
        STDDEV(possession_duration) AS std_possession_duration,
        MIN(possession_duration) AS min_possession_duration,
        MAX(possession_duration) AS max_possession_duration,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY possession_duration) AS median_possession_duration,
        
        -- Possession outcomes
        SUM(CASE WHEN possession_outcome = 'SCORE' THEN 1 ELSE 0 END) AS scoring_possessions,
        SUM(CASE WHEN possession_outcome = 'MISS' THEN 1 ELSE 0 END) AS missed_shot_possessions,
        SUM(CASE WHEN possession_outcome = 'TURNOVER' THEN 1 ELSE 0 END) AS turnover_possessions,
        
        -- Pace metrics
        COUNT(DISTINCT possession_sequence) / 
            (CASE 
                WHEN MAX(period) <= 4 THEN 48.0
                ELSE 48.0 + ((MAX(period) - 4) * 5.0)
             END) * 48.0 AS possessions_per_48,
             
        -- Efficiency metrics
        SUM(CASE WHEN possession_outcome = 'SCORE' THEN 1 ELSE 0 END)::FLOAT / 
            NULLIF(COUNT(DISTINCT possession_sequence), 0) AS scoring_efficiency
        
    FROM possession_stats
    GROUP BY game_id
),

team_pace AS (
    SELECT
        game_id,
        possession_team_id,
        
        -- Team possession metrics
        COUNT(DISTINCT possession_sequence) AS team_possessions,
        AVG(possession_duration) AS team_avg_possession_duration,
        STDDEV(possession_duration) AS team_possession_duration_variability,
        
        -- Team possession outcomes
        SUM(CASE WHEN possession_outcome = 'SCORE' THEN 1 ELSE 0 END) AS team_scoring_possessions,
        SUM(CASE WHEN possession_outcome = 'MISS' THEN 1 ELSE 0 END) AS team_missed_shot_possessions,
        SUM(CASE WHEN possession_outcome = 'TURNOVER' THEN 1 ELSE 0 END) AS team_turnover_possessions,
        
        -- Team efficiency
        SUM(CASE WHEN possession_outcome = 'SCORE' THEN 1 ELSE 0 END)::FLOAT / 
            NULLIF(COUNT(DISTINCT possession_sequence), 0) AS team_scoring_efficiency
        
    FROM possession_stats
    GROUP BY game_id, possession_team_id
),

combined_pace AS (
    SELECT
        gp.game_id,
        gp.total_possessions,
        gp.avg_possession_duration,
        gp.possessions_per_48,
        gp.scoring_efficiency,
        
        -- Team-specific pace
        home.possession_team_id AS home_team_id,
        away.possession_team_id AS away_team_id,
        home.team_possessions AS home_team_possessions,
        away.team_possessions AS away_team_possessions,
        home.team_avg_possession_duration AS home_avg_possession_duration,
        away.team_avg_possession_duration AS away_avg_possession_duration,
        
        -- Pace differentials
        home.team_possession_duration_variability - away.team_possession_duration_variability AS possession_variability_diff,
        home.team_scoring_efficiency - away.team_scoring_efficiency AS scoring_efficiency_diff,
        
        -- Game flow metrics
        CASE
            WHEN gp.possessions_per_48 >= 100 THEN 'FAST_PACED'
            WHEN gp.possessions_per_48 >= 95 AND gp.possessions_per_48 < 100 THEN 'MODERATE_FAST'
            WHEN gp.possessions_per_48 >= 90 AND gp.possessions_per_48 < 95 THEN 'MODERATE'
            WHEN gp.possessions_per_48 >= 85 AND gp.possessions_per_48 < 90 THEN 'MODERATE_SLOW'
            ELSE 'SLOW_PACED'
        END AS game_pace_category,
        
        -- Consistency metrics
        CASE
            WHEN ABS(gp.q1_possessions - gp.q4_possessions) <= 3 THEN 'CONSISTENT_PACE'
            WHEN gp.q1_possessions > gp.q2_possessions AND 
                 gp.q2_possessions > gp.q3_possessions AND 
                 gp.q3_possessions > gp.q4_possessions THEN 'DECREASING_PACE'
            WHEN gp.q1_possessions < gp.q2_possessions AND 
                 gp.q2_possessions < gp.q3_possessions AND 
                 gp.q3_possessions < gp.q4_possessions THEN 'INCREASING_PACE'
            ELSE 'VARIABLE_PACE'
        END AS pace_consistency
        
    FROM game_pace gp
    JOIN (SELECT tp1.* FROM team_pace tp1 
          JOIN (SELECT DISTINCT e.game_id, COALESCE(e.player1_team_id, e.player2_team_id, e.player3_team_id) AS team_id 
                FROM events e 
                WHERE e.location = 'home') home_teams
          ON tp1.game_id = home_teams.game_id AND tp1.possession_team_id = home_teams.team_id) home
        ON gp.game_id = home.game_id
    JOIN (SELECT tp2.* FROM team_pace tp2 
          JOIN (SELECT DISTINCT e.game_id, COALESCE(e.player1_team_id, e.player2_team_id, e.player3_team_id) AS team_id 
                FROM events e 
                WHERE e.location = 'away') away_teams
          ON tp2.game_id = away_teams.game_id AND tp2.possession_team_id = away_teams.team_id) away
        ON gp.game_id = away.game_id AND home.possession_team_id != away.possession_team_id
)

SELECT * FROM combined_pace