{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='game_id || player_id',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_id",
            "data_type": "text",
            "granularity": "day"
        },
        indexes=[
            {'columns': ['game_id']},
            {'columns': ['player_id']},
            {'columns': ['game_id', 'player_id']}
        ]
    )
}}

WITH player_events AS (
    SELECT * FROM {{ ref('int__player_game_events') }}
    {% if is_incremental() %}
    WHERE game_id > (SELECT max(game_id) FROM {{ this }})
    {% endif %}
),

player_stats AS (
    SELECT
        game_id,
        player_id,
        player_name,
        team_id,
        team_code,
        
        -- Event counts by type
        SUM(CASE WHEN action_type = 'SHOT' AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS field_goals_made,
        SUM(CASE WHEN action_type = 'SHOT' AND shot_made_flag = FALSE THEN 1 ELSE 0 END) AS field_goals_missed,
        SUM(CASE WHEN sub_type = 'FREE_THROW' AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS free_throws_made,
        SUM(CASE WHEN sub_type = 'FREE_THROW' AND shot_made_flag = FALSE THEN 1 ELSE 0 END) AS free_throws_missed,
        SUM(CASE WHEN action_type = 'REBOUND' THEN 1 ELSE 0 END) AS rebounds,
        SUM(CASE WHEN action_type = 'TURNOVER' THEN 1 ELSE 0 END) AS turnovers,
        SUM(CASE WHEN action_type = 'ASSIST' THEN 1 ELSE 0 END) AS assists,
        SUM(CASE WHEN action_type = 'STEAL' THEN 1 ELSE 0 END) AS steals,
        SUM(CASE WHEN action_type = 'BLOCK' THEN 1 ELSE 0 END) AS blocks,
        SUM(CASE WHEN action_type = 'FOUL' THEN 1 ELSE 0 END) AS fouls,
        
        -- Points
        SUM(COALESCE(points, 0)) AS total_points,
        
        -- Game context
        MAX(period) AS max_period,
        MAX(seconds_elapsed_game)/60.0 AS minutes_played,
        
        -- Performance under pressure
        SUM(CASE WHEN is_clutch_time_end_game AND action_type = 'SHOT' AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS clutch_shots_made,
        SUM(CASE WHEN is_clutch_time_end_game AND action_type = 'SHOT' AND shot_made_flag = FALSE THEN 1 ELSE 0 END) AS clutch_shots_missed,
        
        -- Performance by game segment
        SUM(CASE WHEN period = 1 THEN COALESCE(points, 0) ELSE 0 END) AS q1_points,
        SUM(CASE WHEN period = 2 THEN COALESCE(points, 0) ELSE 0 END) AS q2_points,
        SUM(CASE WHEN period = 3 THEN COALESCE(points, 0) ELSE 0 END) AS q3_points,
        SUM(CASE WHEN period = 4 THEN COALESCE(points, 0) ELSE 0 END) AS q4_points,
        SUM(CASE WHEN period > 4 THEN COALESCE(points, 0) ELSE 0 END) AS ot_points,
        
        -- Performance by score situation
        SUM(CASE WHEN (home_lead > 10 OR away_lead > 10) AND COALESCE(points, 0) > 0 THEN 1 ELSE 0 END) AS blowout_scores,
        SUM(CASE WHEN ABS(COALESCE(score_margin, 0)) <= 5 AND COALESCE(points, 0) > 0 THEN 1 ELSE 0 END) AS close_game_scores
        
    FROM player_events
    GROUP BY game_id, player_id, player_name, team_id, team_code
),

advanced_stats AS (
    SELECT
        *,
        -- Advanced metrics
        field_goals_made + field_goals_missed AS fg_attempts,
        field_goals_made / NULLIF(field_goals_made + field_goals_missed, 0)::FLOAT AS fg_pct,
        free_throws_made + free_throws_missed AS ft_attempts,
        free_throws_made / NULLIF(free_throws_made + free_throws_missed, 0)::FLOAT AS ft_pct,
        
        -- Game impact metrics
        total_points + rebounds + assists + steals + blocks - (field_goals_missed + free_throws_missed + turnovers) AS game_score,
        
        -- Efficiency metrics
        total_points / NULLIF(field_goals_made + field_goals_missed + 0.44 * (free_throws_made + free_throws_missed), 0)::FLOAT AS true_shooting_pct,
        
        -- Pace-adjusted stats (per 36 minutes)
        (total_points / NULLIF(minutes_played, 0)) * 2160 AS points_per_36,
        (assists / NULLIF(minutes_played, 0)) * 2160 AS assists_per_36,
        (rebounds / NULLIF(minutes_played, 0)) * 2160 AS rebounds_per_36,
        
        -- Scoring consistency
        GREATEST(q1_points, q2_points, q3_points, q4_points) - LEAST(q1_points, q2_points, q3_points, q4_points) AS scoring_variance
        
    FROM player_stats
)

SELECT * FROM advanced_stats