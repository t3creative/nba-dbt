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

shooting_events AS (
    SELECT
        game_id,
        player_id,
        player_name,
        team_id,
        team_code,
        shot_zone,
        shot_distance,
        shot_made_flag,
        shot_value,
        points,
        period,
        is_clutch_time_end_game,
        is_clutch_time_end_half,
        home_lead,
        away_lead,
        shot_x,
        shot_y,
        time_since_last_event
    FROM player_events
    WHERE action_type = 'SHOT' OR (sub_type = 'FREE_THROW' AND shot_made_flag IS NOT NULL)
),

shot_features AS (
    SELECT
        player_id,
        player_name,
        game_id,
        
        -- Basic shot counts
        COUNT(*) AS total_shots,
        SUM(CASE WHEN shot_made_flag = TRUE THEN 1 ELSE 0 END) AS made_shots,
        SUM(COALESCE(points, 0)) AS total_points,
        SUM(CASE WHEN shot_value = 3 THEN 1 ELSE 0 END) AS three_pt_attempts,
        SUM(CASE WHEN shot_value = 3 AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS three_pt_made,
        SUM(CASE WHEN shot_value = 2 THEN 1 ELSE 0 END) AS two_pt_attempts,
        SUM(CASE WHEN shot_value = 2 AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS two_pt_made,
        SUM(CASE WHEN shot_value = 1 THEN 1 ELSE 0 END) AS ft_attempts,
        SUM(CASE WHEN shot_value = 1 AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS ft_made,
        
        -- Zone-based shooting
        SUM(CASE WHEN shot_zone = 'At Rim' THEN 1 ELSE 0 END) AS at_rim_attempts,
        SUM(CASE WHEN shot_zone = 'At Rim' AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS at_rim_made,
        SUM(CASE WHEN shot_zone = 'Mid Range' THEN 1 ELSE 0 END) AS mid_range_attempts,
        SUM(CASE WHEN shot_zone = 'Mid Range' AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS mid_range_made,
        SUM(CASE WHEN shot_zone = 'Long Mid Range' THEN 1 ELSE 0 END) AS long_mid_attempts,
        SUM(CASE WHEN shot_zone = 'Long Mid Range' AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS long_mid_made,
        SUM(CASE WHEN shot_zone = '3PT' THEN 1 ELSE 0 END) AS three_pt_zone_attempts,
        SUM(CASE WHEN shot_zone = '3PT' AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS three_pt_zone_made,
        
        -- Clutch shooting
        SUM(CASE WHEN is_clutch_time_end_game THEN 1 ELSE 0 END) AS clutch_attempts,
        SUM(CASE WHEN is_clutch_time_end_game AND shot_made_flag = TRUE THEN 1 ELSE 0 END) AS clutch_made,
        
        -- Shot distribution
        AVG(COALESCE(shot_distance, 0)) AS avg_shot_distance,
        STDDEV(COALESCE(shot_distance, 0)) AS std_shot_distance,
        
        -- Shot timing
        AVG(COALESCE(time_since_last_event, 0)) AS avg_time_between_shots,
        STDDEV(COALESCE(time_since_last_event, 0)) AS std_time_between_shots,
        
        -- Advanced metrics
        AVG(CASE WHEN home_lead > 0 THEN home_lead ELSE away_lead END) AS avg_score_diff_during_shots,
        
        -- Spatial features (can be used for shot charts)
        AVG(COALESCE(shot_x, 0)) AS avg_shot_x,
        AVG(COALESCE(shot_y, 0)) AS avg_shot_y,
        STDDEV(COALESCE(shot_x, 0)) AS std_shot_x,
        STDDEV(COALESCE(shot_y, 0)) AS std_shot_y
        
    FROM shooting_events
    GROUP BY player_id, player_name, game_id
),

shot_efficiencies AS (
    SELECT
        *,
        -- Shooting percentages
        CASE WHEN total_shots > 0 THEN made_shots / NULLIF(total_shots, 0)::FLOAT ELSE NULL END AS fg_pct,
        CASE WHEN three_pt_attempts > 0 THEN three_pt_made / NULLIF(three_pt_attempts, 0)::FLOAT ELSE NULL END AS three_pt_pct,
        CASE WHEN two_pt_attempts > 0 THEN two_pt_made / NULLIF(two_pt_attempts, 0)::FLOAT ELSE NULL END AS two_pt_pct,
        CASE WHEN ft_attempts > 0 THEN ft_made / NULLIF(ft_attempts, 0)::FLOAT ELSE NULL END AS ft_pct,
        
        -- Zone efficiencies
        CASE WHEN at_rim_attempts > 0 THEN at_rim_made / NULLIF(at_rim_attempts, 0)::FLOAT ELSE NULL END AS at_rim_pct,
        CASE WHEN mid_range_attempts > 0 THEN mid_range_made / NULLIF(mid_range_attempts, 0)::FLOAT ELSE NULL END AS mid_range_pct,
        CASE WHEN long_mid_attempts > 0 THEN long_mid_made / NULLIF(long_mid_attempts, 0)::FLOAT ELSE NULL END AS long_mid_pct,
        CASE WHEN three_pt_zone_attempts > 0 THEN three_pt_zone_made / NULLIF(three_pt_zone_attempts, 0)::FLOAT ELSE NULL END AS three_pt_zone_pct,
        
        -- Clutch efficiency
        CASE WHEN clutch_attempts > 0 THEN clutch_made / NULLIF(clutch_attempts, 0)::FLOAT ELSE NULL END AS clutch_fg_pct,
        
        -- Advanced metrics
        (two_pt_made * 2 + three_pt_made * 3 + ft_made) / NULLIF(total_shots, 0)::FLOAT AS points_per_shot,
        (two_pt_made * 2 + three_pt_made * 3) / NULLIF(two_pt_attempts + three_pt_attempts, 0)::FLOAT AS effective_fg_pct,
        (two_pt_made * 2 + three_pt_made * 3 + ft_made) / NULLIF(two_pt_attempts + three_pt_attempts + 0.44 * ft_attempts, 0)::FLOAT AS true_shooting_pct
    FROM shot_features
)

SELECT * FROM shot_efficiencies