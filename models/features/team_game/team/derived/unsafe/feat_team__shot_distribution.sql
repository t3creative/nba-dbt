{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['team_id', 'season_year'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': true},
        {'columns': ['team_id', 'game_date']}
    ],
    tags=['derived', 'features', 'team_derived_metrics', 'shooting_style']
) }}

WITH team_boxscores AS (
    SELECT *
    FROM {{ ref('int_team__combined_boxscore') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}
),

-- Calculate shot distribution metrics
shot_distribution AS (
    SELECT
        tb.team_game_key,
        tb.game_id,
        tb.team_id,
        tb.opponent_id,
        tb.game_date,
        tb.season_year,
        tb.home_away,
        
        -- Shot type distribution
        tb.pct_fga_2pt,
        tb.pct_fga_3pt,
        tb.pct_pts_2pt,
        tb.pct_pts_3pt,
        tb.pct_pts_midrange_2pt,
        tb.pct_pts_in_paint,
        
        -- Play style metrics
        tb.pct_pts_fastbreak,
        tb.pct_pts_off_tov,
        tb.pct_pts_ft,
        
        -- Creation type
        tb.pct_assisted_2pt,
        tb.pct_unassisted_2pt,
        tb.pct_assisted_3pt,
        tb.pct_unassisted_3pt,
        tb.pct_assisted_fgm,
        tb.pct_unassisted_fgm,
        
        -- Calculate relative rates (compared to league avg)
        tb.pct_fga_3pt / 
            AVG(tb.pct_fga_3pt) OVER(PARTITION BY tb.season_year) 
            AS rel_3pt_attempt_rate,
            
        tb.pct_pts_in_paint / 
            AVG(tb.pct_pts_in_paint) OVER(PARTITION BY tb.season_year) 
            AS rel_paint_scoring_rate,
            
        tb.pct_pts_fastbreak / 
            AVG(tb.pct_pts_fastbreak) OVER(PARTITION BY tb.season_year) 
            AS rel_fastbreak_rate,
            
        tb.pct_assisted_fgm / 
            AVG(tb.pct_assisted_fgm) OVER(PARTITION BY tb.season_year) 
            AS rel_assisted_rate
    FROM team_boxscores tb
),

-- Calculate rolling shot distribution metrics
rolling_shot_metrics AS (
    SELECT
        sd.*,
        
        -- 5-game rolling averages
        AVG(sd.pct_fga_3pt) OVER(
            PARTITION BY sd.team_id 
            ORDER BY sd.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_l5_3pt_att_rate,
        
        AVG(sd.pct_pts_in_paint) OVER(
            PARTITION BY sd.team_id 
            ORDER BY sd.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_l5_paint_scoring_rate,
        
        AVG(sd.pct_pts_fastbreak) OVER(
            PARTITION BY sd.team_id 
            ORDER BY sd.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_l5_fastbreak_rate,
        
        AVG(sd.pct_assisted_fgm) OVER(
            PARTITION BY sd.team_id 
            ORDER BY sd.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_l5_assisted_rate,
        
        -- Season averages
        AVG(sd.pct_fga_3pt) OVER(
            PARTITION BY sd.team_id, sd.season_year
            ORDER BY sd.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS team_season_3pt_att_rate,
        
        AVG(sd.pct_pts_in_paint) OVER(
            PARTITION BY sd.team_id, sd.season_year
            ORDER BY sd.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS team_season_paint_scoring_rate,
        
        AVG(sd.pct_pts_fastbreak) OVER(
            PARTITION BY sd.team_id, sd.season_year
            ORDER BY sd.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS team_season_fastbreak_rate,
        
        AVG(sd.pct_assisted_fgm) OVER(
            PARTITION BY sd.team_id, sd.season_year
            ORDER BY sd.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS team_season_assisted_rate
    FROM shot_distribution sd
)

SELECT
    rsm.team_game_key,
    rsm.game_id,
    rsm.team_id,
    rsm.opponent_id,
    rsm.game_date,
    rsm.season_year,
    rsm.home_away,
    
    -- Current game distribution
    rsm.pct_fga_3pt,
    rsm.pct_pts_in_paint,
    rsm.pct_pts_fastbreak,
    rsm.pct_assisted_fgm,
    
    -- Rolling metrics
    COALESCE(rsm.team_l5_3pt_att_rate, rsm.team_season_3pt_att_rate, 0) AS team_l5_3pt_att_rate,
    COALESCE(rsm.team_l5_paint_scoring_rate, rsm.team_season_paint_scoring_rate, 0) AS team_l5_paint_scoring_rate,
    COALESCE(rsm.team_l5_fastbreak_rate, rsm.team_season_fastbreak_rate, 0) AS team_l5_fastbreak_rate,
    COALESCE(rsm.team_l5_assisted_rate, rsm.team_season_assisted_rate, 0) AS team_l5_assisted_rate,
    
    COALESCE(rsm.team_season_3pt_att_rate, 0) AS team_season_3pt_att_rate,
    COALESCE(rsm.team_season_paint_scoring_rate, 0) AS team_season_paint_scoring_rate,
    COALESCE(rsm.team_season_fastbreak_rate, 0) AS team_season_fastbreak_rate,
    COALESCE(rsm.team_season_assisted_rate, 0) AS team_season_assisted_rate,
    
    -- Relative metrics
    COALESCE(rsm.rel_3pt_attempt_rate, 1) AS rel_3pt_attempt_rate,
    COALESCE(rsm.rel_paint_scoring_rate, 1) AS rel_paint_scoring_rate,
    COALESCE(rsm.rel_fastbreak_rate, 1) AS rel_fastbreak_rate,
    COALESCE(rsm.rel_assisted_rate, 1) AS rel_assisted_rate,
    
    -- Team playstyle classification
    CASE
        WHEN rsm.rel_3pt_attempt_rate > 1.2 THEN 'THREE_POINT_HEAVY'
        WHEN rsm.rel_paint_scoring_rate > 1.2 THEN 'PAINT_FOCUSED'
        WHEN rsm.rel_fastbreak_rate > 1.2 THEN 'FAST_PACED'
        WHEN rsm.rel_assisted_rate > 1.2 THEN 'BALL_MOVEMENT'
        ELSE 'BALANCED'
    END AS team_playstyle,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM rolling_shot_metrics rsm