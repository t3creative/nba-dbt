{{ config(
    schema='intermediate',
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
    tags=['team', 'usage', 'intermediate']
) }}

WITH player_boxscores AS (
    SELECT *
    FROM {{ ref('int_player__combined_boxscore') }}
    WHERE min >= 5  -- Filter out minimal playing time
    {% if is_incremental() %}
    AND game_date > (SELECT MAX(game_date) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}
),

-- Calculate team usage concentration metrics
usage_distribution AS (
    SELECT
        pb.game_id,
        pb.team_id,
        pb.opponent_id,
        pb.game_date,
        pb.season_year,
        pb.home_away,
        MD5(CONCAT(pb.game_id, '-', pb.team_id)) AS team_game_key,
        
        -- Usage concentration metrics
        MAX(pb.usage_pct) AS max_usage_pct,
        
        -- Top 3 usage players
        SUM(CASE WHEN usage_rank <= 3 THEN pb.usage_pct ELSE 0 END) AS top3_usage_pct,
        
        -- Usage distribution metrics (Gini coefficient approximation)
        1 - (
            SUM(POWER(pb.pct_of_team_pts - (1.0/pb.player_count), 2)) / 
            (2 * pb.player_count * (1.0/pb.player_count))
        ) AS pts_distribution_equality,
        
        -- Standard deviation of usage
        STDDEV(pb.usage_pct) AS usage_pct_stddev,
        
        -- Count players with high usage
        SUM(CASE WHEN pb.usage_pct >= 25 THEN 1 ELSE 0 END) AS num_high_usage_players,
        
        -- Identify "hero ball" games
        CASE 
            WHEN MAX(pb.usage_pct) > 35 THEN TRUE 
            ELSE FALSE 
        END AS is_hero_ball_game
        
    FROM (
        SELECT
            pb.*,
            ROW_NUMBER() OVER(
                PARTITION BY pb.game_id, pb.team_id
                ORDER BY pb.usage_pct DESC
            ) AS usage_rank,
            COUNT(*) OVER (PARTITION BY pb.game_id, pb.team_id) AS player_count
        FROM player_boxscores pb
    ) pb
    GROUP BY
        pb.game_id,
        pb.team_id,
        pb.opponent_id,
        pb.game_date,
        pb.season_year,
        pb.home_away,
        pb.player_count
),

-- Calculate rolling usage metrics
rolling_usage_metrics AS (
    SELECT
        ud.*,
        
        -- 5-game rolling averages
        AVG(ud.max_usage_pct) OVER(
            PARTITION BY ud.team_id 
            ORDER BY ud.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_l5_max_usage,
        
        AVG(ud.top3_usage_pct) OVER(
            PARTITION BY ud.team_id 
            ORDER BY ud.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_l5_top3_usage,
        
        AVG(ud.pts_distribution_equality) OVER(
            PARTITION BY ud.team_id 
            ORDER BY ud.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_l5_pts_distribution,
        
        -- Season averages
        AVG(ud.max_usage_pct) OVER(
            PARTITION BY ud.team_id, ud.season_year
            ORDER BY ud.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS team_season_max_usage,
        
        AVG(ud.top3_usage_pct) OVER(
            PARTITION BY ud.team_id, ud.season_year
            ORDER BY ud.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS team_season_top3_usage,
        
        AVG(ud.pts_distribution_equality) OVER(
            PARTITION BY ud.team_id, ud.season_year
            ORDER BY ud.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS team_season_pts_distribution
        
    FROM usage_distribution ud
)

SELECT
    rum.team_game_key,
    rum.game_id,
    rum.team_id,
    rum.opponent_id,
    rum.game_date,
    rum.season_year,
    rum.home_away,
    
    -- Current game metrics
    rum.max_usage_pct,
    rum.top3_usage_pct,
    rum.pts_distribution_equality,
    rum.usage_pct_stddev,
    rum.num_high_usage_players,
    rum.is_hero_ball_game,
    
    -- Rolling metrics
    COALESCE(rum.team_l5_max_usage, rum.team_season_max_usage, 0) AS team_l5_max_usage,
    COALESCE(rum.team_l5_top3_usage, rum.team_season_top3_usage, 0) AS team_l5_top3_usage,
    COALESCE(rum.team_l5_pts_distribution, rum.team_season_pts_distribution, 0) AS team_l5_pts_distribution,
    
    COALESCE(rum.team_season_max_usage, 0) AS team_season_max_usage,
    COALESCE(rum.team_season_top3_usage, 0) AS team_season_top3_usage,
    COALESCE(rum.team_season_pts_distribution, 0) AS team_season_pts_distribution,
    
    -- Team offensive structure classification
    CASE
        WHEN rum.team_l5_max_usage > 32 THEN 'STAR_DOMINANT'
        WHEN rum.team_l5_top3_usage > 75 THEN 'TOP_HEAVY'
        WHEN rum.team_l5_pts_distribution > 0.8 THEN 'BALANCED'
        ELSE 'MODERATE_DISTRIBUTION'
    END AS team_offensive_structure,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM rolling_usage_metrics rum