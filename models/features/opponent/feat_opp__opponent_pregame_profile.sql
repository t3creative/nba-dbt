{{ config(
    materialized='incremental',
    unique_key='game_team_key', 
    tags=['intermediate', 'opponent', 'prediction'],
    indexes=[
        {'columns': ['game_team_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']}
    ]
) }}

WITH opponent_stats AS (
    SELECT 
        os.*,
        -- Calculate game number within season for each team
        ROW_NUMBER() OVER (
            PARTITION BY os.opponent_id, os.season_year 
            ORDER BY os.game_date
        ) AS opponent_game_num_in_season
    FROM {{ ref('int_opp__opponent_stats') }} os
    {% if is_incremental() %}
    WHERE os.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Pre-game rolling stats (last 5, 10, 20 games)
opponent_rolling_stats AS (
    SELECT
        os.*,
        
        -- Last 5 games
        AVG(os.opp_def_rating) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS opp_l5_def_rating,
        
        AVG(os.opp_off_rating) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS opp_l5_off_rating,
        
        AVG(os.opp_pace) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS opp_l5_pace,
        
        AVG(os.opp_pts_per_game) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS opp_l5_pts,
        
        AVG(os.opp_allowed_pts_in_paint) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS opp_l5_allowed_paint_pts,
        
        AVG(os.opp_def_reb_pct) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS opp_l5_def_reb_pct,
        
        AVG(os.opp_blocks_per_game) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS opp_l5_blocks,
        
        -- Last 10 games (similar pattern as above)
        AVG(os.opp_def_rating) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS opp_l10_def_rating,
        
        AVG(os.opp_off_rating) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS opp_l10_off_rating,
        
        AVG(os.opp_pace) OVER(
            PARTITION BY os.opponent_id 
            ORDER BY os.game_date 
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS opp_l10_pace,
        
        -- Season-to-date averages
        AVG(os.opp_def_rating) OVER(
            PARTITION BY os.opponent_id, os.season_year
            ORDER BY os.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS opp_season_def_rating,
        
        AVG(os.opp_pace) OVER(
            PARTITION BY os.opponent_id, os.season_year
            ORDER BY os.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS opp_season_pace
    FROM opponent_stats os
)

SELECT
    -- All columns except timestamps
    ors.game_id,
    ors.game_date,
    ors.game_team_key,
    ors.team_id,
    ors.opponent_id,
    ors.season_year,
    ors.home_away,
    ors.opponent_game_num_in_season,
    -- Include all other columns from ors except created_at and updated_at
    ors.opp_def_rating,
    ors.opp_off_rating,
    ors.opp_pace,
    ors.opp_pts_per_game,
    ors.opp_allowed_pts_in_paint,
    ors.opp_allowed_pts_off_tov,
    ors.opp_reb_pct,
    ors.opp_off_reb_pct,
    ors.opp_def_reb_pct,
    ors.opp_blocks_per_game,
    ors.opp_steals_per_game,
    ors.opp_fg_pct,
    ors.opp_fg3_pct,
    ors.opp_ft_pct,
    ors.opp_tov_ratio,
    
    -- Rolling stats from opponent_rolling_stats
    ors.opp_l5_def_rating,
    ors.opp_l5_off_rating,
    ors.opp_l5_pace,
    ors.opp_l5_pts,
    ors.opp_l5_allowed_paint_pts,
    ors.opp_l5_def_reb_pct,
    ors.opp_l5_blocks,
    ors.opp_l10_def_rating,
    ors.opp_l10_off_rating,
    ors.opp_l10_pace,
    ors.opp_season_def_rating,
    ors.opp_season_pace,
    
    -- Calculated columns
    COALESCE(ors.opp_l5_def_rating, ors.opp_season_def_rating, 0) AS opp_adjusted_def_rating,
    COALESCE(ors.opp_l5_pace, ors.opp_season_pace, 0) AS opp_adjusted_pace,
    
    -- Z-scores
    (ors.opp_l10_def_rating - 
        AVG(ors.opp_l10_def_rating) OVER(PARTITION BY ors.season_year)) / 
        NULLIF(STDDEV(ors.opp_l10_def_rating) OVER(PARTITION BY ors.season_year), 0) 
        AS opp_def_rating_z_score,
        
    (ors.opp_l10_pace - 
        AVG(ors.opp_l10_pace) OVER(PARTITION BY ors.season_year)) / 
        NULLIF(STDDEV(ors.opp_l10_pace) OVER(PARTITION BY ors.season_year), 0) 
        AS opp_pace_z_score,
    
    -- Fresh timestamps
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM opponent_rolling_stats ors