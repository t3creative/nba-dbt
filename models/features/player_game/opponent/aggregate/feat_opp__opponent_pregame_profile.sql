{{ config(
    materialized='incremental',
    unique_key='team_game_key', 
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
        ROW_NUMBER() OVER (
            PARTITION BY os.opponent_id, os.season_year 
            ORDER BY os.game_date
        ) AS opponent_game_num_in_season
    FROM {{ ref('feat_opp__opponent_stats') }} os
    {% if is_incremental() %}
    WHERE os.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Stage 1: Calculate all basic prior rolling stats for each game
opponent_rolling_stats AS (
    SELECT
        os.team_game_key, -- Changed from os.game_team_key to match source
        os.game_id,
        os.game_date,
        os.team_id,
        os.opponent_id,
        os.season_year,
        os.home_away,
        os.opponent_game_num_in_season,
        
        -- Last 5 games rolling averages (PRIOR games)
        AVG(os.opp_def_rating) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_def_rating_prior,
        AVG(os.opp_off_rating) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_off_rating_prior,
        AVG(os.opp_pace) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_pace_prior,
        AVG(os.opp_pts_per_game) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_pts_prior,
        AVG(os.opp_allowed_pts_in_paint) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_allowed_paint_pts_prior,
        AVG(os.opp_def_reb_pct) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_def_reb_pct_prior,
        AVG(os.opp_blocks_per_game) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_blocks_prior,
        AVG(os.opp_fg_pct) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_fg_pct_prior,
        AVG(os.opp_fg3_pct) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS opp_l5_fg3_pct_prior,

        -- Last 10 games rolling averages (PRIOR games) - These will be used for Z-scores
        AVG(os.opp_def_rating) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_l10_def_rating_prior,
        AVG(os.opp_off_rating) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_l10_off_rating_prior,
        AVG(os.opp_pace) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_l10_pace_prior,
        AVG(os.opp_fg_pct) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_l10_fg_pct_prior,
        AVG(os.opp_fg3_pct) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_l10_fg3_pct_prior,
        AVG(os.opp_allowed_pts_in_paint) OVER(PARTITION BY os.opponent_id ORDER BY os.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_l10_allowed_paint_pts_prior,

        -- Season-to-date averages (PRIOR games)
        AVG(os.opp_def_rating) OVER(PARTITION BY os.opponent_id, os.season_year ORDER BY os.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS opp_season_def_rating_prior,
        AVG(os.opp_pace) OVER(PARTITION BY os.opponent_id, os.season_year ORDER BY os.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS opp_season_pace_prior,
        AVG(os.opp_fg_pct) OVER(PARTITION BY os.opponent_id, os.season_year ORDER BY os.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS opp_season_fg_pct_prior,
        AVG(os.opp_fg3_pct) OVER(PARTITION BY os.opponent_id, os.season_year ORDER BY os.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS opp_season_fg3_pct_prior,
        AVG(os.opp_allowed_pts_in_paint) OVER(PARTITION BY os.opponent_id, os.season_year ORDER BY os.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS opp_season_allowed_paint_pts_prior
    FROM opponent_stats os
),

-- Stage 2: Calculate seasonal baselines (mean and stddev) of the L10 prior stats for Z-score normalization
seasonal_baselines_for_zscore AS (
    SELECT
        ors.team_game_key, -- Changed from ors.game_team_key
        ors.game_date,     -- For ordering the rolling seasonal baseline
        ors.season_year,   -- For partitioning the seasonal baseline
        
        -- Calculate rolling seasonal average of L10 prior def_rating
        AVG(ors.opp_l10_def_rating_prior) OVER (
            PARTITION BY ors.season_year 
            ORDER BY ors.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_of_l10_def_rating_prior,
        
        -- Calculate rolling seasonal stddev of L10 prior def_rating
        STDDEV(ors.opp_l10_def_rating_prior) OVER (
            PARTITION BY ors.season_year 
            ORDER BY ors.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_stddev_of_l10_def_rating_prior,

        -- Calculate rolling seasonal average of L10 prior pace
        AVG(ors.opp_l10_pace_prior) OVER (
            PARTITION BY ors.season_year 
            ORDER BY ors.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_of_l10_pace_prior,

        -- Calculate rolling seasonal stddev of L10 prior pace
        STDDEV(ors.opp_l10_pace_prior) OVER (
            PARTITION BY ors.season_year 
            ORDER BY ors.game_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_stddev_of_l10_pace_prior
    FROM opponent_rolling_stats ors
)

SELECT
    ors.game_id,
    ors.game_date,
    ors.team_game_key AS game_team_key, -- Aliased to match unique_key in config
    ors.team_id, 
    ors.opponent_id, 
    ors.season_year,
    ors.home_away, 
    ors.opponent_game_num_in_season,
    
    ors.opp_l5_def_rating_prior,
    ors.opp_l5_off_rating_prior,
    ors.opp_l5_pace_prior,
    ors.opp_l5_pts_prior,
    ors.opp_l5_allowed_paint_pts_prior,
    ors.opp_l5_def_reb_pct_prior,
    ors.opp_l5_blocks_prior,
    ors.opp_l5_fg_pct_prior,
    ors.opp_l5_fg3_pct_prior,

    ors.opp_l10_def_rating_prior,
    ors.opp_l10_off_rating_prior,
    ors.opp_l10_pace_prior,
    ors.opp_l10_fg_pct_prior,
    ors.opp_l10_fg3_pct_prior,
    ors.opp_l10_allowed_paint_pts_prior,

    ors.opp_season_def_rating_prior,
    ors.opp_season_pace_prior,
    ors.opp_season_fg_pct_prior,
    ors.opp_season_fg3_pct_prior,
    ors.opp_season_allowed_paint_pts_prior,
    
    COALESCE(ors.opp_l5_def_rating_prior, ors.opp_season_def_rating_prior) AS opp_adjusted_def_rating_prior,
    COALESCE(ors.opp_l5_pace_prior, ors.opp_season_pace_prior) AS opp_adjusted_pace_prior,
    COALESCE(ors.opp_l5_allowed_paint_pts_prior, ors.opp_season_allowed_paint_pts_prior) AS opp_adjusted_allowed_paint_pts_prior,
    COALESCE(ors.opp_l5_fg_pct_prior, ors.opp_season_fg_pct_prior) AS opp_adjusted_fg_pct_prior,
    COALESCE(ors.opp_l5_fg3_pct_prior, ors.opp_season_fg3_pct_prior) AS opp_adjusted_fg3_pct_prior,
    
    -- Z-scores calculated using the staged seasonal baselines
    (ors.opp_l10_def_rating_prior - sbs.season_avg_of_l10_def_rating_prior) / 
        NULLIF(sbs.season_stddev_of_l10_def_rating_prior, 0) 
        AS opp_def_rating_z_score_prior,
        
    (ors.opp_l10_pace_prior - sbs.season_avg_of_l10_pace_prior) / 
        NULLIF(sbs.season_stddev_of_l10_pace_prior, 0) 
        AS opp_pace_z_score_prior,

    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM opponent_rolling_stats ors
JOIN seasonal_baselines_for_zscore sbs
    ON ors.team_game_key = sbs.team_game_key -- Changed from game_team_key to team_game_key