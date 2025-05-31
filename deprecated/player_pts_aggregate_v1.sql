{{ config(
    schema='features',
    materialized='incremental',
    tags=['features', 'player_game', 'player', 'derived', 'scoring_features'],
    unique_key='player_game_key',
    incremental_strategy='merge',
    indexes=[
            {'columns': ['player_game_key'], 'unique': True},
            {'columns': ['player_id']},
            {'columns': ['game_id']},
            {'columns': ['game_date']}
        ]
    )
}}

WITH metadata AS (
    SELECT
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_year,
        team_id,
        opponent_id

    FROM {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    -- Replicate the incremental logic from base_boxscores to ensure consistency
    WHERE game_date >= (
        SELECT LEAST(
                   MAX(game_date) - INTERVAL '{{ var('feature_recalc_window_days', 90) }} days',
                   MIN(game_date) 
               )
        FROM {{ this }}
    ) OR game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

milestone_stats AS (
    SELECT
        player_game_key,
        player_id,
        game_id,
        game_date,
        season_year,
        -- Milestone game flags (renamed for clarity with pt_ prefix for points)
        is_30_plus_pt_game,
        is_40_plus_pt_game,
        is_50_plus_pt_game,
        is_60_plus_pt_game,
        is_70_plus_pt_game,
        is_80_plus_pt_game,
        
        -- Career counts (historical records prior to game)
        career_30_plus_pt_games,
        career_40_plus_pt_games,
        career_50_plus_pt_games,
        career_60_plus_pt_games,
        career_70_plus_pt_games,
        career_80_plus_pt_games,
        
        -- Season totals (historical records prior to game)
        thirty_plus_pt_games_this_season,
        forty_plus_pt_games_this_season,
        fifty_plus_pt_games_this_season,
        sixty_plus_pt_games_this_season,
        seventy_plus_pt_games_this_season,
        eighty_plus_pt_games_this_season,
        
        -- Previous season totals
        thirty_plus_pt_games_last_season,
        forty_plus_pt_games_last_season,
        fifty_plus_pt_games_last_season,
        sixty_plus_pt_games_last_season,
        seventy_plus_pt_games_last_season,
        eighty_plus_pt_games_last_season,
        
        -- Latest milestone dates
        latest_30_plus_pt_game_date,
        latest_40_plus_pt_game_date,
        latest_50_plus_pt_game_date,
        latest_60_plus_pt_game_date,
        latest_70_plus_pt_game_date,
        latest_80_plus_pt_game_date
    FROM {{ ref('int_player__milestone_games_v2') }}
),

usage_patterns AS (
    SELECT
        player_game_key,
        
        -- Rolling usage patterns (all properly calculated without leakage)
        pct_of_team_fga_roll_3g_avg,
        pct_of_team_fga_roll_5g_avg,
        pct_of_team_fga_roll_10g_avg,
        pct_of_team_fga_roll_3g_stddev,
        pct_of_team_fga_roll_5g_stddev,
        pct_of_team_fga_roll_10g_stddev,
        
        pct_of_team_fg3a_roll_3g_avg,
        pct_of_team_fg3a_roll_5g_avg,
        pct_of_team_fg3a_roll_10g_avg,
        
        pct_of_team_pts_roll_3g_avg,
        pct_of_team_pts_roll_5g_avg,
        pct_of_team_pts_roll_10g_avg,
        pct_of_team_pts_roll_3g_stddev,
        pct_of_team_pts_roll_5g_stddev,
        pct_of_team_pts_roll_10g_stddev
    FROM {{ ref('feat_player__usage_rolling_v2') }}
),

career_stats AS (
    SELECT
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_year,
        
        -- Career stats calculated properly to avoid leakage
        career_ppg_to_date AS career_ppg_prior_game,
        career_games_to_date AS career_games_prior_game,
        career_high_pts_to_date AS career_high_pts_prior_game
    FROM {{ ref('int_player__career_to_date_stats_v1') }}
),

base_boxscores AS (
    SELECT
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_year,
        team_id,
        opponent_id,
        
        -- Basic stats (not for current game prediction, but for creating lagged features)
        pts,
        min,
        fgm,
        fga,
        fg_pct,
        fg3m,
        fg3a,
        fg3_pct,
        ftm,
        fta,
        ft_pct,
        ts_pct,
        eff_fg_pct,
        usage_pct
    FROM {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    -- Include data for a window of time to properly calculate features
    WHERE game_date >= (
        SELECT LEAST(
                   MAX(game_date) - INTERVAL '{{ var('feature_recalc_window_days', 90) }} days',
                   MIN(game_date) 
               )
        FROM {{ this }}
    ) OR game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

derived_scoring_metrics AS (
    SELECT
        player_game_key,
        scoring_efficiency_composite_roll_10g,
        points_opportunity_ratio_roll_10g,
        shot_creation_index_roll_10g,
        defensive_attention_factor_roll_10g,
        scoring_versatility_ratio_roll_10g,
        offensive_role_factor_roll_10g,
        adjusted_usage_with_defense_roll_10g,
        pts_per_half_court_poss_roll_10g,
        usage_weighted_ts_roll_10g,
        three_pt_value_efficiency_index_roll_10g,
        paint_reliance_index_roll_10g,
        assisted_shot_efficiency_roll_10g,
        pace_adjusted_points_per_minute_roll_10g,
        second_chance_conversion_rate_roll_10g,
        contested_vs_uncontested_fg_pct_diff_roll_10g,
        shooting_volume_per_min_roll_10g,
        effective_three_point_contribution_rate_roll_10g,
        free_throw_generation_aggressiveness_roll_10g,
        self_created_scoring_rate_per_min_roll_10g,
        opportunistic_scoring_rate_per_min_roll_10g,
        scoring_focus_ratio_roll_10g,
        contested_fg_makes_per_minute_roll_10g,
        scoring_profile_balance_3pt_vs_paint_roll_10g
    FROM {{ ref('feat_player__derived_scoring_metrics_v2') }}
),

derived_features AS (
    SELECT
        md.player_game_key,
        md.player_id,
        md.player_name,
        md.game_id,
        md.game_date,
        md.season_year,
        md.team_id,
        md.opponent_id,
        
        -- Get milestone games historical data
        COALESCE(ms.career_30_plus_pt_games, 0) AS career_30_plus_pt_games,
        COALESCE(ms.career_40_plus_pt_games, 0) AS career_40_plus_pt_games,
        COALESCE(ms.career_50_plus_pt_games, 0) AS career_50_plus_pt_games,
        COALESCE(ms.career_60_plus_pt_games, 0) AS career_60_plus_pt_games,
        
        -- Season milestone stats
        COALESCE(ms.thirty_plus_pt_games_this_season, 0) AS thirty_plus_pt_games_this_season,
        COALESCE(ms.forty_plus_pt_games_this_season, 0) AS forty_plus_pt_games_this_season,
        COALESCE(ms.fifty_plus_pt_games_this_season, 0) AS fifty_plus_pt_games_this_season,
        
        -- Previous season milestone stats
        COALESCE(ms.thirty_plus_pt_games_last_season, 0) AS thirty_plus_pt_games_last_season,
        COALESCE(ms.forty_plus_pt_games_last_season, 0) AS forty_plus_pt_games_last_season,
        
        -- Days since last milestone games
        CASE 
            WHEN ms.latest_30_plus_pt_game_date IS NOT NULL 
            THEN (md.game_date::date - ms.latest_30_plus_pt_game_date::date)
            ELSE NULL 
        END AS days_since_30_plus_pt_game,
        
        CASE 
            WHEN ms.latest_40_plus_pt_game_date IS NOT NULL 
            THEN (md.game_date::date - ms.latest_40_plus_pt_game_date::date)
            ELSE NULL 
        END AS days_since_40_plus_pt_game,
        
        -- Career stats
        COALESCE(cs.career_ppg_prior_game, 0) AS career_ppg,
        COALESCE(cs.career_games_prior_game, 0) AS career_games,
        COALESCE(cs.career_high_pts_prior_game, 0) AS career_high_pts,
        
        -- Usage pattern features
        COALESCE(up.pct_of_team_pts_roll_3g_avg, 0) AS pts_share_roll_3g_avg,
        COALESCE(up.pct_of_team_pts_roll_5g_avg, 0) AS pts_share_roll_5g_avg,
        COALESCE(up.pct_of_team_pts_roll_10g_avg, 0) AS pts_share_roll_10g_avg,
        COALESCE(up.pct_of_team_pts_roll_5g_stddev, 0) AS pts_share_roll_5g_stddev,
        
        COALESCE(up.pct_of_team_fga_roll_3g_avg, 0) AS fga_share_roll_3g_avg,
        COALESCE(up.pct_of_team_fga_roll_5g_avg, 0) AS fga_share_roll_5g_avg,
        COALESCE(up.pct_of_team_fga_roll_10g_avg, 0) AS fga_share_roll_10g_avg,
        COALESCE(up.pct_of_team_fga_roll_5g_stddev, 0) AS fga_share_roll_5g_stddev,
        
        -- Usage trajectory (is player's usage increasing or decreasing?)
        CASE
            WHEN up.pct_of_team_fga_roll_3g_avg > up.pct_of_team_fga_roll_10g_avg THEN 
                (up.pct_of_team_fga_roll_3g_avg / NULLIF(up.pct_of_team_fga_roll_10g_avg, 0)) - 1
            ELSE 0
        END AS usage_trajectory_ratio,
        
        -- Derived feature: career high potential (how close is player to career high?)
        CASE
            WHEN cs.career_high_pts_prior_game > 0 THEN
                -- Calculate recent scoring as % of career high
                (LAG(bb.pts, 1) OVER (PARTITION BY bb.player_id ORDER BY bb.game_date, bb.game_id)) / 
                NULLIF(cs.career_high_pts_prior_game, 0)
            ELSE 0
        END AS last_game_to_career_high_ratio,
        
        -- Lag features for scoring - properly ensuring no data leakage
        LAG(bb.pts, 1) OVER (PARTITION BY bb.player_id ORDER BY bb.game_date, bb.game_id) AS pts_lag_1g,
        LAG(bb.pts, 2) OVER (PARTITION BY bb.player_id ORDER BY bb.game_date, bb.game_id) AS pts_lag_2g,
        LAG(bb.pts, 3) OVER (PARTITION BY bb.player_id ORDER BY bb.game_date, bb.game_id) AS pts_lag_3g,
        
        -- Advanced scoring features - consistency
        (
            STDDEV(bb.pts) OVER (
                PARTITION BY bb.player_id 
                ORDER BY bb.game_date, bb.game_id
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ) / NULLIF(
                AVG(bb.pts) OVER (
                    PARTITION BY bb.player_id 
                    ORDER BY bb.game_date, bb.game_id
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ), 0)
        ) AS scoring_consistency_ratio,
        
        -- Scoring momentum (comparing recent 3 games vs previous 7 games)
        (
            AVG(bb.pts) OVER (
                PARTITION BY bb.player_id 
                ORDER BY bb.game_date, bb.game_id
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) / NULLIF(
                AVG(bb.pts) OVER (
                    PARTITION BY bb.player_id 
                    ORDER BY bb.game_date, bb.game_id
                    ROWS BETWEEN 10 PRECEDING AND 4 PRECEDING
                ), 0)
        ) - 1 AS scoring_momentum_ratio,
        
        -- Milestone game likelihood features
        CASE
            WHEN ms.thirty_plus_pt_games_this_season > 0 AND bb.season_year = ms.season_year THEN
                ms.thirty_plus_pt_games_this_season::float / NULLIF(cs.career_games_prior_game, 0)
            ELSE 0
        END AS thirty_plus_game_frequency_this_season,
        
        CASE
            WHEN ms.career_30_plus_pt_games > 0 THEN
                ms.career_30_plus_pt_games::float / NULLIF(cs.career_games_prior_game, 0)
            ELSE 0
        END AS thirty_plus_game_frequency_career,
        
        -- Time since last milestone game relative to player's career length
        CASE
            WHEN ms.latest_30_plus_pt_game_date IS NOT NULL AND cs.career_games_prior_game > 0 THEN
                (bb.game_date::date - ms.latest_30_plus_pt_game_date::date) / 
                NULLIF(cs.career_games_prior_game, 0)
            ELSE NULL
        END AS days_per_game_since_last_30_plus_pt_game,
        
        current_timestamp as _dbt_feature_processed_at
        
    FROM metadata md
    LEFT JOIN base_boxscores bb
        ON md.player_game_key = bb.player_game_key
    LEFT JOIN milestone_stats ms
        ON md.player_game_key = ms.player_game_key
    LEFT JOIN usage_patterns up
        ON md.player_game_key = up.player_game_key
    LEFT JOIN career_stats cs
        ON md.player_game_key = cs.player_game_key
)

SELECT
    df.*,
    -- Derived scoring metrics (from derived_scoring_metrics CTE)
    dsm.scoring_efficiency_composite_roll_10g,
    dsm.points_opportunity_ratio_roll_10g,
    dsm.shot_creation_index_roll_10g,
    dsm.defensive_attention_factor_roll_10g,
    dsm.scoring_versatility_ratio_roll_10g,
    dsm.offensive_role_factor_roll_10g,
    dsm.adjusted_usage_with_defense_roll_10g,
    dsm.pts_per_half_court_poss_roll_10g,
    dsm.usage_weighted_ts_roll_10g,
    dsm.three_pt_value_efficiency_index_roll_10g,
    dsm.paint_reliance_index_roll_10g,
    dsm.assisted_shot_efficiency_roll_10g,
    dsm.pace_adjusted_points_per_minute_roll_10g,
    dsm.second_chance_conversion_rate_roll_10g,
    dsm.contested_vs_uncontested_fg_pct_diff_roll_10g,
    dsm.shooting_volume_per_min_roll_10g,
    dsm.effective_three_point_contribution_rate_roll_10g,
    dsm.free_throw_generation_aggressiveness_roll_10g,
    dsm.self_created_scoring_rate_per_min_roll_10g,
    dsm.opportunistic_scoring_rate_per_min_roll_10g,
    dsm.scoring_focus_ratio_roll_10g,
    dsm.contested_fg_makes_per_minute_roll_10g,
    dsm.scoring_profile_balance_3pt_vs_paint_roll_10g
FROM derived_features df
LEFT JOIN derived_scoring_metrics dsm ON df.player_game_key = dsm.player_game_key