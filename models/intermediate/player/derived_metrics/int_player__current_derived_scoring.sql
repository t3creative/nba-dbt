{{ config(
    schema='intermediate',
    materialized='incremental',
    tags=['intermediate', 'player_derived_metrics', 'scoring_metrics'],
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

WITH combined_boxscore_data AS (
    SELECT *
    FROM {{ ref('int_player__combined_boxscore') }} -- This model must provide all base fields like pts, min, fga, fta, ast_pct, touches, pct_unassisted_fgm etc.
    {% if is_incremental() %}
    WHERE game_date >= (SELECT MAX(game_date) FROM {{ this }}) - INTERVAL '{{ var('feature_recalc_window_days', 7) }} days'
       OR game_date > (SELECT MAX(game_date) FROM {{ this }}) -- Ensures truly new games are also picked up if MAX(game_date) is NULL or very old
    {% endif %}
),

career_stats_raw AS (
    -- Get "to_date" stats (including current game) and current game's new high flag
    SELECT
        player_game_key,
        player_id, -- for partitioning
        game_date, -- for ordering
        game_id,   -- for tie-breaking in ordering
        career_ppg_to_date,
        career_games_to_date,
        career_high_pts_to_date,
        is_new_career_high -- This flag is true if the current game's PTS resulted in a new career high
    FROM {{ ref('int_player__career_to_date_stats') }}
),

career_stats_source AS (
    -- Create true "prior_game" stats using LAG
    SELECT
        player_game_key,
        LAG(career_ppg_to_date, 1, 0) OVER (PARTITION BY player_id ORDER BY game_date, game_id) as career_ppg_prior_game,
        LAG(career_games_to_date, 1, 0) OVER (PARTITION BY player_id ORDER BY game_date, game_id) as career_games_prior_game,
        LAG(career_high_pts_to_date, 1, 0) OVER (PARTITION BY player_id ORDER BY game_date, game_id) as career_high_pts_prior_game,
        is_new_career_high -- Pass through: is current game's PTS a new career high?
    FROM career_stats_raw
),

milestone_games_source AS (
    -- This CTE should provide historical milestone data *before* the current game
    SELECT
        player_game_key,
        latest_30_plus_date_prior, -- Renamed: Date of last 30+ game *before* current
        latest_40_plus_date_prior, -- Renamed: Date of last 40+ game *before* current
        thirty_plus_games_career_prior, -- Renamed: Count of 30+ games *before* current
        forty_plus_games_career_prior,  -- Renamed: Count of 40+ games *before* current
        thirty_plus_games_this_season_prior, -- Renamed: Count of 30+ games this season *before* current
        forty_plus_games_this_season_prior,  -- Renamed: Count of 40+ games this season *before* current
        thirty_plus_games_prev_season,
        forty_plus_games_prev_season
    FROM {{ ref('int_player__milestone_games') }}
),

usage_patterns_source AS (
    -- This CTE should provide usage patterns based on games *prior* to the current game
    SELECT
        player_game_key,
        avg_usage_in_30_plus_career_prior, -- Renamed
        avg_usage_in_40_plus_career_prior, -- Renamed
        avg_usage_0_to_9_pts_prior,        -- Renamed
        avg_usage_10_to_19_pts_prior,      -- Renamed
        avg_usage_20_to_29_pts_prior       -- Renamed
    FROM {{ ref('feat_player__usage_patterns') }}
),

final_joined_data AS (
    SELECT
        cb.*, -- Selects all columns from combined_boxscore_data (current game stats)

        cs.career_ppg_prior_game,
        cs.career_games_prior_game,
        cs.career_high_pts_prior_game,
        cs.is_new_career_high, -- Flag: is current game's PTS a new career high relative to career_high_pts_prior_game?

        CASE WHEN cb.pts >= 30 THEN 1 ELSE 0 END as is_30_plus_game_current_flag,
        CASE WHEN cb.pts >= 40 THEN 1 ELSE 0 END as is_40_plus_game_current_flag,

        mg.latest_30_plus_date_prior,
        mg.latest_40_plus_date_prior,
        COALESCE(mg.thirty_plus_games_career_prior, 0) as thirty_plus_games_career_prior,
        COALESCE(mg.forty_plus_games_career_prior, 0) as forty_plus_games_career_prior,
        COALESCE(mg.thirty_plus_games_this_season_prior, 0) as thirty_plus_games_season_prior,
        COALESCE(mg.forty_plus_games_this_season_prior, 0) as forty_plus_games_season_prior,
        COALESCE(mg.thirty_plus_games_prev_season, 0) as thirty_plus_games_prev_season,
        COALESCE(mg.forty_plus_games_prev_season, 0) as forty_plus_games_prev_season,

        up.avg_usage_in_30_plus_career_prior,
        up.avg_usage_in_40_plus_career_prior,
        up.avg_usage_0_to_9_pts_prior,
        up.avg_usage_10_to_19_pts_prior,
        up.avg_usage_20_to_29_pts_prior
    FROM combined_boxscore_data cb
    LEFT JOIN career_stats_source cs ON cb.player_game_key = cs.player_game_key
    LEFT JOIN milestone_games_source mg ON cb.player_game_key = mg.player_game_key
    LEFT JOIN usage_patterns_source up ON cb.player_game_key = up.player_game_key
)

SELECT
    t.player_game_key,
    t.player_id,
    t.player_name,
    t.game_id,
    t.team_id,
    t.season_year,
    t.game_date,

    -- Current game scoring-related performance stats (and those needed for derived scoring metrics)
    t.pts,
    t.min,
    t.fgm,
    t.fga,
    t.fg_pct,
    t.fg3m,
    t.fg3a,
    t.fg3_pct,
    t.ftm,
    t.fta,
    t.ft_pct,
    t.off_reb, -- Used in second_chance_conversion_rate
    t.ts_pct,
    t.eff_fg_pct,
    t.usage_pct,
    t.possessions, -- Used in pts_per_half_court_poss
    COALESCE(t.touches, 0) as touches, -- Used in points_opportunity_ratio. Assumed in combined_boxscore_data
    COALESCE(t.pct_unassisted_fgm, 0) as pct_unassisted_fgm, -- Used in shot_creation_index, self_created_scoring_rate. Assumed in combined_boxscore_data
    COALESCE(t.pct_of_team_fga, 0) as pct_of_team_fga, -- Used in shot_creation_index, offensive_role_factor. Assumed in combined_boxscore_data
    COALESCE(t.pct_of_team_ast, 0) as pct_of_team_ast, -- Used in offensive_role_factor. Assumed in combined_boxscore_data
    COALESCE(t.ast_pct, 0) as ast_pct,                 -- Used in scoring_focus_ratio. Assumed in combined_boxscore_data
    COALESCE(t.cont_fga, 0) as cont_fga,               -- Used in defensive_attention_factor. Assumed in combined_boxscore_data
    COALESCE(t.cont_fgm, 0) as cont_fgm,               -- Used in contested_fg_makes_per_minute. Assumed in combined_boxscore_data
    COALESCE(t.cont_fg_pct, 0) as cont_fg_pct,         -- Used in contested_vs_uncontested_fg_pct_diff. Assumed in combined_boxscore_data
    COALESCE(t.uncont_fg_pct, 0) as uncont_fg_pct,     -- Used in contested_vs_uncontested_fg_pct_diff. Assumed in combined_boxscore_data
    COALESCE(t.def_at_rim_fga, 0) as def_at_rim_fga,   -- Used in defensive_attention_factor. Assumed in combined_boxscore_data
    COALESCE(t.pct_pts_2pt, 0) as pct_pts_2pt,         -- Used in scoring_versatility_ratio. Assumed in combined_boxscore_data
    COALESCE(t.pct_pts_3pt, 0) as pct_pts_3pt,         -- Used in scoring_versatility_ratio, scoring_profile_balance. Assumed in combined_boxscore_data
    COALESCE(t.pct_pts_ft, 0) as pct_pts_ft,           -- Used in scoring_versatility_ratio. Assumed in combined_boxscore_data
    COALESCE(t.pct_pts_in_paint, 0) as pct_pts_in_paint, -- Used in paint_reliance_index, scoring_profile_balance. Assumed in combined_boxscore_data
    COALESCE(t.pct_fga_2pt, 0) as pct_fga_2pt,         -- Used in paint_reliance_index. Assumed in combined_boxscore_data
    COALESCE(t.pct_fga_3pt, 0) as pct_fga_3pt,         -- Used in three_pt_value_efficiency_index. Assumed in combined_boxscore_data
    COALESCE(t.pct_assisted_fgm, 0) as pct_assisted_fgm, -- Used in assisted_shot_efficiency. Assumed in combined_boxscore_data
    COALESCE(t.matchup_fg_pct, 0.5) as matchup_fg_pct, -- Used in adjusted_usage_with_defense. Assumed in combined_boxscore_data or joined
    COALESCE(t.fastbreak_pts, 0) as fastbreak_pts,     -- Used in pts_per_half_court_poss, opportunistic_scoring. Assumed in combined_boxscore_data
    COALESCE(t.pts_off_tov, 0) as pts_off_tov,         -- Used in opportunistic_scoring. Assumed in combined_boxscore_data
    COALESCE(t.pace_per_40, 0) as pace_per_40,         -- Used in pace_adjusted_points_per_minute. Assumed in combined_boxscore_data
    COALESCE(t.second_chance_pts, 0) as second_chance_pts, -- Used in second_chance_conversion_rate. Assumed in combined_boxscore_data


    -- Flags for current game scoring performance
    t.is_30_plus_game_current_flag,
    t.is_40_plus_game_current_flag,
    COALESCE(t.is_new_career_high, 0) as is_new_career_high_flag,

    -- Historical scoring context (prior to current game)
    t.career_high_pts_prior_game,
    t.career_ppg_prior_game,
    t.career_games_prior_game,
    CASE WHEN t.latest_30_plus_date_prior IS NOT NULL AND t.game_date IS NOT NULL THEN (t.game_date::date - t.latest_30_plus_date_prior::date) ELSE NULL END as days_since_last_30_plus_game_prior,
    CASE WHEN t.latest_40_plus_date_prior IS NOT NULL AND t.game_date IS NOT NULL THEN (t.game_date::date - t.latest_40_plus_date_prior::date) ELSE NULL END as days_since_last_40_plus_game_prior,
    t.thirty_plus_games_career_prior,
    t.forty_plus_games_career_prior,
    t.thirty_plus_games_season_prior,
    t.forty_plus_games_season_prior,
    t.thirty_plus_games_prev_season,
    t.forty_plus_games_prev_season,

    -- Historical usage patterns related to scoring (prior to current game)
    COALESCE(t.avg_usage_in_30_plus_career_prior, 0) as avg_usage_in_30_plus_career_prior,
    COALESCE(t.avg_usage_in_40_plus_career_prior, 0) as avg_usage_in_40_plus_career_prior,
    COALESCE(t.avg_usage_0_to_9_pts_prior, 0) as avg_usage_0_to_9_pts_prior,
    COALESCE(t.avg_usage_10_to_19_pts_prior, 0) as avg_usage_10_to_19_pts_prior,
    COALESCE(t.avg_usage_20_to_29_pts_prior, 0) as avg_usage_20_to_29_pts_prior,

    -- Derived features for the CURRENT game using CURRENT game's base stats (t.*)
    CASE
      WHEN t.fga > 0 OR t.fta > 0
      THEN (COALESCE(t.ts_pct, 0) * 0.6) + (COALESCE(t.eff_fg_pct, 0) * 0.4)
      ELSE 0
    END AS scoring_efficiency_composite_current,

    CASE
      WHEN t.touches > 0
      THEN t.pts / NULLIF(t.touches, 0)
      ELSE 0
    END AS points_opportunity_ratio_current,

    CASE
      WHEN t.fga > 0
      THEN t.fga * t.pct_unassisted_fgm * t.pct_of_team_fga
      ELSE 0
    END AS shot_creation_index_current,

    CASE
      WHEN t.fga > 0
      THEN (t.cont_fga / NULLIF(t.fga, 0)) * (1 + t.def_at_rim_fga / NULLIF(t.fga, 0))
      ELSE 0
    END AS defensive_attention_factor_current,

    1 - (
        ABS(t.pct_pts_2pt - 0.3333) +
        ABS(t.pct_pts_3pt - 0.3333) +
        ABS(t.pct_pts_ft - 0.3333)
    ) AS scoring_versatility_ratio_current,

    CASE
      WHEN t.usage_pct > 0 AND t.pct_of_team_fga > 0
      THEN (t.usage_pct * 0.7) + (t.pct_of_team_ast / NULLIF(t.pct_of_team_fga, 0) * 0.3)
      WHEN t.usage_pct > 0
      THEN t.usage_pct * 0.7
      ELSE 0
    END AS offensive_role_factor_current,

    CASE
      WHEN t.min > 0
      THEN t.usage_pct * (1 + (t.matchup_fg_pct - 0.5))
      ELSE 0
    END AS adjusted_usage_with_defense_current,

    CASE
      WHEN (t.possessions - (t.fastbreak_pts / 2.0)) > 0
      THEN (t.pts - t.fastbreak_pts) / NULLIF((t.possessions - (t.fastbreak_pts / 2.0)), 0)
      ELSE 0
    END AS pts_per_half_court_poss_current,

    (t.ts_pct * t.usage_pct) AS usage_weighted_ts_current,

    (t.pct_fga_3pt * t.fg3_pct) AS three_pt_value_efficiency_index_current,

    (t.pct_pts_in_paint * t.pct_fga_2pt) AS paint_reliance_index_current,

    (t.pct_assisted_fgm * t.eff_fg_pct) AS assisted_shot_efficiency_current,

    CASE
      WHEN t.pace_per_40 > 0 AND t.min > 0
      THEN (t.pts / NULLIF(t.min,0)) * (100.0 / NULLIF(t.pace_per_40, 0))
      ELSE 0
    END AS pace_adjusted_points_per_minute_current,

    CASE
      WHEN t.off_reb > 0
      THEN t.second_chance_pts::decimal / t.off_reb
      ELSE 0
    END AS second_chance_conversion_rate_current,

    (t.cont_fg_pct - t.uncont_fg_pct) AS contested_vs_uncontested_fg_pct_diff_current,

    CASE
      WHEN t.min > 0
      THEN (t.fga + 0.44 * t.fta) / t.min
      ELSE 0
    END AS shooting_volume_per_min_current,

    CASE
      WHEN t.fga > 0
      THEN (t.fg3m * 3) / NULLIF(t.fga, 0)
      ELSE 0
    END AS effective_three_point_contribution_rate_current,

    CASE
      WHEN t.fga > 0
      THEN t.fta / NULLIF(t.fga, 0)
      ELSE 0
    END AS free_throw_generation_aggressiveness_current,

    CASE
      WHEN t.min > 0 AND t.fgm > 0
      THEN (t.pct_unassisted_fgm * t.fgm) / t.min
      ELSE 0
    END AS self_created_scoring_rate_per_min_current,

    CASE
      WHEN t.min > 0
      THEN (t.pts_off_tov + t.fastbreak_pts) / t.min
      ELSE 0
    END AS opportunistic_scoring_rate_per_min_current,

    CASE
      WHEN (t.ast_pct + 1e-6) > 0
      THEN t.usage_pct / (t.ast_pct + 1e-6)
      ELSE 0
    END AS scoring_focus_ratio_current,

    CASE
      WHEN t.min > 0
      THEN t.cont_fgm / t.min
      ELSE 0
    END AS contested_fg_makes_per_minute_current,

    (t.pct_pts_3pt - t.pct_pts_in_paint) AS scoring_profile_balance_3pt_vs_paint_current

FROM final_joined_data t
