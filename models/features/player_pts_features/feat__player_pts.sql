{{ config(materialized='incremental',
    tags=['features', 'player_pts_features'],
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    indexes=[
            {'columns': ['player_game_key'], 'unique': True},
            {'columns': ['player_id']},
            {'columns': ['game_id']},
            {'columns': ['game_date']}
        ]
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('int__combined_player_boxscore') }} -- This refers to the model defined by int__combined_player_boxscore.sql
)

SELECT
    t.player_game_key, -- Unique key from the base model
    t.player_id,
    t.game_id,
    t.game_date,       -- Useful for partitioning or ordering, included for context with recent_minutes_trend

    -- Feature Engineering Calculations
    -- Note: The column 't.game_min' used in 'scoring_load_index' is assumed to exist in the `player_boxscores` model.
    -- If 'game_min' (total minutes in a game, e.g., 48 for regulation) is not present, that feature will need adjustment.
    -- CASE
    --   WHEN t.min > 0 AND t.pct_of_team_pts > 0 AND t.game_min > 0 -- Assuming t.game_min is available from base
    --   THEN (t.pct_of_team_pts * t.pct_of_team_fga) / (t.min / NULLIF(t.game_min, 0))
    --   ELSE 0
    -- END AS scoring_load_index,

    CASE
      WHEN t.fga > 0 OR t.fta > 0 -- Either FGA or FTA can be non-zero for TS% or EFG% to be meaningful
      THEN (COALESCE(t.ts_pct, 0) * 0.6) + (COALESCE(t.eff_fg_pct, 0) * 0.4)
      ELSE 0
    END AS scoring_efficiency_composite,

    CASE
      WHEN t.touches > 0
      THEN t.pts / NULLIF(t.touches, 0)
      ELSE 0
    END AS points_opportunity_ratio,

    CASE
      WHEN t.fga > 0
      THEN t.fga * COALESCE(t.pct_unassisted_fgm, 0) * COALESCE(t.pct_of_team_fga, 0)
      ELSE 0
    END AS shot_creation_index,

    CASE
      WHEN t.fga > 0
      THEN (COALESCE(t.cont_fga, 0) / NULLIF(t.fga, 0)) * (1 + COALESCE(t.def_at_rim_fga, 0) / NULLIF(t.fga, 0))
      ELSE 0
    END AS defensive_attention_factor,

    1 - (
        ABS(COALESCE(t.pct_pts_2pt, 0) - 0.3333) +
        ABS(COALESCE(t.pct_pts_3pt, 0) - 0.3333) +
        ABS(COALESCE(t.pct_pts_ft, 0) - 0.3333)
    ) AS scoring_versatility_ratio, -- Normalized between 0 and 1, closer to 1 is more versatile

    CASE
      WHEN COALESCE(t.usage_pct, 0) > 0 AND COALESCE(t.pct_of_team_fga, 0) > 0
      THEN (COALESCE(t.usage_pct, 0) * 0.7) + (COALESCE(t.pct_of_team_ast, 0) / NULLIF(t.pct_of_team_fga, 0) * 0.3)
      WHEN COALESCE(t.usage_pct, 0) > 0 -- Fallback if pct_of_team_fga is zero but usage_pct is not
      THEN COALESCE(t.usage_pct, 0) * 0.7
      ELSE 0
    END AS offensive_role_factor,

    CASE
      WHEN t.min > 0
      THEN COALESCE(t.usage_pct, 0) * (1 + (COALESCE(t.matchup_fg_pct, 0.5) - 0.5)) -- Assume neutral matchup_fg_pct if NULL
      ELSE 0
    END AS adjusted_usage_with_defense,

    COALESCE(t.min, 0) - LAG(COALESCE(t.min, 0), 3, 0) OVER (PARTITION BY t.player_id ORDER BY t.game_date) AS recent_minutes_trend,

    CASE
      WHEN (COALESCE(t.possessions, 0) - (COALESCE(t.fastbreak_pts, 0) / 2.0)) > 0
      THEN (COALESCE(t.pts, 0) - COALESCE(t.fastbreak_pts, 0)) / NULLIF((COALESCE(t.possessions, 0) - (COALESCE(t.fastbreak_pts, 0) / 2.0)), 0)
      ELSE 0
    END AS pts_per_half_court_poss,

    (COALESCE(t.ts_pct, 0) * COALESCE(t.usage_pct, 0)) AS usage_weighted_ts,

    (COALESCE(t.pct_fga_3pt, 0) * COALESCE(t.fg3_pct, 0)) AS three_pt_value_efficiency_index, -- Renamed for clarity

    (COALESCE(t.pct_pts_in_paint, 0) * COALESCE(t.pct_fga_2pt, 0)) AS paint_reliance_index, -- Renamed for clarity

    (COALESCE(t.pct_assisted_fgm, 0) * COALESCE(t.eff_fg_pct, 0)) AS assisted_shot_efficiency, -- Renamed for clarity

    CASE
      WHEN COALESCE(t.pace_per_40, 0) > 0
      THEN COALESCE(t.pts_per_min, 0) * (100.0 / t.pace_per_40) -- pts_per_min is from base model
      ELSE 0
    END AS pace_adjusted_points_per_minute, -- Renamed for clarity

    CASE
      WHEN COALESCE(t.off_reb, 0) > 0
      THEN COALESCE(t.second_chance_pts, 0)::decimal / t.off_reb -- Using user-provided casting
      ELSE 0
    END AS second_chance_conversion_rate, -- Renamed for clarity

    (COALESCE(t.cont_fg_pct, 0) - COALESCE(t.uncont_fg_pct, 0)) AS contested_vs_uncontested_fg_pct_diff, -- Renamed for clarity

    CASE
      WHEN COALESCE(t.min, 0) > 0
      THEN (COALESCE(t.fga, 0) + 0.44 * COALESCE(t.fta, 0)) / t.min
      ELSE 0
    END AS shooting_volume_per_min, -- True shooting attempts per minute

    CASE
      WHEN COALESCE(t.fga, 0) > 0
      THEN (COALESCE(t.fg3m, 0) * 3) / t.fga
      ELSE 0
    END AS effective_three_point_contribution_rate,

    CASE
      WHEN COALESCE(t.fga, 0) > 0
      THEN COALESCE(t.fta, 0) / t.fga
      ELSE 0
    END AS free_throw_generation_aggressiveness,

    CASE
      WHEN COALESCE(t.min, 0) > 0 AND COALESCE(t.fgm, 0) > 0
      THEN (COALESCE(t.pct_unassisted_fgm, 0) * t.fgm) / t.min
      ELSE 0
    END AS self_created_scoring_rate_per_min,

    CASE
      WHEN COALESCE(t.min, 0) > 0
      THEN (COALESCE(t.pts_off_tov, 0) + COALESCE(t.fastbreak_pts, 0)) / t.min
      ELSE 0
    END AS opportunistic_scoring_rate_per_min,

    CASE
      WHEN (COALESCE(t.ast_pct, 0) + 1e-6) > 0 -- Add epsilon to prevent division by exact zero after COALESCE
      THEN COALESCE(t.usage_pct, 0) / (COALESCE(t.ast_pct, 0) + 1e-6)
      ELSE 0
    END AS scoring_focus_ratio, -- Higher means more usage relative to assist percentage

    CASE
      WHEN COALESCE(t.min, 0) > 0
      THEN COALESCE(t.cont_fgm, 0) / t.min
      ELSE 0
    END AS contested_fg_makes_per_minute,

    (COALESCE(t.pct_pts_3pt, 0) - COALESCE(t.pct_pts_in_paint, 0)) AS scoring_profile_balance_3pt_vs_paint -- Positive means more 3pt reliance, negative more paint reliance

FROM base t
