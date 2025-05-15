{{ config(materialized='incremental',
    tags=['features', 'player_pts_features'],
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
    FROM {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    WHERE game_date >= (SELECT MAX(game_date) FROM {{ this }}) - INTERVAL '{{ var('feature_recalc_window_days', 7) }} days'
       OR game_date > (SELECT MAX(game_date) FROM {{ this }}) -- Ensures truly new games are also picked up if MAX(game_date) is NULL or very old
    {% endif %}
),

career_stats_source AS (
    SELECT
        player_game_key,
        career_ppg_to_date,
        career_games_to_date,
        career_high_pts_to_date,
        is_new_career_high
    FROM {{ ref('int_player__career_to_date_stats') }}
),

milestone_games_source AS (
    SELECT
        player_game_key,
        is_30_plus_game,
        is_40_plus_game,
        latest_30_plus_date,
        latest_40_plus_date,
        thirty_plus_games_career,
        forty_plus_games_career,
        thirty_plus_games_season,
        forty_plus_games_season,
        thirty_plus_games_prev_season,
        forty_plus_games_prev_season
    FROM {{ ref('int_player__milestone_games') }}
),

usage_patterns_source AS (
    SELECT
        player_game_key,
        avg_usage_in_30_plus_career,
        avg_usage_in_40_plus_career,
        avg_usage_0_to_9_pts,
        avg_usage_10_to_19_pts,
        avg_usage_20_to_29_pts
    FROM {{ ref('feat_player__usage_patterns') }}
),

final_joined_data AS (
    SELECT
        cb.*, -- Selects all columns from combined_boxscore_data
        cs.career_ppg_to_date,
        cs.career_games_to_date,
        cs.career_high_pts_to_date,
        cs.is_new_career_high,
        -- Default milestone columns to appropriate values when NULL
        COALESCE(mg.is_30_plus_game, CASE WHEN cb.pts >= 30 THEN 1 ELSE 0 END) as is_30_plus_game,
        COALESCE(mg.is_40_plus_game, CASE WHEN cb.pts >= 40 THEN 1 ELSE 0 END) as is_40_plus_game,
        mg.latest_30_plus_date,
        mg.latest_40_plus_date,
        COALESCE(mg.thirty_plus_games_career, 0) as thirty_plus_games_career,
        COALESCE(mg.forty_plus_games_career, 0) as forty_plus_games_career,
        COALESCE(mg.thirty_plus_games_season, 0) as thirty_plus_games_season,
        COALESCE(mg.forty_plus_games_season, 0) as forty_plus_games_season,
        COALESCE(mg.thirty_plus_games_prev_season, 0) as thirty_plus_games_prev_season,
        COALESCE(mg.forty_plus_games_prev_season, 0) as forty_plus_games_prev_season,
        up.avg_usage_in_30_plus_career,
        up.avg_usage_in_40_plus_career,
        up.avg_usage_0_to_9_pts,
        up.avg_usage_10_to_19_pts,
        up.avg_usage_20_to_29_pts
    FROM combined_boxscore_data cb
    LEFT JOIN career_stats_source cs ON cb.player_game_key = cs.player_game_key
    LEFT JOIN milestone_games_source mg ON cb.player_game_key = mg.player_game_key
    LEFT JOIN usage_patterns_source up ON cb.player_game_key = up.player_game_key
)

SELECT
    t.player_game_key,
    t.player_id,
    t.player_name, -- Assumed to be in int_player__combined_boxscore
    t.game_id,
    t.team_id,     -- Assumed to be in int_player__combined_boxscore
    t.season_year, -- Assumed to be in int_player__combined_boxscore
    t.game_date,

    -- Features from the former int__player_scoring_features.sql
    t.pts, -- Already available from combined_boxscore_data (aliased as t)
    t.is_30_plus_game,
    t.is_40_plus_game,
    COALESCE(t.is_new_career_high, 0) as is_new_career_high,
    COALESCE(t.career_high_pts_to_date, t.pts) as career_high_pts_to_date,
    COALESCE(t.career_ppg_to_date, t.pts) as career_ppg_to_date,
    COALESCE(t.career_games_to_date, 1) as career_games_to_date,
    {{ days_since_milestone('t.game_date', 't.latest_30_plus_date') }} as days_since_last_30_plus_game,
    {{ days_since_milestone('t.game_date', 't.latest_40_plus_date') }} as days_since_last_40_plus_game,
    t.thirty_plus_games_career,
    t.forty_plus_games_career,
    t.thirty_plus_games_season as thirty_plus_games_this_season,
    t.forty_plus_games_season as forty_plus_games_this_season,
    t.thirty_plus_games_prev_season as thirty_plus_games_last_season,
    t.forty_plus_games_prev_season as forty_plus_games_last_season,
    COALESCE(t.avg_usage_in_30_plus_career, 0) as avg_usage_in_30_plus_career,
    COALESCE(t.avg_usage_in_40_plus_career, 0) as avg_usage_in_40_plus_career,
    COALESCE(t.avg_usage_0_to_9_pts, 0) as avg_usage_0_to_9_pts,
    COALESCE(t.avg_usage_10_to_19_pts, 0) as avg_usage_10_to_19_pts,
    COALESCE(t.avg_usage_20_to_29_pts, 0) as avg_usage_20_to_29_pts,

    -- Features from the original feat_player__scoring.sql
    -- scoring_load_index: This feature requires 'game_min' (total minutes in a game, e.g., 48 for regulation).
    -- 'game_min' is typically a game-level attribute not present in player boxscores.
    -- If 'int_player__combined_boxscore' does not include 'game_min' (or an equivalent game-level total minutes field),
    -- this feature will calculate incorrectly or error. It's commented out pending verification/addition of 'game_min'.
    /*
    CASE
      WHEN t.min > 0 AND t.pct_of_team_pts > 0 AND t.game_min > 0 -- t.game_min must be available
      THEN (t.pct_of_team_pts * t.pct_of_team_fga) / (t.min / NULLIF(t.game_min, 0))
      ELSE 0
    END AS scoring_load_index,
    */

    CASE
      WHEN t.fga > 0 OR t.fta > 0
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
    ) AS scoring_versatility_ratio,

    CASE
      WHEN COALESCE(t.usage_pct, 0) > 0 AND COALESCE(t.pct_of_team_fga, 0) > 0
      THEN (COALESCE(t.usage_pct, 0) * 0.7) + (COALESCE(t.pct_of_team_ast, 0) / NULLIF(t.pct_of_team_fga, 0) * 0.3)
      WHEN COALESCE(t.usage_pct, 0) > 0
      THEN COALESCE(t.usage_pct, 0) * 0.7
      ELSE 0
    END AS offensive_role_factor,

    CASE
      WHEN t.min > 0
      THEN COALESCE(t.usage_pct, 0) * (1 + (COALESCE(t.matchup_fg_pct, 0.5) - 0.5))
      ELSE 0
    END AS adjusted_usage_with_defense,

    COALESCE(t.min, 0) - LAG(COALESCE(t.min, 0), 3, 0) OVER (PARTITION BY t.player_id ORDER BY t.game_date) AS recent_minutes_trend,

    CASE
      WHEN (COALESCE(t.possessions, 0) - (COALESCE(t.fastbreak_pts, 0) / 2.0)) > 0
      THEN (COALESCE(t.pts, 0) - COALESCE(t.fastbreak_pts, 0)) / NULLIF((COALESCE(t.possessions, 0) - (COALESCE(t.fastbreak_pts, 0) / 2.0)), 0)
      ELSE 0
    END AS pts_per_half_court_poss,

    (COALESCE(t.ts_pct, 0) * COALESCE(t.usage_pct, 0)) AS usage_weighted_ts,

    (COALESCE(t.pct_fga_3pt, 0) * COALESCE(t.fg3_pct, 0)) AS three_pt_value_efficiency_index,

    (COALESCE(t.pct_pts_in_paint, 0) * COALESCE(t.pct_fga_2pt, 0)) AS paint_reliance_index,

    (COALESCE(t.pct_assisted_fgm, 0) * COALESCE(t.eff_fg_pct, 0)) AS assisted_shot_efficiency,

    CASE
      WHEN COALESCE(t.pace_per_40, 0) > 0 AND t.pts_per_min IS NOT NULL -- pts_per_min is assumed from combined_boxscore_data
      THEN t.pts_per_min * (100.0 / t.pace_per_40)
      ELSE 0
    END AS pace_adjusted_points_per_minute,

    CASE
      WHEN COALESCE(t.off_reb, 0) > 0
      THEN COALESCE(t.second_chance_pts, 0)::decimal / t.off_reb
      ELSE 0
    END AS second_chance_conversion_rate,

    (COALESCE(t.cont_fg_pct, 0) - COALESCE(t.uncont_fg_pct, 0)) AS contested_vs_uncontested_fg_pct_diff,

    CASE
      WHEN COALESCE(t.min, 0) > 0
      THEN (COALESCE(t.fga, 0) + 0.44 * COALESCE(t.fta, 0)) / t.min
      ELSE 0
    END AS shooting_volume_per_min,

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
      WHEN (COALESCE(t.ast_pct, 0) + 1e-6) > 0
      THEN COALESCE(t.usage_pct, 0) / (COALESCE(t.ast_pct, 0) + 1e-6)
      ELSE 0
    END AS scoring_focus_ratio,

    CASE
      WHEN COALESCE(t.min, 0) > 0
      THEN COALESCE(t.cont_fgm, 0) / t.min
      ELSE 0
    END AS contested_fg_makes_per_minute,

    (COALESCE(t.pct_pts_3pt, 0) - COALESCE(t.pct_pts_in_paint, 0)) AS scoring_profile_balance_3pt_vs_paint

FROM final_joined_data t