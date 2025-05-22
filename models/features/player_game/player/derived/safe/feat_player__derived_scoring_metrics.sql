{{ config(
    schema='features',
    materialized='incremental',
    tags=['derived', 'features', 'player_derived_metrics', 'scoring_metrics'],
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

-- Join with rolling stats models
-- Assuming these models exist and contain 10-game rolling averages
-- Adjust model names and aliases as necessary based on your project structure
WITH rolling_traditional AS (
    SELECT * FROM {{ ref('feat_player__traditional_rolling') }}
),
rolling_usage AS (
    SELECT * FROM {{ ref('feat_player__usage_rolling') }}
),
rolling_scoring AS (
    SELECT * FROM {{ ref('feat_player__scoring_rolling') }}
),
rolling_advanced AS (
    SELECT * FROM {{ ref('feat_player__advanced_rolling') }}
),
rolling_misc AS (
    SELECT * FROM {{ ref('feat_player__misc_rolling') }}
),
rolling_tracking AS (
    SELECT * FROM {{ ref('feat_player__tracking_rolling') }}
),
rolling_defensive AS (
    SELECT * FROM {{ ref('feat_player__defensive_rolling') }}
)

SELECT
    rtrad.player_game_key,
    rtrad.player_id,
    rtrad.player_name,
    rtrad.game_id,
    rtrad.team_id,
    rtrad.season_year,
    rtrad.game_date,

    -- Select necessary rolling averages
    rtrad.pts_roll_10g_avg,
    rtrad.fga_roll_10g_avg,
    rtrad.fgm_roll_10g_avg,
    rtrad.fg3m_roll_10g_avg,
    rtrad.fta_roll_10g_avg,
    rtrad.fg3_pct_roll_10g_avg,
    rtrad.off_reb_roll_10g_avg,
    rtrad.min_roll_10g_avg,
    
    -- Rolling tracking stats
    rtrack.touches_roll_10g_avg,
    rtrack.cont_fga_roll_10g_avg,
    rtrack.cont_fgm_roll_10g_avg,
    rtrack.cont_fg_pct_roll_10g_avg,
    rtrack.uncont_fg_pct_roll_10g_avg,
    rtrack.def_at_rim_fga_roll_10g_avg,

    -- Rolling advanced stats
    radv.eff_fg_pct_roll_10g_avg,
    radv.ts_pct_roll_10g_avg,
    radv.usage_pct_roll_10g_avg,
    radv.possessions_roll_10g_avg,
    radv.pace_per_40_roll_10g_avg,

    -- Rolling usage stats
    ruse.pct_of_team_fga_roll_10g_avg,
    ruse.pct_of_team_ast_roll_10g_avg,

    -- Rolling misc stats    
    rmisc.fastbreak_pts_roll_10g_avg,
    rmisc.pts_off_tov_roll_10g_avg,
    rmisc.second_chance_pts_roll_10g_avg,

    -- Rolling scoring stats
    rsc.pct_unassisted_fgm_roll_10g_avg,
    rsc.pct_pts_2pt_roll_10g_avg,
    rsc.pct_pts_3pt_roll_10g_avg,
    rsc.pct_pts_ft_roll_10g_avg,
    rsc.pct_pts_in_paint_roll_10g_avg,
    rsc.pct_fga_2pt_roll_10g_avg,
    rsc.pct_fga_3pt_roll_10g_avg,
    rsc.pct_assisted_fgm_roll_10g_avg,


-- Derived features using ROLLING 10-game averages
    CASE
      WHEN rtrad.fga_roll_10g_avg > 0 OR rtrad.fta_roll_10g_avg > 0
      THEN (COALESCE(radv.ts_pct_roll_10g_avg, 0) * 0.6) + (COALESCE(radv.eff_fg_pct_roll_10g_avg, 0) * 0.4)
      ELSE 0
    END AS scoring_efficiency_composite_roll_10g,

    CASE
      WHEN rtrack.touches_roll_10g_avg > 0
      THEN rtrad.pts_roll_10g_avg / NULLIF(rtrack.touches_roll_10g_avg, 0)
      ELSE 0
    END AS points_opportunity_ratio_roll_10g,

    CASE
      WHEN rtrad.fga_roll_10g_avg > 0
      THEN rtrad.fga_roll_10g_avg * rsc.pct_unassisted_fgm_roll_10g_avg * ruse.pct_of_team_fga_roll_10g_avg
      ELSE 0
    END AS shot_creation_index_roll_10g,

    CASE
      WHEN rtrad.fga_roll_10g_avg > 0
      THEN (rtrack.cont_fga_roll_10g_avg / NULLIF(rtrad.fga_roll_10g_avg, 0)) * (1 + rtrack.def_at_rim_fga_roll_10g_avg / NULLIF(rtrad.fga_roll_10g_avg, 0))
      ELSE 0
    END AS defensive_attention_factor_roll_10g,

    1 - (
        ABS(rsc.pct_pts_2pt_roll_10g_avg - 0.3333) +
        ABS(rsc.pct_pts_3pt_roll_10g_avg - 0.3333) +
        ABS(rsc.pct_pts_ft_roll_10g_avg - 0.3333)
    ) AS scoring_versatility_ratio_roll_10g,

    CASE
      WHEN radv.usage_pct_roll_10g_avg > 0 AND ruse.pct_of_team_fga_roll_10g_avg > 0
      THEN (radv.usage_pct_roll_10g_avg * 0.7) + (ruse.pct_of_team_ast_roll_10g_avg / NULLIF(ruse.pct_of_team_fga_roll_10g_avg, 0) * 0.3)
      WHEN radv.usage_pct_roll_10g_avg > 0
      THEN radv.usage_pct_roll_10g_avg * 0.7
      ELSE 0
    END AS offensive_role_factor_roll_10g,

    CASE
      WHEN rtrad.min_roll_10g_avg > 0
      THEN radv.usage_pct_roll_10g_avg * (1 + (rdef.matchup_fg_pct_roll_10g_avg - 0.5))
      ELSE 0
    END AS adjusted_usage_with_defense_roll_10g,

    CASE
      WHEN (radv.possessions_roll_10g_avg - (rmisc.fastbreak_pts_roll_10g_avg / 2.0)) > 0
      THEN (rtrad.pts_roll_10g_avg - rmisc.fastbreak_pts_roll_10g_avg) / NULLIF((radv.possessions_roll_10g_avg - (rmisc.fastbreak_pts_roll_10g_avg / 2.0)), 0)
      ELSE 0
    END AS pts_per_half_court_poss_roll_10g,

    (radv.ts_pct_roll_10g_avg * radv.usage_pct_roll_10g_avg) AS usage_weighted_ts_roll_10g,

    (rsc.pct_fga_3pt_roll_10g_avg * rtrad.fg3_pct_roll_10g_avg) AS three_pt_value_efficiency_index_roll_10g,

    (rsc.pct_pts_in_paint_roll_10g_avg * rsc.pct_fga_2pt_roll_10g_avg) AS paint_reliance_index_roll_10g,

    (rsc.pct_assisted_fgm_roll_10g_avg * radv.eff_fg_pct_roll_10g_avg) AS assisted_shot_efficiency_roll_10g,

    CASE
      WHEN radv.pace_per_40_roll_10g_avg > 0 AND rtrad.min_roll_10g_avg > 0
      THEN (rtrad.pts_roll_10g_avg / NULLIF(rtrad.min_roll_10g_avg,0)) * (100.0 / NULLIF(radv.pace_per_40_roll_10g_avg, 0))
      ELSE 0
    END AS pace_adjusted_points_per_minute_roll_10g,

    CASE
      WHEN rtrad.off_reb_roll_10g_avg > 0
      THEN rmisc.second_chance_pts_roll_10g_avg::decimal / rtrad.off_reb_roll_10g_avg
      ELSE 0
    END AS second_chance_conversion_rate_roll_10g,

    (rtrack.cont_fg_pct_roll_10g_avg - rtrack.uncont_fg_pct_roll_10g_avg) AS contested_vs_uncontested_fg_pct_diff_roll_10g,

    CASE
      WHEN rtrad.min_roll_10g_avg > 0
      THEN (rtrad.fga_roll_10g_avg + 0.44 * rtrad.fta_roll_10g_avg) / rtrad.min_roll_10g_avg
      ELSE 0
    END AS shooting_volume_per_min_roll_10g,

    CASE
      WHEN rtrad.fga_roll_10g_avg > 0
      THEN (rtrad.fg3m_roll_10g_avg * 3) / NULLIF(rtrad.fga_roll_10g_avg, 0)
      ELSE 0
    END AS effective_three_point_contribution_rate_roll_10g,

    CASE
      WHEN rtrad.fga_roll_10g_avg > 0
      THEN rtrad.fta_roll_10g_avg / NULLIF(rtrad.fga_roll_10g_avg, 0)
      ELSE 0
    END AS free_throw_generation_aggressiveness_roll_10g,

    CASE
      WHEN rtrad.min_roll_10g_avg > 0 AND rtrad.fgm_roll_10g_avg > 0
      THEN (rsc.pct_unassisted_fgm_roll_10g_avg * rtrad.fgm_roll_10g_avg) / rtrad.min_roll_10g_avg
      ELSE 0
    END AS self_created_scoring_rate_per_min_roll_10g,

    CASE
      WHEN rtrad.min_roll_10g_avg > 0
      THEN (rmisc.pts_off_tov_roll_10g_avg + rmisc.fastbreak_pts_roll_10g_avg) / rtrad.min_roll_10g_avg
      ELSE 0
    END AS opportunistic_scoring_rate_per_min_roll_10g,

    CASE
      WHEN (ruse.pct_of_team_ast_roll_10g_avg + 1e-6) > 0
      THEN radv.usage_pct_roll_10g_avg / (ruse.pct_of_team_ast_roll_10g_avg + 1e-6)
      ELSE 0
    END AS scoring_focus_ratio_roll_10g,

    CASE
      WHEN rtrad.min_roll_10g_avg > 0
      THEN rtrack.cont_fgm_roll_10g_avg / rtrad.min_roll_10g_avg
      ELSE 0
    END AS contested_fg_makes_per_minute_roll_10g,

    (rsc.pct_pts_3pt_roll_10g_avg - rsc.pct_pts_in_paint_roll_10g_avg) AS scoring_profile_balance_3pt_vs_paint_roll_10g

FROM rolling_traditional rtrad
LEFT JOIN rolling_usage ruse ON rtrad.player_game_key = ruse.player_game_key
LEFT JOIN rolling_scoring rsc ON rtrad.player_game_key = rsc.player_game_key
LEFT JOIN rolling_advanced radv ON rtrad.player_game_key = radv.player_game_key
LEFT JOIN rolling_misc rmisc ON rtrad.player_game_key = rmisc.player_game_key
LEFT JOIN rolling_tracking rtrack ON rtrad.player_game_key = rtrack.player_game_key
LEFT JOIN rolling_defensive rdef ON rtrad.player_game_key = rdef.player_game_key

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE rt.game_date > (SELECT MAX(game_date) FROM {{ this }})
{% endif %}