{{ config(
    schema='features',
    materialized='incremental',
    tags=['features', 'player_game', 'player', 'aggregate', 'scoring_profile'],
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

-- CTE for derived scoring metrics
WITH derived_scoring_metrics_source AS (
    SELECT *
    FROM {{ ref('feat_player__derived_scoring_metrics') }}
    {% if is_incremental() %}
    WHERE game_date >= (
        SELECT LEAST(
                   MAX(game_date) - INTERVAL '{{ var('feature_recalc_window_days', 90) }} days',
                   MIN(game_date)
               )
        FROM {{ this }}
    ) OR game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- CTE for interior scoring deviations
interior_scoring_deviations_source AS (
    SELECT *
    FROM {{ ref('feat_player__interior_scoring_deviations') }}
    {% if is_incremental() %}
    WHERE game_date >= (
        SELECT LEAST(
                   MAX(game_date) - INTERVAL '{{ var('feature_recalc_window_days', 90) }} days',
                   MIN(game_date)
               )
        FROM {{ this }}
    ) OR game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- CTE for contextual projections
contextual_projections_source AS (
    SELECT *
    FROM {{ ref('feat_player__contextual_projections') }}
),

-- CTE for game context attributes
game_context_attributes_source AS (
    SELECT *
    FROM {{ ref('feat_player__game_context_attributes') }}
),

-- Combined features
combined_features AS (
    SELECT
        -- Key from derived_scoring_metrics_source as the base
        dsm.player_game_key,
        dsm.player_id,
        dsm.game_id,
        dsm.game_date,
        dsm.season_year,
        dsm.player_name, -- Assuming player_name is consistently available or can be taken from one primary source
        dsm.team_id,

        -- Features from derived_scoring_metrics_source
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
        dsm.scoring_profile_balance_3pt_vs_paint_roll_10g,
        -- Include other necessary base columns from derived_scoring_metrics if needed for joins or context
        dsm.pts_roll_10g_avg, -- Example, if needed

        -- Features from interior_scoring_deviations_source
        isd.paint_scoring_reliance_deviation,
        isd.rim_finishing_efficiency_deviation,
        isd.rim_attempt_rate_deviation,
        -- Raw metrics from interior_scoring for context if desired
        isd.pct_pts_in_paint AS game_pct_pts_in_paint,
        isd.def_at_rim_fg_pct AS game_def_at_rim_fg_pct,
        isd.def_at_rim_fga_rate AS game_def_at_rim_fga_rate,


        -- Features from contextual_projections_source
        cp.player_l5_pts,
        cp.player_l5_reb,
        cp.player_l5_ast,
        cp.player_l5_usage,
        cp.player_l5_ts_pct,
        cp.team_l5_pace,
        cp.team_l5_off_rating,
        cp.team_form AS team_form_contextual, -- aliasing if names conflict
        cp.team_playstyle AS team_playstyle_contextual,
        cp.team_offensive_structure AS team_offensive_structure_contextual,
        cp.player_offensive_role AS player_offensive_role_contextual,
        cp.player_team_style_fit_score,
        cp.pace_impact_on_player AS pace_impact_on_player_contextual,
        cp.usage_opportunity AS usage_opportunity_contextual,
        cp.team_form_player_impact AS team_form_player_impact_contextual,
        cp.team_playstyle_stat_impact AS team_playstyle_stat_impact_contextual,
        cp.pts_in_team_hot_streaks,
        cp.pts_in_team_cold_streaks,
        cp.pts_in_star_dominant_system,
        cp.pts_in_balanced_system,
        cp.pace_multiplier,
        cp.usage_multiplier,
        cp.team_form_multiplier,
        cp.playstyle_stat_multiplier,
        cp.style_fit_multiplier,
        cp.team_adjusted_pts_projection,
        cp.team_adjusted_reb_projection,
        cp.team_adjusted_ast_projection,
        cp.team_context_impact,

        -- Features from game_context_attributes_source
        gca.home_away,
        gca.position,
        -- Player stats (current game from game_context_attributes - be careful with leakage if this model is for pre-game)
        -- gca.min AS game_min_context, -- Example: if needed and non-leaky
        -- gca.pts AS game_pts_context,
        -- gca.usage_pct AS game_usage_pct_context,
        -- gca.ts_pct AS game_ts_pct_context,
        gca.pct_of_team_pts AS game_pct_of_team_pts_context,
        -- Lagged team context from game_context_attributes
        gca.team_l5_pace_lagged,
        gca.team_l5_off_rating_lagged,
        gca.team_l5_def_rating_lagged,
        gca.team_form_lagged,
        gca.team_offense_trend_lagged,
        gca.team_playstyle_lagged,
        gca.team_offensive_structure_lagged,
        -- Player-team relationship metrics from game_context_attributes
        gca.efficiency_vs_team_avg,
        gca.player_offensive_role AS player_offensive_role_attrs,
        gca.player_team_style_fit_score AS player_team_style_fit_score_attrs,
        gca.pace_impact_on_player AS pace_impact_on_player_attrs,
        gca.usage_opportunity AS usage_opportunity_attrs,
        gca.team_form_player_impact AS team_form_player_impact_attrs,
        gca.team_playstyle_stat_impact AS team_playstyle_stat_impact_attrs,
        
        current_timestamp AS _dbt_feature_processed_at

    FROM derived_scoring_metrics_source dsm
    LEFT JOIN interior_scoring_deviations_source isd
        ON dsm.player_game_key = isd.player_game_key
    LEFT JOIN contextual_projections_source cp
        ON dsm.player_game_key = cp.player_game_key
    LEFT JOIN game_context_attributes_source gca
        ON dsm.player_game_key = gca.player_game_key
)

SELECT
    *
FROM combined_features

{% if is_incremental() %}
WHERE player_game_key IN (
    SELECT player_game_key FROM derived_scoring_metrics_source
    UNION
    SELECT player_game_key FROM interior_scoring_deviations_source
)
{% endif %}