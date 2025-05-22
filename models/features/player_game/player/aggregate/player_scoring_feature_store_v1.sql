--models/features/player_game/player/aggregate/player_scoring_feature_store_v1.sql

{{ config(
    schema='features',
    materialized='table',
    unique_key='player_game_key',
    incremental_strategy='delete+insert'
) }}

SELECT
    dm.player_game_key,
    dm.player_id,
    dm.player_name,
    dm.game_id,
    dm.team_id,
    dm.season_year,
    dm.game_date,
    
    -- Context info
    COALESCE(gc.home_away, 'UNKNOWN') as home_away,
    COALESCE(gc.position, 'UNKNOWN') as position,
    COALESCE(id.opponent_id, 0) as opponent_id,
    
    -- Derived metrics (always present) - with 3-decimal precision
    ROUND(dm.pts_roll_10g_avg, 3) as pts_roll_10g_avg,
    ROUND(dm.ts_pct_roll_10g_avg, 3) as ts_pct_roll_10g_avg,
    ROUND(dm.usage_pct_roll_10g_avg, 3) as usage_pct_roll_10g_avg,
    ROUND(dm.offensive_role_factor_roll_10g, 3) as offensive_role_factor_roll_10g,
    ROUND(dm.scoring_efficiency_composite_roll_10g, 3) as scoring_efficiency_composite_roll_10g,
    ROUND(dm.usage_weighted_ts_roll_10g, 3) as usage_weighted_ts_roll_10g,
    ROUND(dm.points_opportunity_ratio_roll_10g, 3) as points_opportunity_ratio_roll_10g,
    ROUND(dm.shot_creation_index_roll_10g, 3) as shot_creation_index_roll_10g,
    ROUND(dm.self_created_scoring_rate_per_min_roll_10g, 3) as self_created_scoring_rate_per_min_roll_10g,
    ROUND(dm.shooting_volume_per_min_roll_10g, 3) as shooting_volume_per_min_roll_10g,
    ROUND(dm.free_throw_generation_aggressiveness_roll_10g, 3) as free_throw_generation_aggressiveness_roll_10g,
    ROUND(dm.contested_vs_uncontested_fg_pct_diff_roll_10g, 3) as contested_vs_uncontested_fg_pct_diff_roll_10g,
    ROUND(dm.contested_fg_makes_per_minute_roll_10g, 3) as contested_fg_makes_per_minute_roll_10g,
    ROUND(dm.scoring_versatility_ratio_roll_10g, 3) as scoring_versatility_ratio_roll_10g,
    ROUND(dm.three_pt_value_efficiency_index_roll_10g, 3) as three_pt_value_efficiency_index_roll_10g,
    ROUND(dm.paint_reliance_index_roll_10g, 3) as paint_reliance_index_roll_10g,
    ROUND(dm.scoring_profile_balance_3pt_vs_paint_roll_10g, 3) as scoring_profile_balance_3pt_vs_paint_roll_10g,
    ROUND(dm.scoring_focus_ratio_roll_10g, 3) as scoring_focus_ratio_roll_10g,
    ROUND(dm.pace_adjusted_points_per_minute_roll_10g, 3) as pace_adjusted_points_per_minute_roll_10g,
    ROUND(dm.opportunistic_scoring_rate_per_min_roll_10g, 3) as opportunistic_scoring_rate_per_min_roll_10g,
    
    -- Role consistency
    CASE
        WHEN cp.player_offensive_role = 'PRIMARY_OPTION' AND dm.offensive_role_factor_roll_10g > 0.25 THEN 'CONSISTENT_PRIMARY'
        WHEN cp.player_offensive_role = 'SECONDARY_OPTION' AND dm.offensive_role_factor_roll_10g BETWEEN 0.15 AND 0.30 THEN 'CONSISTENT_SECONDARY'
        WHEN cp.player_offensive_role = 'SUPPORTING_ROLE' AND dm.offensive_role_factor_roll_10g < 0.20 THEN 'CONSISTENT_SUPPORT'
        ELSE 'ROLE_MISMATCH'
    END AS role_consistency_indicator,
    
    -- Contextual features (now properly joined) - with 3-decimal precision
    ROUND(cp.team_l5_pace, 3) as team_l5_pace,
    ROUND(cp.team_l5_off_rating, 3) as team_l5_off_rating,
    cp.team_form,
    cp.team_playstyle,
    cp.team_offensive_structure,
    cp.player_offensive_role,
    ROUND(cp.player_team_style_fit_score, 3) as player_team_style_fit_score,
    cp.pace_impact_on_player,
    cp.usage_opportunity,
    cp.team_form_player_impact,
    cp.team_playstyle_stat_impact,
    ROUND(cp.pace_multiplier, 3) as pace_multiplier,
    ROUND(cp.usage_multiplier, 3) as usage_multiplier,
    ROUND(cp.team_form_multiplier, 3) as team_form_multiplier,
    ROUND(cp.playstyle_stat_multiplier, 3) as playstyle_stat_multiplier,
    ROUND(cp.style_fit_multiplier, 3) as style_fit_multiplier,
    ROUND(cp.team_adjusted_pts_projection, 3) as team_adjusted_pts_projection,
    ROUND(cp.team_adjusted_reb_projection, 3) as team_adjusted_reb_projection,
    ROUND(cp.team_adjusted_ast_projection, 3) as team_adjusted_ast_projection,
    cp.team_context_impact,
    ROUND(cp.player_l5_pts, 3) as player_l5_pts,
    ROUND(cp.player_l5_reb, 3) as player_l5_reb,
    ROUND(cp.player_l5_ast, 3) as player_l5_ast,
    ROUND(cp.pts_in_team_hot_streaks, 3) as pts_in_team_hot_streaks,
    ROUND(cp.pts_in_team_cold_streaks, 3) as pts_in_team_cold_streaks,
    ROUND(cp.pts_in_star_dominant_system, 3) as pts_in_star_dominant_system,
    ROUND(cp.pts_in_balanced_system, 3) as pts_in_balanced_system,
    
    -- Combined context multiplier - with 3-decimal precision
    ROUND(COALESCE(
        (cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
         cp.playstyle_stat_multiplier * cp.style_fit_multiplier), 1.0
    ), 3) AS combined_context_multiplier,
    
    -- Context sensitivity tier
    CASE
        WHEN COALESCE((cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
                      cp.playstyle_stat_multiplier * cp.style_fit_multiplier), 1.0) > 1.15 THEN 'HIGH_CONTEXT_BOOST'
        WHEN COALESCE((cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
                      cp.playstyle_stat_multiplier * cp.style_fit_multiplier), 1.0) > 1.05 THEN 'MODERATE_CONTEXT_BOOST'
        WHEN COALESCE((cp.pace_multiplier * cp.usage_multiplier * cp.team_form_multiplier * 
                      cp.playstyle_stat_multiplier * cp.style_fit_multiplier), 1.0) < 0.85 THEN 'CONTEXT_LIMITATION'
        ELSE 'CONTEXT_NEUTRAL'
    END AS context_sensitivity_tier,
    
    -- Projection accuracy metrics - with 3-decimal precision
    ROUND(COALESCE(ABS(cp.team_adjusted_pts_projection - dm.pts_roll_10g_avg), 0), 3) AS projection_vs_baseline_pts_diff,
    
    ROUND(CASE
        WHEN dm.pts_roll_10g_avg > 0 
        THEN COALESCE(ABS(cp.team_adjusted_pts_projection - dm.pts_roll_10g_avg) / dm.pts_roll_10g_avg, 0)
        ELSE 0
    END, 3) AS projection_accuracy_ratio,
    
    -- Composite efficiency reliability - with 3-decimal precision
    ROUND(COALESCE(
        (dm.scoring_efficiency_composite_roll_10g * 0.4 + 
         dm.usage_weighted_ts_roll_10g * 0.3 + 
         dm.points_opportunity_ratio_roll_10g * 0.3), 0
    ), 3) AS composite_efficiency_reliability,
    
    -- Pressure performance tier
    CASE
        WHEN dm.contested_fg_makes_per_minute_roll_10g > 0.5 AND dm.usage_pct_roll_10g_avg > 0.25 THEN 'HIGH_PRESSURE_PERFORMER'
        WHEN dm.contested_vs_uncontested_fg_pct_diff_roll_10g > -0.05 THEN 'PRESSURE_RESILIENT'
        WHEN dm.contested_vs_uncontested_fg_pct_diff_roll_10g < -0.15 THEN 'PRESSURE_SENSITIVE'
        ELSE 'PRESSURE_NEUTRAL'
    END AS pressure_performance_tier,
    
    -- Scoring archetype
    CASE
        WHEN COALESCE(id.pct_pts_in_paint, 0) > 0.6 AND dm.paint_reliance_index_roll_10g > 0.4 THEN 'INTERIOR_DOMINANT'
        WHEN dm.three_pt_value_efficiency_index_roll_10g > 0.15 AND COALESCE(id.pct_pts_in_paint, 0) < 0.4 THEN 'PERIMETER_FOCUSED'
        WHEN ABS(COALESCE(dm.scoring_profile_balance_3pt_vs_paint_roll_10g, 0)) < 0.1 THEN 'BALANCED_SCORER'
        ELSE 'SITUATIONAL_SCORER'
    END AS scoring_archetype,
    
    -- Interior scoring - with 3-decimal precision
    ROUND(COALESCE(id.pct_pts_in_paint, 0), 3) AS pct_pts_in_paint,
    ROUND(COALESCE(id.def_at_rim_fg_pct, 0), 3) AS def_at_rim_fg_pct,
    ROUND(COALESCE(id.def_at_rim_fga_rate, 0), 3) AS def_at_rim_fga_rate,
    ROUND(COALESCE(id.paint_scoring_reliance_deviation, 0), 3) AS paint_scoring_reliance_deviation,
    ROUND(COALESCE(id.rim_finishing_efficiency_deviation, 0), 3) AS rim_finishing_efficiency_deviation,
    ROUND(COALESCE(id.rim_attempt_rate_deviation, 0), 3) AS rim_attempt_rate_deviation,
    
    -- Matchup adaptability index - with 3-decimal precision
    ROUND(COALESCE(
        (ABS(COALESCE(id.paint_scoring_reliance_deviation, 0)) + 
         ABS(COALESCE(id.rim_finishing_efficiency_deviation, 0)) + 
         ABS(COALESCE(id.rim_attempt_rate_deviation, 0))) * 
         COALESCE(dm.scoring_versatility_ratio_roll_10g, 0), 0
    ), 3) AS matchup_adaptability_index,
    
    -- Team contribution - with 3-decimal precision
    ROUND(COALESCE(gc.pct_of_team_pts, 0), 3) AS pct_of_team_pts,
    ROUND(COALESCE(gc.pct_of_team_reb, 0), 3) AS pct_of_team_reb,
    ROUND(COALESCE(gc.pct_of_team_ast, 0), 3) AS pct_of_team_ast,
    ROUND(COALESCE(gc.pct_of_team_fga, 0), 3) AS pct_of_team_fga,
    COALESCE(gc.efficiency_vs_team_avg, 'UNKNOWN') AS efficiency_vs_team_avg,
    
    -- Team offensive impact magnitude - with 3-decimal precision
    ROUND(COALESCE(
        (COALESCE(gc.pct_of_team_pts, 0) * COALESCE(dm.offensive_role_factor_roll_10g, 0) * 
         COALESCE(cp.player_team_style_fit_score, 3) / 5.0), 0
    ), 3) AS team_offensive_impact_magnitude,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM {{ ref('feat_player__derived_scoring_metrics') }} dm

-- Component-based joins for reliable data integration
LEFT JOIN {{ ref('feat_player__contextual_projections') }} cp 
    ON dm.player_id = cp.player_id 
    AND dm.game_id = cp.game_id

LEFT JOIN {{ ref('feat_player__interior_scoring_deviations') }} id 
    ON dm.player_id = id.player_id 
    AND dm.game_id = id.game_id
    
LEFT JOIN {{ ref('feat_player__game_context_attributes') }} gc 
    ON dm.player_id = gc.player_id 
    AND dm.game_id = gc.game_id

{% if is_incremental() %}
WHERE dm.game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }})
{% endif %}