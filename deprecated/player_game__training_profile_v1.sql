{{ config(
    schema='features',
    materialized='table',
    unique_key='player_game_key',
    tags=['feature', 'pga', 'profile', 'training']
) }}

WITH player_boxscore_features AS (
    SELECT * FROM {{ ref('int_player__combined_boxscore') }}
),

player_prop_features AS (
    SELECT
        *
    FROM {{ ref('feat_betting__player_props_probabilities') }}
    WHERE market = 'Points O/U' -- Filter for Points O/U market
),

player_scoring_features AS (
    SELECT * FROM {{ ref('player_scoring_features_v2') }}
),

player_milestone_features AS (
    SELECT * FROM {{ ref('int_player__milestone_games_v2') }}
),

opponent_profile_features AS (
    SELECT * FROM {{ ref('opponent_pregame_profile_features_v2') }}
),

opponent_position_defense_features AS (
    SELECT * FROM {{ ref('opponent_position_defense_features_v2') }}
),

player_pts_aggregate_features AS (
    SELECT * FROM {{ ref('player_pts_aggregate_v1') }}
)

SELECT
    -- Core Identifiers and Metadata from player_matchup_features
    pbf.game_id,
    pbf.player_id,
    pbf.player_name,
    pbf.team_id,
    pbf.opponent_id,
    pbf.game_date,
    pbf.season_year,
    pbf.home_away,
    pbf.position,

    -- Target variable and related betting line from player_scoring_features
    pbf.pts as target_pts,
    ppf.line as prop_line,

    -- Features from player_scoring_features (explicitly listed)
    psf.pts_roll_3g_avg,
    psf.ts_pct_roll_3g_avg,
    psf.usage_pct_roll_3g_avg,
    psf.offensive_role_factor_roll_3g,
    psf.scoring_efficiency_composite_roll_3g,
    psf.usage_weighted_ts_roll_3g,
    psf.points_opportunity_ratio_roll_3g,
    psf.shot_creation_index_roll_3g,
    psf.self_created_scoring_rate_per_min_roll_3g,
    psf.shooting_volume_per_min_roll_3g,
    psf.free_throw_generation_aggressiveness_roll_3g,
    psf.contested_vs_uncontested_fg_pct_diff_roll_3g,
    psf.contested_fg_makes_per_minute_roll_3g,
    psf.scoring_versatility_ratio_roll_3g,
    psf.three_pt_value_efficiency_index_roll_3g,
    psf.paint_reliance_index_roll_3g,
    psf.scoring_profile_balance_3pt_vs_paint_roll_3g,
    psf.scoring_focus_ratio_roll_3g,
    psf.pace_adjusted_points_per_minute_roll_3g,
    psf.opportunistic_scoring_rate_per_min_roll_3g,
    psf.pts_roll_5g_avg,
    psf.ts_pct_roll_5g_avg,
    psf.usage_pct_roll_5g_avg,
    psf.offensive_role_factor_roll_5g,
    psf.scoring_efficiency_composite_roll_5g,
    psf.usage_weighted_ts_roll_5g,
    psf.points_opportunity_ratio_roll_5g,
    psf.shot_creation_index_roll_5g,
    psf.self_created_scoring_rate_per_min_roll_5g,
    psf.shooting_volume_per_min_roll_5g,
    psf.free_throw_generation_aggressiveness_roll_5g,
    psf.contested_vs_uncontested_fg_pct_diff_roll_5g,
    psf.contested_fg_makes_per_minute_roll_5g,
    psf.scoring_versatility_ratio_roll_5g,
    psf.three_pt_value_efficiency_index_roll_5g,
    psf.paint_reliance_index_roll_5g,
    psf.scoring_profile_balance_3pt_vs_paint_roll_5g,
    psf.scoring_focus_ratio_roll_5g,
    psf.pace_adjusted_points_per_minute_roll_5g,
    psf.opportunistic_scoring_rate_per_min_roll_5g,
    psf.pts_roll_10g_avg,
    psf.ts_pct_roll_10g_avg,
    psf.usage_pct_roll_10g_avg,
    psf.offensive_role_factor_roll_10g,
    psf.scoring_efficiency_composite_roll_10g,
    psf.usage_weighted_ts_roll_10g,
    psf.points_opportunity_ratio_roll_10g,
    psf.shot_creation_index_roll_10g,
    psf.self_created_scoring_rate_per_min_roll_10g,
    psf.shooting_volume_per_min_roll_10g,
    psf.free_throw_generation_aggressiveness_roll_10g,
    psf.contested_vs_uncontested_fg_pct_diff_roll_10g,
    psf.contested_fg_makes_per_minute_roll_10g,
    psf.scoring_versatility_ratio_roll_10g,
    psf.three_pt_value_efficiency_index_roll_10g,
    psf.paint_reliance_index_roll_10g,
    psf.scoring_profile_balance_3pt_vs_paint_roll_10g,
    psf.scoring_focus_ratio_roll_10g,
    psf.pace_adjusted_points_per_minute_roll_10g,
    psf.opportunistic_scoring_rate_per_min_roll_10g,
    psf.role_consistency_indicator,
    psf.team_l5_pace,
    psf.team_l5_off_rating,
    psf.team_form,
    psf.team_playstyle,
    psf.team_offensive_structure,
    psf.player_offensive_role,
    psf.player_team_style_fit_score,
    psf.pace_impact_on_player,
    psf.usage_opportunity,
    psf.team_form_player_impact,
    psf.team_playstyle_stat_impact,
    psf.pace_multiplier,
    psf.usage_multiplier,
    psf.team_form_multiplier,
    psf.playstyle_stat_multiplier,
    psf.style_fit_multiplier,
    psf.team_adjusted_pts_projection,
    psf.team_adjusted_reb_projection,
    psf.team_adjusted_ast_projection,
    psf.team_context_impact,
    psf.player_l5_pts,
    psf.player_l5_reb,
    psf.player_l5_ast,
    psf.pts_in_team_hot_streaks,
    psf.pts_in_team_cold_streaks,
    psf.pts_in_star_dominant_system,
    psf.pts_in_balanced_system,
    psf.combined_context_multiplier,
    psf.context_sensitivity_tier,
    psf.projection_vs_baseline_pts_diff,
    psf.projection_accuracy_ratio,
    psf.composite_efficiency_reliability,
    psf.pressure_performance_tier,
    psf.scoring_archetype,
    psf.pct_pts_in_paint_roll_10g,
    psf.def_at_rim_fg_pct_roll_10g,
    psf.def_at_rim_fga_rate_roll_10g,
    psf.paint_scoring_reliance_deviation,
    psf.rim_finishing_efficiency_deviation,
    psf.rim_attempt_rate_deviation,
    psf.matchup_adaptability_index,
    psf.efficiency_vs_team_avg,
    psf.team_offensive_impact_magnitude_3g_avg,
    psf.team_offensive_impact_magnitude_5g_avg,
    psf.team_offensive_impact_magnitude_10g_avg,

    -- Features from player_matchup_features (explicitly listed)
    pmf.opponent_defensive_rating,
    pmf.opponent_pace,
    pmf.opponent_adjusted_def_rating,
    -- pmf.avg_pts_allowed_to_position, -- These are now included from opdf
    -- pmf.pts_vs_league_avg, -- These are now included from opdf
    pmf.pts_matchup_label,
    pmf.hist_avg_pts_vs_opp,
    pmf.hist_games_vs_opp,
    pmf.hist_performance_flag,
    pmf.hist_confidence,
    pmf.hist_recent_pts_vs_opp,
    pmf.team_recent_off_rating,
    pmf.is_missing_teammate,
    pmf.blended_pts_projection,


    -- Features from player_milestone_features (include all columns)
    {{ dbt_utils.star(from=ref('int_player__milestone_games_v2'), relation_alias='pmsf', except=[
        "player_game_key", "player_id", "player_name", "game_date", "season_id", "season_year", "pts",
        "is_30_plus_pt_game", "is_40_plus_pt_game", "is_50_plus_pt_game", "is_60_plus_pt_game", 
        "is_70_plus_pt_game", "is_80_plus_pt_game", "game_id" 
    ]) }},

    -- Features from opponent_profile_features
    {{ dbt_utils.star(from=ref('opponent_pregame_profile_features_v2'), relation_alias='opf', except=[
        "team_game_key", "game_id", "game_date", "team_id", "opponent_id", "season_year", "home_away",
        "created_at", "updated_at"
    ]) }},

    -- Features from opponent_position_defense_features (include all columns except join keys)
    {{ dbt_utils.star(from=ref('opponent_position_defense_features_v2'), relation_alias='opdf', except=[
        "opponent_id", "position", "season_year", "game_date"
    ]) }},

    -- Features from player_pts_aggregate_features
    {{ dbt_utils.star(from=ref('player_pts_aggregate_v1'), relation_alias='ppaf', except=[
        "player_game_key", "player_id", "player_name", "game_id", "game_date", "season_year", 
        "team_id", "opponent_id", "_dbt_feature_processed_at"
    ]) }},

    CURRENT_TIMESTAMP AS _model_created_at,
    CURRENT_TIMESTAMP AS _model_updated_at

FROM player_boxscore_features pbf
LEFT JOIN player_matchup_features pmf
    ON pbf.player_game_key = pmf.player_game_key
LEFT JOIN player_prop_features ppf
    ON pbf.player_id = ppf.player_id AND pbf.game_date = ppf.game_date
LEFT JOIN player_scoring_features psf
    ON pbf.player_game_key = psf.player_game_key
LEFT JOIN player_milestone_features pmsf
    ON pbf.player_game_key = pmsf.player_game_key
LEFT JOIN opponent_profile_features opf
    ON pbf.game_id = opf.game_id AND pmf.opponent_id = opf.team_id
LEFT JOIN opponent_position_defense_features opdf
    ON pbf.opponent_id = opdf.opponent_id
    AND pbf.position = opdf.position
    AND pbf.season_year = opdf.season_year
    AND pbf.game_date = opdf.game_date
LEFT JOIN player_pts_aggregate_features ppaf
    ON pbf.player_game_key = ppaf.player_game_key