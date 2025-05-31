{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'scoring', 'comprehensive', 'ml_safe'],
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['player_id', 'team_id', 'season_year'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']}
    ]
) }}

/*
COMPREHENSIVE ML-SAFE PLAYER SCORING FEATURE STORE
Consolidates all scoring-related features from our v2 model ecosystem.
ZERO DATA LEAKAGE - No actual game outcomes included as features.

Replaces: player_scoring_features_v1 (which had critical data leakage)
Integrates: All our refactored v2 models + milestone games + interior scoring
*/

with base_data as (
    select
        player_game_key,
        player_id,
        player_name,
        game_id,
        team_id,
        opponent_id,
        home_away,
        season_year,
        game_date,
        position,
        pts as target_pts,
        updated_at
    from {{ ref('int_player__combined_boxscore') }}
),

-- Get comprehensive derived scoring metrics (ML-safe)
derived_scoring_metrics as (
    select * from {{ ref('feat_player__derived_scoring_metrics_v2') }}
),

-- Get team context projections (our consolidated model)+
team_context_projections as (
    select * from {{ ref('feat_player__team_context_projections_v2') }}
),

-- Get interior scoring analysis
interior_scoring_analysis as (
    select * from {{ ref('feat_player__interior_scoring_deviations_v2') }}
),

-- Get milestone games analysis
milestone_games as (
    select * from {{ ref('int_player__milestone_games_v2') }}
),

-- Get additional rolling metrics for comprehensive coverage
player_traditional_rolling as (
    select * from {{ ref('feat_player__traditional_rolling_v2') }}
),

player_usage_rolling as (
    select * from {{ ref('feat_player__usage_rolling_v2') }}
),

-- Get opponent context for matchup analysis
opponent_context as (
    select * from {{ ref('feat_opp__opponent_stats_v2') }}
),

comprehensive_scoring_features as (
    select
        bd.player_game_key,
        bd.player_id,
        bd.player_name,
        bd.game_id,
        bd.team_id,
        bd.opponent_id,
        bd.home_away,
        bd.season_year,
        bd.game_date,
        bd.position,
        bd.target_pts,
        
        -- =================================================================
        -- CORE SCORING PERFORMANCE (3, 5, 10-GAME ROLLING AVERAGES)
        -- =================================================================
        
        -- Points Production
        round(coalesce(ptr.pts_avg_l3, 0), 3) as pts_avg_l3,
        round(coalesce(ptr.pts_avg_l5, 0), 3) as pts_avg_l5,
        round(coalesce(ptr.pts_avg_l10, 0), 3) as pts_avg_l10,
        
        -- Shooting Efficiency  
        round(coalesce(ptr.fg_pct_avg_l3, 0), 3) as fg_pct_avg_l3,
        round(coalesce(ptr.fg_pct_avg_l5, 0), 3) as fg_pct_avg_l5,
        round(coalesce(ptr.fg_pct_avg_l10, 0), 3) as fg_pct_avg_l10,
        round(coalesce(ptr.fg3_pct_avg_l3, 0), 3) as fg3_pct_avg_l3,
        round(coalesce(ptr.fg3_pct_avg_l5, 0), 3) as fg3_pct_avg_l5,
        round(coalesce(ptr.fg3_pct_avg_l10, 0), 3) as fg3_pct_avg_l10,
        round(coalesce(ptr.ft_pct_avg_l5, 0), 3) as ft_pct_avg_l5,
        
        -- Shot Volume
        round(coalesce(ptr.fga_avg_l3, 0), 3) as fga_avg_l3,
        round(coalesce(ptr.fga_avg_l5, 0), 3) as fga_avg_l5,
        round(coalesce(ptr.fga_avg_l10, 0), 3) as fga_avg_l10,
        round(coalesce(ptr.fg3a_avg_l5, 0), 3) as fg3a_avg_l5,
        round(coalesce(ptr.fta_avg_l5, 0), 3) as fta_avg_l5,
        
        -- Playing Time Context
        round(coalesce(ptr.min_avg_l3, 0), 3) as min_avg_l3,
        round(coalesce(ptr.min_avg_l5, 0), 3) as min_avg_l5,
        round(coalesce(ptr.min_avg_l10, 0), 3) as min_avg_l10,
        
        -- =================================================================
        -- DERIVED SCORING METRICS (FROM OUR V2 MODEL)
        -- =================================================================
        
        -- Scoring Efficiency Composites
        round(coalesce(dsm.scoring_efficiency_composite_l3, 0), 3) as scoring_efficiency_composite_l3,
        round(coalesce(dsm.scoring_efficiency_composite_l5, 0), 3) as scoring_efficiency_composite_l5,
        round(coalesce(dsm.scoring_efficiency_composite_l10, 0), 3) as scoring_efficiency_composite_l10,
        
        -- Usage-Weighted Metrics
        round(coalesce(dsm.usage_weighted_ts_l3, 0), 3) as usage_weighted_ts_l3,
        round(coalesce(dsm.usage_weighted_ts_l5, 0), 3) as usage_weighted_ts_l5,
        round(coalesce(dsm.usage_weighted_ts_l10, 0), 3) as usage_weighted_ts_l10,
        
        -- Points per Touch Efficiency
        round(coalesce(dsm.points_per_touch_l3, 0), 3) as points_per_touch_l3,
        round(coalesce(dsm.points_per_touch_l5, 0), 3) as points_per_touch_l5,
        round(coalesce(dsm.points_per_touch_l10, 0), 3) as points_per_touch_l10,
        
        -- Shot Creation Analysis
        round(coalesce(dsm.shot_creation_index_l3, 0), 3) as shot_creation_index_l3,
        round(coalesce(dsm.shot_creation_index_l5, 0), 3) as shot_creation_index_l5,
        round(coalesce(dsm.shot_creation_index_l10, 0), 3) as shot_creation_index_l10,
        
        -- Defensive Attention Impact
        round(coalesce(dsm.defensive_attention_factor_l3, 0), 3) as defensive_attention_factor_l3,
        round(coalesce(dsm.defensive_attention_factor_l5, 0), 3) as defensive_attention_factor_l5,
        round(coalesce(dsm.defensive_attention_factor_l10, 0), 3) as defensive_attention_factor_l10,
        
        -- Scoring Versatility
        round(coalesce(dsm.scoring_versatility_l3, 0), 3) as scoring_versatility_l3,
        round(coalesce(dsm.scoring_versatility_l5, 0), 3) as scoring_versatility_l5,
        round(coalesce(dsm.scoring_versatility_l10, 0), 3) as scoring_versatility_l10,
        
        -- Three-Point Value Analysis
        round(coalesce(dsm.three_point_value_index_l3, 0), 3) as three_point_value_index_l3,
        round(coalesce(dsm.three_point_value_index_l5, 0), 3) as three_point_value_index_l5,
        round(coalesce(dsm.three_point_value_index_l10, 0), 3) as three_point_value_index_l10,
        
        -- Paint Dominance
        round(coalesce(dsm.paint_dominance_index_l3, 0), 3) as paint_dominance_index_l3,
        round(coalesce(dsm.paint_dominance_index_l5, 0), 3) as paint_dominance_index_l5,
        round(coalesce(dsm.paint_dominance_index_l10, 0), 3) as paint_dominance_index_l10,
        
        -- Assisted Shot Efficiency
        round(coalesce(dsm.assisted_shot_efficiency_l3, 0), 3) as assisted_shot_efficiency_l3,
        round(coalesce(dsm.assisted_shot_efficiency_l5, 0), 3) as assisted_shot_efficiency_l5,
        round(coalesce(dsm.assisted_shot_efficiency_l10, 0), 3) as assisted_shot_efficiency_l10,
        
        -- Pace-Adjusted Scoring
        round(coalesce(dsm.pace_adjusted_scoring_rate_l3, 0), 3) as pace_adjusted_scoring_rate_l3,
        round(coalesce(dsm.pace_adjusted_scoring_rate_l5, 0), 3) as pace_adjusted_scoring_rate_l5,
        round(coalesce(dsm.pace_adjusted_scoring_rate_l10, 0), 3) as pace_adjusted_scoring_rate_l10,
        
        -- Second Chance Scoring
        round(coalesce(dsm.second_chance_conversion_rate_l3, 0), 3) as second_chance_conversion_rate_l3,
        round(coalesce(dsm.second_chance_conversion_rate_l5, 0), 3) as second_chance_conversion_rate_l5,
        round(coalesce(dsm.second_chance_conversion_rate_l10, 0), 3) as second_chance_conversion_rate_l10,
        
        -- Shooting Under Pressure
        round(coalesce(dsm.shooting_pressure_differential_l3, 0), 3) as shooting_pressure_differential_l3,
        round(coalesce(dsm.shooting_pressure_differential_l5, 0), 3) as shooting_pressure_differential_l5,
        round(coalesce(dsm.shooting_pressure_differential_l10, 0), 3) as shooting_pressure_differential_l10,
        
        -- True Shooting Attempts Rate
        round(coalesce(dsm.tsa_per_minute_l3, 0), 3) as tsa_per_minute_l3,
        round(coalesce(dsm.tsa_per_minute_l5, 0), 3) as tsa_per_minute_l5,
        round(coalesce(dsm.tsa_per_minute_l10, 0), 3) as tsa_per_minute_l10,
        
        -- Free Throw Generation
        round(coalesce(dsm.ft_generation_rate_l3, 0), 3) as ft_generation_rate_l3,
        round(coalesce(dsm.ft_generation_rate_l5, 0), 3) as ft_generation_rate_l5,
        round(coalesce(dsm.ft_generation_rate_l10, 0), 3) as ft_generation_rate_l10,
        
        -- Self-Created Scoring
        round(coalesce(dsm.self_created_scoring_rate_l3, 0), 3) as self_created_scoring_rate_l3,
        round(coalesce(dsm.self_created_scoring_rate_l5, 0), 3) as self_created_scoring_rate_l5,
        round(coalesce(dsm.self_created_scoring_rate_l10, 0), 3) as self_created_scoring_rate_l10,
        
        -- Opportunistic Scoring
        round(coalesce(dsm.opportunistic_scoring_rate_l3, 0), 3) as opportunistic_scoring_rate_l3,
        round(coalesce(dsm.opportunistic_scoring_rate_l5, 0), 3) as opportunistic_scoring_rate_l5,
        round(coalesce(dsm.opportunistic_scoring_rate_l10, 0), 3) as opportunistic_scoring_rate_l10,
        
        -- Scoring vs Playmaking Balance
        round(coalesce(dsm.scoring_playmaking_ratio_l3, 0), 3) as scoring_playmaking_ratio_l3,
        round(coalesce(dsm.scoring_playmaking_ratio_l5, 0), 3) as scoring_playmaking_ratio_l5,
        round(coalesce(dsm.scoring_playmaking_ratio_l10, 0), 3) as scoring_playmaking_ratio_l10,
        
        -- Performance Trends
        round(coalesce(dsm.scoring_efficiency_trend_3v10, 0), 3) as scoring_efficiency_trend_3v10,
        round(coalesce(dsm.points_per_touch_trend_3v10, 0), 3) as points_per_touch_trend_3v10,
        round(coalesce(dsm.usage_weighted_ts_trend_3v10, 0), 3) as usage_weighted_ts_trend_3v10,
        round(coalesce(dsm.shot_creation_trend_3v10, 0), 3) as shot_creation_trend_3v10,
        
        -- =================================================================
        -- TEAM CONTEXT & PROJECTIONS
        -- =================================================================
        
        -- Player Team Role Analysis
        tmcp.player_offensive_role,
        tmcp.player_team_structure_fit,
        round(coalesce(tmcp.playstyle_compatibility_score, 0), 3) as playstyle_compatibility_score,
        
        -- Team Context Metrics
        round(coalesce(tmcp.team_strength_score, 50), 3) as team_strength_score,
        tmcp.team_current_form,
        round(coalesce(tmcp.team_pace_avg_l5, 0), 3) as team_pace_avg_l5,
        tmcp.team_playstyle_primary,
        tmcp.team_offensive_structure,
        
        -- Context Multipliers
        round(coalesce(tmcp.pace_multiplier, 1.0), 3) as pace_multiplier,
        round(coalesce(tmcp.team_form_multiplier, 1.0), 3) as team_form_multiplier,
        round(coalesce(tmcp.usage_opportunity_multiplier, 1.0), 3) as usage_opportunity_multiplier,
        round(coalesce(tmcp.opponent_defense_multiplier, 1.0), 3) as opponent_defense_multiplier,
        round(coalesce(tmcp.playstyle_synergy_multiplier, 1.0), 3) as playstyle_synergy_multiplier,
        round(coalesce(tmcp.total_context_multiplier, 1.0), 3) as total_context_multiplier,
        
        -- Context-Adjusted Projections
        round(coalesce(tmcp.context_adjusted_pts_projection_l3, 0), 3) as context_adjusted_pts_projection_l3,
        round(coalesce(tmcp.context_adjusted_pts_projection_l5, 0), 3) as context_adjusted_pts_projection_l5,
        round(coalesce(tmcp.context_adjusted_reb_projection, 0), 3) as context_adjusted_reb_projection,
        round(coalesce(tmcp.context_adjusted_ast_projection, 0), 3) as context_adjusted_ast_projection,
        
        -- Projection Quality Metrics
        round(coalesce(tmcp.projection_confidence_score, 50), 3) as projection_confidence_score,
        tmcp.overall_context_impact,
        
        -- =================================================================
        -- INTERIOR SCORING SPECIALIZATION
        -- =================================================================
        
        -- Current Interior Profile
        round(coalesce(isa.current_paint_scoring_pct, 0), 3) as current_paint_scoring_pct,
        round(coalesce(isa.current_midrange_scoring_pct, 0), 3) as current_midrange_scoring_pct,
        round(coalesce(isa.current_rim_fg_pct, 0), 3) as current_rim_fg_pct,
        round(coalesce(isa.current_rim_attempt_rate, 0), 3) as current_rim_attempt_rate,
        
        -- Interior Scoring Deviations
        round(coalesce(isa.paint_scoring_reliance_deviation, 0), 3) as paint_scoring_reliance_deviation,
        round(coalesce(isa.rim_efficiency_deviation, 0), 3) as rim_efficiency_deviation,
        round(coalesce(isa.rim_attempt_rate_deviation, 0), 3) as rim_attempt_rate_deviation,
        round(coalesce(isa.second_chance_efficiency_deviation, 0), 3) as second_chance_efficiency_deviation,
        
        -- Interior Scoring Trends
        round(coalesce(isa.paint_scoring_short_term_trend, 0), 3) as paint_scoring_short_term_trend,
        round(coalesce(isa.rim_efficiency_short_term_trend, 0), 3) as rim_efficiency_short_term_trend,
        
        -- Interior Scoring Z-Scores
        round(coalesce(isa.paint_scoring_z_score, 0), 3) as paint_scoring_z_score,
        round(coalesce(isa.rim_efficiency_z_score, 0), 3) as rim_efficiency_z_score,
        
        -- Interior Scoring Composite Indices
        round(coalesce(isa.interior_dominance_index, 0), 3) as interior_dominance_index,
        round(coalesce(isa.interior_shot_balance_score, 0), 3) as interior_shot_balance_score,
        round(coalesce(isa.interior_self_creation_index, 0), 3) as interior_self_creation_index,
        
        -- Interior Scoring Categories
        isa.paint_scoring_deviation_category,
        isa.rim_efficiency_deviation_category,
        
        -- =================================================================
        -- MILESTONE GAMES ANALYSIS
        -- =================================================================
        
        -- Career Milestone Counts (PRIOR to current game)
        mg.career_30_plus_pt_games,
        mg.career_40_plus_pt_games,
        mg.career_50_plus_pt_games,
        mg.career_60_plus_pt_games,
        mg.career_70_plus_pt_games,
        mg.career_80_plus_pt_games,
        
        -- Season Milestone Context
        mg.thirty_plus_pt_games_this_season,
        mg.forty_plus_pt_games_this_season,
        mg.fifty_plus_pt_games_this_season,
        mg.thirty_plus_pt_games_last_season,
        mg.forty_plus_pt_games_last_season,
        mg.fifty_plus_pt_games_last_season,
        
        -- Latest Milestone Dates
        mg.latest_30_plus_pt_game_date,
        mg.latest_40_plus_pt_game_date,
        mg.latest_50_plus_pt_game_date,
        
        -- =================================================================
        -- USAGE & TEAM ROLE CONTEXT
        -- =================================================================
        
        -- Usage Percentages
        round(coalesce(pur.pct_of_team_pts_avg_l3, 0), 3) as team_pts_share_l3,
        round(coalesce(pur.pct_of_team_pts_avg_l5, 0), 3) as team_pts_share_l5,
        round(coalesce(pur.pct_of_team_pts_avg_l10, 0), 3) as team_pts_share_l10,
        round(coalesce(pur.pct_of_team_fga_avg_l5, 0), 3) as team_fga_share_l5,
        round(coalesce(pur.pct_of_team_ast_avg_l5, 0), 3) as team_ast_share_l5,
        
        -- =================================================================
        -- OPPONENT MATCHUP CONTEXT
        -- =================================================================
        
        -- Opponent Defensive Quality
        round(coalesce(oc.opp_def_rating_avg_l5, 0), 3) as opp_def_rating_avg_l5,
        round(coalesce(oc.opp_defensive_pressure_l5, 0), 3) as opp_defensive_pressure_l5,
        round(coalesce(oc.opp_pace_avg_l5, 0), 3) as opp_pace_avg_l5,
        
        -- Opponent Trends
        round(coalesce(oc.opp_def_rating_trend_3v10, 0), 3) as opp_defensive_momentum,
        
        -- =================================================================
        -- COMPOSITE SCORING INDICES & CLASSIFICATIONS  
        -- =================================================================
        
        -- Role Consistency Assessment
        case
            when tmcp.player_offensive_role = 'PRIMARY_OPTION' and coalesce(pur.pct_of_team_pts_avg_l5, 0) > 25 then 'CONSISTENT_PRIMARY'
            when tmcp.player_offensive_role = 'SECONDARY_OPTION' and coalesce(pur.pct_of_team_pts_avg_l5, 0) between 18 and 30 then 'CONSISTENT_SECONDARY'
            when tmcp.player_offensive_role = 'SUPPORTING_ROLE' and coalesce(pur.pct_of_team_pts_avg_l5, 0) < 20 then 'CONSISTENT_SUPPORT'
            else 'ROLE_TRANSITIONING'
        end as role_consistency_indicator,
        
        -- Pressure Performance Classification
        case
            when coalesce(dsm.shooting_pressure_differential_l10, 0) > -0.02 then 'PRESSURE_ELITE'
            when coalesce(dsm.shooting_pressure_differential_l10, 0) > -0.05 then 'PRESSURE_RESILIENT'
            when coalesce(dsm.shooting_pressure_differential_l10, 0) > -0.10 then 'PRESSURE_AVERAGE'
            when coalesce(dsm.shooting_pressure_differential_l10, 0) > -0.15 then 'PRESSURE_SENSITIVE'
            else 'PRESSURE_VULNERABLE'
        end as pressure_performance_tier,
        
        -- Scoring Archetype Classification
        case
            when coalesce(isa.current_paint_scoring_pct, 0) > 60 and coalesce(isa.interior_dominance_index, 0) > 0.6 then 'INTERIOR_DOMINANT'
            when coalesce(dsm.three_point_value_index_l10, 0) > 0.15 and coalesce(isa.current_paint_scoring_pct, 0) < 40 then 'PERIMETER_SPECIALIST'
            when abs(coalesce(isa.interior_shot_balance_score, 0.5) - 0.5) < 0.15 then 'BALANCED_SCORER'
            when coalesce(dsm.self_created_scoring_rate_l10, 0) > 0.8 then 'SHOT_CREATOR'
            when coalesce(dsm.assisted_shot_efficiency_l10, 0) > 0.5 then 'SYSTEM_SCORER'
            else 'SITUATIONAL_SCORER'
        end as scoring_archetype,
        
        -- Context Sensitivity Classification
        case
            when coalesce(tmcp.total_context_multiplier, 1.0) > 1.15 then 'HIGH_CONTEXT_BOOST'
            when coalesce(tmcp.total_context_multiplier, 1.0) > 1.05 then 'MODERATE_CONTEXT_BOOST'
            when coalesce(tmcp.total_context_multiplier, 1.0) < 0.90 then 'CONTEXT_CHALLENGED'
            when coalesce(tmcp.total_context_multiplier, 1.0) < 0.85 then 'CONTEXT_LIMITED'
            else 'CONTEXT_NEUTRAL'
        end as context_sensitivity_tier,
        
        -- Composite Efficiency Reliability Score
        round(coalesce(
            (dsm.scoring_efficiency_composite_l10 * 0.4 + 
             dsm.usage_weighted_ts_l10 * 0.3 + 
             dsm.points_per_touch_l10 * 0.3), 0
        ), 3) as composite_efficiency_reliability,
        
        -- Team Offensive Impact Magnitude
        round(coalesce(
            tmcp.player_team_pts_share_l5 * 
            tmcp.total_context_multiplier * 
            tmcp.playstyle_compatibility_score / 5.0, 0
        ), 3) as team_offensive_impact_magnitude,
        
        -- Matchup Adaptability Index
        round(coalesce(
            (abs(coalesce(isa.paint_scoring_reliance_deviation, 0)) + 
             abs(coalesce(isa.rim_efficiency_deviation, 0)) + 
             abs(coalesce(dsm.scoring_versatility_l10, 0))) * 
             coalesce(dsm.scoring_versatility_l10, 0), 0
        ), 3) as matchup_adaptability_index,
        
        -- Milestone Proximity Indicator
        case
            when coalesce(ptr.pts_avg_l5, 0) >= 28 and mg.career_30_plus_pt_games = 0 then 'APPROACHING_FIRST_30'
            when coalesce(ptr.pts_avg_l5, 0) >= 38 and mg.career_40_plus_pt_games = 0 then 'APPROACHING_FIRST_40'
            when coalesce(ptr.pts_avg_l5, 0) >= 48 and mg.career_50_plus_pt_games = 0 then 'APPROACHING_FIRST_50'
            when coalesce(ptr.pts_avg_l5, 0) >= 25 and mg.career_30_plus_pt_games > 0 then 'MILESTONE_CAPABLE'
            else 'MILESTONE_UNLIKELY'
        end as milestone_proximity_indicator,
        
        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        greatest(
            coalesce(bd.updated_at, '1900-01-01'::timestamp),
            coalesce(dsm.updated_at, '1900-01-01'::timestamp),
            coalesce(tmcp.updated_at, '1900-01-01'::timestamp),
            coalesce(isa.updated_at, '1900-01-01'::timestamp),
            coalesce(ptr.updated_at, '1900-01-01'::timestamp),
            coalesce(pur.updated_at, '1900-01-01'::timestamp),
            coalesce(oc.updated_at, '1900-01-01'::timestamp)
        ) as updated_at
        
    from base_data bd
    left join derived_scoring_metrics dsm on bd.player_game_key = dsm.player_game_key
    left join team_context_projections tmcp on bd.player_game_key = tmcp.player_game_key
    left join interior_scoring_analysis isa on bd.player_game_key = isa.player_game_key
    left join milestone_games mg on bd.player_game_key = mg.player_game_key
    left join player_traditional_rolling ptr on bd.player_game_key = ptr.player_game_key
    left join player_usage_rolling pur on bd.player_game_key = pur.player_game_key
    left join opponent_context oc on bd.game_id = oc.game_id and bd.team_id = oc.team_id

)

select * from comprehensive_scoring_features