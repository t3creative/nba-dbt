{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'team', 'context', 'projections', 'ml_safe'],
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
ML-SAFE Player-Team Context Projections - COMPREHENSIVE MODEL
Combines player performance history with team context for projections.
ZERO DATA LEAKAGE - Uses only historical data for predictions.

Replaces the previous pipeline:
- feat_player__game_context_attributes (leaky)
- feat_player__contextual_projections (leaky) 

With a single, comprehensive, ML-safe model.
*/

with base_data as (
    select
        player_game_key,
        player_id,
        player_name,
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        home_away,
        position,
        updated_at
    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

-- Get comprehensive player rolling metrics (ML-safe)
player_traditional_rolling as (
    select * from {{ ref('feat_player__traditional_rolling_v2') }}
),

player_advanced_rolling as (
    select * from {{ ref('feat_player__advanced_rolling_v2') }}
),

player_usage_rolling as (
    select * from {{ ref('feat_player__usage_rolling_v2') }}
),

player_scoring_rolling as (
    select * from {{ ref('feat_player__scoring_rolling_v2') }}
),

-- Get comprehensive team context (ML-safe)
team_performance_context as (
    select * from {{ ref('feat_team__performance_metrics_v2') }}
),

team_shot_distribution_context as (
    select * from {{ ref('feat_team__shot_distribution_v2') }}
),

team_usage_distribution_context as (
    select * from {{ ref('feat_team__usage_distribution_v2') }}
),

-- Get opponent context
opponent_context as (
    select * from {{ ref('feat_opp__opponent_stats_v2') }}
),

-- Combine all contexts into comprehensive player-team profile
comprehensive_player_context as (
    select
        bd.player_game_key,
        bd.player_id,
        bd.player_name,
        bd.game_id,
        bd.team_id,
        bd.opponent_id,
        bd.season_year,
        bd.game_date,
        bd.home_away,
        bd.position,
        
        -- =================================================================
        -- PLAYER RECENT PERFORMANCE (ML-SAFE ROLLING AVERAGES)
        -- =================================================================
        
        -- Traditional Performance (3, 5, 10-game averages)
        coalesce(ptr.pts_avg_l3, 0) as player_pts_avg_l3,
        coalesce(ptr.pts_avg_l5, 0) as player_pts_avg_l5,
        coalesce(ptr.pts_avg_l10, 0) as player_pts_avg_l10,
        coalesce(ptr.reb_avg_l3, 0) as player_reb_avg_l3,
        coalesce(ptr.reb_avg_l5, 0) as player_reb_avg_l5,
        coalesce(ptr.reb_avg_l10, 0) as player_reb_avg_l10,
        coalesce(ptr.ast_avg_l3, 0) as player_ast_avg_l3,
        coalesce(ptr.ast_avg_l5, 0) as player_ast_avg_l5,
        coalesce(ptr.ast_avg_l10, 0) as player_ast_avg_l10,
        coalesce(ptr.min_avg_l5, 0) as player_min_avg_l5,
        coalesce(ptr.stl_avg_l5, 0) as player_stl_avg_l5,
        coalesce(ptr.blk_avg_l5, 0) as player_blk_avg_l5,
        
        -- Advanced Performance
        coalesce(par.usage_pct_avg_l3, 0) as player_usage_pct_avg_l3,
        coalesce(par.usage_pct_avg_l5, 0) as player_usage_pct_avg_l5,
        coalesce(par.usage_pct_avg_l10, 0) as player_usage_pct_avg_l10,
        coalesce(par.ts_pct_avg_l5, 0) as player_ts_pct_avg_l5,
        coalesce(par.eff_fg_pct_avg_l5, 0) as player_eff_fg_pct_avg_l5,
        coalesce(par.off_rating_avg_l5, 0) as player_off_rating_avg_l5,
        coalesce(par.pie_avg_l5, 0) as player_pie_avg_l5,
        
        -- Team Share Metrics (Player's role within team)
        coalesce(pur.pct_of_team_pts_avg_l3, 0) as player_team_pts_share_l3,
        coalesce(pur.pct_of_team_pts_avg_l5, 0) as player_team_pts_share_l5,
        coalesce(pur.pct_of_team_pts_avg_l10, 0) as player_team_pts_share_l10,
        coalesce(pur.pct_of_team_ast_avg_l5, 0) as player_team_ast_share_l5,
        coalesce(pur.pct_of_team_reb_avg_l5, 0) as player_team_reb_share_l5,
        coalesce(pur.pct_of_team_fga_avg_l5, 0) as player_team_fga_share_l5,
        
        -- Scoring Profile
        coalesce(psr.pct_pts_3pt_avg_l5, 0) as player_pct_pts_3pt_l5,
        coalesce(psr.pct_pts_in_paint_avg_l5, 0) as player_pct_pts_paint_l5,
        coalesce(psr.pct_unassisted_fgm_avg_l5, 0) as player_self_creation_rate_l5,
        
        -- Performance Consistency
        coalesce(ptr.pts_stddev_l5, 0) as player_pts_volatility_l5,
        coalesce(par.usage_pct_stddev_l5, 0) as player_usage_volatility_l5,
        coalesce(ptr.pts_trend_3v10, 0) as player_scoring_momentum,
        
        -- =================================================================
        -- TEAM PERFORMANCE CONTEXT (ML-SAFE)
        -- =================================================================
        
        -- Team Quality & Form
        coalesce(tpc.recent_form_score, 50) as team_recent_form_score,
        coalesce(tpc.overall_team_strength_score, 50) as team_strength_score,
        tpc.team_strength_tier,
        tpc.team_current_form,
        coalesce(tpc.prior_win_percentage, 0.5) as team_win_pct,
        
        -- Team Performance Metrics
        coalesce(tpc.recent_scoring_avg, 0) as team_scoring_avg_l5,
        coalesce(tpc.recent_off_rating, 0) as team_off_rating_avg_l5,
        coalesce(tpc.recent_def_rating, 0) as team_def_rating_avg_l5,
        coalesce(tpc.recent_pace, 0) as team_pace_avg_l5,
        coalesce(tpc.recent_ts_pct, 0) as team_ts_pct_avg_l5,
        
        -- Team Momentum (using available trend indicators)
        0 as team_offensive_momentum,
        0 as team_defensive_momentum, 
        0 as team_scoring_momentum,
        
        -- =================================================================
        -- TEAM PLAYSTYLE CONTEXT (ML-SAFE)
        -- =================================================================
        
        -- Shot Distribution Profile
        coalesce(tsdc.recent_3pt_attempt_pct, 0) as team_3pt_attempt_rate_l5,
        coalesce(tsdc.recent_paint_scoring_pct, 0) as team_paint_scoring_rate_l5,
        coalesce(tsdc.recent_fastbreak_pct, 0) as team_fastbreak_rate_l5,
        coalesce(tsdc.recent_assisted_fgm_pct, 0) as team_assisted_rate_l5,
        tsdc.team_playstyle_primary,
        tsdc.shot_creation_style,
        tsdc.interior_perimeter_balance,
        
        -- Advanced Playstyle Metrics
        coalesce(tsdc.modern_offense_index, 0) as team_modern_offense_index,
        coalesce(tsdc.pace_space_index, 0) as team_pace_space_index,
        coalesce(tsdc.shot_distribution_stability_score, 50) as team_shot_stability,
        
        -- Usage Distribution
        coalesce(tudc.recent_max_usage_pct, 0) as team_max_usage_pct_l5,
        coalesce(tudc.recent_usage_equality, 0.5) as team_usage_equality_l5,
        tudc.team_offensive_structure,
        tudc.offensive_style_modifier,
        coalesce(tudc.star_dependence_index, 50) as team_star_dependence,
        coalesce(tudc.offensive_balance_index, 50) as team_offensive_balance,
        
        -- =================================================================
        -- OPPONENT CONTEXT (ML-SAFE)
        -- =================================================================
        
        -- Opponent Defensive Quality
        coalesce(oc.opp_def_rating_avg_l5, 0) as opp_def_rating_avg_l5,
        coalesce(oc.opp_defensive_pressure_l5, 0) as opp_defensive_pressure,
        coalesce(oc.opp_def_rating_trend_3v10, 0) as opp_defensive_momentum,
        
        -- Opponent Pace Impact
        coalesce(oc.opp_pace_avg_l5, 0) as opp_pace_avg_l5,
        coalesce(oc.opp_pace_factor_l5, 1.0) as opp_pace_factor,
        
        -- Opponent Specific Threats
        coalesce(oc.opp_three_point_threat_l5, 0) as opp_perimeter_threat,
        coalesce(oc.opp_interior_threat_l5, 0) as opp_interior_threat,
        
        greatest(
            coalesce(bd.updated_at, '1900-01-01'::timestamp),
            coalesce(ptr.updated_at, '1900-01-01'::timestamp),
            coalesce(par.updated_at, '1900-01-01'::timestamp),
            coalesce(pur.updated_at, '1900-01-01'::timestamp),
            coalesce(psr.updated_at, '1900-01-01'::timestamp),
            coalesce(tpc.updated_at, '1900-01-01'::timestamp),
            coalesce(tsdc.updated_at, '1900-01-01'::timestamp),
            coalesce(tudc.updated_at, '1900-01-01'::timestamp),
            coalesce(oc.updated_at, '1900-01-01'::timestamp)
        ) as updated_at
        
    from base_data bd
    left join player_traditional_rolling ptr on bd.player_game_key = ptr.player_game_key
    left join player_advanced_rolling par on bd.player_game_key = par.player_game_key
    left join player_usage_rolling pur on bd.player_game_key = pur.player_game_key
    left join player_scoring_rolling psr on bd.player_game_key = psr.player_game_key
    left join team_performance_context tpc on bd.game_id = tpc.game_id and bd.team_id = tpc.team_id
    left join team_shot_distribution_context tsdc on bd.game_id = tsdc.game_id and bd.team_id = tsdc.team_id
    left join team_usage_distribution_context tudc on bd.game_id = tudc.game_id and bd.team_id = tudc.team_id
    left join opponent_context oc on bd.game_id = oc.game_id and bd.team_id = oc.team_id
),

contextual_projections as (
    select
        *,
        
        -- =================================================================
        -- PLAYER-TEAM FIT ANALYSIS
        -- =================================================================
        
        -- Offensive Role Classification
        case 
            when player_team_pts_share_l5 > 25 then 'PRIMARY_OPTION'
            when player_team_pts_share_l5 > 18 then 'SECONDARY_OPTION'  
            when player_team_pts_share_l5 > 12 then 'TERTIARY_OPTION'
            else 'SUPPORTING_ROLE'
        end as player_offensive_role,
        
        -- Usage vs Team Structure Fit
        case
            when team_offensive_structure = 'STAR_DOMINANT' and player_team_pts_share_l5 > 25 then 'PERFECT_FIT'
            when team_offensive_structure = 'BALANCED' and player_team_pts_share_l5 between 15 and 25 then 'GOOD_FIT'
            when team_offensive_structure = 'TOP_HEAVY' and player_team_pts_share_l5 > 20 then 'GOOD_FIT'
            when abs(player_usage_pct_avg_l5 - team_max_usage_pct_l5) < 5 then 'USAGE_ALIGNED'
            else 'MODERATE_FIT'
        end as player_team_structure_fit,
        
        -- Playstyle Compatibility Score (1-5 scale)
        case
            when position = 'G' and team_playstyle_primary = 'FAST_PACED' then 5
            when position = 'G' and team_playstyle_primary = 'BALL_MOVEMENT' then 5
            when position = 'F' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 5
            when position = 'C' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 5
            when position = 'G' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 4
            when position = 'F' and interior_perimeter_balance = 'BALANCED_ATTACK' then 4
            when team_playstyle_primary = 'BALANCED' then 3
            when position = 'C' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 2
            when position = 'G' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 2
            else 3
        end as playstyle_compatibility_score,
        
        -- =================================================================
        -- CONTEXT-BASED MULTIPLIERS (SCIENTIFIC APPROACH)
        -- =================================================================
        
        -- Pace Impact Multiplier
        case
            when team_pace_avg_l5 > 102 and opp_pace_avg_l5 > 102 then 1.12  -- High pace games
            when team_pace_avg_l5 > 100 and opp_pace_avg_l5 > 100 then 1.06  -- Above average pace
            when team_pace_avg_l5 < 96 and opp_pace_avg_l5 < 96 then 0.92    -- Slow pace games
            when team_pace_avg_l5 < 98 and opp_pace_avg_l5 < 98 then 0.96    -- Below average pace
            else 1.0  -- Neutral pace impact
        end as pace_multiplier,
        
        -- Team Form Impact Multiplier (using pts share instead of role)
        case
            when team_current_form = 'VERY_HOT' and player_team_pts_share_l5 <= 12 then 1.08
            when team_current_form = 'HOT' and player_team_pts_share_l5 <= 12 then 1.05
            when team_current_form = 'VERY_COLD' and player_team_pts_share_l5 > 25 then 1.12
            when team_current_form = 'COLD' and player_team_pts_share_l5 > 25 then 1.06
            when team_current_form in ('VERY_HOT', 'HOT') then 1.03
            when team_current_form in ('VERY_COLD', 'COLD') then 0.97
            else 1.0
        end as team_form_multiplier,
        
        -- Usage Opportunity Multiplier (using pts share instead of role)
        case
            when team_offensive_structure = 'STAR_DOMINANT' and player_team_pts_share_l5 > 25 then 1.15
            when team_offensive_structure = 'BALANCED' and player_team_pts_share_l5 between 12 and 25 then 1.05
            when team_star_dependence > 80 and player_team_pts_share_l5 > 25 then 1.12
            when team_offensive_balance > 75 then 1.02
            else 1.0
        end as usage_opportunity_multiplier,
        
        -- Opponent Defense Multiplier
        case
            when opp_def_rating_avg_l5 < 105 then 0.94  -- Elite defense
            when opp_def_rating_avg_l5 < 110 then 0.97  -- Good defense
            when opp_def_rating_avg_l5 > 115 then 1.06  -- Poor defense
            when opp_def_rating_avg_l5 > 112 then 1.03  -- Below average defense
            else 1.0  -- Average defense
        end as opponent_defense_multiplier,
        
        -- Playstyle Synergy Multiplier (using inline calculation)
        case
            when (case
                when position = 'G' and team_playstyle_primary = 'FAST_PACED' then 5
                when position = 'G' and team_playstyle_primary = 'BALL_MOVEMENT' then 5
                when position = 'F' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 5
                when position = 'C' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 5
                when position = 'G' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 4
                when position = 'F' and interior_perimeter_balance = 'BALANCED_ATTACK' then 4
                when team_playstyle_primary = 'BALANCED' then 3
                when position = 'C' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 2
                when position = 'G' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 2
                else 3
            end) >= 5 then 1.08
            when (case
                when position = 'G' and team_playstyle_primary = 'FAST_PACED' then 5
                when position = 'G' and team_playstyle_primary = 'BALL_MOVEMENT' then 5
                when position = 'F' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 5
                when position = 'C' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 5
                when position = 'G' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 4
                when position = 'F' and interior_perimeter_balance = 'BALANCED_ATTACK' then 4
                when team_playstyle_primary = 'BALANCED' then 3
                when position = 'C' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 2
                when position = 'G' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 2
                else 3
            end) >= 4 then 1.04
            when (case
                when position = 'G' and team_playstyle_primary = 'FAST_PACED' then 5
                when position = 'G' and team_playstyle_primary = 'BALL_MOVEMENT' then 5
                when position = 'F' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 5
                when position = 'C' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 5
                when position = 'G' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 4
                when position = 'F' and interior_perimeter_balance = 'BALANCED_ATTACK' then 4
                when team_playstyle_primary = 'BALANCED' then 3
                when position = 'C' and team_playstyle_primary = 'THREE_POINT_HEAVY' then 2
                when position = 'G' and interior_perimeter_balance = 'INTERIOR_DOMINANT' then 2
                else 3
            end) <= 2 then 0.94
            else 1.0
        end as playstyle_synergy_multiplier,
        
        -- =================================================================
        -- POSITION-SPECIFIC CONTEXT MULTIPLIERS
        -- =================================================================
        
        -- Guard-Specific Multipliers
        case
            when position = 'G' and team_assisted_rate_l5 > 65 then 1.06  -- High assist rate boosts guard production
            when position = 'G' and team_pace_space_index > 0.7 then 1.05  -- Pace and space offense
            when position = 'G' and opp_defensive_pressure > 7 then 0.96   -- High pressure defense
            else 1.0
        end as guard_context_multiplier,
        
        -- Forward-Specific Multipliers  
        case
            when position = 'F' and team_modern_offense_index > 10 then 1.05  -- Modern offense fits forwards
            when position = 'F' and team_3pt_attempt_rate_l5 > 40 then 1.04   -- High 3PT rate
            else 1.0
        end as forward_context_multiplier,
        
        -- Center-Specific Multipliers
        case
            when position = 'C' and team_paint_scoring_rate_l5 > 45 then 1.08  -- Interior-focused offense
            when position = 'C' and opp_interior_threat < 30 then 1.05         -- Weak interior defense
            when position = 'C' and team_3pt_attempt_rate_l5 > 45 then 0.96    -- Perimeter-heavy offense
            else 1.0
        end as center_context_multiplier
        
    from comprehensive_player_context
),

final_projections as (
    select
        -- Base Identifiers
        player_game_key,
        player_id,
        player_name,
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        home_away,
        position,
        
        -- =================================================================
        -- PLAYER PERFORMANCE BASELINE (RECENT FORM)
        -- =================================================================
        player_pts_avg_l3,
        player_pts_avg_l5,
        player_pts_avg_l10,
        player_reb_avg_l3,
        player_reb_avg_l5,
        player_reb_avg_l10,
        player_ast_avg_l3,
        player_ast_avg_l5,
        player_ast_avg_l10,
        player_min_avg_l5,
        player_usage_pct_avg_l5,
        player_ts_pct_avg_l5,
        
        -- Performance Context
        player_scoring_momentum,
        player_pts_volatility_l5,
        player_team_pts_share_l5,
        player_offensive_role,
        
        -- =================================================================
        -- TEAM & OPPONENT CONTEXT
        -- =================================================================
        team_strength_score,
        team_current_form,
        team_offensive_momentum,
        team_pace_avg_l5,
        team_playstyle_primary,
        team_offensive_structure,
        opp_def_rating_avg_l5,
        opp_pace_avg_l5,
        
        -- =================================================================
        -- FIT & SYNERGY ANALYSIS
        -- =================================================================
        player_team_structure_fit,
        playstyle_compatibility_score,
        
        -- =================================================================
        -- CONTEXT MULTIPLIERS
        -- =================================================================
        pace_multiplier,
        team_form_multiplier,
        usage_opportunity_multiplier,
        opponent_defense_multiplier,
        playstyle_synergy_multiplier,
        guard_context_multiplier,
        forward_context_multiplier,
        center_context_multiplier,
        
        -- Combined Context Multiplier
        pace_multiplier * 
        team_form_multiplier * 
        usage_opportunity_multiplier * 
        opponent_defense_multiplier * 
        playstyle_synergy_multiplier *
        case 
            when position = 'G' then guard_context_multiplier
            when position = 'F' then forward_context_multiplier 
            when position = 'C' then center_context_multiplier
            else 1.0
        end as total_context_multiplier,
        
        -- =================================================================
        -- CONTEXT-ADJUSTED PROJECTIONS
        -- =================================================================
        
        -- Points Projection (Multiple Approaches)
        round(player_pts_avg_l3 * (pace_multiplier * team_form_multiplier * usage_opportunity_multiplier * 
              opponent_defense_multiplier * playstyle_synergy_multiplier *
              case when position = 'G' then guard_context_multiplier
                   when position = 'F' then forward_context_multiplier 
                   when position = 'C' then center_context_multiplier
                   else 1.0 end), 1) as context_adjusted_pts_projection_l3,
        
        round(player_pts_avg_l5 * (pace_multiplier * team_form_multiplier * usage_opportunity_multiplier * 
              opponent_defense_multiplier * playstyle_synergy_multiplier *
              case when position = 'G' then guard_context_multiplier
                   when position = 'F' then forward_context_multiplier 
                   when position = 'C' then center_context_multiplier
                   else 1.0 end), 1) as context_adjusted_pts_projection_l5,
        
        -- Rebounds Projection (Center-focused adjustments)
        round(player_reb_avg_l5 * 
              case when position = 'C' then center_context_multiplier * pace_multiplier
                   else pace_multiplier * 1.02 end, 1) as context_adjusted_reb_projection,
        
        -- Assists Projection (Guard-focused adjustments)
        round(player_ast_avg_l5 * 
              case when position = 'G' then guard_context_multiplier * team_form_multiplier
                   else team_form_multiplier end, 1) as context_adjusted_ast_projection,
        
        -- =================================================================
        -- PROJECTION CONFIDENCE & RANGE
        -- =================================================================
        
        -- Confidence Score (0-100, based on data quality and consistency)
        least(100, greatest(0,
            -- Base confidence from games played
            case when player_pts_avg_l5 > 0 then 70 else 30 end +
            -- Consistency bonus (lower volatility = higher confidence)
            case when player_pts_volatility_l5 < 5 then 15
                 when player_pts_volatility_l5 < 8 then 10
                 when player_pts_volatility_l5 < 12 then 5
                 else 0 end +
            -- Team stability bonus
            case when team_shot_stability > 80 then 10
                 when team_shot_stability > 60 then 5
                 else 0 end +
            -- Fit quality bonus
            case when playstyle_compatibility_score >= 4 then 5 else 0 end
        )) as projection_confidence_score,
        
-- Overall Context Impact Assessment (using inline calculation)
        case
            when (pace_multiplier * 
                  team_form_multiplier * 
                  usage_opportunity_multiplier * 
                  opponent_defense_multiplier * 
                  playstyle_synergy_multiplier *
                  case 
                      when position = 'G' then guard_context_multiplier
                      when position = 'F' then forward_context_multiplier 
                      when position = 'C' then center_context_multiplier
                      else 1.0
                  end) > 1.15 then 'VERY_POSITIVE'
            when (pace_multiplier * 
                  team_form_multiplier * 
                  usage_opportunity_multiplier * 
                  opponent_defense_multiplier * 
                  playstyle_synergy_multiplier *
                  case 
                      when position = 'G' then guard_context_multiplier
                      when position = 'F' then forward_context_multiplier 
                      when position = 'C' then center_context_multiplier
                      else 1.0
                  end) > 1.08 then 'POSITIVE'
            when (pace_multiplier * 
                  team_form_multiplier * 
                  usage_opportunity_multiplier * 
                  opponent_defense_multiplier * 
                  playstyle_synergy_multiplier *
                  case 
                      when position = 'G' then guard_context_multiplier
                      when position = 'F' then forward_context_multiplier 
                      when position = 'C' then center_context_multiplier
                      else 1.0
                  end) < 0.92 then 'NEGATIVE'
            when (pace_multiplier * 
                  team_form_multiplier * 
                  usage_opportunity_multiplier * 
                  opponent_defense_multiplier * 
                  playstyle_synergy_multiplier *
                  case 
                      when position = 'G' then guard_context_multiplier
                      when position = 'F' then forward_context_multiplier 
                      when position = 'C' then center_context_multiplier
                      else 1.0
                  end) < 0.85 then 'VERY_NEGATIVE'
            else 'NEUTRAL'
        end as overall_context_impact,
        
        updated_at
        
    from contextual_projections
)

select * from final_projections