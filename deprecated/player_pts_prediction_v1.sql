{{
    config(
        schema='training',
        materialized='table',
        unique_key= ['player_id', 'game_date'],
        tags=['betting', 'player_props', 'ml', 'training'],
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['player_id', 'market']
    )
}}

with player_props as (
    select
        pp.player_prop_key,
        pp.player_id,
        pp.player_name,
        pp.market_id,
        pp.market,
        'PTS' as market_cleaned,
        pp.line,
        pp.consensus_over_odds_decimal as over_odds,
        pp.consensus_under_odds_decimal as under_odds,
        pp.consensus_over_implied_prob as over_implied_prob,
        pp.consensus_under_implied_prob as under_implied_prob,
        pp.consensus_over_no_vig_prob as no_vig_over_prob,
        pp.consensus_under_no_vig_prob as no_vig_under_prob,
        pp.consensus_hold_percentage as vig_percentage,
        pp.game_date
    from {{ ref('feat_betting__player_props_probabilities') }} pp
    where pp.market = 'Points O/U'
),

player_teams as (
    select
        ptb.player_id,
        ptb.team_id,
        ptb.valid_from,
        ptb.valid_to
    from {{ ref('player_team_bridge') }} ptb
    where ptb.valid_from <= current_date
),

player_position as (
    select
        pos.player_id,
        pos.position
    from {{ ref('dim__players') }} pos
),

player_props_with_games as (
    select
        pp.*,
        coalesce(gc.game_id, go.game_id) as game_id,
        pt.team_id,
        ppos.position
    from player_props pp
    left join player_teams pt
        on pp.player_id = pt.player_id
        and pp.game_date >= pt.valid_from 
        and (pp.game_date < pt.valid_to or pt.valid_to is null)
    left join {{ ref('int_game__schedules') }} gc 
        on pp.game_date = gc.game_date 
        and (gc.home_team_id = pt.team_id or gc.away_team_id = pt.team_id)
    left join {{ ref('feat_opp__game_opponents_v2') }} go
        on pp.game_date = go.game_date
        and pt.team_id = go.team_id
    left join player_position ppos
        on pp.player_id = ppos.player_id
),

player_outcomes as (
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS' as stat_name,
        pbs.pts as stat_value,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
),

game_context as (
    select
        go.game_id,
        go.season_year,
        go.game_date,
        go.team_id,
        go.home_away,
        go.opponent_id,
        t.team_tricode as team_tricode,
        opp.team_tricode as opponent_tricode,
        case 
            when go.home_away = 'home' then true
            else false
        end as is_home,
        gc.home_rest_days,
        gc.away_rest_days,
        case
            when go.home_away = 'home' then gc.home_rest_days
            else gc.away_rest_days
        end as team_rest_days,
        case
            when go.home_away = 'home' then gc.away_rest_days
            else gc.home_rest_days
        end as opponent_rest_days,
        case
            when go.home_away = 'home' then gc.home_back_to_back
            else gc.away_back_to_back
        end as is_back_to_back,
        gc.home_win_pct_last_10,
        gc.away_win_pct_last_10,
        case
            when go.home_away = 'home' then gc.home_win_pct_last_10
            else gc.away_win_pct_last_10
        end as team_win_pct_last_10,
        case
            when go.home_away = 'home' then gc.away_win_pct_last_10
            else gc.home_win_pct_last_10
        end as opponent_win_pct_last_10,
        case
            when go.home_away = 'home' then 
                coalesce(gc.home_win_pct_last_10, 0.5) - coalesce(gc.away_win_pct_last_10, 0.5)
            else 
                coalesce(gc.away_win_pct_last_10, 0.5) - coalesce(gc.home_win_pct_last_10, 0.5)
        end as win_pct_diff_last_10,
        case
            when extract(month from go.game_date) >= 10 then extract(year from go.game_date)::text
            else (extract(year from go.game_date) - 1)::text
        end as nba_season
    from {{ ref('feat_opp__game_opponents_v2') }} go
    left join {{ ref('int_game__schedules') }} gc on go.game_id = gc.game_id
    left join {{ ref('stg__teams') }} t on go.team_id = t.team_id
    left join {{ ref('stg__teams') }} opp on go.opponent_id = opp.team_id
),

joined_data as (
    select
        pp.player_prop_key,
        pp.player_id,
        pp.player_name,
        pp.market_id,
        pp.market,
        pp.market_cleaned,
        pp.line,
        pp.over_odds,
        pp.under_odds,
        pp.over_implied_prob,
        pp.under_implied_prob,
        pp.no_vig_over_prob,
        pp.no_vig_under_prob,
        pp.vig_percentage,
        pp.game_date,
        pp.game_id,
        gc.is_home,
        gc.team_id,
        gc.team_tricode,
        gc.opponent_id,
        gc.opponent_tricode,
        gc.home_away,
        gc.team_rest_days,
        gc.opponent_rest_days,
        gc.is_back_to_back,
        gc.team_win_pct_last_10,
        gc.opponent_win_pct_last_10,
        gc.win_pct_diff_last_10,
        gc.season_year as season_year,
        po.stat_value as actual_stat_value,

        case
            when po.stat_value > pp.line then 'OVER'
            when po.stat_value < pp.line then 'UNDER'
            when po.stat_value = pp.line then 'PUSH'
            else NULL
        end as outcome,

        case
            when pp.no_vig_over_prob is not null then pp.no_vig_over_prob
            else pp.over_implied_prob / (pp.over_implied_prob + pp.under_implied_prob)
        end as fair_over_prob,

        case
            when pp.no_vig_under_prob is not null then pp.no_vig_under_prob
            else pp.under_implied_prob / (pp.over_implied_prob + pp.under_implied_prob)
        end as fair_under_prob,

        case
            when po.stat_value > pp.line then 1
            when po.stat_value < pp.line then 0
            when po.stat_value = pp.line then null
            else null
        end as beat_line_flag,
        pp.position

    from player_props_with_games pp
    left join game_context gc
        on pp.game_id = gc.game_id
        and pp.team_id = gc.team_id
    left join player_outcomes po
        on pp.player_id = po.player_id
        and pp.game_id = po.game_id
        and pp.market_cleaned = po.stat_name
),

combined_features as (
    select
        jd.*,
        
        -- Player Game Matchup Features (all features from the store)
        pgmf.player_recent_pts,
        pgmf.player_recent_reb,
        pgmf.player_recent_ast,
        pgmf.player_recent_min,
        pgmf.player_form,
        pgmf.player_rest_days,
        pgmf.opponent_defensive_rating,
        pgmf.opponent_pace,
        pgmf.opponent_adjusted_def_rating,
        pgmf.avg_pts_allowed_to_position,
        pgmf.pts_vs_league_avg,
        pgmf.pts_matchup_label,
        pgmf.reb_matchup_label,
        pgmf.ast_matchup_label,
        pgmf.hist_avg_pts_vs_opp,
        pgmf.hist_avg_reb_vs_opp,
        pgmf.hist_avg_ast_vs_opp,
        pgmf.hist_games_vs_opp,
        pgmf.hist_performance_flag,
        pgmf.hist_confidence,
        pgmf.hist_recent_pts_vs_opp,
        pgmf.team_recent_off_rating,
        pgmf.is_missing_teammate,
        pgmf.is_back_to_back as matchup_is_back_to_back,
        pgmf.is_home as matchup_is_home,
        pgmf.blended_pts_projection,
        pgmf.blended_reb_projection,
        pgmf.blended_ast_projection,
        
        -- Player Scoring Feature Store (all features from the store)
        psfs.pts_roll_10g_avg,
        psfs.ts_pct_roll_10g_avg,
        psfs.usage_pct_roll_10g_avg,
        psfs.offensive_role_factor_roll_10g,
        psfs.scoring_efficiency_composite_roll_10g,
        psfs.usage_weighted_ts_roll_10g,
        psfs.points_opportunity_ratio_roll_10g,
        psfs.shot_creation_index_roll_10g,
        psfs.self_created_scoring_rate_per_min_roll_10g,
        psfs.shooting_volume_per_min_roll_10g,
        psfs.free_throw_generation_aggressiveness_roll_10g,
        psfs.contested_vs_uncontested_fg_pct_diff_roll_10g,
        psfs.contested_fg_makes_per_minute_roll_10g,
        psfs.scoring_versatility_ratio_roll_10g,
        psfs.three_pt_value_efficiency_index_roll_10g,
        psfs.paint_reliance_index_roll_10g,
        psfs.scoring_profile_balance_3pt_vs_paint_roll_10g,
        psfs.scoring_focus_ratio_roll_10g,
        psfs.pace_adjusted_points_per_minute_roll_10g,
        psfs.opportunistic_scoring_rate_per_min_roll_10g,
        psfs.role_consistency_indicator,
        psfs.team_l5_pace,
        psfs.team_l5_off_rating,
        psfs.team_form,
        psfs.team_playstyle,
        psfs.team_offensive_structure,
        psfs.player_offensive_role,
        psfs.player_team_style_fit_score,
        psfs.pace_impact_on_player,
        psfs.usage_opportunity,
        psfs.team_form_player_impact,
        psfs.team_playstyle_stat_impact,
        psfs.pace_multiplier,
        psfs.usage_multiplier,
        psfs.team_form_multiplier,
        psfs.playstyle_stat_multiplier,
        psfs.style_fit_multiplier,
        psfs.team_adjusted_pts_projection,
        psfs.team_adjusted_reb_projection,
        psfs.team_adjusted_ast_projection,
        psfs.team_context_impact,
        psfs.player_l5_pts,
        psfs.player_l5_reb,
        psfs.player_l5_ast,
        psfs.pts_in_team_hot_streaks,
        psfs.pts_in_team_cold_streaks,
        psfs.pts_in_star_dominant_system,
        psfs.pts_in_balanced_system,
        psfs.combined_context_multiplier,
        psfs.context_sensitivity_tier,
        psfs.projection_vs_baseline_pts_diff,
        psfs.projection_accuracy_ratio,
        psfs.composite_efficiency_reliability,
        psfs.pressure_performance_tier,
        psfs.scoring_archetype,
        psfs.pct_pts_in_paint,
        psfs.def_at_rim_fg_pct,
        psfs.def_at_rim_fga_rate,
        psfs.paint_scoring_reliance_deviation,
        psfs.rim_finishing_efficiency_deviation,
        psfs.rim_attempt_rate_deviation,
        psfs.matchup_adaptability_index,
        psfs.pct_of_team_pts,
        psfs.pct_of_team_reb,
        psfs.pct_of_team_ast,
        psfs.pct_of_team_fga,
        psfs.efficiency_vs_team_avg,
        psfs.team_offensive_impact_magnitude,
        
        -- Opponent Pregame Profile Features (all features from the store)
        oppf.opp_l5_def_rating_prior,
        oppf.opp_l5_off_rating_prior,
        oppf.opp_l5_pace_prior,
        oppf.opp_l5_pts_prior,
        oppf.opp_l5_allowed_paint_pts_prior,
        oppf.opp_l5_def_reb_pct_prior,
        oppf.opp_l5_blocks_prior,
        oppf.opp_l5_fg_pct_prior,
        oppf.opp_l5_fg3_pct_prior,
        oppf.opp_l10_def_rating_prior,
        oppf.opp_l10_off_rating_prior,
        oppf.opp_l10_pace_prior,
        oppf.opp_l10_fg_pct_prior,
        oppf.opp_l10_fg3_pct_prior,
        oppf.opp_l10_allowed_paint_pts_prior,
        oppf.opp_season_def_rating_prior,
        oppf.opp_season_pace_prior,
        oppf.opp_season_fg_pct_prior,
        oppf.opp_season_fg3_pct_prior,
        oppf.opp_season_allowed_paint_pts_prior,
        oppf.opp_adjusted_def_rating_prior,
        oppf.opp_adjusted_pace_prior,
        oppf.opp_adjusted_allowed_paint_pts_prior,
        oppf.opp_adjusted_fg_pct_prior,
        oppf.opp_adjusted_fg3_pct_prior,
        oppf.opp_def_rating_z_score_prior,
        oppf.opp_pace_z_score_prior,
        
        -- Opponent Position Defense Features (all features from the store)
        opdf.avg_pts_allowed_to_position as opd_avg_pts_allowed_to_position,
        opdf.avg_reb_allowed_to_position as opd_avg_reb_allowed_to_position,
        opdf.avg_ast_allowed_to_position as opd_avg_ast_allowed_to_position,
        opdf.avg_stl_allowed_to_position,
        opdf.avg_blk_allowed_to_position,
        opdf.avg_fg3m_allowed_to_position,
        opdf.avg_fg_pct_allowed_to_position,
        opdf.avg_fg3_pct_allowed_to_position,
        opdf.avg_pts_ast_allowed_to_position,
        opdf.avg_pts_reb_allowed_to_position,
        opdf.avg_pts_reb_ast_allowed_to_position,
        opdf.avg_ast_reb_allowed_to_position,
        opdf.l10_pts_allowed_to_position,
        opdf.league_avg_pts_by_position,
        opdf.league_avg_reb_by_position,
        opdf.league_avg_ast_by_position,
        opdf.league_avg_stl_by_position,
        opdf.league_avg_blk_by_position,
        opdf.league_avg_fg3m_by_position,
        opdf.league_avg_fg_pct_by_position,
        opdf.league_avg_fg3_pct_by_position,
        opdf.league_avg_pts_ast_by_position,
        opdf.league_avg_pts_reb_by_position,
        opdf.league_avg_pts_reb_ast_by_position,
        opdf.league_avg_ast_reb_by_position,
        opdf.pts_vs_league_avg as opd_pts_vs_league_avg,
        opdf.reb_vs_league_avg,
        opdf.ast_vs_league_avg,
        opdf.stl_vs_league_avg,
        opdf.blk_vs_league_avg,
        opdf.fg3m_vs_league_avg,
        opdf.fg_pct_vs_league_avg,
        opdf.fg3_pct_vs_league_avg,
        opdf.pts_ast_vs_league_avg,
        opdf.pts_reb_vs_league_avg,
        opdf.pts_reb_ast_vs_league_avg,
        opdf.ast_reb_vs_league_avg,
        
        -- Derived features
        jd.line - coalesce(pgmf.player_recent_pts, psfs.pts_roll_10g_avg, 0) as line_vs_recent_avg,
        case
            when coalesce(pgmf.player_recent_pts, psfs.pts_roll_10g_avg, 0) > 0 
            then jd.line / coalesce(pgmf.player_recent_pts, psfs.pts_roll_10g_avg, 1)
            else null
        end as line_pct_of_avg,
        
        current_timestamp as generated_at

    from joined_data jd
    
    -- Join Player Scoring Feature Store on player_id + game_id
    left join {{ ref('player_scoring_features_v2') }} psfs
        on jd.player_id = psfs.player_id
        and jd.game_id = psfs.game_id
    
    -- Join Opponent Pregame Profile on team_id + game_id (construct team_game_key logic)
    left join {{ ref('opponent_pregame_profile_features_v2') }} oppf
        on jd.team_id = oppf.team_id
        and jd.game_id = oppf.game_id
    
    -- Join Opponent Position Defense on opponent_id + position + game_date + season_year
    left join {{ ref('opponent_position_defense_features_v2') }} opdf
        on jd.opponent_id = opdf.opponent_id
        and jd.position = opdf.position
        and jd.game_date = opdf.game_date
        and jd.season_year = opdf.season_year
)

select
    season_year,
    game_date,
    player_name,
    is_home,
    team_tricode,
    opponent_tricode,
    home_away,
    team_rest_days,
    opponent_rest_days,
    is_back_to_back,
    market_id,
    market,
    market_cleaned,
    line,
    round(over_odds::numeric, 3) as over_odds,
    round(under_odds::numeric, 3) as under_odds,
    round(over_implied_prob::numeric, 3) as over_implied_prob,
    round(under_implied_prob::numeric, 3) as under_implied_prob,
    round(no_vig_over_prob::numeric, 3) as no_vig_over_prob,
    round(no_vig_under_prob::numeric, 3) as no_vig_under_prob,
    round(vig_percentage::numeric, 3) as vig_percentage,
    round(team_win_pct_last_10::numeric, 3) as team_win_pct_last_10,
    round(opponent_win_pct_last_10::numeric, 3) as opponent_win_pct_last_10,
    round(win_pct_diff_last_10::numeric, 3) as win_pct_diff_last_10,
    round(fair_over_prob::numeric, 3) as fair_over_prob,
    round(fair_under_prob::numeric, 3) as fair_under_prob,
    
    -- All feature store columns with proper rounding
    round(player_recent_pts::numeric, 3) as player_recent_pts,
    round(player_recent_reb::numeric, 3) as player_recent_reb,
    round(player_recent_ast::numeric, 3) as player_recent_ast,
    round(player_recent_min::numeric, 3) as player_recent_min,
    player_form,
    player_rest_days,
    round(opponent_defensive_rating::numeric, 3) as opponent_defensive_rating,
    round(opponent_pace::numeric, 3) as opponent_pace,
    round(opponent_adjusted_def_rating::numeric, 3) as opponent_adjusted_def_rating,
    round(avg_pts_allowed_to_position::numeric, 3) as avg_pts_allowed_to_position,
    round(pts_vs_league_avg::numeric, 3) as pts_vs_league_avg,
    pts_matchup_label,
    reb_matchup_label,
    ast_matchup_label,
    round(hist_avg_pts_vs_opp::numeric, 3) as hist_avg_pts_vs_opp,
    round(hist_avg_reb_vs_opp::numeric, 3) as hist_avg_reb_vs_opp,
    round(hist_avg_ast_vs_opp::numeric, 3) as hist_avg_ast_vs_opp,
    hist_games_vs_opp,
    hist_performance_flag,
    hist_confidence,
    round(hist_recent_pts_vs_opp::numeric, 3) as hist_recent_pts_vs_opp,
    round(team_recent_off_rating::numeric, 3) as team_recent_off_rating,
    is_missing_teammate,
    round(blended_pts_projection::numeric, 3) as blended_pts_projection,
    round(blended_reb_projection::numeric, 3) as blended_reb_projection,
    round(blended_ast_projection::numeric, 3) as blended_ast_projection,
    
    -- Player scoring features (already rounded in source)
    pts_roll_10g_avg,
    ts_pct_roll_10g_avg,
    usage_pct_roll_10g_avg,
    offensive_role_factor_roll_10g,
    scoring_efficiency_composite_roll_10g,
    usage_weighted_ts_roll_10g,
    points_opportunity_ratio_roll_10g,
    shot_creation_index_roll_10g,
    self_created_scoring_rate_per_min_roll_10g,
    shooting_volume_per_min_roll_10g,
    free_throw_generation_aggressiveness_roll_10g,
    contested_vs_uncontested_fg_pct_diff_roll_10g,
    contested_fg_makes_per_minute_roll_10g,
    scoring_versatility_ratio_roll_10g,
    three_pt_value_efficiency_index_roll_10g,
    paint_reliance_index_roll_10g,
    scoring_profile_balance_3pt_vs_paint_roll_10g,
    scoring_focus_ratio_roll_10g,
    pace_adjusted_points_per_minute_roll_10g,
    opportunistic_scoring_rate_per_min_roll_10g,
    role_consistency_indicator,
    team_l5_pace,
    team_l5_off_rating,
    team_form,
    team_playstyle,
    team_offensive_structure,
    player_offensive_role,
    player_team_style_fit_score,
    pace_impact_on_player,
    usage_opportunity,
    team_form_player_impact,
    team_playstyle_stat_impact,
    pace_multiplier,
    usage_multiplier,
    team_form_multiplier,
    playstyle_stat_multiplier,
    style_fit_multiplier,
    team_adjusted_pts_projection,
    team_adjusted_reb_projection,
    team_adjusted_ast_projection,
    team_context_impact,
    player_l5_pts,
    player_l5_reb,
    player_l5_ast,
    pts_in_team_hot_streaks,
    pts_in_team_cold_streaks,
    pts_in_star_dominant_system,
    pts_in_balanced_system,
    combined_context_multiplier,
    context_sensitivity_tier,
    projection_vs_baseline_pts_diff,
    projection_accuracy_ratio,
    composite_efficiency_reliability,
    pressure_performance_tier,
    scoring_archetype,
    pct_pts_in_paint,
    def_at_rim_fg_pct,
    def_at_rim_fga_rate,
    paint_scoring_reliance_deviation,
    rim_finishing_efficiency_deviation,
    rim_attempt_rate_deviation,
    matchup_adaptability_index,
    pct_of_team_pts,
    pct_of_team_reb,
    pct_of_team_ast,
    pct_of_team_fga,
    efficiency_vs_team_avg,
    team_offensive_impact_magnitude,
    
    -- Opponent features (with proper rounding)
    round(opp_l5_def_rating_prior::numeric, 3) as opp_l5_def_rating_prior,
    round(opp_l5_off_rating_prior::numeric, 3) as opp_l5_off_rating_prior,
    round(opp_l5_pace_prior::numeric, 3) as opp_l5_pace_prior,
    round(opp_l5_pts_prior::numeric, 3) as opp_l5_pts_prior,
    round(opp_l5_allowed_paint_pts_prior::numeric, 3) as opp_l5_allowed_paint_pts_prior,
    round(opp_l5_def_reb_pct_prior::numeric, 3) as opp_l5_def_reb_pct_prior,
    round(opp_l5_blocks_prior::numeric, 3) as opp_l5_blocks_prior,
    round(opp_l5_fg_pct_prior::numeric, 3) as opp_l5_fg_pct_prior,
    round(opp_l5_fg3_pct_prior::numeric, 3) as opp_l5_fg3_pct_prior,
    round(opp_l10_def_rating_prior::numeric, 3) as opp_l10_def_rating_prior,
    round(opp_l10_off_rating_prior::numeric, 3) as opp_l10_off_rating_prior,
    round(opp_l10_pace_prior::numeric, 3) as opp_l10_pace_prior,
    round(opp_l10_fg_pct_prior::numeric, 3) as opp_l10_fg_pct_prior,
    round(opp_l10_fg3_pct_prior::numeric, 3) as opp_l10_fg3_pct_prior,
    round(opp_l10_allowed_paint_pts_prior::numeric, 3) as opp_l10_allowed_paint_pts_prior,
    round(opp_season_def_rating_prior::numeric, 3) as opp_season_def_rating_prior,
    round(opp_season_pace_prior::numeric, 3) as opp_season_pace_prior,
    round(opp_season_fg_pct_prior::numeric, 3) as opp_season_fg_pct_prior,
    round(opp_season_fg3_pct_prior::numeric, 3) as opp_season_fg3_pct_prior,
    round(opp_season_allowed_paint_pts_prior::numeric, 3) as opp_season_allowed_paint_pts_prior,
    round(opp_adjusted_def_rating_prior::numeric, 3) as opp_adjusted_def_rating_prior,
    round(opp_adjusted_pace_prior::numeric, 3) as opp_adjusted_pace_prior,
    round(opp_adjusted_allowed_paint_pts_prior::numeric, 3) as opp_adjusted_allowed_paint_pts_prior,
    round(opp_adjusted_fg_pct_prior::numeric, 3) as opp_adjusted_fg_pct_prior,
    round(opp_adjusted_fg3_pct_prior::numeric, 3) as opp_adjusted_fg3_pct_prior,
    round(opp_def_rating_z_score_prior::numeric, 3) as opp_def_rating_z_score_prior,
    round(opp_pace_z_score_prior::numeric, 3) as opp_pace_z_score_prior,
    
    -- Opponent position defense features (with proper rounding)
    round(opd_avg_pts_allowed_to_position::numeric, 3) as opd_avg_pts_allowed_to_position,
    round(opd_avg_reb_allowed_to_position::numeric, 3) as opd_avg_reb_allowed_to_position,
    round(opd_avg_ast_allowed_to_position::numeric, 3) as opd_avg_ast_allowed_to_position,
    round(avg_stl_allowed_to_position::numeric, 3) as avg_stl_allowed_to_position,
    round(avg_blk_allowed_to_position::numeric, 3) as avg_blk_allowed_to_position,
    round(avg_fg3m_allowed_to_position::numeric, 3) as avg_fg3m_allowed_to_position,
    round(avg_fg_pct_allowed_to_position::numeric, 3) as avg_fg_pct_allowed_to_position,
    round(avg_fg3_pct_allowed_to_position::numeric, 3) as avg_fg3_pct_allowed_to_position,
    round(avg_pts_ast_allowed_to_position::numeric, 3) as avg_pts_ast_allowed_to_position,
    round(avg_pts_reb_allowed_to_position::numeric, 3) as avg_pts_reb_allowed_to_position,
    round(avg_pts_reb_ast_allowed_to_position::numeric, 3) as avg_pts_reb_ast_allowed_to_position,
    round(avg_ast_reb_allowed_to_position::numeric, 3) as avg_ast_reb_allowed_to_position,
    round(l10_pts_allowed_to_position::numeric, 3) as l10_pts_allowed_to_position,
    round(league_avg_pts_by_position::numeric, 3) as league_avg_pts_by_position,
    round(league_avg_reb_by_position::numeric, 3) as league_avg_reb_by_position,
    round(league_avg_ast_by_position::numeric, 3) as league_avg_ast_by_position,
    round(league_avg_stl_by_position::numeric, 3) as league_avg_stl_by_position,
    round(league_avg_blk_by_position::numeric, 3) as league_avg_blk_by_position,
    round(league_avg_fg3m_by_position::numeric, 3) as league_avg_fg3m_by_position,
    round(league_avg_fg_pct_by_position::numeric, 3) as league_avg_fg_pct_by_position,
    round(league_avg_fg3_pct_by_position::numeric, 3) as league_avg_fg3_pct_by_position,
    round(league_avg_pts_ast_by_position::numeric, 3) as league_avg_pts_ast_by_position,
    round(league_avg_pts_reb_by_position::numeric, 3) as league_avg_pts_reb_by_position,
    round(league_avg_pts_reb_ast_by_position::numeric, 3) as league_avg_pts_reb_ast_by_position,
    round(league_avg_ast_reb_by_position::numeric, 3) as league_avg_ast_reb_by_position,
    round(opd_pts_vs_league_avg::numeric, 3) as opd_pts_vs_league_avg,
    round(reb_vs_league_avg::numeric, 3) as reb_vs_league_avg,
    round(ast_vs_league_avg::numeric, 3) as ast_vs_league_avg,
    round(stl_vs_league_avg::numeric, 3) as stl_vs_league_avg,
    round(blk_vs_league_avg::numeric, 3) as blk_vs_league_avg,
    round(fg3m_vs_league_avg::numeric, 3) as fg3m_vs_league_avg,
    round(fg_pct_vs_league_avg::numeric, 3) as fg_pct_vs_league_avg,
    round(fg3_pct_vs_league_avg::numeric, 3) as fg3_pct_vs_league_avg,
    round(pts_ast_vs_league_avg::numeric, 3) as pts_ast_vs_league_avg,
    round(pts_reb_vs_league_avg::numeric, 3) as pts_reb_vs_league_avg,
    round(pts_reb_ast_vs_league_avg::numeric, 3) as pts_reb_ast_vs_league_avg,
    round(ast_reb_vs_league_avg::numeric, 3) as ast_reb_vs_league_avg,
    
    -- Derived features
    round(line_vs_recent_avg::numeric, 3) as line_vs_recent_avg,
    round(line_pct_of_avg::numeric, 3) as line_pct_of_avg,
    
    -- Core identifiers and targets
    player_id,
    team_id,
    game_id,
    opponent_id,
    player_prop_key,
    actual_stat_value,
    outcome,
    beat_line_flag,
    position,
    generated_at
    
from combined_features
where player_id is not null
and game_id is not null
and market_cleaned != 'UNKNOWN'