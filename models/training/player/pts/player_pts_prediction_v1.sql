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

{% set team_rolling_window = var('rolling_window_games', 10) %}

with player_props as (
    select
        pp.player_prop_key,
        pp.player_id,
        pp.player_name,
        pp.market_id,
        pp.market,
        
        -- Market is now filtered to 'Points O/U', so market_cleaned is always 'PTS'
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
    where pp.game_date >= '{{ var("start_date_filter", "2017-10-01") }}'
    and pp.market = 'Points O/U' -- Filter for PTS market only
),

-- Get player-team relationships with proper historical context
player_teams as (
    select
        ptb.player_id,
        ptb.team_id,
        ptb.valid_from,
        ptb.valid_to
    from {{ ref('player_team_bridge') }} ptb
    where ptb.valid_from <= current_date  -- Only get relevant team assignments
),

player_position as (
    select
        pos.player_id,
        pos.position
    from {{ ref('dim__players') }} pos
),

-- Link player props to games through player team affiliations
player_props_with_games as (
    select
        pp.*,  -- Include all columns from player_props
        coalesce(gc.game_id, go.game_id) as game_id,
        pt.team_id,
        ppos.position
    from player_props pp
    -- Join to get historically accurate team_id from player_team_bridge
    left join player_teams pt
        on pp.player_id = pt.player_id
        and pp.game_date >= pt.valid_from 
        and (pp.game_date < pt.valid_to or pt.valid_to is null)
    -- Join to get game information
    left join {{ ref('int_game__schedules') }} gc 
        on pp.game_date = gc.game_date 
        and (gc.home_team_id = pt.team_id or gc.away_team_id = pt.team_id)
    left join {{ ref('feat_opp__game_opponents') }} go
        on pp.game_date = go.game_date
        and pt.team_id = go.team_id
    left join player_position ppos -- Join to get player's position
        on pp.player_id = ppos.player_id
),

-- Get player game stats for actual outcomes using boxscores
player_outcomes as (
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS' as stat_name,
        pbs.pts as stat_value,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= '2017-10-01'  -- Align with props date range
),

-- Get game context information
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
        -- Calculate win percentage difference
        case
            when go.home_away = 'home' then 
                coalesce(gc.home_win_pct_last_10, 0.5) - coalesce(gc.away_win_pct_last_10, 0.5)
            else 
                coalesce(gc.away_win_pct_last_10, 0.5) - coalesce(gc.home_win_pct_last_10, 0.5)
        end as win_pct_diff_last_10,
        -- Add NBA season mapping
        case
            when extract(month from go.game_date) >= 10 then extract(year from go.game_date)::text
            else (extract(year from go.game_date) - 1)::text
        end as nba_season
    from {{ ref('feat_opp__game_opponents') }} go
    left join {{ ref('int_game__schedules') }} gc on go.game_id = gc.game_id
    left join {{ ref('stg__teams') }} t on go.team_id = t.team_id
    left join {{ ref('stg__teams') }} opp on go.opponent_id = opp.team_id
    where go.game_date >= '2017-10-01'
),

-- Get player form using rolling stats
player_form_traditional as (
    select 
        pft.player_id,
        pft.game_id,
        pft.game_date,
        pft.pts_roll_3g_avg,
        pft.pts_roll_5g_avg,
        pft.pts_roll_10g_avg,
        pft.pts_roll_3g_stddev,
        pft.pts_roll_5g_stddev,
        pft.pts_roll_10g_stddev,
        pft.fg3m_roll_3g_avg, -- Kept as FG3M contributes to PTS
        pft.fg3m_roll_5g_avg,
        pft.fg3m_roll_10g_avg,
        pft.fg3m_roll_3g_stddev,
        pft.fg3m_roll_5g_stddev,
        pft.fg3m_roll_10g_stddev,
        pft.min_roll_3g_avg, -- Kept as general context
        pft.min_roll_5g_avg,
        pft.min_roll_10g_avg,
        pft.min_roll_3g_stddev,
        pft.min_roll_5g_stddev,
        pft.min_roll_10g_stddev
    from {{ ref('feat_player__traditional_rolling') }} pft
    where pft.game_date >= '2017-10-01'
),

player_form_advanced as (
    select
        pfa.player_id,
        pfa.game_id,
        pfa.game_date,
        pfa.usage_pct_roll_3g_avg, -- Kept as general context
        pfa.usage_pct_roll_5g_avg,
        pfa.usage_pct_roll_10g_avg,
        pfa.usage_pct_roll_3g_stddev,
        pfa.usage_pct_roll_5g_stddev,
        pfa.usage_pct_roll_10g_stddev,
        pfa.ts_pct_roll_3g_avg, -- Kept as general context (True Shooting %)
        pfa.ts_pct_roll_5g_avg,
        pfa.ts_pct_roll_10g_avg,
        pfa.ts_pct_roll_3g_stddev,
        pfa.ts_pct_roll_5g_stddev,
        pfa.ts_pct_roll_10g_stddev
    from {{ ref('feat_player__advanced_rolling') }} pfa
    where pfa.game_date >= '2017-10-01'
),

player_form_usage as (
    select
        pfu.player_id,
        pfu.game_id,
        pfu.game_date,
        pfu.pct_of_team_pts_roll_3g_avg, -- Kept for PTS
        pfu.pct_of_team_pts_roll_5g_avg,
        pfu.pct_of_team_pts_roll_10g_avg,
        pfu.pct_of_team_pts_roll_3g_stddev,
        pfu.pct_of_team_pts_roll_5g_stddev,
        pfu.pct_of_team_pts_roll_10g_stddev,
        -- Removed pct_of_team_reb_roll_*
        -- Removed pct_of_team_ast_roll_*
        pfu.pct_of_team_fg3m_roll_3g_avg, -- Kept as FG3M contributes to PTS
        pfu.pct_of_team_fg3m_roll_5g_avg,
        pfu.pct_of_team_fg3m_roll_10g_avg,
        pfu.pct_of_team_fg3m_roll_3g_stddev,
        pfu.pct_of_team_fg3m_roll_5g_stddev,
        pfu.pct_of_team_fg3m_roll_10g_stddev
    from {{ ref('feat_player__usage_rolling') }} pfu
    where pfu.game_date >= '2017-10-01'
),

player_form_tracking as (
    select
        pft.player_id,
        pft.game_id,
        pft.game_date,
        -- Touches indicate scoring opportunities
        pft.touches_roll_3g_avg,
        pft.touches_roll_5g_avg,
        pft.touches_roll_10g_avg,
        pft.touches_roll_3g_stddev,
        pft.touches_roll_5g_stddev,
        pft.touches_roll_10g_stddev,
        -- Contested vs uncontested shooting stats
        pft.cont_fg_pct_roll_3g_avg,
        pft.cont_fg_pct_roll_5g_avg,
        pft.cont_fg_pct_roll_10g_avg,
        pft.cont_fg_pct_roll_3g_stddev,
        pft.cont_fg_pct_roll_5g_stddev,
        pft.cont_fg_pct_roll_10g_stddev,
        pft.uncont_fg_pct_roll_3g_avg,
        pft.uncont_fg_pct_roll_5g_avg,
        pft.uncont_fg_pct_roll_10g_avg,
        pft.uncont_fg_pct_roll_3g_stddev,
        pft.uncont_fg_pct_roll_5g_stddev,
        pft.uncont_fg_pct_roll_10g_stddev,
        -- Defended at rim shooting stats
        pft.def_at_rim_fgm_roll_3g_avg,
        pft.def_at_rim_fgm_roll_5g_avg,
        pft.def_at_rim_fgm_roll_10g_avg,
        pft.def_at_rim_fga_roll_3g_avg,
        pft.def_at_rim_fga_roll_5g_avg,
        pft.def_at_rim_fga_roll_10g_avg,
        pft.def_at_rim_fg_pct_roll_3g_avg,
        pft.def_at_rim_fg_pct_roll_5g_avg,
        pft.def_at_rim_fg_pct_roll_10g_avg
    from {{ ref('feat_player__tracking_rolling') }} pft
    where pft.game_date >= '2017-10-01'
),

-- REFACTORED CTE: Using feat_opp__player_vs_opponent
player_vs_opponent_features as (
    select
        fpo.player_id,
        fpo.game_id,
        -- Historical matchup features
        fpo.hist_avg_pts_vs_opp,
        fpo.hist_avg_reb_vs_opp,
        fpo.hist_avg_ast_vs_opp,
        fpo.hist_games_vs_opp,
        fpo.hist_performance_flag,
        fpo.hist_confidence,
        fpo.hist_recent_pts_vs_opp,
        -- Blended prediction features
        fpo.blended_pts_projection,
        -- Opponent team features (subset from opponent_pregame_profile_features_v1)
        fpo.opponent_defensive_rating, -- This is opp_l10_def_rating_prior
        fpo.opponent_pace, -- This is opp_l10_pace_prior
        fpo.opponent_adjusted_def_rating, -- This is opp_adjusted_def_rating_prior
        -- Position matchup features (subset from feat_opp__player_position_matchup)
        fpo.avg_pts_allowed_to_position,
        fpo.pts_vs_league_avg,
        fpo.pts_matchup_label,
        -- Team context features
        fpo.team_recent_off_rating, -- This is team_last5_off_rating
        fpo.is_missing_teammate
    from {{ ref('feat_opp__player_vs_opponent') }} fpo
    -- game_date filter is implicitly handled by join with jd which is filtered by game_date
),

player_scoring_features as (
    select
        psf.player_id,
        psf.game_id,
        psf.game_date, -- Ensure game_date is selected for the filter if not already
        psf.career_high_pts_prior_game as career_high_pts_to_date,
        psf.career_ppg_prior_game as career_ppg_to_date,
        psf.career_games_prior_game as career_games_to_date,
        psf.career_avg_minutes_prior_game,
        psf.days_since_last_30_plus_game_prior as days_since_last_30_plus_game,
        psf.days_since_last_40_plus_game_prior as days_since_last_40_plus_game,
        psf.thirty_plus_games_career_prior as thirty_plus_games_career,
        psf.forty_plus_games_career_prior as forty_plus_games_career,
        psf.thirty_plus_games_season_prior as thirty_plus_games_this_season,
        psf.forty_plus_games_season_prior as forty_plus_games_this_season,
        psf.thirty_plus_games_prev_season as thirty_plus_games_last_season,
        psf.forty_plus_games_prev_season as forty_plus_games_last_season,
        psf.avg_usage_in_30_plus_career_prior as avg_usage_in_30_plus_career,
        psf.avg_usage_in_40_plus_career_prior as avg_usage_in_40_plus_career,
        psf.avg_usage_0_to_9_pts_prior as avg_usage_0_to_9_pts,
        psf.avg_usage_10_to_19_pts_prior as avg_usage_10_to_19_pts,
        psf.avg_usage_20_to_29_pts_prior as avg_usage_20_to_29_pts,
        psf.scoring_efficiency_composite_lag1 as scoring_efficiency_composite,
        psf.points_opportunity_ratio_lag1 as points_opportunity_ratio,
        psf.shot_creation_index_lag1 as shot_creation_index,
        psf.defensive_attention_factor_lag1 as defensive_attention_factor,
        psf.scoring_versatility_ratio_lag1 as scoring_versatility_ratio,
        psf.offensive_role_factor_lag1 as offensive_role_factor,
        psf.adjusted_usage_with_defense_lag1 as adjusted_usage_with_defense,
        psf.pts_per_half_court_poss_lag1 as pts_per_half_court_poss,
        psf.usage_weighted_ts_lag1 as usage_weighted_ts,
        psf.three_pt_value_efficiency_index_lag1 as three_pt_value_efficiency_index,
        psf.paint_reliance_index_lag1 as paint_reliance_index,
        psf.assisted_shot_efficiency_lag1 as assisted_shot_efficiency,
        psf.pace_adjusted_points_per_minute_lag1 as pace_adjusted_points_per_minute,
        psf.second_chance_conversion_rate_lag1 as second_chance_conversion_rate,
        psf.contested_vs_uncontested_fg_pct_diff_lag1 as contested_vs_uncontested_fg_pct_diff,
        psf.shooting_volume_per_min_lag1 as shooting_volume_per_min,
        psf.effective_three_point_contribution_rate_lag1 as effective_three_point_contribution_rate,
        psf.free_throw_generation_aggressiveness_lag1 as free_throw_generation_aggressiveness,
        psf.self_created_scoring_rate_per_min_lag1 as self_created_scoring_rate_per_min,
        psf.opportunistic_scoring_rate_per_min_lag1 as opportunistic_scoring_rate_per_min,
        psf.scoring_focus_ratio_lag1 as scoring_focus_ratio,
        psf.contested_fg_makes_per_minute_lag1 as contested_fg_makes_per_minute,
        psf.scoring_profile_balance_3pt_vs_paint_lag1 as scoring_profile_balance_3pt_vs_paint
    from {{ ref('feat_player__scoring_features') }} psf
    where psf.game_date >= '{{ var("start_date_filter", "2017-10-01") }}' 
      and psf.career_games_prior_game >= 25
      and psf.career_avg_minutes_prior_game >= 10
),

-- Join player props with outcomes and game context
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

        -- Determine outcome
        case
            when po.stat_value > pp.line then 'OVER'
            when po.stat_value < pp.line then 'UNDER'
            when po.stat_value = pp.line then 'PUSH'
            else NULL -- Game not played yet or missing data
        end as outcome,

        -- Calculate implied win probabilities
        case
            when pp.no_vig_over_prob is not null then pp.no_vig_over_prob
            else pp.over_implied_prob / (pp.over_implied_prob + pp.under_implied_prob)
        end as fair_over_prob,

        case
            when pp.no_vig_under_prob is not null then pp.no_vig_under_prob
            else pp.under_implied_prob / (pp.over_implied_prob + pp.under_implied_prob)
        end as fair_under_prob,

        -- Classification target
        case
            when po.stat_value > pp.line then 1
            when po.stat_value < pp.line then 0
            when po.stat_value = pp.line then null -- push
            else null -- missing data
        end as beat_line_flag,
        ppos.position

    from player_props_with_games pp
    left join game_context gc
        on pp.game_id = gc.game_id
        and pp.team_id = gc.team_id  -- Join on both game_id and team_id
    left join player_outcomes po
        on pp.player_id = po.player_id
        and pp.game_id = po.game_id
        and pp.market_cleaned = po.stat_name
    left join player_position ppos -- Join to get player's position
        on pp.player_id = ppos.player_id
),

combined_features as (
    select
        jd.player_prop_key,
        jd.player_id,
        jd.player_name,
        jd.market_id,
        jd.market,
        jd.market_cleaned,
        jd.line,
        jd.over_odds,
        jd.under_odds,
        jd.over_implied_prob,
        jd.under_implied_prob,
        jd.no_vig_over_prob,
        jd.no_vig_under_prob,
        jd.vig_percentage,
        jd.game_date,
        jd.game_id,
        jd.is_home,
        jd.team_id,
        jd.team_tricode,
        jd.opponent_id,
        jd.opponent_tricode,
        jd.home_away,
        jd.team_rest_days,
        jd.opponent_rest_days,
        jd.is_back_to_back,
        jd.team_win_pct_last_10,
        jd.opponent_win_pct_last_10,
        jd.win_pct_diff_last_10,
        jd.season_year,
        jd.actual_stat_value,
        jd.outcome,
        jd.fair_over_prob,
        jd.fair_under_prob,
        jd.beat_line_flag,
        jd.position,

        pft.pts_roll_3g_avg,
        pft.pts_roll_5g_avg,
        pft.pts_roll_10g_avg,
        pft.pts_roll_3g_stddev,
        pft.pts_roll_5g_stddev,
        pft.pts_roll_10g_stddev,
        pft.fg3m_roll_3g_avg,
        pft.fg3m_roll_5g_avg,
        pft.fg3m_roll_10g_avg,
        pft.fg3m_roll_3g_stddev,
        pft.fg3m_roll_5g_stddev,
        pft.fg3m_roll_10g_stddev,
        pft.min_roll_3g_avg,
        pft.min_roll_5g_avg,
        pft.min_roll_10g_avg,
        pft.min_roll_3g_stddev,
        pft.min_roll_5g_stddev,
        pft.min_roll_10g_stddev,
        pfa.usage_pct_roll_3g_avg,
        pfa.usage_pct_roll_5g_avg,
        pfa.usage_pct_roll_10g_avg,
        pfa.usage_pct_roll_3g_stddev,
        pfa.usage_pct_roll_5g_stddev,
        pfa.usage_pct_roll_10g_stddev,
        pfu.pct_of_team_pts_roll_3g_avg,
        pfu.pct_of_team_pts_roll_5g_avg,
        pfu.pct_of_team_pts_roll_10g_avg,
        pfu.pct_of_team_pts_roll_3g_stddev,
        pfu.pct_of_team_pts_roll_5g_stddev,
        pfu.pct_of_team_pts_roll_10g_stddev,
        pfu.pct_of_team_fg3m_roll_3g_avg,
        pfu.pct_of_team_fg3m_roll_5g_avg,
        pfu.pct_of_team_fg3m_roll_10g_avg,
        pfu.pct_of_team_fg3m_roll_3g_stddev,
        pfu.pct_of_team_fg3m_roll_5g_stddev,
        pfu.pct_of_team_fg3m_roll_10g_stddev,
        pfa.ts_pct_roll_3g_avg,
        pfa.ts_pct_roll_5g_avg,
        pfa.ts_pct_roll_10g_avg,
        pfa.ts_pct_roll_3g_stddev,
        pfa.ts_pct_roll_5g_stddev,
        pfa.ts_pct_roll_10g_stddev,

        -- Features from player_vs_opponent_features (fpvof)
        fpvof.opponent_defensive_rating, 
        fpvof.opponent_pace, 
        fpvof.opponent_adjusted_def_rating,
        fpvof.avg_pts_allowed_to_position, -- This is the one from feat_opp__player_vs_opponent
        fpvof.pts_vs_league_avg,          -- This is the one from feat_opp__player_vs_opponent
        fpvof.pts_matchup_label,          -- This is the one from feat_opp__player_vs_opponent

        fpvof.hist_games_vs_opp,
        fpvof.hist_avg_pts_vs_opp,
        fpvof.hist_avg_reb_vs_opp,
        fpvof.hist_avg_ast_vs_opp,
        fpvof.hist_recent_pts_vs_opp,
        fpvof.hist_performance_flag,
        fpvof.hist_confidence,
        fpvof.blended_pts_projection,
        fpvof.team_recent_off_rating,
        fpvof.is_missing_teammate,

        psf.career_high_pts_to_date,
        psf.career_ppg_to_date,
        psf.career_games_to_date,
        psf.days_since_last_30_plus_game,
        psf.days_since_last_40_plus_game,
        psf.thirty_plus_games_career,
        psf.forty_plus_games_career,
        psf.thirty_plus_games_this_season,
        psf.forty_plus_games_this_season,
        psf.thirty_plus_games_last_season,
        psf.forty_plus_games_last_season,
        psf.avg_usage_in_30_plus_career,
        psf.avg_usage_in_40_plus_career,
        psf.avg_usage_0_to_9_pts,
        psf.avg_usage_10_to_19_pts,
        psf.avg_usage_20_to_29_pts,
        psf.scoring_efficiency_composite,
        psf.points_opportunity_ratio,
        psf.shot_creation_index,
        psf.defensive_attention_factor,
        psf.scoring_versatility_ratio,
        psf.offensive_role_factor,
        psf.adjusted_usage_with_defense,
        psf.pts_per_half_court_poss,
        psf.usage_weighted_ts,
        psf.three_pt_value_efficiency_index,
        psf.paint_reliance_index,
        psf.assisted_shot_efficiency,
        psf.pace_adjusted_points_per_minute,
        psf.second_chance_conversion_rate,
        psf.contested_vs_uncontested_fg_pct_diff,
        psf.shooting_volume_per_min,
        psf.effective_three_point_contribution_rate,
        psf.free_throw_generation_aggressiveness,
        psf.self_created_scoring_rate_per_min,
        psf.opportunistic_scoring_rate_per_min,
        psf.scoring_focus_ratio,
        psf.contested_fg_makes_per_minute,
        psf.scoring_profile_balance_3pt_vs_paint,

        pftk.touches_roll_3g_avg,
        pftk.touches_roll_5g_avg,
        pftk.touches_roll_10g_avg,
        pftk.touches_roll_3g_stddev,
        pftk.touches_roll_5g_stddev,
        pftk.touches_roll_10g_stddev,
        pftk.cont_fg_pct_roll_3g_avg,
        pftk.cont_fg_pct_roll_5g_avg,
        pftk.cont_fg_pct_roll_10g_avg,
        pftk.cont_fg_pct_roll_3g_stddev,
        pftk.cont_fg_pct_roll_5g_stddev,
        pftk.cont_fg_pct_roll_10g_stddev,
        pftk.uncont_fg_pct_roll_3g_avg,
        pftk.uncont_fg_pct_roll_5g_avg,
        pftk.uncont_fg_pct_roll_10g_avg,
        pftk.uncont_fg_pct_roll_3g_stddev,
        pftk.uncont_fg_pct_roll_5g_stddev,
        pftk.uncont_fg_pct_roll_10g_stddev,
        pftk.def_at_rim_fgm_roll_3g_avg,
        pftk.def_at_rim_fgm_roll_5g_avg,
        pftk.def_at_rim_fgm_roll_10g_avg,
        pftk.def_at_rim_fga_roll_3g_avg,
        pftk.def_at_rim_fga_roll_5g_avg,
        pftk.def_at_rim_fga_roll_10g_avg,
        pftk.def_at_rim_fg_pct_roll_3g_avg,
        pftk.def_at_rim_fg_pct_roll_5g_avg,
        pftk.def_at_rim_fg_pct_roll_10g_avg,

        jd.line - pft.pts_roll_5g_avg as line_vs_last_5_avg,
        jd.line - pft.pts_roll_10g_avg as line_vs_last_10_avg,
        case
            when pft.pts_roll_10g_avg > 0 then jd.line / pft.pts_roll_10g_avg
            else null
        end as line_pct_of_avg,
        current_timestamp as generated_at

    from joined_data jd
    left join player_form_traditional pft
        on jd.player_id = pft.player_id
        and jd.game_id = pft.game_id
    left join player_form_advanced pfa
        on jd.player_id = pfa.player_id
        and jd.game_id = pfa.game_id
    left join player_form_usage pfu
        on jd.player_id = pfu.player_id
        and jd.game_id = pfu.game_id
    left join player_form_tracking pftk
        on jd.player_id = pftk.player_id
        and jd.game_id = pftk.game_id
    left join {{ ref('opponent_position_defense_features_v1') }} opd -- RE-ADDED join
        ON jd.opponent_id = opd.opponent_id
        AND jd.game_date::date = opd.game_date 
        AND jd.position = opd.position
        AND jd.season_year = opd.season_year
    inner join player_scoring_features psf  -- <<<< CHANGED FROM LEFT JOIN TO INNER JOIN
        on jd.player_id = psf.player_id
        and jd.game_id = psf.game_id
    left join player_vs_opponent_features fpvof 
        on jd.player_id = fpvof.player_id
        and jd.game_id = fpvof.game_id
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
    
    round(pts_roll_3g_avg::numeric, 3) as pts_roll_3g_avg,
    round(pts_roll_5g_avg::numeric, 3) as pts_roll_5g_avg,
    round(pts_roll_10g_avg::numeric, 3) as pts_roll_10g_avg,
    round(pts_roll_3g_stddev::numeric, 3) as pts_roll_3g_stddev,
    round(pts_roll_5g_stddev::numeric, 3) as pts_roll_5g_stddev,
    round(pts_roll_10g_stddev::numeric, 3) as pts_roll_10g_stddev,
    round(fg3m_roll_3g_avg::numeric, 3) as fg3m_roll_3g_avg,
    round(fg3m_roll_5g_avg::numeric, 3) as fg3m_roll_5g_avg,
    round(fg3m_roll_10g_avg::numeric, 3) as fg3m_roll_10g_avg,
    round(fg3m_roll_3g_stddev::numeric, 3) as fg3m_roll_3g_stddev,
    round(fg3m_roll_5g_stddev::numeric, 3) as fg3m_roll_5g_stddev,
    round(fg3m_roll_10g_stddev::numeric, 3) as fg3m_roll_10g_stddev,
    round(min_roll_3g_avg::numeric, 3) as min_roll_3g_avg,
    round(min_roll_5g_avg::numeric, 3) as min_roll_5g_avg,
    round(min_roll_10g_avg::numeric, 3) as min_roll_10g_avg,
    round(min_roll_3g_stddev::numeric, 3) as min_roll_3g_stddev,
    round(min_roll_5g_stddev::numeric, 3) as min_roll_5g_stddev,
    round(min_roll_10g_stddev::numeric, 3) as min_roll_10g_stddev,
    
    round(usage_pct_roll_3g_avg::numeric, 3) as usage_pct_roll_3g_avg,
    round(usage_pct_roll_5g_avg::numeric, 3) as usage_pct_roll_5g_avg,
    round(usage_pct_roll_10g_avg::numeric, 3) as usage_pct_roll_10g_avg,
    round(usage_pct_roll_3g_stddev::numeric, 3) as usage_pct_roll_3g_stddev,
    round(usage_pct_roll_5g_stddev::numeric, 3) as usage_pct_roll_5g_stddev,
    round(usage_pct_roll_10g_stddev::numeric, 3) as usage_pct_roll_10g_stddev,
    round(ts_pct_roll_3g_avg::numeric, 3) as ts_pct_roll_3g_avg,
    round(ts_pct_roll_5g_avg::numeric, 3) as ts_pct_roll_5g_avg,
    round(ts_pct_roll_10g_avg::numeric, 3) as ts_pct_roll_10g_avg,
    round(ts_pct_roll_3g_stddev::numeric, 3) as ts_pct_roll_3g_stddev,
    round(ts_pct_roll_5g_stddev::numeric, 3) as ts_pct_roll_5g_stddev,
    round(ts_pct_roll_10g_stddev::numeric, 3) as ts_pct_roll_10g_stddev,
    
    round(pct_of_team_pts_roll_3g_avg::numeric, 3) as pct_of_team_pts_roll_3g_avg,
    round(pct_of_team_pts_roll_5g_avg::numeric, 3) as pct_of_team_pts_roll_5g_avg,
    round(pct_of_team_pts_roll_10g_avg::numeric, 3) as pct_of_team_pts_roll_10g_avg,
    round(pct_of_team_pts_roll_3g_stddev::numeric, 3) as pct_of_team_pts_roll_3g_stddev,
    round(pct_of_team_pts_roll_5g_stddev::numeric, 3) as pct_of_team_pts_roll_5g_stddev,
    round(pct_of_team_pts_roll_10g_stddev::numeric, 3) as pct_of_team_pts_roll_10g_stddev,
    round(pct_of_team_fg3m_roll_3g_avg::numeric, 3) as pct_of_team_fg3m_roll_3g_avg,
    round(pct_of_team_fg3m_roll_5g_avg::numeric, 3) as pct_of_team_fg3m_roll_5g_avg,
    round(pct_of_team_fg3m_roll_10g_avg::numeric, 3) as pct_of_team_fg3m_roll_10g_avg,
    round(pct_of_team_fg3m_roll_3g_stddev::numeric, 3) as pct_of_team_fg3m_roll_3g_stddev,
    round(pct_of_team_fg3m_roll_5g_stddev::numeric, 3) as pct_of_team_fg3m_roll_5g_stddev,
    round(pct_of_team_fg3m_roll_10g_stddev::numeric, 3) as pct_of_team_fg3m_roll_10g_stddev,
    
    round(opponent_defensive_rating::numeric, 3) as opponent_l10_def_rating,
    round(opponent_pace::numeric, 3) as opponent_l10_pace,
    round(opponent_adjusted_def_rating::numeric, 3) as opponent_adjusted_def_rating,
    
    round(career_high_pts_to_date::numeric, 3) as career_high_pts_to_date,
    round(career_ppg_to_date::numeric, 3) as career_ppg_to_date,
    career_games_to_date,
    days_since_last_30_plus_game,
    days_since_last_40_plus_game,
    thirty_plus_games_career,
    forty_plus_games_career,
    thirty_plus_games_this_season,
    forty_plus_games_this_season,
    thirty_plus_games_last_season,
    forty_plus_games_last_season,
    round(avg_usage_in_30_plus_career::numeric, 3) as avg_usage_in_30_plus_career,
    round(avg_usage_in_40_plus_career::numeric, 3) as avg_usage_in_40_plus_career,
    round(avg_usage_0_to_9_pts::numeric, 3) as avg_usage_0_to_9_pts,
    round(avg_usage_10_to_19_pts::numeric, 3) as avg_usage_10_to_19_pts,
    round(avg_usage_20_to_29_pts::numeric, 3) as avg_usage_20_to_29_pts,
    round(scoring_efficiency_composite::numeric, 3) as scoring_efficiency_composite,
    round(points_opportunity_ratio::numeric, 3) as points_opportunity_ratio,
    round(shot_creation_index::numeric, 3) as shot_creation_index,
    round(defensive_attention_factor::numeric, 3) as defensive_attention_factor,
    round(scoring_versatility_ratio::numeric, 3) as scoring_versatility_ratio,
    round(offensive_role_factor::numeric, 3) as offensive_role_factor,
    round(adjusted_usage_with_defense::numeric, 3) as adjusted_usage_with_defense,
    round(pts_per_half_court_poss::numeric, 3) as pts_per_half_court_poss,
    round(usage_weighted_ts::numeric, 3) as usage_weighted_ts,
    round(three_pt_value_efficiency_index::numeric, 3) as three_pt_value_efficiency_index,
    round(paint_reliance_index::numeric, 3) as paint_reliance_index,
    round(assisted_shot_efficiency::numeric, 3) as assisted_shot_efficiency,
    round(pace_adjusted_points_per_minute::numeric, 3) as pace_adjusted_points_per_minute,
    round(second_chance_conversion_rate::numeric, 3) as second_chance_conversion_rate,
    round(contested_vs_uncontested_fg_pct_diff::numeric, 3) as contested_vs_uncontested_fg_pct_diff,
    round(shooting_volume_per_min::numeric, 3) as shooting_volume_per_min,
    round(effective_three_point_contribution_rate::numeric, 3) as effective_three_point_contribution_rate,
    round(free_throw_generation_aggressiveness::numeric, 3) as free_throw_generation_aggressiveness,
    round(self_created_scoring_rate_per_min::numeric, 3) as self_created_scoring_rate_per_min,
    round(opportunistic_scoring_rate_per_min::numeric, 3) as opportunistic_scoring_rate_per_min,
    round(scoring_focus_ratio::numeric, 3) as scoring_focus_ratio,
    round(contested_fg_makes_per_minute::numeric, 3) as contested_fg_makes_per_minute,
    round(scoring_profile_balance_3pt_vs_paint::numeric, 3) as scoring_profile_balance_3pt_vs_paint,

    round(touches_roll_3g_avg::numeric, 3) as touches_roll_3g_avg,
    round(touches_roll_5g_avg::numeric, 3) as touches_roll_5g_avg,
    round(touches_roll_10g_avg::numeric, 3) as touches_roll_10g_avg,
    round(touches_roll_3g_stddev::numeric, 3) as touches_roll_3g_stddev,
    round(touches_roll_5g_stddev::numeric, 3) as touches_roll_5g_stddev,
    round(touches_roll_10g_stddev::numeric, 3) as touches_roll_10g_stddev,
    round(cont_fg_pct_roll_3g_avg::numeric, 3) as cont_fg_pct_roll_3g_avg,
    round(cont_fg_pct_roll_5g_avg::numeric, 3) as cont_fg_pct_roll_5g_avg,
    round(cont_fg_pct_roll_10g_avg::numeric, 3) as cont_fg_pct_roll_10g_avg,
    round(cont_fg_pct_roll_3g_stddev::numeric, 3) as cont_fg_pct_roll_3g_stddev,
    round(cont_fg_pct_roll_5g_stddev::numeric, 3) as cont_fg_pct_roll_5g_stddev,
    round(cont_fg_pct_roll_10g_stddev::numeric, 3) as cont_fg_pct_roll_10g_stddev,
    round(uncont_fg_pct_roll_3g_avg::numeric, 3) as uncont_fg_pct_roll_3g_avg,
    round(uncont_fg_pct_roll_5g_avg::numeric, 3) as uncont_fg_pct_roll_5g_avg,
    round(uncont_fg_pct_roll_10g_avg::numeric, 3) as uncont_fg_pct_roll_10g_avg,
    round(uncont_fg_pct_roll_3g_stddev::numeric, 3) as uncont_fg_pct_roll_3g_stddev,
    round(uncont_fg_pct_roll_5g_stddev::numeric, 3) as uncont_fg_pct_roll_5g_stddev,
    round(uncont_fg_pct_roll_10g_stddev::numeric, 3) as uncont_fg_pct_roll_10g_stddev,
    round(def_at_rim_fgm_roll_3g_avg::numeric, 3) as def_at_rim_fgm_roll_3g_avg,
    round(def_at_rim_fgm_roll_5g_avg::numeric, 3) as def_at_rim_fgm_roll_5g_avg,
    round(def_at_rim_fgm_roll_10g_avg::numeric, 3) as def_at_rim_fgm_roll_10g_avg,
    round(def_at_rim_fga_roll_3g_avg::numeric, 3) as def_at_rim_fga_roll_3g_avg,
    round(def_at_rim_fga_roll_5g_avg::numeric, 3) as def_at_rim_fga_roll_5g_avg,
    round(def_at_rim_fga_roll_10g_avg::numeric, 3) as def_at_rim_fga_roll_10g_avg,
    round(def_at_rim_fg_pct_roll_3g_avg::numeric, 3) as def_at_rim_fg_pct_roll_3g_avg,
    round(def_at_rim_fg_pct_roll_5g_avg::numeric, 3) as def_at_rim_fg_pct_roll_5g_avg,
    round(def_at_rim_fg_pct_roll_10g_avg::numeric, 3) as def_at_rim_fg_pct_roll_10g_avg,

    hist_games_vs_opp as games_vs_opponent,
    round(hist_avg_pts_vs_opp::numeric, 3) as avg_pts_vs_opponent,
    round(hist_avg_reb_vs_opp::numeric, 3) as avg_reb_vs_opponent,
    round(hist_avg_ast_vs_opp::numeric, 3) as avg_ast_vs_opponent,
    round(hist_recent_pts_vs_opp::numeric, 3) as hist_recency_weighted_pts_vs_opp,
    hist_performance_flag as hist_pts_performance_flag_vs_opp,
    hist_confidence as hist_sample_confidence_vs_opp,
    round(blended_pts_projection::numeric, 3) as blended_pts_projection,
    is_missing_teammate as is_missing_key_teammate,

    round(line_vs_last_5_avg::numeric, 3) as line_vs_last_5_avg,
    round(line_vs_last_10_avg::numeric, 3) as line_vs_last_10_avg,
    round(line_pct_of_avg::numeric, 3) as line_pct_of_avg,
    
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
