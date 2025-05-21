{{
    config(
        schema='training',
        materialized='table',
        unique_key= ['player_id', 'game_date'],
        tags=['betting', 'player_props', 'ml', 'training', 'top_features'],
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
        pp.market,
        'PTS' as market_cleaned, -- Market is now filtered to 'Points O/U'
        pp.line,
        pp.game_date
    from {{ ref('int_betting__player_props_probabilities') }} pp
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
        'PTS' as stat_name, -- Corresponds to market_cleaned
        pbs.pts as stat_value -- Corresponds to actual_stat_value
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= '2017-10-01'  -- Align with props date range
),

-- Get game context information
game_context as (
    select
        go.game_id,
        go.game_date,
        go.team_id,
        go.home_away, -- Still needed for joins/logic potentially
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
        pft.fg3m_roll_3g_stddev,
        pft.min_roll_10g_stddev
    from {{ ref('feat_player__traditional_rolling') }} pft
    where pft.game_date >= '2017-10-01'
),

player_form_advanced as (
    select
        pfa.player_id,
        pfa.game_id,
        pfa.game_date,
        pfa.usage_pct_roll_3g_stddev
    from {{ ref('feat_player__advanced_rolling') }} pfa
    where pfa.game_date >= '2017-10-01'
),

player_form_usage as (
    select
        pfu.player_id,
        pfu.game_id,
        pfu.game_date,
        pfu.pct_of_team_fg3m_roll_5g_avg
    from {{ ref('feat_player__usage_rolling') }} pfu
    where pfu.game_date >= '2017-10-01'
),

player_form_tracking as (
    select
        pftk.player_id,
        pftk.game_id,
        pftk.game_date,
        pftk.touches_roll_10g_stddev,
        pftk.uncont_fg_pct_roll_3g_avg,
        pftk.uncont_fg_pct_roll_10g_stddev,
        pftk.def_at_rim_fga_roll_5g_avg,
        pftk.touches_roll_3g_stddev
    from {{ ref('feat_player__tracking_rolling') }} pftk
    where pftk.game_date >= '2017-10-01'
),

player_opponent_history as (
    select
        poh.player_game_opponent_key, -- Key for joining
        poh.player_id,
        poh.opponent_id,
        poh.avg_pts_vs_opponent_prior,
        poh.pts_lag1_vs_opp
    from {{ ref('feat_opp__player_vs_opponent_history') }} poh
    -- No date filter here, as we want all historical records to be available for joining
    -- The join in combined_features will use the game-specific key
),

player_scoring_features as (
    select
        psf.player_id,
        psf.game_id,
        psf.game_date, 
        psf.adjusted_usage_with_defense_lag1 as adjusted_usage_with_defense,
        psf.contested_fg_makes_per_minute_lag1 as contested_fg_makes_per_minute
    from {{ ref('feat_player__scoring_features') }} psf
    where psf.game_date >= '{{ var("start_date_filter", "2017-10-01") }}' 
),

-- Join player props with outcomes and game context
joined_data as (
    select
        pp.player_prop_key,
        pp.player_id,
        pp.player_name,
        pp.market,
        pp.market_cleaned,
        pp.line,
        pp.game_date,
        pp.game_id,
        gc.is_home,
        gc.team_id,
        gc.team_tricode,
        gc.opponent_id,
        gc.opponent_tricode,
        gc.nba_season,
        po.stat_value as actual_stat_value,
        case
            when po.stat_value > pp.line then 'OVER'
            when po.stat_value < pp.line then 'UNDER'
            when po.stat_value = pp.line then 'PUSH'
            else NULL
        end as outcome,
        case
            when po.stat_value > pp.line then 1
            when po.stat_value < pp.line then 0
            when po.stat_value = pp.line then null
            else null
        end as beat_line_flag,
        ppos.position

    from player_props_with_games pp
    left join game_context gc
        on pp.game_id = gc.game_id and pp.team_id = gc.team_id -- Ensuring correct game context for player's team
    left join player_outcomes po
        on pp.player_id = po.player_id
        and pp.game_id = po.game_id
        and pp.market_cleaned = po.stat_name
    left join player_position ppos -- Join to get player's position
        on pp.player_id = ppos.player_id
),

-- Preserve existing combined_features CTE structure
combined_features as (
    select
        jd.player_prop_key,
        jd.player_id,
        jd.player_name,
        jd.market,
        jd.market_cleaned,
        jd.line,
        jd.game_date,
        jd.game_id,
        jd.is_home,
        jd.team_tricode,
        jd.opponent_id,
        jd.opponent_tricode,
        jd.nba_season,
        jd.actual_stat_value,
        jd.outcome,
        jd.beat_line_flag,
        jd.position,

        -- Player form features (from pft, pfa, pfu, pftk)
        pft.fg3m_roll_3g_stddev,
        pft.min_roll_10g_stddev,
        pfa.usage_pct_roll_3g_stddev,
        pfu.pct_of_team_fg3m_roll_5g_avg,
        pftk.touches_roll_10g_stddev,
        pftk.uncont_fg_pct_roll_3g_avg,
        pftk.uncont_fg_pct_roll_10g_stddev,
        pftk.def_at_rim_fga_roll_5g_avg,
        pftk.touches_roll_3g_stddev,

        -- Opponent position defense features (from feat_opp__position_defense_profile)
        opd.l10_pts_allowed_to_position,
        opd.fg_pct_vs_league_avg as opp_fg_pct_vs_league_avg, -- aliasing for final select
        opd.fg3m_vs_league_avg as opp_fg3m_vs_league_avg,   -- aliasing for final select

        -- Player scoring features (from feat_player__scoring_features)
        psf.adjusted_usage_with_defense,
        psf.contested_fg_makes_per_minute,

        -- Player vs Opponent History features (from poh)
        poh.avg_pts_vs_opponent_prior, -- will be aliased in final select
        poh.pts_lag1_vs_opp as hist_pts_last_1_vs_opp, -- Aliased to match final select

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
    left join {{ ref('feat_opp__position_defense_profile') }} opd
        on jd.opponent_id = opd.opponent_id
        and jd.game_date::date = opd.game_date 
        and jd.position = opd.position
        and jd.nba_season = opd.season_year
    left join player_scoring_features psf
        on jd.player_id = psf.player_id
        and jd.game_id = psf.game_id
    left join player_opponent_history poh
        on jd.player_id || '-' || jd.game_id || '-' || jd.opponent_id = poh.player_game_opponent_key
)

select
    -- Essential Identifiers & Target
    player_prop_key,
    game_date,
    player_id,
    player_name,
    game_id,
    opponent_id,
    market,
    market_cleaned,
    actual_stat_value, 
    outcome, 
    beat_line_flag,
    is_home,
    team_tricode,
    opponent_tricode,
    nba_season,
    generated_at,

    -- Top 20 SHAP Features (aliased to match expected output names)
    round(contested_fg_makes_per_minute::numeric, 3) as contested_fg_makes_per_minute,
    round(usage_pct_roll_3g_stddev::numeric, 3) as usage_pct_roll_3g_stddev,
    round(l10_pts_allowed_to_position::numeric, 3) as opp_l10_pts_allowed_to_position, -- Aliased to match final output
    round(uncont_fg_pct_roll_3g_avg::numeric, 3) as uncont_fg_pct_roll_3g_avg,
    line, -- Direct input, no rounding needed
    round(opp_fg_pct_vs_league_avg::numeric, 3) as opp_fg_pct_vs_league_avg, -- This comes from opd.fg_pct_vs_league_avg
    round(adjusted_usage_with_defense::numeric, 3) as adjusted_usage_with_defense,
    round(pct_of_team_fg3m_roll_5g_avg::numeric, 3) as pct_of_team_fg3m_roll_5g_avg,
    round(touches_roll_10g_stddev::numeric, 3) as touches_roll_10g_stddev,
    round(avg_pts_vs_opponent_prior::numeric, 3) as avg_pts_vs_opponent, -- Aliased to match final output
    round(min_roll_10g_stddev::numeric, 3) as min_roll_10g_stddev,
    round(fg3m_roll_3g_stddev::numeric, 3) as fg3m_roll_3g_stddev,
    round(uncont_fg_pct_roll_10g_stddev::numeric, 3) as uncont_fg_pct_roll_10g_stddev,
    round(def_at_rim_fga_roll_5g_avg::numeric, 3) as def_at_rim_fga_roll_5g_avg,
    round(touches_roll_3g_stddev::numeric, 3) as touches_roll_3g_stddev,
    round(hist_pts_last_1_vs_opp::numeric, 3) as hist_pts_last_1_vs_opp,
    round(opp_fg3m_vs_league_avg::numeric, 3) as opp_fg3m_vs_league_avg -- This comes from opd.fg3m_vs_league_avg

from combined_features
where player_id is not null
    and game_id is not null
    and market_cleaned != 'UNKNOWN'