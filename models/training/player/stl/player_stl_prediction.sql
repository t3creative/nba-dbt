{{
    config(
        schema='training',
        materialized='table',
        unique_key= 'player_prop_key',
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
        
        -- Convert market names to standardized formats for easier mapping
        case 
            when pp.market = 'Steals O/U' then 'STL'
            else 'UNKNOWN'
        end as market_cleaned,
        
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
    where pp.player_id = '{{ var("player_id_filter", "2544") }}' -- "LeBron James" is a default
    and pp.game_date >= '{{ var("start_date_filter", "2017-10-01") }}'
    and pp.market = 'Steals O/U' -- Filter for STL market only
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
    where pos.player_id = '{{ var("player_id_filter", "2544") }}' -- "LeBron James" is a default
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
        'STL' as stat_name,
        pbs.stl as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= '2017-10-01'
),

-- Get game context information
game_context as (
    select
        go.game_id,
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
        pft.min_roll_5g_avg,
        pft.min_roll_10g_avg,
        pft.min_roll_20g_avg,
        pft.min_roll_5g_stddev,
        pft.min_roll_10g_stddev,
        pft.min_roll_20g_stddev
    from {{ ref('feat_player__traditional_rolling') }} pft
    where pft.game_date >= '2017-10-01'
),

player_form_advanced as (
    select
        pfa.player_id,
        pfa.game_id,
        pfa.game_date,
        pfa.usage_pct_roll_5g_avg,
        pfa.usage_pct_roll_10g_avg,
        pfa.usage_pct_roll_20g_avg,
        pfa.usage_pct_roll_5g_stddev,
        pfa.usage_pct_roll_10g_stddev,
        pfa.usage_pct_roll_20g_stddev,
        pfa.ts_pct_roll_5g_avg,
        pfa.ts_pct_roll_10g_avg,
        pfa.ts_pct_roll_20g_avg,
        pfa.ts_pct_roll_5g_stddev,
        pfa.ts_pct_roll_10g_stddev,
        pfa.ts_pct_roll_20g_stddev
    from {{ ref('feat_player__advanced_rolling') }} pfa
    where pfa.game_date >= '2017-10-01'
),

player_form_usage as (
    select
        pfu.player_id,
        pfu.game_id,
        pfu.game_date
    from {{ ref('feat_player__usage_rolling') }} pfu
    where pfu.game_date >= '2017-10-01'
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
        gc.nba_season,
        po.stat_value as actual_stat_value,
        po.minutes,

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

-- Preserve existing combined_features CTE structure
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
        jd.actual_stat_value,
        jd.minutes,
        jd.outcome,
        jd.fair_over_prob,
        jd.fair_under_prob,
        jd.beat_line_flag,
        jd.nba_season,
        jd.position,

        -- Player form features (pft, pfa)
        pft.min_roll_5g_avg,
        pft.min_roll_10g_avg,
        pft.min_roll_20g_avg,
        pft.min_roll_5g_stddev,
        pft.min_roll_10g_stddev,
        pft.min_roll_20g_stddev,
        pfa.usage_pct_roll_5g_avg,
        pfa.usage_pct_roll_10g_avg,
        pfa.usage_pct_roll_20g_avg,
        pfa.usage_pct_roll_5g_stddev,
        pfa.usage_pct_roll_10g_stddev,
        pfa.usage_pct_roll_20g_stddev,
        pfa.ts_pct_roll_5g_avg,
        pfa.ts_pct_roll_10g_avg,
        pfa.ts_pct_roll_20g_avg,
        pfa.ts_pct_roll_5g_stddev,
        pfa.ts_pct_roll_10g_stddev,
        pfa.ts_pct_roll_20g_stddev,

        -- Opponent pregame profile features (from opponent_pregame_profile_features_v1)
        opp.opp_adjusted_def_rating as opponent_adjusted_def_rating,
        opp.opp_adjusted_pace as opponent_adjusted_pace,
        opp.opp_allowed_pts_in_paint as opponent_allowed_pts_in_paint,
        opp.opp_def_reb_pct as opponent_def_reb_pct,
        opp.opp_fg_pct as opponent_fg_pct,
        opp.opp_fg3_pct as opponent_fg3_pct,

        -- Opponent position defense features (from opponent_position_defense_features_v1)
        opd.avg_stl_allowed_to_position,

        -- Numerical matchup quality indicators (_vs_league_avg)
        opd.stl_vs_league_avg,

        current_timestamp as generated_at

    from joined_data jd
    left join {{ ref('feat_player__traditional_rolling') }} pft
        on jd.player_id = pft.player_id
        and jd.game_id = pft.game_id
    left join {{ ref('feat_player__advanced_rolling') }} pfa
        on jd.player_id = pfa.player_id
        and jd.game_id = pfa.game_id
    left join {{ ref('feat_player__usage_rolling') }} pfu
        on jd.player_id = pfu.player_id
        and jd.game_id = pfu.game_id
    left join {{ ref('opponent_pregame_profile_features_v1') }} opp
        on jd.opponent_id = opp.opponent_id
        and jd.game_id = opp.game_id
    left join {{ ref('opponent_position_defense_features_v1') }} opd
        on jd.opponent_id = opd.opponent_id
        and jd.game_date::date = opd.game_date
        and jd.position = opd.position
        and jd.nba_season = opd.season_year
)

select
    nba_season,
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
    
    -- Traditional rolling features (MIN only)
    round(min_roll_5g_avg::numeric, 3) as min_roll_5g_avg,
    round(min_roll_10g_avg::numeric, 3) as min_roll_10g_avg,
    round(min_roll_20g_avg::numeric, 3) as min_roll_20g_avg,
    round(min_roll_5g_stddev::numeric, 3) as min_roll_5g_stddev,
    round(min_roll_10g_stddev::numeric, 3) as min_roll_10g_stddev,
    round(min_roll_20g_stddev::numeric, 3) as min_roll_20g_stddev,
    
    -- Advanced metrics (unchanged)
    round(usage_pct_roll_5g_avg::numeric, 3) as usage_pct_roll_5g_avg,
    round(usage_pct_roll_10g_avg::numeric, 3) as usage_pct_roll_10g_avg,
    round(usage_pct_roll_20g_avg::numeric, 3) as usage_pct_roll_20g_avg,
    round(usage_pct_roll_5g_stddev::numeric, 3) as usage_pct_roll_5g_stddev,
    round(usage_pct_roll_10g_stddev::numeric, 3) as usage_pct_roll_10g_stddev,
    round(usage_pct_roll_20g_stddev::numeric, 3) as usage_pct_roll_20g_stddev,
    round(ts_pct_roll_5g_avg::numeric, 3) as ts_pct_roll_5g_avg,
    round(ts_pct_roll_10g_avg::numeric, 3) as ts_pct_roll_10g_avg,
    round(ts_pct_roll_20g_avg::numeric, 3) as ts_pct_roll_20g_avg,
    round(ts_pct_roll_5g_stddev::numeric, 3) as ts_pct_roll_5g_stddev,
    round(ts_pct_roll_10g_stddev::numeric, 3) as ts_pct_roll_10g_stddev,
    round(ts_pct_roll_20g_stddev::numeric, 3) as ts_pct_roll_20g_stddev,
    
    -- Opponent pregame profile features (unchanged)
    round(opponent_adjusted_def_rating::numeric, 3) as opponent_adjusted_def_rating,
    round(opponent_adjusted_pace::numeric, 3) as opponent_adjusted_pace,
    round(opponent_allowed_pts_in_paint::numeric, 3) as opponent_allowed_pts_in_paint,
    round(opponent_def_reb_pct::numeric, 3) as opponent_def_reb_pct,
    round(opponent_fg_pct::numeric, 3) as opponent_fg_pct,
    round(opponent_fg3_pct::numeric, 3) as opponent_fg3_pct,

    -- UPDATED: Opponent position defense features (STL only)
    round(avg_stl_allowed_to_position::numeric, 3) as opp_avg_stl_allowed_to_position,

    -- Numerical matchup quality indicators (_vs_league_avg) (STL only)
    round(stl_vs_league_avg::numeric, 3) as opp_stl_vs_league_avg,

    -- Locally generated Categorical matchup labels (STL only)
    CASE
        WHEN COALESCE(stl_vs_league_avg, 0.0) > 0.5 THEN 'Great'
        WHEN COALESCE(stl_vs_league_avg, 0.0) > 0.2 THEN 'Good'
        WHEN COALESCE(stl_vs_league_avg, 0.0) >= -0.2 THEN 'Average'
        WHEN COALESCE(stl_vs_league_avg, 0.0) >= -0.5 THEN 'Poor'
        ELSE 'Bad'
    END AS opp_stl_matchup_label,

    -- Identifiers and metadata
    player_id,
    team_id,
    game_id,
    opponent_id,
    player_prop_key,
    actual_stat_value,
    outcome,
    beat_line_flag,
    generated_at
from combined_features
where player_id is not null
and game_id is not null
and market_cleaned != 'UNKNOWN'

