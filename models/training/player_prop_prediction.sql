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
            when pp.market = 'Points O/U' then 'PTS'
            when pp.market = 'Rebounds O/U' then 'REB'
            when pp.market = 'Assists O/U' then 'AST'
            when pp.market = 'Blocks O/U' then 'BLK'
            when pp.market = 'Steals O/U' then 'STL'
            when pp.market = 'Threes O/U' then 'FG3M'
            when pp.market = 'Pts+Reb O/U' then 'PTS_REB'
            when pp.market = 'Pts+Ast O/U' then 'PTS_AST'
            when pp.market = 'Reb+Ast O/U' then 'REB_AST'
            when pp.market = 'Pts+Reb+Ast O/U' then 'PTS_REB_AST'
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
    from {{ ref('int_betting__player_props_probabilities') }} pp
    where pp.game_date >= current_date - interval '730 days' -- Two years of historical data
),

-- Get player-team relationships
player_teams as (
    select
        ptb.player_id,
        ptb.team_id,
        ptb.is_current
    from {{ ref('player_team_bridge') }} ptb
    where ptb.is_current = true
),

-- Link player props to games through player team affiliations
player_props_with_games as (
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
        
        -- Extract game_id from player props if possible
        coalesce(
            gc.game_id,
            go.game_id
        ) as game_id,
        
        -- Get team_id from player_team_bridge
        pt.team_id as team_id
    from player_props pp
    -- Join to get team_id from player_team_bridge using is_current flag
    left join player_teams pt
        on pp.player_id = pt.player_id
        and pt.is_current = true
    -- Join to get game information
    left join {{ ref('int__game_context') }} gc 
        on pp.game_date = gc.game_date 
        and (gc.home_team_id = pt.team_id or gc.away_team_id = pt.team_id)
    left join {{ ref('int_opp__game_opponents') }} go
        on pp.game_date = go.game_date
        and pt.team_id = go.team_id
),

-- Get player game stats for actual outcomes using boxscores
player_outcomes as (
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS' as stat_name,
        pbs.pts as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'REB' as stat_name,
        pbs.reb as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'AST' as stat_name,
        pbs.ast as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'BLK' as stat_name,
        pbs.blk as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'STL' as stat_name,
        pbs.stl as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'FG3M' as stat_name,
        pbs.fg3m as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS_REB' as stat_name,
        pbs.pts + pbs.reb as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS_AST' as stat_name,
        pbs.pts + pbs.ast as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'REB_AST' as stat_name,
        pbs.reb + pbs.ast as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS_REB_AST' as stat_name,
        pbs.pts + pbs.reb + pbs.ast as stat_value,
        pbs.min as minutes,
        pbs.team_id
    from {{ ref('int_player__combined_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
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
        end as win_pct_diff_last_10
    from {{ ref('int_opp__game_opponents') }} go
    left join {{ ref('int__game_context') }} gc on go.game_id = gc.game_id
    left join {{ ref('stg__teams') }} t on go.team_id = t.team_id
    left join {{ ref('stg__teams') }} opp on go.opponent_id = opp.team_id
    where go.game_date >= current_date - interval '730 days'
),

-- Get player form using rolling stats
player_form as (
    select 
        rs.player_id,
        rs.game_id,
        rs.game_date,
        rs.pts_roll_5g_avg,
        rs.pts_roll_10g_avg,
        rs.pts_roll_20g_avg,
        rs.pts_roll_5g_stddev,
        rs.pts_roll_10g_stddev,
        rs.pts_roll_20g_stddev,
        rs.reb_roll_5g_avg,
        rs.reb_roll_10g_avg,
        rs.reb_roll_20g_avg,
        rs.reb_roll_5g_stddev,
        rs.reb_roll_10g_stddev,
        rs.reb_roll_20g_stddev,
        rs.ast_roll_5g_avg,
        rs.ast_roll_10g_avg,
        rs.ast_roll_20g_avg,
        rs.ast_roll_5g_stddev,
        rs.ast_roll_10g_stddev,
        rs.ast_roll_20g_stddev,
        rs.blk_roll_5g_avg,
        rs.blk_roll_10g_avg,
        rs.blk_roll_20g_avg,
        rs.blk_roll_5g_stddev,
        rs.blk_roll_10g_stddev,
        rs.blk_roll_20g_stddev,
        rs.stl_roll_5g_avg,
        rs.stl_roll_10g_avg,
        rs.stl_roll_20g_avg,
        rs.stl_roll_5g_stddev,
        rs.stl_roll_10g_stddev,
        rs.stl_roll_20g_stddev,
        rs.fg3m_roll_5g_avg,
        rs.fg3m_roll_10g_avg,
        rs.fg3m_roll_20g_avg,
        rs.fg3m_roll_5g_stddev,
        rs.fg3m_roll_10g_stddev,
        rs.fg3m_roll_20g_stddev,
        rs.min_roll_5g_avg,
        rs.min_roll_10g_avg,
        rs.min_roll_20g_avg,
        rs.usage_pct_roll_10g_avg,
        rs.ts_pct_roll_10g_avg,
        rs.pct_of_team_pts_roll_10g_avg,
        rs.pct_of_team_reb_roll_10g_avg,
        rs.pct_of_team_ast_roll_10g_avg
    from {{ ref('feat_player__rolling_stats') }} rs
    where rs.game_date >= current_date - interval '730 days'
),

-- Standardized team defense stats 
team_defense as (
    select
        td.team_id,
        td.season_year,
        td.avg_def_rating,
        td.avg_opp_eff_fg_pct,
        td.avg_opp_pts_in_paint,
        td.avg_deflections_per_game,
        td.avg_cont_2pt_per_game,
        td.avg_cont_3pt_per_game,
        td.def_rating_rank
    from {{ ref('feat_team__season_defensive_summary') }} td
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
        end as beat_line_flag
        
    from player_props_with_games pp
    left join game_context gc 
        on pp.game_id = gc.game_id 
        and pp.team_id = gc.team_id  -- Join on both game_id and team_id
    left join player_outcomes po 
        on pp.player_id = po.player_id 
        and pp.game_id = po.game_id
        and pp.market_cleaned = po.stat_name
),

-- Calculate advanced features based on rolling stats and line
stat_features as (
    select
        jd.player_prop_key,
        jd.player_id,
        jd.market_cleaned,
        jd.game_id,
        jd.game_date,
        
        -- Extract the appropriate averages based on market
        case 
            when jd.market_cleaned = 'PTS' then pf.pts_roll_5g_avg
            when jd.market_cleaned = 'REB' then pf.reb_roll_5g_avg
            when jd.market_cleaned = 'AST' then pf.ast_roll_5g_avg
            when jd.market_cleaned = 'BLK' then pf.blk_roll_5g_avg
            when jd.market_cleaned = 'STL' then pf.stl_roll_5g_avg
            when jd.market_cleaned = 'FG3M' then pf.fg3m_roll_5g_avg
            when jd.market_cleaned = 'PTS_REB' then pf.pts_roll_5g_avg + pf.reb_roll_5g_avg
            when jd.market_cleaned = 'PTS_AST' then pf.pts_roll_5g_avg + pf.ast_roll_5g_avg
            when jd.market_cleaned = 'REB_AST' then pf.reb_roll_5g_avg + pf.ast_roll_5g_avg
            when jd.market_cleaned = 'PTS_REB_AST' then pf.pts_roll_5g_avg + pf.reb_roll_5g_avg + pf.ast_roll_5g_avg
            else null
        end as last_5_avg,
        
        case 
            when jd.market_cleaned = 'PTS' then pf.pts_roll_10g_avg
            when jd.market_cleaned = 'REB' then pf.reb_roll_10g_avg
            when jd.market_cleaned = 'AST' then pf.ast_roll_10g_avg
            when jd.market_cleaned = 'BLK' then pf.blk_roll_10g_avg
            when jd.market_cleaned = 'STL' then pf.stl_roll_10g_avg
            when jd.market_cleaned = 'FG3M' then pf.fg3m_roll_10g_avg
            when jd.market_cleaned = 'PTS_REB' then pf.pts_roll_10g_avg + pf.reb_roll_10g_avg
            when jd.market_cleaned = 'PTS_AST' then pf.pts_roll_10g_avg + pf.ast_roll_10g_avg
            when jd.market_cleaned = 'REB_AST' then pf.reb_roll_10g_avg + pf.ast_roll_10g_avg
            when jd.market_cleaned = 'PTS_REB_AST' then pf.pts_roll_10g_avg + pf.reb_roll_10g_avg + pf.ast_roll_10g_avg
            else null
        end as last_10_avg,
        
        case 
            when jd.market_cleaned = 'PTS' then pf.pts_roll_20g_avg
            when jd.market_cleaned = 'REB' then pf.reb_roll_20g_avg
            when jd.market_cleaned = 'AST' then pf.ast_roll_20g_avg
            when jd.market_cleaned = 'BLK' then pf.blk_roll_20g_avg
            when jd.market_cleaned = 'STL' then pf.stl_roll_20g_avg
            when jd.market_cleaned = 'FG3M' then pf.fg3m_roll_20g_avg
            when jd.market_cleaned = 'PTS_REB' then pf.pts_roll_20g_avg + pf.reb_roll_20g_avg
            when jd.market_cleaned = 'PTS_AST' then pf.pts_roll_20g_avg + pf.ast_roll_20g_avg
            when jd.market_cleaned = 'REB_AST' then pf.reb_roll_20g_avg + pf.ast_roll_20g_avg
            when jd.market_cleaned = 'PTS_REB_AST' then pf.pts_roll_20g_avg + pf.reb_roll_20g_avg + pf.ast_roll_20g_avg
            else null
        end as last_20_avg,
        
        -- Extract the appropriate standard deviations based on market
        case 
            when jd.market_cleaned = 'PTS' then pf.pts_roll_10g_stddev
            when jd.market_cleaned = 'REB' then pf.reb_roll_10g_stddev
            when jd.market_cleaned = 'AST' then pf.ast_roll_10g_stddev
            when jd.market_cleaned = 'BLK' then pf.blk_roll_10g_stddev
            when jd.market_cleaned = 'STL' then pf.stl_roll_10g_stddev
            when jd.market_cleaned = 'FG3M' then pf.fg3m_roll_10g_stddev
            else null
        end as last_10_stddev,
        
        -- Season average (using 20 game rolling as proxy)
        case 
            when jd.market_cleaned = 'PTS' then pf.pts_roll_20g_avg
            when jd.market_cleaned = 'REB' then pf.reb_roll_20g_avg
            when jd.market_cleaned = 'AST' then pf.ast_roll_20g_avg
            when jd.market_cleaned = 'BLK' then pf.blk_roll_20g_avg
            when jd.market_cleaned = 'STL' then pf.stl_roll_20g_avg
            when jd.market_cleaned = 'FG3M' then pf.fg3m_roll_20g_avg
            when jd.market_cleaned = 'PTS_REB' then pf.pts_roll_20g_avg + pf.reb_roll_20g_avg
            when jd.market_cleaned = 'PTS_AST' then pf.pts_roll_20g_avg + pf.ast_roll_20g_avg
            when jd.market_cleaned = 'REB_AST' then pf.reb_roll_20g_avg + pf.ast_roll_20g_avg
            when jd.market_cleaned = 'PTS_REB_AST' then pf.pts_roll_20g_avg + pf.reb_roll_20g_avg + pf.ast_roll_20g_avg
            else null
        end as season_avg,
        
        -- Minutes and usage stats
        pf.min_roll_10g_avg as avg_minutes,
        pf.usage_pct_roll_10g_avg,
        pf.ts_pct_roll_10g_avg,
        
        -- Player team contribution
        case
            when jd.market_cleaned = 'PTS' then pf.pct_of_team_pts_roll_10g_avg
            when jd.market_cleaned = 'REB' then pf.pct_of_team_reb_roll_10g_avg
            when jd.market_cleaned = 'AST' then pf.pct_of_team_ast_roll_10g_avg
            else null
        end as pct_of_team_stat,
        
        -- Volatility metric (coefficient of variation)
        case 
            when jd.market_cleaned = 'PTS' and pf.pts_roll_10g_avg > 0 then pf.pts_roll_10g_stddev / pf.pts_roll_10g_avg
            when jd.market_cleaned = 'REB' and pf.reb_roll_10g_avg > 0 then pf.reb_roll_10g_stddev / pf.reb_roll_10g_avg
            when jd.market_cleaned = 'AST' and pf.ast_roll_10g_avg > 0 then pf.ast_roll_10g_stddev / pf.ast_roll_10g_avg
            when jd.market_cleaned = 'BLK' and pf.blk_roll_10g_avg > 0 then pf.blk_roll_10g_stddev / pf.blk_roll_10g_avg
            when jd.market_cleaned = 'STL' and pf.stl_roll_10g_avg > 0 then pf.stl_roll_10g_stddev / pf.stl_roll_10g_avg
            when jd.market_cleaned = 'FG3M' and pf.fg3m_roll_10g_avg > 0 then pf.fg3m_roll_10g_stddev / pf.fg3m_roll_10g_avg
            else null
        end as coefficient_of_variation
        
    from joined_data jd
    left join player_form pf 
        on jd.player_id = pf.player_id 
        and jd.game_id = pf.game_id
),

-- Combine all features
combined_features as (
    select
        jd.player_prop_key,
        jd.player_id,
        jd.player_name,
        jd.team_id,
        jd.team_tricode,
        jd.market_id,
        jd.market,
        jd.market_cleaned,
        jd.line,
        jd.over_odds,
        jd.under_odds,
        jd.fair_over_prob,
        jd.fair_under_prob,
        jd.vig_percentage,
        jd.game_date,
        jd.game_id,
        jd.is_home,
        jd.opponent_id,
        jd.opponent_tricode,
        jd.home_away as team_home_away,
        jd.actual_stat_value,
        jd.minutes,
        jd.outcome,
        jd.beat_line_flag,
        
        -- Game context features
        jd.team_rest_days,
        jd.opponent_rest_days,
        jd.is_back_to_back,
        jd.team_win_pct_last_10,
        jd.opponent_win_pct_last_10,
        jd.win_pct_diff_last_10,
        
        -- Player form features
        sf.last_5_avg,
        sf.last_10_avg,
        sf.last_20_avg,
        sf.season_avg,
        sf.last_10_stddev,
        sf.avg_minutes,
        sf.usage_pct_roll_10g_avg,
        sf.ts_pct_roll_10g_avg,
        sf.pct_of_team_stat,
        sf.coefficient_of_variation,
        
        -- Team defense features
        td.avg_def_rating as opponent_def_rating,
        td.avg_opp_eff_fg_pct as opponent_eff_fg_pct,
        td.avg_opp_pts_in_paint as opponent_pts_in_paint_allowed,
        td.avg_deflections_per_game as opponent_deflections_per_game,
        td.avg_cont_2pt_per_game as opponent_contested_2pt_per_game,
        td.avg_cont_3pt_per_game as opponent_contested_3pt_per_game,
        td.def_rating_rank as opponent_def_rank,
        
        -- Custom feature calculations
        -- Line vs performance features
        jd.line - coalesce(sf.last_5_avg, sf.season_avg) as line_vs_last_5_avg,
        jd.line - coalesce(sf.last_10_avg, sf.season_avg) as line_vs_last_10_avg,
        jd.line - sf.season_avg as line_vs_season_avg,
        
        -- Line as a percentage of player average
        case
            when sf.last_10_avg > 0 then jd.line / sf.last_10_avg
            else null
        end as line_pct_of_avg,
        
        -- Home/away adjustment
        case 
            when jd.is_home then jd.line - coalesce(sf.season_avg, 0) * 0.05 -- Assume 5% home advantage
            else jd.line + coalesce(sf.season_avg, 0) * 0.05 -- Assume 5% road disadvantage
        end as home_away_adjusted_line,
        
        -- Z-score (how many standard deviations from historical average)
        case
            when sf.last_10_stddev > 0 then (jd.line - sf.last_10_avg) / sf.last_10_stddev
            else 0
        end as line_z_score,
        
        -- Minutes adjustment
        case
            when sf.avg_minutes > 0 then jd.line / (sf.avg_minutes / 36.0)
            else jd.line
        end as per_36_adjusted_line,
        
        -- Consistency score (lower std deviation = higher consistency)
        case
            when sf.last_10_avg > 0 then 1 - (coalesce(sf.last_10_stddev, 0) / sf.last_10_avg)
            else 0
        end as consistency_score,
        
        -- Actual difference from line (regression target)
        coalesce(jd.actual_stat_value, 0) - jd.line as line_difference,
        
        -- Feature metadata
        current_timestamp as generated_at
        
    from joined_data jd
    left join stat_features sf 
        on jd.player_prop_key = sf.player_prop_key
    left join team_defense td 
        on jd.opponent_id = td.team_id 
        and extract(year from jd.game_date) = td.season_year
)

select * from combined_features
where player_id is not null
  and game_id is not null
  and market_cleaned != 'UNKNOWN' 