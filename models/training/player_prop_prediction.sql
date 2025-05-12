{{
    config(
        schema='training',
        materialized='table',
        tags=['betting', 'player_props', 'ml', 'training'],
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['player_id', 'market_id']
    )
}}

with player_props as (
    select
        pp.player_id,
        pp.player_name,
        pp.team_id,
        t.team_tricode as team_abbr,
        pp.market_id,
        pp.market,
        pp.line,
        pp.over_odds,
        pp.under_odds,
        pp.over_implied_prob,
        pp.under_implied_prob,
        pp.no_vig_over_prob,
        pp.no_vig_under_prob,
        pp.vig_percentage,
        pp.sportsbook,
        pp.game_date,
        pp.game_id,
        -- Generate a unique identifier for each prop
        {{ dbt_utils.generate_surrogate_key(['pp.player_id', 'pp.market_id', 'pp.game_id']) }} as prop_id
    from {{ ref('int__player_props_normalized') }} pp
    left join {{ ref('stg__teams') }} t on pp.team_id = t.team_id
    where pp.sportsbook = 'Consensus' -- Use consensus lines for training
    and pp.game_date >= current_date - interval '730 days' -- Two years of historical data
),

-- Get player game stats for actual outcomes using boxscores
player_outcomes as (
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS' as stat_name,
        pbs.pts as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'REB' as stat_name,
        pbs.reb as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'AST' as stat_name,
        pbs.ast as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'BLK' as stat_name,
        pbs.blk as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'STL' as stat_name,
        pbs.stl as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'FG3M' as stat_name,
        pbs.fg3m as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS_REB' as stat_name,
        pbs.pts + pbs.reb as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS_AST' as stat_name,
        pbs.pts + pbs.ast as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'REB_AST' as stat_name,
        pbs.reb + pbs.ast as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
    
    union all
    
    select
        pbs.player_id,
        pbs.game_id,
        pbs.game_date,
        'PTS_REB_AST' as stat_name,
        pbs.pts + pbs.reb + pbs.ast as stat_value,
        pbs.min as minutes
    from {{ ref('int__combined_player_boxscore') }} pbs
    where pbs.game_date >= current_date - interval '730 days'
),

-- Map market types to corresponding stat names
market_stat_mapping as (
    select 
        market_id,
        market,
        case 
            when market = 'Points O/U' then 'PTS'
            when market = 'Rebounds O/U' then 'REB'
            when market = 'Assists O/U' then 'AST'
            when market = 'Blocks O/U' then 'BLK'
            when market = 'Steals O/U' then 'STL'
            when market = 'Threes O/U' then 'FG3M'
            when market = 'Pts+Reb O/U' then 'PTS_REB'
            when market = 'Pts+Ast O/U' then 'PTS_AST'
            when market = 'Reb+Ast O/U' then 'REB_AST'
            when market = 'Pts+Reb+Ast O/U' then 'PTS_REB_AST'
            else null
        end as stat_name
    from (select distinct market_id, market from player_props) m
),

-- Get game context information
game_context as (
    select
        s.game_id,
        s.game_date,
        s.team_id,
        s.is_home,
        s.opponent_id,
        opp.team_tricode as opponent_abbr,
        s.matchup
    from {{ ref('int__game_context') }} s
    left join {{ ref('stg__teams') }} opp on s.opponent_id = opp.team_id
    where s.game_date >= current_date - interval '730 days'
),

-- Get player form using rolling stats
player_form as (
    select 
        rs.player_id,
        rs.game_id,
        rs.game_date,
        case 
            when msm.market = 'Points O/U' then 'PTS'
            when msm.market = 'Rebounds O/U' then 'REB'
            when msm.market = 'Assists O/U' then 'AST'
            when msm.market = 'Blocks O/U' then 'BLK'
            when msm.market = 'Steals O/U' then 'STL'
            when msm.market = 'Threes O/U' then 'FG3M'
            when msm.market = 'Pts+Reb O/U' then 'PTS_REB'
            when msm.market = 'Pts+Ast O/U' then 'PTS_AST'
            when msm.market = 'Reb+Ast O/U' then 'REB_AST'
            when msm.market = 'Pts+Reb+Ast O/U' then 'PTS_REB_AST'
            else null
        end as stat_name,
        case
            when msm.market = 'Points O/U' then rs.pts_roll_5g_avg
            when msm.market = 'Rebounds O/U' then rs.reb_roll_5g_avg
            when msm.market = 'Assists O/U' then rs.ast_roll_5g_avg
            when msm.market = 'Blocks O/U' then rs.blk_roll_5g_avg
            when msm.market = 'Steals O/U' then rs.stl_roll_5g_avg
            when msm.market = 'Threes O/U' then rs.fg3m_roll_5g_avg
            when msm.market = 'Pts+Reb O/U' then rs.pts_roll_5g_avg + rs.reb_roll_5g_avg
            when msm.market = 'Pts+Ast O/U' then rs.pts_roll_5g_avg + rs.ast_roll_5g_avg
            when msm.market = 'Reb+Ast O/U' then rs.reb_roll_5g_avg + rs.ast_roll_5g_avg
            when msm.market = 'Pts+Reb+Ast O/U' then rs.pts_roll_5g_avg + rs.reb_roll_5g_avg + rs.ast_roll_5g_avg
            else null
        end as last_5_avg,
        case
            when msm.market = 'Points O/U' then rs.pts_roll_10g_avg
            when msm.market = 'Rebounds O/U' then rs.reb_roll_10g_avg
            when msm.market = 'Assists O/U' then rs.ast_roll_10g_avg
            when msm.market = 'Blocks O/U' then rs.blk_roll_10g_avg
            when msm.market = 'Steals O/U' then rs.stl_roll_10g_avg
            when msm.market = 'Threes O/U' then rs.fg3m_roll_10g_avg
            when msm.market = 'Pts+Reb O/U' then rs.pts_roll_10g_avg + rs.reb_roll_10g_avg
            when msm.market = 'Pts+Ast O/U' then rs.pts_roll_10g_avg + rs.ast_roll_10g_avg
            when msm.market = 'Reb+Ast O/U' then rs.reb_roll_10g_avg + rs.ast_roll_10g_avg
            when msm.market = 'Pts+Reb+Ast O/U' then rs.pts_roll_10g_avg + rs.reb_roll_10g_avg + rs.ast_roll_10g_avg
            else null
        end as last_10_avg,
        case
            when msm.market = 'Points O/U' then rs.pts_roll_20g_avg
            when msm.market = 'Rebounds O/U' then rs.reb_roll_20g_avg
            when msm.market = 'Assists O/U' then rs.ast_roll_20g_avg
            when msm.market = 'Blocks O/U' then rs.blk_roll_20g_avg
            when msm.market = 'Steals O/U' then rs.stl_roll_20g_avg
            when msm.market = 'Threes O/U' then rs.fg3m_roll_20g_avg
            when msm.market = 'Pts+Reb O/U' then rs.pts_roll_20g_avg + rs.reb_roll_20g_avg
            when msm.market = 'Pts+Ast O/U' then rs.pts_roll_20g_avg + rs.ast_roll_20g_avg
            when msm.market = 'Reb+Ast O/U' then rs.reb_roll_20g_avg + rs.ast_roll_20g_avg
            when msm.market = 'Pts+Reb+Ast O/U' then rs.pts_roll_20g_avg + rs.reb_roll_20g_avg + rs.ast_roll_20g_avg
            else null
        end as last_20_avg,
        -- Use last 20 game average as proxy for season average
        case
            when msm.market = 'Points O/U' then rs.pts_roll_20g_avg
            when msm.market = 'Rebounds O/U' then rs.reb_roll_20g_avg
            when msm.market = 'Assists O/U' then rs.ast_roll_20g_avg
            when msm.market = 'Blocks O/U' then rs.blk_roll_20g_avg
            when msm.market = 'Steals O/U' then rs.stl_roll_20g_avg
            when msm.market = 'Threes O/U' then rs.fg3m_roll_20g_avg
            when msm.market = 'Pts+Reb O/U' then rs.pts_roll_20g_avg + rs.reb_roll_20g_avg
            when msm.market = 'Pts+Ast O/U' then rs.pts_roll_20g_avg + rs.ast_roll_20g_avg
            when msm.market = 'Reb+Ast O/U' then rs.reb_roll_20g_avg + rs.ast_roll_20g_avg
            when msm.market = 'Pts+Reb+Ast O/U' then rs.pts_roll_20g_avg + rs.reb_roll_20g_avg + rs.ast_roll_20g_avg
            else null
        end as season_avg,
        rs.min_roll_10g_avg as avg_minutes,
        -- Not available in source data, using null as placeholders
        null as home_avg,
        null as away_avg,
        null as last_vs_team_avg,
        null as injury_status
    from {{ ref('player_rolling_stats') }} rs
    cross join (select distinct market_id, market from player_props) msm
    where rs.game_date >= current_date - interval '730 days'
),

-- Join player props with outcomes and game context
joined_data as (
    select
        pp.prop_id,
        pp.player_id,
        pp.player_name,
        pp.team_id,
        pp.team_abbr,
        pp.market_id,
        pp.market,
        msm.stat_name,
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
        gc.opponent_id,
        gc.opponent_abbr,
        gc.matchup,
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
        end as fair_under_prob
        
    from player_props pp
    join market_stat_mapping msm on pp.market_id = msm.market_id
    left join game_context gc on pp.game_id = gc.game_id and pp.team_id = gc.team_id
    left join player_outcomes po 
        on pp.player_id = po.player_id 
        and pp.game_id = po.game_id
        and msm.stat_name = po.stat_name
),

-- Get features from player_prop_market_features
market_features as (
    select
        player_id,
        market_id,
        avg_line,
        stddev_line,
        line_position,
        line_vs_market_avg,
        total_props,
        total_games
    from {{ ref('player_prop_market_features') }}
),

-- Get features from player_prop_market_time_series_features
time_series_features as (
    select
        player_id,
        market_id,
        avg_abs_line_change,
        stddev_line_change,
        pct_line_increases,
        pct_line_decreases,
        line_volatility_class,
        line_direction_bias,
        line_predictability_score
    from {{ ref('player_prop_market_time_series_features') }}
),

-- Get features from player_prop_market_sportsbook_features
sportsbook_features as (
    select
        player_id,
        market_id,
        sportsbook,
        avg_line_vs_consensus,
        pct_higher_than_consensus,
        pct_lower_than_consensus,
        potential_edge
    from {{ ref('player_prop_market_sportsbook_features') }}
    where sportsbook = 'Consensus' -- Just use consensus lines for training
),

-- Simulate team defense features - not available in current models
team_defense as (
    select
        td.opponent_id as team_id,
        td.stat_name as stat_allowed,
        avg(td.stat_value) as opponent_avg_allowed,
        100.0 as defensive_rating, -- Placeholder
        100.0 as pace_factor, -- Placeholder
        50 as defensive_efficiency_rank -- Placeholder
    from (
        select 
            po.stat_value,
            po.stat_name,
            gc.team_id,
            gc.opponent_id,
            gc.game_date
        from player_outcomes po
        join game_context gc 
            on po.game_id = gc.game_id
        where gc.game_date >= current_date - interval '730 days'
    ) td
    group by td.opponent_id, td.stat_name
),

-- Combine all features
combined_features as (
    select
        jd.prop_id,
        jd.player_id,
        jd.player_name,
        jd.team_id,
        jd.team_abbr,
        jd.market_id,
        jd.market,
        jd.stat_name,
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
        jd.opponent_abbr,
        jd.matchup,
        jd.actual_stat_value,
        jd.minutes,
        jd.outcome,
        
        -- Betting market features
        mf.avg_line as historical_avg_line,
        mf.stddev_line as historical_stddev_line,
        mf.line_position as market_position,
        mf.line_vs_market_avg as line_vs_market_avg,
        mf.total_props as historical_prop_count,
        mf.total_games as historical_game_count,
        
        -- Time series features
        tsf.avg_abs_line_change,
        tsf.stddev_line_change,
        tsf.pct_line_increases,
        tsf.pct_line_decreases,
        tsf.line_volatility_class,
        tsf.line_direction_bias,
        tsf.line_predictability_score,
        
        -- Sportsbook features
        sf.avg_line_vs_consensus,
        sf.pct_higher_than_consensus,
        sf.pct_lower_than_consensus,
        sf.potential_edge,
        
        -- Player form features
        pf.last_5_avg,
        pf.last_10_avg,
        pf.last_20_avg,
        pf.season_avg,
        pf.home_avg,
        pf.away_avg,
        pf.avg_minutes,
        pf.injury_status,
        
        -- Team defense features
        td.opponent_avg_allowed,
        td.defensive_rating,
        td.pace_factor,
        
        -- Custom feature calculations
        
        -- Line vs performance features
        jd.line - coalesce(pf.last_5_avg, pf.season_avg) as line_vs_last_5_avg,
        jd.line - coalesce(pf.last_10_avg, pf.season_avg) as line_vs_last_10_avg,
        jd.line - pf.season_avg as line_vs_season_avg,
        
        -- Home/away adjustment (simplified since we don't have home/away splits)
        case 
            when jd.is_home then jd.line - coalesce(pf.season_avg, 0) * 0.05 -- Assume 5% home advantage
            else jd.line + coalesce(pf.season_avg, 0) * 0.05 -- Assume 5% road disadvantage
        end as home_away_adjusted_line,
        
        -- Opponent adjustment
        jd.line - td.opponent_avg_allowed as line_vs_opponent_avg,
        
        -- Z-score (how many standard deviations from historical average)
        case
            when mf.stddev_line > 0 then (jd.line - mf.avg_line) / mf.stddev_line
            else 0
        end as line_z_score,
        
        -- Minutes adjustment
        case
            when pf.avg_minutes > 0 then jd.line / (pf.avg_minutes / 36.0)
            else jd.line
        end as per_36_adjusted_line,
        
        -- Value rating
        case
            when mf.stddev_line > 0 then
                case
                    when jd.line < mf.avg_line then (mf.avg_line - jd.line) / mf.stddev_line
                    else (jd.line - mf.avg_line) / mf.stddev_line
                end
            else 0
        end as value_rating,
        
        -- Consistency score (lower stddev = higher consistency)
        case
            when mf.avg_line > 0 then 1 - (coalesce(mf.stddev_line, 0) / mf.avg_line)
            else 0
        end as consistency_score,
        
        -- Edge calculation
        case
            when jd.outcome = 'OVER' then 1 - jd.fair_over_prob
            when jd.outcome = 'UNDER' then 1 - jd.fair_under_prob
            else 0
        end as edge
        
    from joined_data jd
    left join market_features mf 
        on jd.player_id = mf.player_id 
        and jd.market_id = mf.market_id
    left join time_series_features tsf 
        on jd.player_id = tsf.player_id 
        and jd.market_id = tsf.market_id
    left join sportsbook_features sf 
        on jd.player_id = sf.player_id 
        and jd.market_id = sf.market_id
    left join player_form pf 
        on jd.player_id = pf.player_id 
        and jd.stat_name = pf.stat_name 
        and jd.game_id = pf.game_id
    left join team_defense td 
        on jd.opponent_id = td.team_id 
        and jd.stat_name = td.stat_allowed
),

final as (
    select
        *,
        -- Label for training (binary classification)
        case
            when outcome = 'OVER' then 1
            when outcome = 'UNDER' then 0
            else null -- For PUSH or missing outcomes
        end as over_label,
        
        -- Label for regression (actual difference from line)
        coalesce(actual_stat_value, 0) - line as line_difference,
        
        -- Feature metadata
        current_timestamp as generated_at
    from combined_features
)

select * from final 