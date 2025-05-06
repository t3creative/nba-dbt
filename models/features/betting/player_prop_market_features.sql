{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='feature_key',
        tags=['betting', 'features', 'player_props', 'ml'],
        partition_by={
            "field": "feature_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['player_id', 'market_id']
    )
}}

with player_props as (
    select
        player_id,
        player_name,
        market_id,
        market,
        line,
        over_odds,
        under_odds,
        over_implied_prob,
        under_implied_prob,
        no_vig_over_prob,
        no_vig_under_prob,
        vig_percentage,
        sportsbook,
        game_date
    from {{ ref('player_prop_market_analysis') }}
    
    {% if is_incremental() %}
        where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

player_market_stats as (
    select
        player_id,
        player_name,
        market_id,
        market,
        
        -- Historical counts and frequencies
        count(*) as total_props,
        count(distinct game_date) as total_games,
        count(distinct sportsbook) as sportsbooks_count,
        
        -- Line statistics
        avg(line) as avg_line,
        percentile_cont(0.5) within group (order by line) as median_line,
        stddev(line) as stddev_line,
        min(line) as min_line,
        max(line) as max_line,
        
        -- Odds statistics
        avg(over_odds) as avg_over_odds,
        avg(under_odds) as avg_under_odds,
        stddev(over_odds) as stddev_over_odds,
        stddev(under_odds) as stddev_under_odds,
        
        -- Probability statistics
        avg(over_implied_prob) as avg_over_implied_prob,
        avg(under_implied_prob) as avg_under_implied_prob,
        avg(no_vig_over_prob) as avg_no_vig_over_prob,
        avg(no_vig_under_prob) as avg_no_vig_under_prob,
        
        -- Vig statistics
        avg(vig_percentage) as avg_vig_pct,
        stddev(vig_percentage) as stddev_vig_pct,
        
        -- Time metrics
        min(game_date) as first_appearance,
        max(game_date) as last_appearance,
        (max(game_date) - min(game_date))::integer as days_in_market,
        
        -- Most recent values
        array_agg(line order by game_date desc)[1:3] as recent_lines,
        array_agg(sportsbook order by game_date desc)[1:3] as recent_books,
        
        -- Generate feature key
        {{ dbt_utils.generate_surrogate_key(['player_id', 'market_id']) }} as feature_key
        
    from player_props
    group by player_id, player_name, market_id, market
),

-- Get market-wide statistics for comparison
market_stats as (
    select
        market_id,
        market,
        count(distinct player_id) as player_count,
        avg(line) as market_avg_line,
        stddev(line) as market_stddev_line,
        avg(vig_percentage) as market_avg_vig
    from player_props
    group by market_id, market
),

-- Get player overall statistics (across all markets)
player_stats as (
    select
        player_id,
        player_name,
        count(distinct market_id) as markets_count,
        count(*) as total_props_all_markets,
        avg(vig_percentage) as player_avg_vig
    from player_props
    group by player_id, player_name
),

-- Analyze distribution of lines by market type
line_distribution as (
    select
        market_id,
        percentile_cont(0.1) within group (order by line) as p10_line,
        percentile_cont(0.25) within group (order by line) as p25_line,
        percentile_cont(0.75) within group (order by line) as p75_line,
        percentile_cont(0.9) within group (order by line) as p90_line
    from player_props
    group by market_id
),

final as (
    select
        pms.feature_key,
        pms.player_id,
        pms.player_name,
        pms.market_id,
        pms.market,
        
        -- Frequency metrics
        pms.total_props,
        pms.total_games,
        pms.sportsbooks_count,
        
        -- Line statistics
        pms.avg_line,
        pms.median_line,
        pms.stddev_line,
        pms.min_line,
        pms.max_line,
        
        -- Line distribution  
        ld.p10_line as market_p10_line,
        ld.p25_line as market_p25_line,
        ld.p75_line as market_p75_line,
        ld.p90_line as market_p90_line,
        
        -- Line position in distribution
        case 
            when pms.avg_line <= ld.p10_line then 'Very Low'
            when pms.avg_line <= ld.p25_line then 'Low'
            when pms.avg_line >= ld.p90_line then 'Very High'
            when pms.avg_line >= ld.p75_line then 'High'
            else 'Average'
        end as line_position,
        
        -- Odds statistics
        pms.avg_over_odds,
        pms.avg_under_odds,
        pms.stddev_over_odds,
        pms.stddev_under_odds,
        
        -- Probability statistics
        pms.avg_over_implied_prob,
        pms.avg_under_implied_prob,
        pms.avg_no_vig_over_prob,
        pms.avg_no_vig_under_prob,
        
        -- Vig statistics
        pms.avg_vig_pct,
        pms.stddev_vig_pct,
        
        -- Market comparison
        pms.avg_line - ms.market_avg_line as line_vs_market_avg,
        pms.avg_vig_pct - ms.market_avg_vig as vig_vs_market_avg,
        pms.avg_vig_pct - ps.player_avg_vig as vig_vs_player_avg,
        pms.stddev_line / nullif(ms.market_stddev_line, 0) as rel_line_volatility,
        
        -- Time metrics
        pms.first_appearance,
        pms.last_appearance,
        pms.days_in_market,
        current_date - pms.last_appearance as days_since_last,
        
        -- Player context
        ps.markets_count as player_total_markets,
        ps.total_props_all_markets as player_total_props,
        (pms.total_props::float / nullif(ps.total_props_all_markets, 0)) * 100 as market_prop_pct,
        
        -- Market context
        ms.player_count as market_player_count,
        
        -- Recent values (as string to store in table)
        pms.recent_lines[1] as most_recent_line,
        pms.recent_lines[2] as second_recent_line,
        pms.recent_lines[3] as third_recent_line,
        pms.recent_books[1] as most_recent_book,
        
        -- Feature metadata
        current_date as feature_date
    from player_market_stats pms
    join market_stats ms on pms.market_id = ms.market_id
    join player_stats ps on pms.player_id = ps.player_id
    join line_distribution ld on pms.market_id = ld.market_id
)

select * from final 