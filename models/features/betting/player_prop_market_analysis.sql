{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='market_analysis_key',
        tags=['betting', 'features', 'player_props', 'analysis'],
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['player_id', 'market_id', 'sportsbook']
    )
}}

with source_props as (
    select
        prop_id,
        prop_key,
        player_id,
        player_name,
        team_id,
        team_tricode,
        team_full_name,
        market_id,
        market,
        line,
        sportsbook,
        over_odds,
        under_odds,
        -- Use calculated fields from normalized model
        over_implied_prob,
        under_implied_prob,
        total_implied_prob,
        over_no_vig_prob as no_vig_over_prob,
        under_no_vig_prob as no_vig_under_prob,
        hold_percentage as vig_percentage,
        game_date
    from {{ ref('int__player_props_normalized') }}
    
    {% if is_incremental() %}
        where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Only calculate fair odds (not already in normalized model)
enhanced_props as (
    select
        *,
        -- Calculate fair odds from no-vig probabilities
        case 
            when no_vig_over_prob < 0.5 
            then ((1 / no_vig_over_prob) - 1) * 100
            else -100 / ((1 / no_vig_over_prob) - 1)
        end as fair_over_odds,
        case 
            when no_vig_under_prob < 0.5 
            then ((1 / no_vig_under_prob) - 1) * 100
            else -100 / ((1 / no_vig_under_prob) - 1)
        end as fair_under_odds,
        -- Generate a unique key for this analysis record
        {{ dbt_utils.generate_surrogate_key(['prop_key', 'sportsbook']) }} as market_analysis_key
    from source_props
),

-- Add market context - typical lines per market
market_context as (
    select
        market_id,
        market,
        count(*) as market_count,
        avg(line) as avg_market_line,
        percentile_cont(0.5) within group (order by line) as median_market_line,
        stddev(line) as stddev_market_line,
        min(line) as min_market_line,
        max(line) as max_market_line,
        avg(total_implied_prob) as avg_market_vig,
        avg(vig_percentage) as avg_market_vig_pct
    from enhanced_props
    group by market_id, market
),

-- Add player context - typical lines per player by market
player_context as (
    select
        player_id,
        player_name as player_name,
        market_id,
        market,
        count(*) as player_market_count,
        avg(line) as avg_player_market_line,
        stddev(line) as stddev_player_market_line,
        min(line) as min_player_market_line,
        max(line) as max_player_market_line
    from enhanced_props
    group by player_id, player_name, market_id, market
),

-- Add sportsbook context - typical vig per book
sportsbook_context as (
    select
        sportsbook,
        count(*) as sportsbook_count,
        avg(total_implied_prob) as avg_sportsbook_vig,
        avg(vig_percentage) as avg_sportsbook_vig_pct,
        stddev(vig_percentage) as stddev_sportsbook_vig_pct
    from enhanced_props
    group by sportsbook
),

final as (
    select
        ep.market_analysis_key,
        ep.prop_id,
        ep.prop_key,
        ep.player_id,
        ep.player_name,
        ep.team_id,
        ep.team_tricode,
        ep.team_full_name,
        ep.market_id,
        ep.market,
        
        -- Odds and lines
        ep.line,
        ep.over_odds,
        ep.under_odds,
        ep.over_implied_prob,
        ep.under_implied_prob,
        ep.no_vig_over_prob,
        ep.no_vig_under_prob,
        ep.fair_over_odds,
        ep.fair_under_odds,
        
        -- Vig/juice metrics
        ep.total_implied_prob,
        ep.vig_percentage,
        
        -- Market context
        mc.avg_market_line,
        mc.median_market_line,
        mc.stddev_market_line,
        mc.min_market_line,
        mc.max_market_line,
        mc.avg_market_vig,
        mc.avg_market_vig_pct,
        
        -- Player context
        pc.avg_player_market_line,
        pc.stddev_player_market_line,
        pc.min_player_market_line,
        pc.max_player_market_line,
        
        -- Sportsbook context
        ep.sportsbook,
        sc.avg_sportsbook_vig,
        sc.avg_sportsbook_vig_pct,
        sc.stddev_sportsbook_vig_pct,
        
        -- Comparison metrics
        ep.line - mc.avg_market_line as line_vs_market_avg,
        ep.line - pc.avg_player_market_line as line_vs_player_avg,
        ep.vig_percentage - sc.avg_sportsbook_vig_pct as vig_vs_book_avg,
        ep.vig_percentage - mc.avg_market_vig_pct as vig_vs_market_avg,
        
        -- Time data
        ep.game_date,
        current_timestamp as analysis_timestamp
    from enhanced_props ep
    join market_context mc on ep.market_id = mc.market_id
    join player_context pc on ep.player_id = pc.player_id and ep.market_id = pc.market_id
    join sportsbook_context sc on ep.sportsbook = sc.sportsbook

)

select * from final 