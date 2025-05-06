{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='market_analysis_key',
        tags=['betting', 'intermediate', 'player_props', 'analysis'],
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
        player_name_standardized,
        team_id,
        team_abbreviation_standardized,
        market_id,
        market,
        line,
        sportsbook,
        over_odds,
        under_odds,
        over_implied_probability,
        under_implied_probability,
        game_date
    from {{ ref('int__player_props_normalized') }}
    
    {% if is_incremental() %}
        where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Calculate no-vig probabilities and related metrics
enhanced_props as (
    select
        *,
        -- Calculate no-vig probabilities (removing juice)
        over_implied_probability / (over_implied_probability + under_implied_probability) as no_vig_over_prob,
        under_implied_probability / (over_implied_probability + under_implied_probability) as no_vig_under_prob,
        -- Calculate total implied probability (1 + vig)
        over_implied_probability + under_implied_probability as total_implied_prob,
        -- Calculate vig percentage
        (over_implied_probability + under_implied_probability - 1) * 100 as vig_percentage,
        -- Calculate fair odds (no vig)
        case 
            when over_implied_probability / (over_implied_probability + under_implied_probability) < 0.5 
            then ((1 / (over_implied_probability / (over_implied_probability + under_implied_probability))) - 1) * 100
            else -100 / ((1 / (over_implied_probability / (over_implied_probability + under_implied_probability))) - 1)
        end as fair_over_odds,
        case 
            when under_implied_probability / (over_implied_probability + under_implied_probability) < 0.5 
            then ((1 / (under_implied_probability / (over_implied_probability + under_implied_probability))) - 1) * 100
            else -100 / ((1 / (under_implied_probability / (over_implied_probability + under_implied_probability))) - 1)
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
        player_name_standardized,
        market_id,
        market,
        count(*) as player_market_count,
        avg(line) as avg_player_market_line,
        stddev(line) as stddev_player_market_line,
        min(line) as min_player_market_line,
        max(line) as max_player_market_line
    from enhanced_props
    group by player_id, player_name_standardized, market_id, market
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
        ep.player_name_standardized,
        ep.team_id,
        ep.team_abbreviation_standardized,
        ep.market_id,
        ep.market,
        
        -- Odds and lines
        ep.line,
        ep.over_odds,
        ep.under_odds,
        ep.over_implied_probability,
        ep.under_implied_probability,
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
        current_timestamp() as analysis_timestamp
    from enhanced_props ep
    join market_context mc on ep.market_id = mc.market_id
    join player_context pc on ep.player_id = pc.player_id and ep.market_id = pc.market_id
    join sportsbook_context sc on ep.sportsbook = sc.sportsbook
)

select * from final 