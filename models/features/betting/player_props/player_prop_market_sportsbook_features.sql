{{
    config(
        schema='features',
        materialized='table',
        tags=['betting', 'features', 'player_props', 'sportsbooks', 'ml'],
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
        player_name_standardized,
        market_id,
        market,
        line,
        over_odds,
        under_odds,
        over_implied_probability,
        under_implied_probability,
        no_vig_over_prob,
        no_vig_under_prob,
        vig_percentage,
        sportsbook,
        game_date
    from {{ ref('int__player_props_normalized') }}
    where game_date >= current_date - interval '365 days'
),

-- Get consensus data to compare against
consensus_props as (
    select
        player_id,
        market_id,
        game_date,
        avg(line) as consensus_line,
        avg(over_odds) as consensus_over_odds,
        avg(under_odds) as consensus_under_odds,
        avg(over_implied_probability) as consensus_over_prob,
        avg(under_implied_probability) as consensus_under_prob
    from player_props
    group by player_id, market_id, game_date
),

-- Join player props with consensus to calculate deviations
joined as (
    select
        pp.player_id,
        pp.player_name_standardized,
        pp.market_id,
        pp.market,
        pp.sportsbook,
        pp.game_date,
        pp.line,
        cp.consensus_line,
        pp.line - cp.consensus_line as line_vs_consensus,
        pp.over_odds,
        cp.consensus_over_odds,
        pp.over_odds - cp.consensus_over_odds as over_odds_vs_consensus,
        pp.under_odds,
        cp.consensus_under_odds,
        pp.under_odds - cp.consensus_under_odds as under_odds_vs_consensus,
        pp.over_implied_probability,
        cp.consensus_over_prob,
        pp.over_implied_probability - cp.consensus_over_prob as over_prob_vs_consensus,
        pp.under_implied_probability,
        cp.consensus_under_prob,
        pp.under_implied_probability - cp.consensus_under_prob as under_prob_vs_consensus,
        pp.vig_percentage
    from player_props pp
    join consensus_props cp
        on pp.player_id = cp.player_id
        and pp.market_id = cp.market_id
        and pp.game_date = cp.game_date
),

-- Analyze sportsbook patterns by player-market-sportsbook
sportsbook_analysis as (
    select
        player_id,
        player_name_standardized,
        market_id,
        market,
        sportsbook,
        
        -- Sample size
        count(*) as sample_size,
        count(distinct game_date) as game_count,
        
        -- Line deviation patterns
        avg(line_vs_consensus) as avg_line_vs_consensus,
        stddev(line_vs_consensus) as stddev_line_vs_consensus,
        percentile_cont(0.5) within group (order by line_vs_consensus) as median_line_vs_consensus,
        
        -- Line position tendency
        sum(case when line_vs_consensus > 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_higher_than_consensus,
        sum(case when line_vs_consensus < 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_lower_than_consensus,
        
        -- Odds deviation patterns
        avg(over_odds_vs_consensus) as avg_over_odds_vs_consensus,
        stddev(over_odds_vs_consensus) as stddev_over_odds_vs_consensus,
        avg(under_odds_vs_consensus) as avg_under_odds_vs_consensus,
        stddev(under_odds_vs_consensus) as stddev_under_odds_vs_consensus,
        
        -- Probability deviation patterns
        avg(over_prob_vs_consensus) as avg_over_prob_vs_consensus,
        avg(under_prob_vs_consensus) as avg_under_prob_vs_consensus,
        
        -- Vig analysis
        avg(vig_percentage) as avg_vig_pct,
        stddev(vig_percentage) as stddev_vig_pct,
        
        -- Generate feature key
        {{ dbt_utils.generate_surrogate_key(['player_id', 'market_id', 'sportsbook']) }} as feature_key
        
    from joined
    group by player_id, player_name_standardized, market_id, market, sportsbook
),

-- Get sportsbook general tendencies (across all players/markets)
sportsbook_general as (
    select
        sportsbook,
        avg(line_vs_consensus) as book_avg_line_deviation,
        avg(over_odds_vs_consensus) as book_avg_over_odds_deviation,
        avg(under_odds_vs_consensus) as book_avg_under_odds_deviation,
        avg(vig_percentage) as book_avg_vig
    from joined
    group by sportsbook
),

final as (
    select
        sa.feature_key,
        sa.player_id,
        sa.player_name_standardized,
        sa.market_id,
        sa.market,
        sa.sportsbook,
        
        -- Sample size metrics
        sa.sample_size,
        sa.game_count,
        
        -- Line deviation metrics
        sa.avg_line_vs_consensus,
        sa.stddev_line_vs_consensus,
        sa.median_line_vs_consensus,
        
        -- Line positioning tendency
        sa.pct_higher_than_consensus,
        sa.pct_lower_than_consensus,
        case
            when sa.pct_higher_than_consensus > 0.65 then 'Consistently Higher'
            when sa.pct_lower_than_consensus > 0.65 then 'Consistently Lower'
            else 'Mixed'
        end as line_positioning_pattern,
        
        -- Odds deviation metrics
        sa.avg_over_odds_vs_consensus,
        sa.stddev_over_odds_vs_consensus,
        sa.avg_under_odds_vs_consensus,
        sa.stddev_under_odds_vs_consensus,
        
        -- Probability deviation metrics
        sa.avg_over_prob_vs_consensus,
        sa.avg_under_prob_vs_consensus,
        
        -- Vig metrics
        sa.avg_vig_pct,
        sa.stddev_vig_pct,
        
        -- Compare to sportsbook general tendencies
        sa.avg_line_vs_consensus - sg.book_avg_line_deviation as specific_line_bias,
        sa.avg_over_odds_vs_consensus - sg.book_avg_over_odds_deviation as specific_over_odds_bias,
        sa.avg_under_odds_vs_consensus - sg.book_avg_under_odds_deviation as specific_under_odds_bias,
        sa.avg_vig_pct - sg.book_avg_vig as specific_vig_bias,
        
        -- Market edge detection
        case
            when sa.avg_line_vs_consensus < -0.5 and sa.pct_lower_than_consensus > 0.6 then 'Potential Under Value'
            when sa.avg_line_vs_consensus > 0.5 and sa.pct_higher_than_consensus > 0.6 then 'Potential Over Value'
            else 'Neutral'
        end as potential_edge,
        
        -- Feature metadata
        current_date as feature_date
    from sportsbook_analysis sa
    join sportsbook_general sg on sa.sportsbook = sg.sportsbook
    where sa.sample_size >= 5 -- Minimum sample for reliability
)

select * from final 