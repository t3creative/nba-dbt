-- depends_on: {{ ref('stg__player_props') }}
{{
    config(
        schema='features',
        materialized='incremental',
        tags=['betting', 'features', 'player_props', 'sportsbooks', 'ml'],
        partition_by={
            "field": "feature_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['player_id', 'market_id']
    )
}}

{%- call statement('get_sportsbooks', fetch_result=True) -%}
    select distinct sportsbook from {{ ref('stg__player_props') }}
    order by 1
{%- endcall -%}

{%- if execute -%}
    {%- set distinct_sportsbooks_raw = load_result('get_sportsbooks')['data'] | map(attribute=0) | list -%}
{%- else -%}
    {%- set distinct_sportsbooks_raw = [] -%}
{%- endif -%}

with base_props as (
    select * from {{ ref('int_betting__player_props_probabilities') }}
    where game_date >= current_date - interval '365 days'
),

-- Unpivot step
unpivoted_data as (
    {% if distinct_sportsbooks_raw | length > 0 %}
    {% for s_book_raw in distinct_sportsbooks_raw %}
    {% set s_book_slug = (s_book_raw | lower | replace(' ', '_') | replace('.', '_') | replace('/', '_') | replace('(', '') | replace(')', '')) %}
    select
        player_prop_key,
        player_id,
        player_slug,
        player_name,
        game_date,
        market_id as market_cleaned,
        market,
        line,

        {{ "'" ~ s_book_raw | replace("'", "''") ~ "'" }} as sportsbook,
        {{ s_book_slug ~ "_over_odds_decimal" }} as over_odds,
        {{ s_book_slug ~ "_under_odds_decimal" }} as under_odds,
        {{ s_book_slug ~ "_over_implied_prob" }} as over_implied_prob,
        {{ s_book_slug ~ "_under_implied_prob" }} as under_implied_prob,
        {{ s_book_slug ~ "_total_implied_prob" }} as total_implied_prob,
        {{ s_book_slug ~ "_over_no_vig_prob" }} as no_vig_over_prob,
        {{ s_book_slug ~ "_under_no_vig_prob" }} as no_vig_under_prob,
        {{ s_book_slug ~ "_hold_percentage" }} as vig_percentage
    from base_props
    where {{ s_book_slug ~ "_over_odds_decimal" }} is not null or {{ s_book_slug ~ "_under_odds_decimal" }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
    {% else %}
    select
        null::text as player_prop_key, null::text as player_id, null::text as player_slug, null::text as player_name,
        null::date as game_date, null::text as market_cleaned, null::text as market, null::numeric as line,
        null::text as sportsbook, null::decimal as over_odds, null::decimal as under_odds,
        null::numeric as over_implied_prob, null::numeric as under_implied_prob,
        null::numeric as total_implied_prob, null::numeric as no_vig_over_prob,
        null::numeric as no_vig_under_prob, null::numeric as vig_percentage
    where 1=0
    {% endif %}
),

player_props as (
    select
        player_id,
        player_name,
        market_cleaned as market_id,
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
    from unpivoted_data
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
        avg(over_implied_prob) as consensus_over_prob,
        avg(under_implied_prob) as consensus_under_prob
    from player_props
    group by player_id, market_id, game_date
),

-- Join player props with consensus to calculate deviations
joined as (
    select
        pp.player_id,
        pp.player_name,
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
        pp.over_implied_prob,
        cp.consensus_over_prob,
        pp.over_implied_prob - cp.consensus_over_prob as over_prob_vs_consensus,
        pp.under_implied_prob,
        cp.consensus_under_prob,
        pp.under_implied_prob - cp.consensus_under_prob as under_prob_vs_consensus,
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
        player_name,
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
    group by player_id, player_name, market_id, market, sportsbook
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
        sa.player_name,
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