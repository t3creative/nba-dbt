-- depends_on: {{ ref('stg__player_props') }}
{{
    config(
        schema='features',
        materialized='incremental',
        tags=['betting', 'features', 'player_props', 'time_series', 'ml'],
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
    {% if is_incremental() %}
        -- If already built, only process the latest 60 days of data to refresh
        and game_date >= (select greatest(max(feature_date) - interval '60 days', current_date - interval '365 days') from {{ this }})
    {% endif %}
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
        {{ s_book_slug ~ "_under_implied_prob" }} as under_implied_prob
    from base_props
    where {{ s_book_slug ~ "_over_odds_decimal" }} is not null or {{ s_book_slug ~ "_under_odds_decimal" }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
    {% else %}
    select
        null::text as player_prop_key, null::text as player_id, null::text as player_slug, null::text as player_name,
        null::date as game_date, null::text as market_cleaned, null::text as market, null::numeric as line,
        null::text as sportsbook, null::decimal as over_odds, null::decimal as under_odds,
        null::numeric as over_implied_prob, null::numeric as under_implied_prob
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
        sportsbook,
        game_date
    from unpivoted_data
    where sportsbook = 'Consensus'
    
    -- Added player-market combination filter for better performance
    -- Only process player-market combinations with at least 3 data points
    and (player_id, market_cleaned) in (
        select player_id, market_cleaned
        from unpivoted_data
        where sportsbook = 'Consensus'
        group by player_id, market_cleaned
        having count(*) >= 3
    )
),

-- Add temporal ordering and compute changes between consecutive games
time_ordered_props as (
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
        game_date,
        
        -- Previous values
        lag(line) over (
            partition by player_id, market_id 
            order by game_date
        ) as previous_line,
        
        lag(over_odds) over (
            partition by player_id, market_id 
            order by game_date
        ) as previous_over_odds,
        
        lag(under_odds) over (
            partition by player_id, market_id 
            order by game_date
        ) as previous_under_odds,
        
        -- Next values (for future trend analysis)
        lead(line) over (
            partition by player_id, market_id 
            order by game_date
        ) as next_line,
        
        -- Days since previous game
        game_date - lag(game_date) over (
            partition by player_id, market_id 
            order by game_date
        ) as days_since_previous,
        
        -- Game number in sequence
        row_number() over (
            partition by player_id, market_id 
            order by game_date
        ) as game_sequence
    from player_props
),

-- Calculate changes and moving statistics
with_changes as (
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
        game_date,
        previous_line,
        previous_over_odds,
        previous_under_odds,
        next_line,
        days_since_previous,
        game_sequence,
        
        -- Calculate changes
        coalesce(line - previous_line, 0) as line_change,
        case 
            when previous_line is null or previous_line = 0 then 0
            else (line - previous_line) / previous_line * 100 
        end as line_pct_change,
        coalesce(over_odds - previous_over_odds, 0) as over_odds_change,
        coalesce(under_odds - previous_under_odds, 0) as under_odds_change,
        
        -- Calculate moving averages
        coalesce(avg(line) over (
            partition by player_id, market_id 
            order by game_date 
            rows between 3 preceding and 1 preceding
        ), line) as ma_3_line,
        
        coalesce(avg(line) over (
            partition by player_id, market_id 
            order by game_date 
            rows between 5 preceding and 1 preceding
        ), line) as ma_5_line,
        
        -- Calculate moving standard deviations
        coalesce(stddev(line) over (
            partition by player_id, market_id 
            order by game_date 
            rows between 5 preceding and 1 preceding
        ), 0) as std_5_line,
        
        -- Trend direction
        case
            when previous_line is null then 'Initial'
            when line > previous_line then 'Up'
            when line < previous_line then 'Down'
            else 'Stable'
        end as trend_direction,
        
        -- Trend strength (how many consecutive games in same direction)
        case
            when previous_line is null then 0
            when line > previous_line and previous_line > lag(line, 2) over (partition by player_id, market_id order by game_date) then 2
            when line < previous_line and previous_line < lag(line, 2) over (partition by player_id, market_id order by game_date) then 2
            when line > previous_line or line < previous_line then 1
            else 0
        end as trend_strength
        
    from time_ordered_props
),

-- Aggregate temporal patterns by player-market
player_market_temporal as (
    select
        player_id,
        player_name,
        market_id,
        market,
        
        -- Sample size
        count(*) as game_count,
        max(game_sequence) as total_games_tracked,
        
        -- Time range
        min(game_date) as first_game_date,
        max(game_date) as last_game_date,
        (max(game_date) - min(game_date))::integer as total_days_tracked,
        
        -- Average days between games
        avg(days_since_previous) as avg_days_between_games,
        
        -- Line change metrics
        avg(abs(line_change)) as avg_abs_line_change,
        stddev(line_change) as stddev_line_change,
        avg(line_pct_change) as avg_line_pct_change,
        
        -- Line trend metrics
        sum(case when line_change > 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_line_increases,
        sum(case when line_change < 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_line_decreases,
        
        -- Maximum consecutive increases/decreases
        max(trend_strength) filter (where trend_direction = 'Up') as max_consecutive_increases,
        max(trend_strength) filter (where trend_direction = 'Down') as max_consecutive_decreases,
        
        -- Volatility metrics
        stddev(line_change) / nullif(avg(abs(line_change)), 0) as line_change_volatility_ratio,
        
        -- Recent trend analysis (limited to most recent 5)
        array_agg(trend_direction ORDER BY game_date DESC) FILTER (
            WHERE trend_direction IS NOT NULL
        ) as recent_trends,
        
        -- Odds change metrics
        avg(over_odds_change) as avg_over_odds_change,
        avg(under_odds_change) as avg_under_odds_change,
        stddev(over_odds_change) as stddev_over_odds_change,
        stddev(under_odds_change) as stddev_under_odds_change,
        
        -- Seasonal trends (group by month)
        -- Just get distinct months without complex aggregation
        array_agg(DISTINCT extract(month from game_date) ORDER BY extract(month from game_date)) as months_with_data,
        
        -- Generate feature key
        {{ dbt_utils.generate_surrogate_key(['player_id', 'market_id']) }} as feature_key
        
    from with_changes
    group by player_id, player_name, market_id, market
),

-- Extract month-by-month averages for seasonal patterns
seasonal_patterns as (
    select
        player_id,
        market_id,
        extract(month from game_date) as month_num,
        to_char(game_date, 'Month') as month_name,
        avg(line) as avg_line_by_month
    from player_props
    group by player_id, market_id, extract(month from game_date), to_char(game_date, 'Month')
),

-- Calculate variance between months
month_variance as (
    select
        player_id,
        market_id,
        stddev(avg_line_by_month) as month_to_month_line_stddev,
        max(avg_line_by_month) - min(avg_line_by_month) as month_to_month_line_range
    from seasonal_patterns
    group by player_id, market_id
),

final as (
    select
        pmt.feature_key,
        pmt.player_id,
        pmt.player_name,
        pmt.market_id,
        pmt.market,
        
        -- Game metrics
        pmt.game_count,
        pmt.total_games_tracked,
        pmt.first_game_date,
        pmt.last_game_date,
        pmt.total_days_tracked,
        pmt.avg_days_between_games,
        
        -- Line change metrics
        pmt.avg_abs_line_change,
        pmt.stddev_line_change,
        pmt.avg_line_pct_change,
        
        -- Line trend metrics
        pmt.pct_line_increases,
        pmt.pct_line_decreases,
        pmt.max_consecutive_increases,
        pmt.max_consecutive_decreases,
        pmt.line_change_volatility_ratio,
        
        -- Volatility classification
        case
            when pmt.stddev_line_change <= 0.5 then 'Very Stable'
            when pmt.stddev_line_change <= 1.0 then 'Stable'
            when pmt.stddev_line_change <= 2.0 then 'Moderate'
            when pmt.stddev_line_change <= 3.0 then 'Volatile'
            else 'Highly Volatile'
        end as line_volatility_class,
        
        -- Directional bias
        case
            when pmt.pct_line_increases > 0.6 then 'Upward Bias'
            when pmt.pct_line_decreases > 0.6 then 'Downward Bias'
            else 'No Clear Bias'
        end as line_direction_bias,
        
        -- Odds change metrics
        pmt.avg_over_odds_change,
        pmt.avg_under_odds_change,
        pmt.stddev_over_odds_change,
        pmt.stddev_under_odds_change,
        
        -- Recent trends as string (limit to 5)
        array_to_string((SELECT array_agg(x) FROM (SELECT unnest(pmt.recent_trends) as x LIMIT 5) t), '-') as recent_trend_pattern,
        
        -- Seasonal metrics
        mv.month_to_month_line_stddev,
        mv.month_to_month_line_range,
        array_to_string(pmt.months_with_data, ', ') as active_months,
        
        -- Predictability score (lower volatility = higher predictability)
        case
            when pmt.stddev_line_change = 0 then 100
            else 100 / (1 + pmt.stddev_line_change)
        end as line_predictability_score,
        
        -- Feature metadata
        current_date as feature_date
        
    from player_market_temporal pmt
    left join month_variance mv
        on pmt.player_id = mv.player_id
        and pmt.market_id = mv.market_id
    where pmt.game_count >= 5 -- Minimum sample for reliability
    and pmt.last_game_date >= current_date - interval '90 days' -- Focus on recent activity
)

select * from final 