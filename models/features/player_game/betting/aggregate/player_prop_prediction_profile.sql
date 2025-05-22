{{
    config(
        materialized='table',
        tags=['betting', 'ml', 'prediction_profile'],
        partition_by={'field': 'prediction_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['player_id', 'market_id'],
        unique_key='prediction_key'
    )
}}

{# Properly structured debug logging #}
{% if execute %}
    {{ log("Starting prediction profile model", info=True) }}
    {{ log("Source models: player_prop_market_time_series_features, player_prop_market_sportsbook_features", info=True) }}
{% endif %}

with date_spine as (
    select generate_series::date as prediction_date
    from generate_series(
        '2021-01-01'::date,
        current_date,
        '1 day'::interval
    )
),

-- First get valid time series features
time_series_latest as (
    select 
        player_id,
        market_id,
        feature_date,
        avg_abs_line_change,
        stddev_line_change,
        line_predictability_score,
        recent_trend_pattern,
        month_to_month_line_stddev,
        -- Get latest feature for each date
        first_value(feature_date) over (
            partition by player_id, market_id 
            order by feature_date desc
        ) as latest_feature_date
    from {{ ref('player_prop_market_time_series_features') }}
    where feature_date < current_date
),

-- Get latest sportsbook features
sportsbook_latest as (
    select
        player_id,
        market_id,
        feature_date,
        avg_line_vs_consensus,
        stddev_line_vs_consensus,
        pct_higher_than_consensus,
        pct_lower_than_consensus,
        potential_edge,
        -- Get latest feature for each date
        first_value(feature_date) over (
            partition by player_id, market_id 
            order by feature_date desc
        ) as latest_feature_date
    from {{ ref('player_prop_market_sportsbook_features') }}
    where feature_date < current_date
),

-- Get valid player-market combinations with their latest features
valid_combinations as (
    select distinct
        coalesce(ts.player_id, sb.player_id) as player_id,
        coalesce(ts.market_id, sb.market_id) as market_id,
        greatest(
            ts.latest_feature_date, 
            sb.latest_feature_date
        ) as latest_feature_date
    from time_series_latest ts
    full outer join sportsbook_latest sb
        on ts.player_id = sb.player_id
        and ts.market_id = sb.market_id
    where coalesce(ts.player_id, sb.player_id) is not null
),

-- Join features only for valid dates and combinations
combined_features as (
    select
        d.prediction_date,
        vc.player_id,
        vc.market_id,
        -- Time series features
        ts.avg_abs_line_change,
        ts.stddev_line_change,
        ts.line_predictability_score,
        ts.recent_trend_pattern,
        ts.month_to_month_line_stddev,
        -- Sportsbook features
        sb.avg_line_vs_consensus,
        sb.stddev_line_vs_consensus,
        sb.pct_higher_than_consensus,
        sb.pct_lower_than_consensus,
        sb.potential_edge
    from date_spine d
    inner join valid_combinations vc
        on d.prediction_date <= vc.latest_feature_date
    left join time_series_latest ts
        on vc.player_id = ts.player_id
        and vc.market_id = ts.market_id
        and d.prediction_date >= ts.feature_date
        and ts.feature_date = ts.latest_feature_date
    left join sportsbook_latest sb
        on vc.player_id = sb.player_id
        and vc.market_id = sb.market_id
        and d.prediction_date >= sb.feature_date
        and sb.feature_date = sb.latest_feature_date
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'prediction_date', 
            'player_id', 
            'market_id'
        ]) }} as prediction_key,
        prediction_date,
        player_id,
        market_id,
        -- Keep features as is, no coalescing
        avg_abs_line_change,
        stddev_line_change,
        line_predictability_score,
        recent_trend_pattern,
        month_to_month_line_stddev,
        avg_line_vs_consensus,
        stddev_line_vs_consensus,
        pct_higher_than_consensus,
        pct_lower_than_consensus,
        potential_edge,
        -- Only calculate stability when both feature sets exist
        case 
            when line_predictability_score is not null 
                 and stddev_line_vs_consensus is not null
            then (line_predictability_score * 0.7) + 
                 (1 - (stddev_line_change / 
                       nullif(stddev_line_vs_consensus + 1, 0))) * 30
        end as market_stability_index
    from combined_features
)

select * from valid_combinations
