-- =============================================================================
-- TIME-BASED FEATURE ENGINEERING MACROS
-- These macros help generate features that depend on the temporal nature of NBA data
-- =============================================================================

{# 
    Advanced Rolling Average Parameters:
    - metric_col: Column to calculate average for
    - partition_cols: Columns to partition by (player_id, team_id, etc.)
    - order_col: Column to order by (usually game_date)
    - window_size: Number of games in window
    - min_periods: Minimum periods required to calculate
    - ignore_nulls: Whether to ignore nulls in calculation
    - include_current: Whether to include current game in window
#}
{% macro advanced_rolling_average(
    metric_col,
    partition_cols=['player_id'],
    order_col='game_date',
    window_size=5,
    min_periods=1,
    ignore_nulls=true,
    include_current=false
) %}
    {%- set frame_end = "current row" if include_current else "1 preceding" -%}
    {%- set partition_by = partition_cols|join(", ") if partition_cols is sequence and partition_cols|length > 0 else partition_cols -%}
    
    {%- if not metric_col -%}
        {{ exceptions.raise_compiler_error("metric_col is required") }}
    {%- endif -%}

    {%- if ignore_nulls -%}
        avg({{ metric_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size }} preceding and {{ frame_end }}
        )
    {%- else -%}
        sum({{ metric_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size }} preceding and {{ frame_end }}
        ) / nullif(count(*) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size }} preceding and {{ frame_end }}
        ), 0)
    {%- endif -%}
{% endmacro %}

{# 
    Recency Weighted Performance Parameters:
    - metric_col: Column to calculate weighted performance for
    - partition_cols: Columns to partition by
    - order_col: Column to order by
    - window_size: Number of games in window
    - weight_type: Type of weight decay (linear, exponential, or custom)
    - alpha: Decay factor for exponential weighting
    - custom_weights: List of custom weights (must match window_size)
    - include_current: Whether to include current game in window
#}
{% macro recency_weighted_performance(
    metric_col,
    partition_cols=['player_id'],
    order_col='game_date',
    window_size=10,
    weight_type='linear',
    alpha=0.8,
    custom_weights=None,
    include_current=false
) %}
    {%- if not metric_col -%}
        {{ exceptions.raise_compiler_error("metric_col is required") }}
    {%- endif -%}

    {%- set frame_end = "current row" if include_current else "1 preceding" -%}
    {%- set partition_by = partition_cols|join(", ") if partition_cols is sequence and partition_cols|length > 0 else partition_cols -%}
    
    {%- if weight_type == 'linear' -%}
        ({{ window_size }} - row_number() over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }} desc
        ) + 1) * {{ metric_col }}
    {%- elif weight_type == 'exponential' -%}
        power({{ alpha }}, row_number() over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }} desc
        ) - 1) * {{ metric_col }}
    {%- else -%}
        {{ metric_col }}  -- Default to equal weights if type is invalid
    {%- endif -%}
{% endmacro %} 