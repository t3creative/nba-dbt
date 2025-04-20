-- =============================================================================
-- STANDARD DEVIATION CALCULATION MACROS
-- These macros help generate features related to player performance consistency
-- =============================================================================

{# 
    Rolling Standard Deviation Parameters:
    - metric_col: Column to calculate standard deviation for
    - partition_cols: Columns to partition by (player_id, team_id, etc.)
    - order_col: Column to order by (usually game_date)
    - window_size: Number of games in window
    - min_periods: Minimum periods required to calculate
    - ignore_nulls: Whether to ignore nulls in calculation
    - include_current: Whether to include current game in window
#}
{% macro rolling_std_dev(
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

    {%- if window_size == 9999 -%}
        {%- if ignore_nulls -%}
            stddev({{ metric_col }}) over (
                {%- if partition_by %}
                partition by {{ partition_by }}
                {%- endif %}
                order by {{ order_col }}
                rows between unbounded preceding and {{ frame_end }}
            )
        {%- else -%}
            stddev({{ metric_col }}) over (
                {%- if partition_by %}
                partition by {{ partition_by }}
                {%- endif %}
                order by {{ order_col }}
                rows between unbounded preceding and {{ frame_end }}
            )
        {%- endif -%}
    {%- else -%}
        {%- if ignore_nulls -%}
            stddev({{ metric_col }}) over (
                {%- if partition_by %}
                partition by {{ partition_by }}
                {%- endif %}
                order by {{ order_col }}
                rows between {{ window_size }} preceding and {{ frame_end }}
            )
        {%- else -%}
            stddev({{ metric_col }}) over (
                {%- if partition_by %}
                partition by {{ partition_by }}
                {%- endif %}
                order by {{ order_col }}
                rows between {{ window_size }} preceding and {{ frame_end }}
            )
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{# 
    Coefficient of Variation:
    - Measures relative variability (standard deviation / mean)
    - Useful for comparing variability across different metrics
#}
{% macro coefficient_of_variation(
    metric_col,
    partition_cols=['player_id'],
    order_col='game_date',
    window_size=5,
    include_current=false
) %}
    {%- set frame_end = "current row" if include_current else "1 preceding" -%}
    {%- set partition_by = partition_cols|join(", ") if partition_cols is sequence and partition_cols|length > 0 else partition_cols -%}
    
    {%- if not metric_col -%}
        {{ exceptions.raise_compiler_error("metric_col is required") }}
    {%- endif -%}

    {%- if window_size == 9999 -%}
        stddev({{ metric_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between unbounded preceding and {{ frame_end }}
        ) / 
        nullif(abs(avg({{ metric_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between unbounded preceding and {{ frame_end }}
        )), 0)
    {%- else -%}
        stddev({{ metric_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size }} preceding and {{ frame_end }}
        ) / 
        nullif(abs(avg({{ metric_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size }} preceding and {{ frame_end }}
        )), 0)
    {%- endif -%}
{% endmacro %} 