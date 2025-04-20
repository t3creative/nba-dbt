-- =============================================================================
-- PERCENTAGE CALCULATION MACROS
-- These macros generate properly calculated percentage features
-- =============================================================================

{% macro rolling_percentage(
    numerator_col,
    denominator_col,
    partition_cols=['player_id'],
    order_col='game_date',
    window_size=5,
    include_current=false
) %}
    {%- set frame_end = "current row" if include_current else "1 preceding" -%}
    {%- set partition_by = partition_cols|join(", ") if partition_cols is sequence and partition_cols|length > 0 else partition_cols -%}
    
    {%- if not numerator_col or not denominator_col -%}
        {{ exceptions.raise_compiler_error("numerator_col and denominator_col are required") }}
    {%- endif -%}

    {%- if window_size == 9999 -%}
        nullif(sum({{ numerator_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between unbounded preceding and {{ frame_end }}
        ), 0) / 
        nullif(sum({{ denominator_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between unbounded preceding and {{ frame_end }}
        ), 0)
    {%- else -%}
        nullif(sum({{ numerator_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size }} preceding and {{ frame_end }}
        ), 0) / 
        nullif(sum({{ denominator_col }}) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size }} preceding and {{ frame_end }}
        ), 0)
    {%- endif -%}
{% endmacro %} 