-- =============================================================================
-- MOMENTUM-BASED FEATURE ENGINEERING MACROS
-- These macros help generate features that capture player performance trends,
-- streaks, and consistency metrics
-- =============================================================================

{# 
    Streak Detection Parameters:
    - metric_col: Column to analyze for streaks
    - partition_cols: Columns to partition by (usually player_id)
    - order_col: Column to order by (usually game_date)
    - threshold: Value to compare against for streak
    - comparison: Type of comparison ('>', '>=', '<', '<=', '=')
    - min_streak_length: Minimum consecutive games to consider a streak
    - include_current: Whether to include current game in streak calculation
#}
{% macro detect_streak(
    metric_col,
    partition_cols=['player_id'],
    order_col='game_date',
    threshold=0,
    comparison='>',
    min_streak_length=3,
    include_current=false
) %}
    {%- if not metric_col -%}
        {{ exceptions.raise_compiler_error("metric_col is required") }}
    {%- endif -%}

    {%- set frame_end = "current row" if include_current else "1 preceding" -%}
    {%- set partition_by = partition_cols|join(", ") if partition_cols is sequence and partition_cols|length > 0 else partition_cols -%}
    
    -- Count consecutive games meeting the condition
    sum(case 
        when {{ metric_col }} {{ comparison }} {{ threshold }} then 1 
        else 0 
    end) over (
        {%- if partition_by %}
        partition by {{ partition_by }}
        {%- endif %}
        order by {{ order_col }}
        rows between {{ min_streak_length - 1 }} preceding and {{ frame_end }}
    ) = {{ min_streak_length }}  -- Returns true if in a streak of exactly min_streak_length
{% endmacro %}

{# 
    Performance Trend Parameters:
    - metric_col: Column to analyze for trend
    - partition_cols: Columns to partition by (usually player_id)
    - order_col: Column to order by (usually game_date)
    - window_size: Number of games to analyze trend over
    - include_current: Whether to include current game in trend calculation
#}
{% macro calculate_trend(
    metric_col,
    partition_cols=['player_id'],
    order_col='game_date',
    window_size=5,
    include_current=false
) %}
    {%- if not metric_col -%}
        {{ exceptions.raise_compiler_error("metric_col is required") }}
    {%- endif -%}

    {%- set frame_end = "current row" if include_current else "1 preceding" -%}
    {%- set partition_by = partition_cols|join(", ") if partition_cols is sequence and partition_cols|length > 0 else partition_cols -%}
    
    -- Use CTE approach to avoid nested window functions
    /* This generates SQL like:
    with numbered_data as (
        select 
            {{ metric_col }},
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    
    'with trend_calc as (
        select 
            *,
            row_number() over (
                {%- if partition_by %}
                partition by {{ partition_by }}
                {%- endif %}
                order by {{ order_col }}
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            {{ metric_col }},
            row_num
        ) over (
            {%- if partition_by %}
            partition by {{ partition_by }}
            {%- endif %}
            order by {{ order_col }}
            rows between {{ window_size - 1 }} preceding and {{ frame_end }}
        ) as trend
    from trend_calc'
{% endmacro %}

{# 
    Performance Delta vs Baseline Parameters:
    - metric_col: Column to calculate delta for
    - partition_cols: Columns to partition by (e.g., ['player_id'])
    - order_col: Column to order by (e.g., 'game_date')
    - recent_window_size: Number of games for the recent average
    - baseline_window_size: Number of games for the baseline average (use a large number like 9999 for season)
    - lag_games: How many games to lag the calculation (usually 1 to exclude current game)
    - handle_nulls: How to handle nulls ('ignore' or 'zero'). 'ignore' uses avg ignoring nulls, 'zero' treats nulls as 0.
    - ignore_current_game: If true, the window ends 1 row before the current row. Defaults to true.
#}
{% macro calculate_performance_delta(
    metric_col,
    partition_cols=['player_id'],
    order_col='game_date',
    recent_window_size=5,
    baseline_window_size=9999,
    lag_games=1,
    handle_nulls='ignore',
    ignore_current_game=true
) %}
    {%- if not metric_col -%}
        {{ exceptions.raise_compiler_error("`metric_col` is required for calculate_performance_delta") }}
    {%- endif -%}
    {%- if lag_games < 0 -%}
        {{ exceptions.raise_compiler_error("`lag_games` must be non-negative") }}
    {%- endif -%}
    {%- if recent_window_size <= 0 or baseline_window_size <= 0 -%}
        {{ exceptions.raise_compiler_error("Window sizes must be positive") }}
    {%- endif -%}

    {%- set partition_by = partition_cols|join(", ") if partition_cols is sequence and partition_cols|length > 0 else partition_cols -%}
    {%- set partition_clause = "partition by " ~ partition_by if partition_by else "" -%}
    
    {%- set recent_frame_start = (recent_window_size + lag_games - 1) if ignore_current_game else (recent_window_size - 1) -%}
    {%- set recent_frame_end = lag_games if ignore_current_game else "current row" -%}
    
    {%- set baseline_frame_start = (baseline_window_size + lag_games - 1) if ignore_current_game else (baseline_window_size - 1) -%}
    {%- set baseline_frame_end = lag_games if ignore_current_game else "current row" -%}

    {%- set metric_expression = metric_col -%}
    {%- if handle_nulls == 'zero' -%}
        {%- set metric_expression = "coalesce(" ~ metric_col ~ ", 0)" -%}
    {%- endif -%}

    (
        avg({{ metric_expression }}) over (
            {{ partition_clause }}
            order by {{ order_col }}
            rows between {{ recent_frame_start }} preceding and {{ recent_frame_end }} preceding 
        )
        -
        avg({{ metric_expression }}) over (
            {{ partition_clause }}
            order by {{ order_col }}
            rows between {{ baseline_frame_start }} preceding and {{ baseline_frame_end }} preceding
        )
    )
{% endmacro %} 