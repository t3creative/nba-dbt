{% macro calculate_rolling_avg(column_name, partition_by, order_by, window_size_var='rolling_window_games') %}

    avg({{ column_name }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ var(window_size_var, 10) }} preceding and 1 preceding
    )

{% endmacro %} 