{% macro calculate_lag(column_name, partition_by, order_by, lag_n=1) %}

    lag({{ column_name }}, {{ lag_n }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
    )

{% endmacro %} 