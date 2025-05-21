{% macro calculate_rolling_stddev(column_name, partition_by, order_by, window_size) %}

    stddev_samp({{ column_name }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ window_size }} preceding and 1 preceding
    )

{% endmacro %} 