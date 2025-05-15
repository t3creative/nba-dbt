{% macro rolling_window_stats(source_table, partition_fields, order_field, stat_fields, window_sizes=[5, 10, 20]) %}
    
    WITH source_data AS (
        SELECT * FROM {{ source_table }}
    ),
    
    /* Generate rolling window statistics for each specified field */
    {% for window_size in window_sizes %}
    window_{{ window_size }} AS (
        SELECT
            {% for partition_field in partition_fields %}
            {{ partition_field }},
            {% endfor %}
            {{ order_field }},
            
            {% for stat_field in stat_fields %}
            AVG({{ stat_field }}) OVER(
                PARTITION BY {% for partition_field in partition_fields %}
                             {{ partition_field }}{% if not loop.last %}, {% endif %}
                             {% endfor %}
                ORDER BY {{ order_field }}
                ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
            ) AS {{ stat_field }}_l{{ window_size }},
            {% endfor %}
        
        FROM source_data
    ){% if not loop.last %},{% endif %}
    {% endfor %}
    
    /* Join all the window CTEs */
    SELECT
        d.*,
        {% for window_size in window_sizes %}
            {% for stat_field in stat_fields %}
                w{{ window_size }}.{{ stat_field }}_l{{ window_size }}{% if not loop.last or not loop.parent.last %},{% endif %}
            {% endfor %}
        {% endfor %}
    FROM source_data d
    {% for window_size in window_sizes %}
    LEFT JOIN window_{{ window_size }} w{{ window_size }}
        ON {% for partition_field in partition_fields %}
           d.{{ partition_field }} = w{{ window_size }}.{{ partition_field }} AND
           {% endfor %}
           d.{{ order_field }} = w{{ window_size }}.{{ order_field }}
    {% endfor %}
    
{% endmacro %}