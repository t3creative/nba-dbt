-- Core Progressive Rolling Window Macro
{% macro progressive_rolling_window_team(
    column_name, 
    window_size, 
    aggregate_function='AVG',
    partition_by='team_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} = 1 THEN NULL
    {% for i in range(2, window_size + 1) %}
    WHEN {{ sequence_column }} = {{ i }} THEN ROUND(
        {{ aggregate_function }}({{ column_name }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
        )::NUMERIC, {{ precision }}
    )
    {% endfor %}
    WHEN {{ sequence_column }} >= {{ window_size + 1 }} THEN ROUND(
        {{ aggregate_function }}({{ column_name }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
        )::NUMERIC, {{ precision }}
    )
    ELSE NULL
END
{% endmacro %}

-- Individual function macros for clean usage
{% macro progressive_rolling_avg_team(column_name, window_size, precision=3, partition_by='team_id') %}
{{ progressive_rolling_window_team(
    column_name=column_name,
    window_size=window_size,
    aggregate_function='AVG',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

{% macro progressive_rolling_stddev_team(column_name, window_size, precision=3, partition_by='team_id') %}
{{ progressive_rolling_window_team(
    column_name=column_name,
    window_size=window_size,
    aggregate_function='STDDEV',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

{% macro progressive_rolling_max_team(column_name, window_size, precision=3, partition_by='team_id') %}
{{ progressive_rolling_window_team(
    column_name=column_name,
    window_size=window_size,
    aggregate_function='MAX',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

{% macro progressive_rolling_min_team(column_name, window_size, precision=3, partition_by='team_id') %}
{{ progressive_rolling_window_team(
    column_name=column_name,
    window_size=window_size,
    aggregate_function='MIN',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

-- Coefficient of Variation (consistency metric)
{% macro progressive_rolling_cv_team(column_name, window_size, precision=3, partition_by='team_id') %}
CASE 
    WHEN {{ progressive_rolling_avg_team(column_name, window_size, precision, partition_by) }} = 0 THEN NULL
    ELSE ROUND(
        ({{ progressive_rolling_stddev_team(column_name, window_size, precision, partition_by) }} / 
         NULLIF({{ progressive_rolling_avg_team(column_name, window_size, precision, partition_by) }}, 0)) * 100,
        {{ precision }}
    )
END
{% endmacro %}

-- Momentum/Trend (short window vs long window performance)
{% macro progressive_rolling_trend_team(column_name, short_window=3, long_window=10, precision=3, partition_by='team_id') %}
CASE 
    WHEN season_game_sequence <= {{ long_window }} THEN NULL
    ELSE ROUND(
        {{ progressive_rolling_avg_team(column_name, short_window, precision, partition_by) }} - 
        {{ progressive_rolling_avg_team(column_name, long_window, precision, partition_by) }},
        {{ precision }}
    )
END
{% endmacro %}

-- Recent performance vs rolling average (momentum detection)
{% macro progressive_rolling_recent_vs_avg_team(column_name, window_size, precision=3, partition_by='team_id') %}
CASE 
    WHEN season_game_sequence = 1 THEN NULL
    ELSE ROUND(
        {{ column_name }} - {{ progressive_rolling_avg_team(column_name, window_size, precision, partition_by) }},
        {{ precision }}
    )
END
{% endmacro %}

-- Generate a single stat with all functions across all windows
{% macro generate_stat_features_team(stat_name, window_sizes=[3, 5, 10]) %}
{% for window in window_sizes %}
{{ progressive_rolling_avg_team(stat_name, window) }} as {{ stat_name }}_avg_l{{ window }},
{{ progressive_rolling_stddev_team(stat_name, window) }} as {{ stat_name }}_stddev_l{{ window }},
{{ progressive_rolling_max_team(stat_name, window) }} as {{ stat_name }}_max_l{{ window }},
{{ progressive_rolling_min_team(stat_name, window) }} as {{ stat_name }}_min_l{{ window }},
{{ progressive_rolling_cv_team(stat_name, window) }} as {{ stat_name }}_cv_l{{ window }},
{{ progressive_rolling_recent_vs_avg_team(stat_name, window) }} as {{ stat_name }}_recent_vs_avg_l{{ window }}{% if not loop.last %},{% endif %}
{% endfor %},
-- Trend features (3v5, 3v10, 5v10 comparisons)
{{ progressive_rolling_trend_team(stat_name, 3, 5) }} as {{ stat_name }}_trend_3v5,
{{ progressive_rolling_trend_team(stat_name, 3, 10) }} as {{ stat_name }}_trend_3v10,
{{ progressive_rolling_trend_team(stat_name, 5, 10) }} as {{ stat_name }}_trend_5v10
{% endmacro %}