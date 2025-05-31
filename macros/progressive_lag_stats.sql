-- Core Predictive Lag Calculation Macro (No Data Leakage)
{% macro predictive_lag_window(
    column_name, 
    lag_offset, 
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3,
    default_value=null
) %}
CASE 
    WHEN {{ sequence_column }} <= {{ lag_offset }} THEN {{ default_value }}
    ELSE ROUND(
        LAG({{ column_name }}, {{ lag_offset }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )::NUMERIC, {{ precision }}
    )
END
{% endmacro %}

-- Simple Lag Feature (Previous N games)
{% macro predictive_lag(column_name, lag_offset, precision=3, partition_by='player_id', default_value=null) %}
{{ predictive_lag_window(
    column_name=column_name,
    lag_offset=lag_offset,
    partition_by=partition_by,
    precision=precision,
    default_value=default_value
) }}
{% endmacro %}

-- Simplified Delta Feature (Current vs Previous) - PostgreSQL Safe
{% macro predictive_delta_simple(
    column_name, 
    lag_offset=1, 
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} <= {{ lag_offset }} THEN NULL
    ELSE ROUND(
        ({{ column_name }} - LAG({{ column_name }}, {{ lag_offset }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ))::NUMERIC, 
        {{ precision }}
    )
END
{% endmacro %}

-- Simplified Percentage Delta (PostgreSQL Safe)
{% macro predictive_delta_pct_simple(
    column_name, 
    lag_offset=1, 
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} <= {{ lag_offset }} THEN NULL
    WHEN LAG({{ column_name }}, {{ lag_offset }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
    ) = 0 THEN NULL
    ELSE ROUND(
        (({{ column_name }} - LAG({{ column_name }}, {{ lag_offset }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )) / NULLIF(LAG({{ column_name }}, {{ lag_offset }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ), 0) * 100)::NUMERIC, 
        {{ precision }}
    )
END
{% endmacro %}

-- Specialized Delta Macros (Simplified)
{% macro predictive_delta(column_name, lag_offset=1, precision=3, partition_by='player_id') %}
{{ predictive_delta_simple(
    column_name=column_name,
    lag_offset=lag_offset,
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

{% macro predictive_delta_pct(column_name, lag_offset=1, precision=3, partition_by='player_id') %}
{{ predictive_delta_pct_simple(
    column_name=column_name,
    lag_offset=lag_offset,
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

-- Streak Feature (Consecutive games meeting criteria) - Simplified
{% macro predictive_streak_window(
    column_name, 
    threshold_value,
    comparison_operator='>', 
    max_streak_length=10,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence'
) %}
CASE 
    WHEN {{ sequence_column }} = 1 THEN 
        CASE WHEN {{ column_name }} {{ comparison_operator }} {{ threshold_value }} THEN 1 ELSE 0 END
    ELSE
        CASE 
            WHEN {{ column_name }} {{ comparison_operator }} {{ threshold_value }} THEN
                CASE 
                    WHEN LAG({{ column_name }}) OVER (
                        PARTITION BY {{ partition_by }}
                        ORDER BY {{ order_by }}
                    ) {{ comparison_operator }} {{ threshold_value }}
                    THEN LEAST(
                        COALESCE(LAG(1) OVER (
                            PARTITION BY {{ partition_by }}
                            ORDER BY {{ order_by }}
                        ), 0) + 1,
                        {{ max_streak_length }}
                    )
                    ELSE 1
                END
            ELSE 0
        END
END
{% endmacro %}

-- Specialized Streak Macros
{% macro predictive_streak_above(column_name, threshold_value, max_streak=10, partition_by='player_id') %}
{{ predictive_streak_window(
    column_name=column_name,
    threshold_value=threshold_value,
    comparison_operator='>',
    max_streak_length=max_streak,
    partition_by=partition_by
) }}
{% endmacro %}

{% macro predictive_streak_below(column_name, threshold_value, max_streak=10, partition_by='player_id') %}
{{ predictive_streak_window(
    column_name=column_name,
    threshold_value=threshold_value,
    comparison_operator='<',
    max_streak_length=max_streak,
    partition_by=partition_by
) }}
{% endmacro %}

-- Multi-Game Change Feature (Change over N games)
{% macro predictive_multi_delta_window(
    column_name, 
    start_lag=1, 
    end_lag=3,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3,
    change_type='absolute'
) %}
CASE 
    WHEN {{ sequence_column }} <= {{ end_lag }} THEN NULL
    ELSE ROUND(
        {% if change_type == 'absolute' %}
        LAG({{ column_name }}, {{ start_lag }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, {{ end_lag }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )
        {% elif change_type == 'slope' %}
        (LAG({{ column_name }}, {{ start_lag }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, {{ end_lag }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )) / NULLIF({{ end_lag - start_lag }}, 0)
        {% elif change_type == 'percentage' %}
        CASE 
            WHEN LAG({{ column_name }}, {{ end_lag }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ) = 0 THEN NULL
            ELSE ((LAG({{ column_name }}, {{ start_lag }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ) - LAG({{ column_name }}, {{ end_lag }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            )) / NULLIF(LAG({{ column_name }}, {{ end_lag }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ), 0)) * 100
        END
        {% endif %}
        ::NUMERIC, {{ precision }}
    )
END
{% endmacro %}

-- Performance Momentum Indicator (Recent vs Historical)
{% macro predictive_momentum_window(
    column_name,
    recent_window=3,
    historical_window=10,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} <= {{ historical_window }} THEN NULL
    ELSE ROUND(
        AVG({{ column_name }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ recent_window }} PRECEDING AND 1 PRECEDING
        ) - AVG({{ column_name }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ historical_window }} PRECEDING AND {{ recent_window + 1 }} PRECEDING
        ),
        {{ precision }}
    )
END
{% endmacro %}

-- Binary Performance State Lag Features
{% macro predictive_binary_lag_window(
    column_name,
    threshold_value,
    comparison_operator='>',
    lag_offset=1,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence'
) %}
CASE 
    WHEN {{ sequence_column }} <= {{ lag_offset }} THEN NULL
    ELSE 
        CASE 
            WHEN LAG({{ column_name }}, {{ lag_offset }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ) {{ comparison_operator }} {{ threshold_value }} THEN 1 
            ELSE 0 
        END
END
{% endmacro %}

-- Simplified Lag Feature Factory (PostgreSQL Safe)
{% macro generate_lag_features(stat_name, lag_offsets=[1, 2, 3], precision=3, partition_by='player_id') %}
-- Basic lag features
{% for lag in lag_offsets %}
{{ predictive_lag(stat_name, lag, precision, partition_by) }} as {{ stat_name }}_lag{{ lag }},
{% endfor %}

-- Delta features (absolute change)
{% for lag in lag_offsets %}
{{ predictive_delta(stat_name, lag, precision, partition_by) }} as {{ stat_name }}_delta{{ lag }},
{% endfor %}

-- Delta percentage features (only for lag 1 to avoid complexity)
{{ predictive_delta_pct(stat_name, 1, precision, partition_by) }} as {{ stat_name }}_delta_pct1
{% endmacro %}

-- NBA-Specific Shooting Performance Lag Features
{% macro generate_shooting_lag_features(makes_stat, attempts_stat, precision=3, partition_by='player_id') %}
-- Shooting percentage lag features
{% for lag in [1, 2, 3] %}
CASE 
    WHEN season_game_sequence <= {{ lag }} THEN NULL
    WHEN LAG({{ attempts_stat }}, {{ lag }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY game_date, game_id
    ) = 0 THEN NULL
    ELSE ROUND(
        (LAG({{ makes_stat }}, {{ lag }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY game_date, game_id
        ) / NULLIF(LAG({{ attempts_stat }}, {{ lag }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY game_date, game_id
        ), 0)) * 100,
        {{ precision }}
    )
END as {{ makes_stat }}_pct_lag{{ lag }},
{% endfor %}

-- Shot making momentum (recent vs historical percentage)
CASE 
    WHEN season_game_sequence <= 10 THEN NULL
    ELSE ROUND(
        (AVG({{ makes_stat }}::FLOAT / NULLIF({{ attempts_stat }}, 0)) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY game_date, game_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) - AVG({{ makes_stat }}::FLOAT / NULLIF({{ attempts_stat }}, 0)) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY game_date, game_id
            ROWS BETWEEN 10 PRECEDING AND 4 PRECEDING
        )) * 100,
        {{ precision }}
    )
END as {{ makes_stat }}_pct_momentum_3v10
{% endmacro %}

-- Performance Consistency Lag Features
{% macro generate_consistency_lag_features(stat_name, precision=3, partition_by='player_id') %}
-- Performance volatility (recent vs lag)
{% for lag in [1, 2, 3] %}
CASE 
    WHEN season_game_sequence <= {{ lag + 2 }} THEN NULL
    ELSE ROUND(
        ABS({{ stat_name }} - LAG({{ stat_name }}, {{ lag }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY game_date, game_id
        )),
        {{ precision }}
    )
END as {{ stat_name }}_volatility_lag{{ lag }},
{% endfor %}

-- Consistency trend (decreasing volatility = increasing consistency)
CASE 
    WHEN season_game_sequence <= 5 THEN NULL
    ELSE ROUND(
        STDDEV({{ stat_name }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY game_date, game_id
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) - STDDEV({{ stat_name }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY game_date, game_id
            ROWS BETWEEN 6 PRECEDING AND 4 PRECEDING
        ),
        {{ precision }}
    )
END as {{ stat_name }}_consistency_trend
{% endmacro %}