-- Core Predictive Rate Calculation Macro (No Data Leakage)
{% macro predictive_rate_window(
    column_name, 
    window_size, 
    rate_type='linear',
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3,
    time_unit='games'
) %}
CASE 
    WHEN {{ sequence_column }} < {{ window_size + 1 }} THEN NULL
    ELSE ROUND(
        {% if rate_type == 'linear' %}
        -- Simple linear rate: (current - oldest) / window_size
        (LAG({{ column_name }}, 1) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, {{ window_size }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )) / NULLIF({{ window_size - 1 }}, 0)
        {% elif rate_type == 'slope' %}
        -- Regression slope rate using least squares approximation
        ({{ window_size }} * (
            SUM({{ column_name }} * ({{ sequence_column }} - 1)) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
            ) - 
            SUM({{ column_name }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
            ) * 
            SUM({{ sequence_column }} - 1) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
            ) / {{ window_size }}
        )) / NULLIF((
            {{ window_size }} * SUM(POWER({{ sequence_column }} - 1, 2)) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
            ) - 
            POWER(SUM({{ sequence_column }} - 1) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
            ), 2)
        ), 0)
        {% elif rate_type == 'weighted' %}
        -- Weighted rate (more recent games have higher impact)
        (2.0 * (LAG({{ column_name }}, 1) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, 2) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )) + 
        1.5 * (LAG({{ column_name }}, 2) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, 3) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )) + 
        1.0 * (LAG({{ column_name }}, 3) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, {{ window_size }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ))) / NULLIF((2.0 + 1.5 + 1.0), 0)
        {% elif rate_type == 'exponential' %}
        -- Exponential weighted rate
        (LAG({{ column_name }}, 1) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - 
        (0.7 * LAG({{ column_name }}, 2) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) + 0.3 * LAG({{ column_name }}, {{ window_size }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ))) / NULLIF({{ window_size - 1 }}, 0)
        {% endif %}
        ::NUMERIC, {{ precision }}
    )
END
{% endmacro %}

-- Simple Linear Rate Features
{% macro predictive_linear_rate(column_name, window_size, precision=3, partition_by='player_id') %}
{{ predictive_rate_window(
    column_name=column_name,
    window_size=window_size,
    rate_type='linear',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

-- Slope-Based Rate (More sophisticated trend detection)
{% macro predictive_slope_rate(column_name, window_size, precision=3, partition_by='player_id') %}
{{ predictive_rate_window(
    column_name=column_name,
    window_size=window_size,
    rate_type='slope',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

-- Weighted Rate (Recent performance weighted higher)
{% macro predictive_weighted_rate(column_name, window_size, precision=3, partition_by='player_id') %}
{{ predictive_rate_window(
    column_name=column_name,
    window_size=window_size,
    rate_type='weighted',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

-- Acceleration Feature (Rate of change in rate)
{% macro predictive_acceleration_window(
    column_name, 
    window_size, 
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} < {{ window_size * 2 }} THEN NULL
    ELSE ROUND(
        -- Current rate minus previous rate
        ((LAG({{ column_name }}, 1) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, {{ window_size }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )) / NULLIF({{ window_size - 1 }}, 0)) - 
        ((LAG({{ column_name }}, {{ window_size }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        ) - LAG({{ column_name }}, {{ window_size * 2 }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
        )) / NULLIF({{ window_size - 1 }}, 0)),
        {{ precision }}
    )
END
{% endmacro %}

-- Velocity Direction Indicator (Positive/Negative/Neutral trending)
{% macro predictive_velocity_direction(
    column_name, 
    window_size=5, 
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence'
) %}
CASE 
    WHEN {{ sequence_column }} < {{ window_size + 1 }} THEN NULL
    WHEN {{ predictive_linear_rate(column_name, window_size, 3, partition_by) }} > 0.1 THEN 1  -- Positive trend
    WHEN {{ predictive_linear_rate(column_name, window_size, 3, partition_by) }} < -0.1 THEN -1 -- Negative trend
    ELSE 0  -- Neutral/stable
END
{% endmacro %}

-- Rate Volatility (Consistency of rate changes)
{% macro predictive_rate_volatility(
    column_name, 
    window_size=5, 
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} < {{ window_size + 3 }} THEN NULL
    ELSE ROUND(
        STDDEV(
            ({{ column_name }} - LAG({{ column_name }}, 1) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ))
        ) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
        ),
        {{ precision }}
    )
END
{% endmacro %}

-- Performance Momentum Rate (Accelerating vs Decelerating)
{% macro predictive_momentum_rate(
    column_name, 
    short_window=3, 
    long_window=10,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} < {{ long_window + 1 }} THEN NULL
    ELSE ROUND(
        {{ predictive_linear_rate(column_name, short_window, precision, partition_by) }} -
        {{ predictive_linear_rate(column_name, long_window, precision, partition_by) }},
        {{ precision }}
    )
END
{% endmacro %}

-- Rate Stability Indicator (How consistent are the rates)
{% macro predictive_rate_stability(
    column_name, 
    window_size=7,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} < {{ window_size + 3 }} THEN NULL
    ELSE ROUND(
        1.0 / (1.0 + {{ predictive_rate_volatility(column_name, window_size, partition_by, order_by, sequence_column, precision) }}),
        {{ precision }}
    )
END
{% endmacro %}

-- Peak Rate Detection (Highest rate achieved in window)
{% macro predictive_peak_rate(
    column_name, 
    window_size=10,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} < {{ window_size + 1 }} THEN NULL
    ELSE ROUND(
        MAX(
            ({{ column_name }} - LAG({{ column_name }}, 1) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ))
        ) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
        ),
        {{ precision }}
    )
END
{% endmacro %}

-- Rate Persistence (How long has current rate direction continued)
{% macro predictive_rate_persistence(
    column_name, 
    window_size=5,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence'
) %}
CASE 
    WHEN {{ sequence_column }} < 3 THEN NULL
    ELSE
        -- Count consecutive games with same rate direction
        CASE 
            WHEN {{ predictive_linear_rate(column_name, window_size, 3, partition_by) }} > 0 THEN
                -- Count positive rate streak
                ROW_NUMBER() OVER (
                    PARTITION BY {{ partition_by }}, 
                    ({{ predictive_linear_rate(column_name, window_size, 3, partition_by) }} <= 0)::int
                    ORDER BY {{ order_by }}
                ) - 1
            WHEN {{ predictive_linear_rate(column_name, window_size, 3, partition_by) }} < 0 THEN
                -- Count negative rate streak  
                -(ROW_NUMBER() OVER (
                    PARTITION BY {{ partition_by }}, 
                    ({{ predictive_linear_rate(column_name, window_size, 3, partition_by) }} >= 0)::int
                    ORDER BY {{ order_by }}
                ) - 1)
            ELSE 0
        END
END
{% endmacro %}

-- Rate Reversal Detection (When trend changes direction)
{% macro predictive_rate_reversal(
    column_name, 
    window_size=5,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence'
) %}
CASE 
    WHEN {{ sequence_column }} < {{ window_size + 2 }} THEN NULL
    WHEN SIGN({{ predictive_linear_rate(column_name, window_size, 3, partition_by) }}) != 
         SIGN(LAG({{ predictive_linear_rate(column_name, window_size, 3, partition_by) }}) OVER (
             PARTITION BY {{ partition_by }}
             ORDER BY {{ order_by }}
         )) THEN 1
    ELSE 0
END
{% endmacro %}

-- Comprehensive Rate Feature Factory  
{% macro generate_rate_features(stat_name, window_sizes=[3, 5, 10], precision=3, partition_by='player_id') %}
-- Linear rate features across multiple windows
{% for window in window_sizes %}
{{ predictive_linear_rate(stat_name, window, precision, partition_by) }} as {{ stat_name }}_rate_linear_{{ window }}g,
{{ predictive_weighted_rate(stat_name, window, precision, partition_by) }} as {{ stat_name }}_rate_weighted_{{ window }}g,
{% endfor %}

-- Acceleration features (rate of change in rate)
{% for window in window_sizes %}
{{ predictive_acceleration_window(stat_name, window, partition_by, precision=precision) }} as {{ stat_name }}_acceleration_{{ window }}g,
{% endfor %}

-- Velocity direction and momentum
{{ predictive_velocity_direction(stat_name, 5, partition_by) }} as {{ stat_name }}_velocity_direction,
{{ predictive_momentum_rate(stat_name, 3, 10, partition_by, precision=precision) }} as {{ stat_name }}_momentum_rate_3v10,

-- Rate stability and volatility
{{ predictive_rate_volatility(stat_name, 7, partition_by, precision=precision) }} as {{ stat_name }}_rate_volatility,
{{ predictive_rate_stability(stat_name, 7, partition_by, precision=precision) }} as {{ stat_name }}_rate_stability,

-- Peak performance and persistence
{{ predictive_peak_rate(stat_name, 10, partition_by, precision=precision) }} as {{ stat_name }}_peak_rate_10g,
{{ predictive_rate_persistence(stat_name, 5, partition_by) }} as {{ stat_name }}_rate_persistence,

-- Rate reversal detection
{{ predictive_rate_reversal(stat_name, 5, partition_by) }} as {{ stat_name }}_rate_reversal
{% endmacro %}

-- NBA-Specific Shooting Rate Features
{% macro generate_shooting_rate_features(makes_stat, attempts_stat, precision=3, partition_by='player_id') %}
-- Shooting percentage rate features
{% for window in [3, 5, 10] %}
CASE 
    WHEN season_game_sequence < {{ window + 1 }} THEN NULL
    ELSE ROUND(
        (CASE 
            WHEN LAG({{ attempts_stat }}, 1) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY game_date, game_id
            ) > 0 THEN
                LAG({{ makes_stat }}, 1) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY game_date, game_id
                ) / LAG({{ attempts_stat }}, 1) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY game_date, game_id
                )
            ELSE NULL
        END - CASE 
            WHEN LAG({{ attempts_stat }}, {{ window }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY game_date, game_id
            ) > 0 THEN
                LAG({{ makes_stat }}, {{ window }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY game_date, game_id
                ) / LAG({{ attempts_stat }}, {{ window }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY game_date, game_id
                )
            ELSE NULL
        END) * 100 / NULLIF({{ window - 1 }}, 0),
        {{ precision }}
    )
END as {{ makes_stat }}_pct_rate_{{ window }}g,
{% endfor %}

-- Shot volume rate (attempts trending up/down)
{% for window in [3, 5, 10] %}
{{ predictive_linear_rate(attempts_stat, window, precision, partition_by) }} as {{ attempts_stat }}_volume_rate_{{ window }}g,
{% endfor %}

-- Hot/Cold shooting momentum
CASE 
    WHEN season_game_sequence < 8 THEN NULL
    ELSE
        CASE 
            WHEN {{ predictive_linear_rate(makes_stat ~ '_pct_calc', 3, precision, partition_by) }} > 2.0 THEN 1  -- Hot
            WHEN {{ predictive_linear_rate(makes_stat ~ '_pct_calc', 3, precision, partition_by) }} < -2.0 THEN -1 -- Cold
            ELSE 0  -- Neutral
        END
END as {{ makes_stat }}_hot_cold_indicator
{% endmacro %}

-- Usage Rate Efficiency Features
{% macro generate_usage_rate_features(usage_stat, production_stat, precision=3, partition_by='player_id') %}
-- Usage vs Production rate alignment
{% for window in [3, 5, 10] %}
({{ predictive_linear_rate(production_stat, window, precision, partition_by) }} - 
 {{ predictive_linear_rate(usage_stat, window, precision, partition_by) }})
as {{ usage_stat }}_vs_{{ production_stat }}_rate_gap_{{ window }}g,
{% endfor %}

-- Efficiency rate (production per unit of usage)
{% for window in [3, 5, 10] %}
CASE 
    WHEN {{ predictive_linear_rate(usage_stat, window, precision, partition_by) }} = 0 THEN NULL
    ELSE ROUND(
        {{ predictive_linear_rate(production_stat, window, precision, partition_by) }} / 
        NULLIF({{ predictive_linear_rate(usage_stat, window, precision, partition_by) }}, 0),
        {{ precision }}
    )
END as {{ production_stat }}_per_{{ usage_stat }}_rate_{{ window }}g,
{% endfor %}

-- Usage sustainability indicator
{{ predictive_rate_stability(usage_stat, 7, partition_by, precision=precision) }} as {{ usage_stat }}_sustainability
{% endmacro %}

-- Performance Breakout Detection (Sudden rate increases)
{% macro generate_breakout_rate_features(stat_name, precision=3, partition_by='player_id') %}
-- Breakout detection (significant positive rate change)
CASE 
    WHEN season_game_sequence < 8 THEN NULL
    WHEN {{ predictive_acceleration_window(stat_name, 3, partition_by, precision=precision) }} > 
         (AVG({{ predictive_acceleration_window(stat_name, 3, partition_by, precision=precision) }}) OVER (
             PARTITION BY {{ partition_by }}
             ORDER BY game_date, game_id
             ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
         ) + 1.5 * STDDEV({{ predictive_acceleration_window(stat_name, 3, partition_by, precision=precision) }}) OVER (
             PARTITION BY {{ partition_by }}
             ORDER BY game_date, game_id
             ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
         )) THEN 1
    ELSE 0
END as {{ stat_name }}_breakout_detected,

-- Regression detection (significant negative rate change)
CASE 
    WHEN season_game_sequence < 8 THEN NULL
    WHEN {{ predictive_acceleration_window(stat_name, 3, partition_by, precision=precision) }} < 
         (AVG({{ predictive_acceleration_window(stat_name, 3, partition_by, precision=precision) }}) OVER (
             PARTITION BY {{ partition_by }}
             ORDER BY game_date, game_id
             ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
         ) - 1.5 * STDDEV({{ predictive_acceleration_window(stat_name, 3, partition_by, precision=precision) }}) OVER (
             PARTITION BY {{ partition_by }}
             ORDER BY game_date, game_id
             ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
         )) THEN 1
    ELSE 0
END as {{ stat_name }}_regression_detected
{% endmacro %}