-- Core Predictive Ratio Calculation Macro (No Data Leakage)
{% macro predictive_ratio_window(
    numerator_column, 
    denominator_column, 
    window_size, 
    ratio_type='basic',
    multiplier=1,
    partition_by='player_id',
    order_by='game_date, game_id',
    sequence_column='season_game_sequence',
    precision=3
) %}
CASE 
    WHEN {{ sequence_column }} = 1 THEN NULL
    {% for i in range(2, window_size + 1) %}
    WHEN {{ sequence_column }} = {{ i }} THEN 
        CASE 
            WHEN SUM({{ denominator_column }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
            ) = 0 THEN NULL
            ELSE ROUND(
                {% if ratio_type == 'per_minute' %}
                (SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ), 0))
                {% elif ratio_type == 'per_36' %}
                ((SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ), 0)) * 36)
                {% elif ratio_type == 'per_100' %}
                ((SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ), 0)) * 100)
                {% elif ratio_type == 'percentage' %}
                ((SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ), 0)) * 100)
                {% else %}
                (SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ i - 1 }} PRECEDING AND 1 PRECEDING
                ), 0)) * {{ multiplier }}
                {% endif %}
                ::NUMERIC, {{ precision }}
            )
        END
    {% endfor %}
    WHEN {{ sequence_column }} >= {{ window_size + 1 }} THEN 
        CASE 
            WHEN SUM({{ denominator_column }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
            ) = 0 THEN NULL
            ELSE ROUND(
                {% if ratio_type == 'per_minute' %}
                (SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ), 0))
                {% elif ratio_type == 'per_36' %}
                ((SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ), 0)) * 36)
                {% elif ratio_type == 'per_100' %}
                ((SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ), 0)) * 100)
                {% elif ratio_type == 'percentage' %}
                ((SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ), 0)) * 100)
                {% else %}
                (SUM({{ numerator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ) / NULLIF(SUM({{ denominator_column }}) OVER (
                    PARTITION BY {{ partition_by }}
                    ORDER BY {{ order_by }}
                    ROWS BETWEEN {{ window_size }} PRECEDING AND 1 PRECEDING
                ), 0)) * {{ multiplier }}
                {% endif %}
                ::NUMERIC, {{ precision }}
            )
        END
    ELSE NULL
END
{% endmacro %}

-- Specialized Predictive Ratio Macros
{% macro predictive_per_minute_ratio(numerator_column, denominator_column, window_size, precision=3, partition_by='player_id') %}
{{ predictive_ratio_window(
    numerator_column=numerator_column,
    denominator_column=denominator_column,
    window_size=window_size,
    ratio_type='per_minute',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

{% macro predictive_per_36_ratio(numerator_column, denominator_column, window_size, precision=3, partition_by='player_id') %}
{{ predictive_ratio_window(
    numerator_column=numerator_column,
    denominator_column=denominator_column,
    window_size=window_size,
    ratio_type='per_36',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

{% macro predictive_per_100_ratio(numerator_column, denominator_column, window_size, precision=3, partition_by='player_id') %}
{{ predictive_ratio_window(
    numerator_column=numerator_column,
    denominator_column=denominator_column,
    window_size=window_size,
    ratio_type='per_100',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

{% macro predictive_percentage_ratio(numerator_column, denominator_column, window_size, precision=3, partition_by='player_id') %}
{{ predictive_ratio_window(
    numerator_column=numerator_column,
    denominator_column=denominator_column,
    window_size=window_size,
    ratio_type='percentage',
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

-- Efficiency Ratio Macros (NBA-Specific)
{% macro predictive_shooting_efficiency(makes_column, attempts_column, window_size, precision=3, partition_by='player_id') %}
{{ predictive_percentage_ratio(
    numerator_column=makes_column,
    denominator_column=attempts_column,
    window_size=window_size,
    precision=precision,
    partition_by=partition_by
) }}
{% endmacro %}

{% macro predictive_ast_to_tov_ratio(ast_column, tov_column, window_size, precision=3, partition_by='player_id') %}
{{ predictive_ratio_window(
    numerator_column=ast_column,
    denominator_column=tov_column,
    window_size=window_size,
    ratio_type='basic',
    multiplier=1,
    partition_by=partition_by,
    precision=precision
) }}
{% endmacro %}

-- Multi-Window Multi-Ratio Feature Factory
{% macro generate_ratio_features(stat_name, denominator_stat, window_sizes=[3, 5, 10], ratio_types=['per_minute', 'per_36', 'per_100']) %}
{% for window in window_sizes %}
    {% for ratio_type in ratio_types %}
        {% if ratio_type == 'per_minute' %}
        {{ predictive_per_minute_ratio(stat_name, denominator_stat, window) }} as {{ stat_name }}_per_min_l{{ window }},
        {% elif ratio_type == 'per_36' %}
        {{ predictive_per_36_ratio(stat_name, denominator_stat, window) }} as {{ stat_name }}_per_36_l{{ window }},
        {% elif ratio_type == 'per_100' %}
        {{ predictive_per_100_ratio(stat_name, denominator_stat, window) }} as {{ stat_name }}_per_100_l{{ window }},
        {% elif ratio_type == 'percentage' %}
        {{ predictive_percentage_ratio(stat_name, denominator_stat, window) }} as {{ stat_name }}_pct_l{{ window }},
        {% endif %}
    {% endfor %}
{% endfor %}

-- Ratio trend features (short vs long window comparisons)
{{ predictive_per_36_ratio(stat_name, denominator_stat, 3) }} - {{ predictive_per_36_ratio(stat_name, denominator_stat, 10) }} as {{ stat_name }}_per_36_trend_3v10,
{{ predictive_per_36_ratio(stat_name, denominator_stat, 5) }} - {{ predictive_per_36_ratio(stat_name, denominator_stat, 10) }} as {{ stat_name }}_per_36_trend_5v10
{% endmacro %}

-- Comprehensive NBA Efficiency Feature Generator
{% macro generate_efficiency_features(stat_name, window_sizes=[3, 5, 10]) %}
-- Shooting efficiency features
{% if stat_name == 'fg' %}
    {% for window in window_sizes %}
    {{ predictive_shooting_efficiency('fgm', 'fga', window) }} as fg_pct_l{{ window }},
    {% endfor %}
{% elif stat_name == 'fg3' %}
    {% for window in window_sizes %}
    {{ predictive_shooting_efficiency('fg3m', 'fg3a', window) }} as fg3_pct_l{{ window }},
    {% endfor %}
{% elif stat_name == 'ft' %}
    {% for window in window_sizes %}
    {{ predictive_shooting_efficiency('ftm', 'fta', window) }} as ft_pct_l{{ window }},
    {% endfor %}
{% endif %}

-- Efficiency trend features
{% if stat_name == 'fg' %}
{{ predictive_shooting_efficiency('fgm', 'fga', 3) }} - {{ predictive_shooting_efficiency('fgm', 'fga', 10) }} as fg_pct_trend_3v10,
{{ predictive_shooting_efficiency('fgm', 'fga', 5) }} - {{ predictive_shooting_efficiency('fgm', 'fga', 10) }} as fg_pct_trend_5v10
{% elif stat_name == 'fg3' %}
{{ predictive_shooting_efficiency('fg3m', 'fg3a', 3) }} - {{ predictive_shooting_efficiency('fg3m', 'fg3a', 10) }} as fg3_pct_trend_3v10,
{{ predictive_shooting_efficiency('fg3m', 'fg3a', 5) }} - {{ predictive_shooting_efficiency('fg3m', 'fg3a', 10) }} as fg3_pct_trend_5v10
{% elif stat_name == 'ft' %}
{{ predictive_shooting_efficiency('ftm', 'fta', 3) }} - {{ predictive_shooting_efficiency('ftm', 'fta', 10) }} as ft_pct_trend_3v10,
{{ predictive_shooting_efficiency('ftm', 'fta', 5) }} - {{ predictive_shooting_efficiency('ftm', 'fta', 10) }} as ft_pct_trend_5v10
{% endif %}
{% endmacro %}

-- Advanced NBA Ratio Factory (Multiple stats, multiple denominators)
{% macro generate_advanced_ratio_features(stats_list, window_sizes=[3, 5, 10]) %}
{% for stat in stats_list %}
    -- Per-minute ratios
    {% for window in window_sizes %}
    {{ predictive_per_minute_ratio(stat, 'min', window) }} as {{ stat }}_per_min_l{{ window }},
    {{ predictive_per_36_ratio(stat, 'min', window) }} as {{ stat }}_per_36_l{{ window }},
    {% endfor %}
    
    -- Per-possession ratios (if possessions available)
    {% for window in window_sizes %}
    {{ predictive_per_100_ratio(stat, 'possessions', window) }} as {{ stat }}_per_100_l{{ window }},
    {% endfor %}
    
    -- Trend features
    {{ predictive_per_36_ratio(stat, 'min', 3) }} - {{ predictive_per_36_ratio(stat, 'min', 10) }} as {{ stat }}_per_36_trend_3v10
    {%- if not loop.last -%},{%- endif %}
{% endfor %}
{% endmacro %}

-- Usage Rate vs Production Efficiency
{% macro generate_usage_efficiency_ratios(window_sizes=[3, 5, 10]) %}
{% for window in window_sizes %}
-- Points per touch efficiency
{{ predictive_ratio_window('pts', 'touches', window, 'basic', 1) }} as pts_per_touch_l{{ window }},

-- Assists per 100 touches (playmaking efficiency)  
{{ predictive_ratio_window('ast', 'touches', window, 'per_100', 100) }} as ast_per_100_touches_l{{ window }},

-- Turnovers per 100 touches (ball security)
{{ predictive_ratio_window('tov', 'touches', window, 'per_100', 100) }} as tov_per_100_touches_l{{ window }},

-- True shooting attempts per minute (shot selection aggression)
{{ predictive_per_minute_ratio('fga + (0.44 * fta)', 'min', window) }} as tsa_per_min_l{{ window }}
{%- if not loop.last -%},{%- endif %}
{% endfor %}
{% endmacro %}