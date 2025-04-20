-- =============================================================================
-- CORE PARAMETERIZED MACROS
-- =============================================================================

-- =============================================================================
-- ROLLING METRICS MACRO
-- =============================================================================
{% macro rolling_metric(
    metric_col,              -- Required: Column to calculate metric for
    calc_type='avg',         -- Optional: Type of calculation to perform
    player_id_col='player_id', -- Optional: ID column for partitioning
    game_date_col='game_date', -- Optional: Date column for ordering
    window_size=5            -- Optional: Number of games in window
) %}
    /*
    Calculates rolling statistics for a given metric over a window of previous games.
    Always excludes current game to prevent data leakage in prediction models.

    Args:
        metric_col (str): Column containing the metric to calculate
        calc_type (str): Type of calculation to perform
            - 'avg': Average value (default)
            - 'stddev': Standard deviation
            - 'sum': Sum of values
            - 'max': Maximum value
            - 'min': Minimum value
            - 'median': Median value (50th percentile)
            - 'percentile_75': 75th percentile
            - 'percentile_25': 25th percentile
            - 'iqr': Interquartile range (75th - 25th percentile)
            - 'ewma': Exponentially weighted moving average
            - 'variance': Statistical variance
            - 'zscore': Z-score normalization
        player_id_col (str): Column for partitioning data by player
        game_date_col (str): Column for ordering data chronologically
        window_size (int): Number of previous games to include

    Returns:
        SQL expression calculating the specified rolling metric

    Example:
        {{ rolling_metric('points', 'stddev', window_size=10) }} as last_10_game_point_stddev
    */

    -- Input validation
    {% if not metric_col %}
        {{ exceptions.raise_compiler_error("metric_col is required") }}
    {% endif %}

    -- Calculation implementation
    {% if calc_type == 'avg' %}
        avg({{ metric_col }})
    {% elif calc_type == 'stddev' %}
        stddev({{ metric_col }})
    {% elif calc_type == 'sum' %}
        sum({{ metric_col }})
    {% elif calc_type == 'max' %}
        max({{ metric_col }})
    {% elif calc_type == 'min' %}
        min({{ metric_col }})
    {% elif calc_type == 'count' %}
        count({{ metric_col }})
    {% elif calc_type == 'median' %}
        percentile_cont(0.5) within group (order by {{ metric_col }})
    {% elif calc_type == 'percentile_75' %}
        percentile_cont(0.75) within group (order by {{ metric_col }})
    {% elif calc_type == 'percentile_25' %}
        percentile_cont(0.25) within group (order by {{ metric_col }})
    {% elif calc_type == 'iqr' %}
        percentile_cont(0.75) within group (order by {{ metric_col }}) - 
        percentile_cont(0.25) within group (order by {{ metric_col }})
    {% elif calc_type == 'ewma' %}
        avg({{ metric_col }} * exp(-0.1 * 
            row_number() over (
                partition by {{ player_id_col }} 
                order by {{ game_date_col }} desc
            )
        ))
    {% elif calc_type == 'variance' %}
        variance({{ metric_col }})
    {% elif calc_type == 'zscore' %}
        ({{ metric_col }} - avg({{ metric_col }})) / nullif(stddev({{ metric_col }}), 0)
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported calc_type: " ~ calc_type) }}
    {% endif %} over (
        partition by {{ player_id_col }}
        order by {{ game_date_col }}
        rows between {{ window_size }} preceding and 1 preceding
    )
{% endmacro %}

-- =============================================================================
-- SHOOTING METRICS MACRO
-- =============================================================================
{% macro shooting_metric(
    calc_type,               -- Required: Type of shooting metric to calculate
    fgm_col='fgm',           -- Optional: Field goals made column
    fga_col='fga',           -- Optional: Field goal attempts column
    fg3m_col='fg3m',         -- Optional: Three pointers made column
    fg3a_col='fg3a',         -- Optional: Three pointer attempts column
    ftm_col='ftm',           -- Optional: Free throws made column
    fta_col='fta',           -- Optional: Free throw attempts column
    pts_col='points',        -- Optional: Points column
    player_id_col='player_id', -- Optional: Player ID column
    game_date_col='game_date', -- Optional: Game date column
    window_size=5            -- Optional: Window size for calculation
) %}
    /*
    Calculates various shooting efficiency metrics over a specified window of games.
    
    Args:
        calc_type (str): Type of shooting metric to calculate
            - 'fg_pct': Field goal percentage (FGM/FGA)
            - 'fg3_pct': Three-point percentage (3PM/3PA)
            - 'ft_pct': Free throw percentage (FTM/FTA)
            - 'efg_pct': Effective field goal percentage ((FGM + 0.5*3PM)/FGA)
            - 'ts_pct': True shooting percentage (PTS/(2*(FGA + 0.44*FTA)))
            - 'shot_distribution': Percentage of shots from 3-point range (3PA/FGA)
        fgm_col (str): Field goals made column
        fga_col (str): Field goal attempts column
        fg3m_col (str): Three pointers made column
        fg3a_col (str): Three pointer attempts column
        ftm_col (str): Free throws made column
        fta_col (str): Free throw attempts column
        pts_col (str): Points column
        player_id_col (str): Player ID column for partitioning
        game_date_col (str): Game date column for ordering
        window_size (int): Number of previous games to include
        
    Returns:
        SQL expression calculating the specified shooting metric
        
    Example:
        {{ shooting_metric('ts_pct', window_size=10) }} as ts_pct_10g
    */
    
    -- Input validation
    {% if not calc_type %}
        {{ exceptions.raise_compiler_error("calc_type is required") }}
    {% endif %}
    
    -- Calculation implementation
    {% if calc_type == 'fg_pct' %}
        sum({{ fgm_col }}) / nullif(sum({{ fga_col }}), 0)
    {% elif calc_type == 'fg3_pct' %}
        sum({{ fg3m_col }}) / nullif(sum({{ fg3a_col }}), 0)
    {% elif calc_type == 'ft_pct' %}
        sum({{ ftm_col }}) / nullif(sum({{ fta_col }}), 0)
    {% elif calc_type == 'efg_pct' %}
        (sum({{ fgm_col }}) + 0.5 * sum({{ fg3m_col }})) / nullif(sum({{ fga_col }}), 0)
    {% elif calc_type == 'ts_pct' %}
        sum({{ pts_col }}) / nullif(2 * (sum({{ fga_col }}) + 0.44 * sum({{ fta_col }})), 0)
    {% elif calc_type == 'shot_distribution' %}
        sum({{ fg3a_col }}) / nullif(sum({{ fga_col }}), 0)
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported calc_type: " ~ calc_type) }}
    {% endif %} over (
        partition by {{ player_id_col }}
        order by {{ game_date_col }}
        rows between {{ window_size }} preceding and 1 preceding
    )
{% endmacro %}

-- =============================================================================
-- SPECIALIZED MACROS
-- =============================================================================

-- =============================================================================
-- USAGE RATE MACRO
-- =============================================================================
{% macro usage_rate(
    fga_col='fga',           -- Optional: Field goal attempts column
    fta_col='fta',           -- Optional: Free throw attempts column
    to_col='turnovers',      -- Optional: Turnovers column
    team_poss_col='team_possessions', -- Optional: Team possessions column
    player_id_col='player_id', -- Optional: Player ID column
    game_date_col='game_date', -- Optional: Game date column
    window_size=5            -- Optional: Window size for calculation
) %}
    /*
    Calculates player usage rate over specified window of games.
    Usage rate estimates percentage of team plays used by a player while on the floor.
    Formula: (FGA + 0.44 * FTA + TO) / Team Possessions * 100

    Args:
        fga_col (str): Field goal attempts column
        fta_col (str): Free throw attempts column
        to_col (str): Turnovers column
        team_poss_col (str): Team possessions column
        player_id_col (str): Player ID column for partitioning
        game_date_col (str): Game date column for ordering
        window_size (int): Number of previous games to include

    Returns:
        SQL expression calculating player usage rate as a percentage

    Example:
        {{ usage_rate(window_size=10) }} as usage_rate_10g
    */

    sum({{ fga_col }} + 0.44 * {{ fta_col }} + {{ to_col }}) * 100.0 /
        nullif(sum({{ team_poss_col }}), 0) over (
            partition by {{ player_id_col }}
            order by {{ game_date_col }}
            rows between {{ window_size }} preceding and 1 preceding
        )
{% endmacro %}

-- =============================================================================
-- PLAYER EFFICIENCY RATING (PER) MACRO
-- =============================================================================
{% macro player_efficiency_rating(
    pts_col='points',         -- Points column
    reb_col='total_rebounds', -- Rebounds column
    ast_col='assists',        -- Assists column
    stl_col='steals',         -- Steals column
    blk_col='blocks',         -- Blocks column
    fga_col='fga',            -- Field goal attempts
    fgm_col='fgm',            -- Field goals made
    fta_col='fta',            -- Free throw attempts
    ftm_col='ftm',            -- Free throws made
    to_col='turnovers',       -- Turnovers
    pf_col='personal_fouls',  -- Personal fouls
    min_col='minutes',        -- Minutes played
    player_id_col='player_id', -- Player ID column
    game_date_col='game_date', -- Game date column
    window_size=5,            -- Window size for calculation
    league_factor=15.0        -- League adjustment factor (typically ~15)
) %}
    /*
    Calculates Player Efficiency Rating (PER) over specified window of games.
    PER is a complex metric developed by John Hollinger to sum up all a player's
    positive contributions, subtract negative ones, and return a per-minute rating.
    
    This is a simplified version of the full PER calculation.

    Args:
        pts_col (str): Points column
        reb_col (str): Rebounds column
        ast_col (str): Assists column
        stl_col (str): Steals column
        blk_col (str): Blocks column
        fga_col (str): Field goal attempts column
        fgm_col (str): Field goals made column
        fta_col (str): Free throw attempts column
        ftm_col (str): Free throws made column
        to_col (str): Turnovers column
        pf_col (str): Personal fouls column
        min_col (str): Minutes played column
        player_id_col (str): Player ID column for partitioning
        game_date_col (str): Game date column for ordering
        window_size (int): Number of previous games to include
        league_factor (float): League adjustment factor (typically ~15)

    Returns:
        SQL expression calculating PER

    Example:
        {{ player_efficiency_rating(window_size=10) }} as per_10g
    */

    -- Simplified PER formula for dbt
    (
        sum({{ pts_col }}) +
        sum({{ reb_col }}) * 0.8 +
        sum({{ ast_col }}) * 0.7 +
        sum({{ stl_col }}) * 1.0 +
        sum({{ blk_col }}) * 0.7 -
        sum({{ fga_col }} - {{ fgm_col }}) * 0.7 -
        sum({{ fta_col }} - {{ ftm_col }}) * 0.4 -
        sum({{ to_col }}) * 1.0 -
        sum({{ pf_col }}) * 0.4
    ) * ({{ league_factor }} / 
        nullif(sum({{ min_col }}), 0)
    ) over (
        partition by {{ player_id_col }}
        order by {{ game_date_col }}
        rows between {{ window_size }} preceding and 1 preceding
    )
{% endmacro %}

-- =============================================================================
-- UTILITY MACROS
-- =============================================================================

-- =============================================================================
-- NULL HANDLING MACRO
-- =============================================================================
{% macro util_handle_nulls(
    column_name,            -- Required: Column to handle nulls for
    strategy='zero',        -- Optional: Strategy for handling nulls
    default_value=0         -- Optional: Default value when using 'value' strategy
) %}
    /*
    Handles NULL values in a column using the specified strategy.
    
    Args:
        column_name (str): Column to handle nulls for
        strategy (str): Strategy for handling nulls
            - 'zero': Replace NULLs with zero (default)
            - 'value': Replace NULLs with specified default_value
            - 'avg': Replace NULLs with column average
            - 'min': Replace NULLs with column minimum
            - 'max': Replace NULLs with column maximum
            - 'median': Replace NULLs with column median
        default_value: Default value when using 'value' strategy
        
    Returns:
        SQL expression with NULL handling applied
        
    Example:
        {{ util_handle_nulls('points', 'avg') }} as points_nulls_handled
    */
    
    {% if strategy == 'zero' %}
        coalesce({{ column_name }}, 0)
    {% elif strategy == 'value' %}
        coalesce({{ column_name }}, {{ default_value }})
    {% elif strategy == 'avg' %}
        coalesce({{ column_name }}, avg({{ column_name }}) over ())
    {% elif strategy == 'min' %}
        coalesce({{ column_name }}, min({{ column_name }}) over ())
    {% elif strategy == 'max' %}
        coalesce({{ column_name }}, max({{ column_name }}) over ())
    {% elif strategy == 'median' %}
        coalesce({{ column_name }}, 
            percentile_cont(0.5) within group (order by {{ column_name }}) over ()
        )
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported strategy: " ~ strategy) }}
    {% endif %}
{% endmacro %}

-- =============================================================================
-- DATE FUNCTIONS MACRO
-- =============================================================================
{% macro util_date_diff(
    start_date_col,         -- Required: Start date column
    end_date_col,           -- Required: End date column
    unit='day'              -- Optional: Unit for difference calculation
) %}
    /*
    Calculates the difference between two dates in the specified unit.
    
    Args:
        start_date_col (str): Starting date column
        end_date_col (str): Ending date column
        unit (str): Unit for difference calculation
            - 'day': Days (default)
            - 'week': Weeks
            - 'month': Months
            - 'year': Years
            
    Returns:
        SQL expression calculating date difference
        
    Example:
        {{ util_date_diff('last_game_date', 'game_date', 'day') }} as days_rest
    */
    
    {% if unit == 'day' %}
        extract(day from ({{ end_date_col }} - {{ start_date_col }}))
    {% elif unit == 'week' %}
        extract(day from ({{ end_date_col }} - {{ start_date_col }})) / 7
    {% elif unit == 'month' %}
        (extract(year from {{ end_date_col }}) - extract(year from {{ start_date_col }})) * 12 +
        (extract(month from {{ end_date_col }}) - extract(month from {{ start_date_col }}))
    {% elif unit == 'year' %}
        extract(year from {{ end_date_col }}) - extract(year from {{ start_date_col }})
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported unit: " ~ unit) }}
    {% endif %}
{% endmacro %}

-- =============================================================================
-- TEST MACROS
-- =============================================================================

-- =============================================================================
-- METRIC RANGE TEST MACRO
-- =============================================================================
{% test metric_in_range(model, column_name, min_val, max_val) %}
    /*
    Tests whether values in a column fall within the specified range.
    
    Args:
        model: dbt model to test
        column_name (str): Column to test
        min_val (numeric): Minimum acceptable value
        max_val (numeric): Maximum acceptable value
        
    Returns:
        Query that returns rows failing the test
        
    Example:
        - name: field_goal_pct
          tests:
            - metric_in_range:
                min_val: 0
                max_val: 1
    */
    
    select 
        *
    from {{ model }}
    where 
        {{ column_name }} is not null
        and (
            {{ column_name }} < {{ min_val }}
            or {{ column_name }} > {{ max_val }}
        )
{% endtest %}