# Zephyrus NBA Analytics Implementation Guide

This guide provides a phased approach to refactoring the Zephyrus NBA Analytics project, focusing on systematic implementation while maintaining functionality.

## Phase 1: Foundation Setup

### 1.1. Set Up Project Structure

```bash
# Create necessary directories
mkdir -p macros/core
mkdir -p macros/specialized
mkdir -p macros/utils
mkdir -p macros/tests
mkdir -p models/staging
mkdir -p models/intermediate
mkdir -p models/ml_features
mkdir -p docs/features
mkdir -p docs/metrics
```

### 1.2. Implement Core Utility Macros

Start by implementing utility macros that will be used throughout the project.

1. Create `macros/utils/null_handling.sql`
   ```sql
   {% macro util_handle_nulls(
       column_name,
       strategy='zero',
       default_value=0
   ) %}
       /* Macro implementation */
   {% endmacro %}
   ```

2. Create `macros/utils/date_functions.sql`
   ```sql
   {% macro util_date_diff(
       start_date_col,
       end_date_col,
       unit='day'
   ) %}
       /* Macro implementation */
   {% endmacro %}
   ```

3. Create `macros/utils/normalization.sql`
   ```sql
   {% macro util_normalize(
       column_name,
       method='zscore'
   ) %}
       /* Macro implementation */
   {% endmacro %}
   ```

### 1.3. Implement Core Parameterized Macros

Implement the core calculation macros that will be used extensively.

1. Create `macros/core/rolling_metrics.sql`
   ```sql
   {% macro rolling_metric(
       metric_col,
       calc_type='avg',
       player_id_col='player_id',
       game_date_col='game_date',
       window_size=5
   ) %}
       /* Macro implementation */
   {% endmacro %}
   ```

2. Create `macros/core/shooting_metrics.sql`
   ```sql
   {% macro shooting_metric(
       calc_type,
       fgm_col='fgm',
       fga_col='fga',
       /* Other parameters */
   ) %}
       /* Macro implementation */
   {% endmacro %}
   ```

## Phase 2: Specialized Metrics

### 2.1. Implement Advanced Basketball Metrics

1. Create `macros/specialized/advanced_metrics.sql`
   ```sql
   {% macro player_efficiency_rating(
       /* Parameters */
   ) %}
       /* Macro implementation */
   {% endmacro %}
   
   {% macro usage_rate(
       /* Parameters */
   ) %}
       /* Macro implementation */
   {% endmacro %}
   
   {% macro true_shooting_percentage(
       /* Parameters */
   ) %}
       /* Macro implementation */
   {% endmacro %}
   ```

2. Create `macros/specialized/momentum_features.sql`
   ```sql
   {% macro hot_cold_streak(
       /* Parameters */
   ) %}
       /* Macro implementation */
   {% endmacro %}
   
   {% macro streak_length(
       /* Parameters */
   ) %}
       /* Macro implementation */
   {% endmacro %}
   ```

### 2.2. Implement Test Macros

Create test macros to validate metric calculations.

1. Create `macros/tests/assert_value_range.sql`
   ```sql
   {% test metric_in_range(model, column_name, min_val, max_val) %}
       /* Test implementation */
   {% endtest %}
   ```

2. Create `macros/tests/assert_calculation.sql`
   ```sql
   {% test calculation_matches_reference(model, test_column, reference_column, tolerance=0.0001) %}
       /* Test implementation */
   {% endtest %}
   ```

## Phase 3: Model Implementation

### 3.1. Create Staging Models

Staging models perform initial cleaning and type casting.

1. Create `models/staging/stg_player_game_logs.sql`
   ```sql
   with source as (
       select * from {{ source('nba', 'player_game_logs') }}
   ),
   
   cleaned as (
       select
           player_id,
           game_id,
           game_date::date as game_date,
           /* Other columns with appropriate type casting */
       from source
   )
   
   select * from cleaned
   ```

2. Create `models/staging/schema.yml` with documentation and tests
   ```yaml
   version: 2
   
   models:
     - name: stg_player_game_logs
       description: "Cleaned player game logs with consistent data types"
       columns:
         - name: player_id
           description: "Unique player identifier"
           tests:
             - not_null
         /* Additional column documentation */
   ```

### 3.2. Create Intermediate Feature Models

Implement feature transformation models using the new macros.

1. Create `models/intermediate/int_player_features.sql`
   ```sql
   with player_games as (
       select * from {{ ref('stg_player_game_logs') }}
   ),
   
   player_features as (
       select
           player_id,
           game_id,
           game_date,
           
           -- Basic stats
           points,
           rebounds,
           assists,
           
           -- Rolling metrics
           {{ rolling_metric('points', 'avg', window_size=5) }} as points_avg_5g,
           {{ rolling_metric('points', 'stddev', window_size=5) }} as points_stddev_5g,
           
           -- Shooting metrics
           {{ shooting_metric('ts_pct', window_size=10) }} as ts_pct_10g,
           
           -- Advanced metrics
           {{ player_efficiency_rating(window_size=10) }} as per_10g
       from player_games
   )
   
   select * from player_features
   ```

### 3.3. Create ML Feature Models

Create models specifically designed for ML pipelines.

1. Create `models/ml_features/player_prediction_features.sql`
   ```sql
   with player_features as (
       select * from {{ ref('int_player_features') }}
   ),
   
   team_features as (
       select * from {{ ref('int_team_features') }}
   ),
   
   matchup_features as (
       select * from {{ ref('int_matchup_features') }}
   ),
   
   ml_features as (
       select
           /* Join player, team, and matchup features */
           
           -- Target variables (what we're predicting)
           p.points as target_points,
           p.assists as target_assists,
           
           -- Feature normalization
           {{ util_normalize('points_avg_5g') }} as points_avg_5g_norm
           /* Additional normalized features */
       from player_features p
       inner join team_features t on p.team_id = t.team_id and p.game_date = t.game_date
       inner join matchup_features m on p.player_id = m.player_id and p.opponent_id = m.opponent_id
   )
   
   select * from ml_features
   ```

## Phase 4: Documentation & Testing

### 4.1. Create Feature Catalog

Document all available features in a centralized catalog.

1. Create `docs/features/features_catalog.md`
   ```markdown
   # NBA Analytics Feature Catalog
   
   This document catalogs all available features in the Zephyrus NBA Analytics project.
   
   ## Player Performance Features
   
   | Feature Name | Description | Calculation | Typical Range |
   |--------------|-------------|-------------|---------------|
   | points_avg_5g | Average points over last 5 games | `{{ rolling_metric('points', 'avg', window_size=5) }}` | 0-40 |
   | ts_pct_10g | True shooting percentage over last 10 games | `{{ shooting_metric('ts_pct', window_size=10) }}` | 0.400-0.700 |
   ```

### 4.2. Implement Tests

Add tests for all models and macros.

1. Add tests to model schema files
   ```yaml
   version: 2
   
   models:
     - name: int_player_features
       description: "Player-level features for NBA analytics"
       columns:
         - name: points_avg_5g
           description: "Average points over last 5 games"
           tests:
             - metric_in_range:
                 min_val: 0
                 max_val: 100
   ```

2. Add dbt_project.yml test configurations
   ```yaml
   tests:
     +store_failures: true
     +severity: warn  # Start with warnings instead of errors
   ```

## Phase 5: Optimization & Refinement

### 5.1. Performance Optimization

Review and optimize macro implementations.

1. PostgreSQL-specific optimizations
   - Use `FILTER (WHERE...)` clause with aggregates
   - Leverage PostgreSQL-specific window functions
   - Use appropriate indexing hints

2. dbt-specific optimizations
   - Use `materialized: table` for complex intermediate calculations
   - Use `materialized: view` for simple transformations
   - Configure appropriate dbt configs for large models

### 5.2. Feature Refinement

Refine features based on domain expertise and model feedback.

1. Add more complex feature interactions
2. Implement feature selection based on importance metrics
3. Add time-based features (day of week, month, etc.)
4. Add matchup-specific features


## Incremental Migration Strategy

To migrate from the existing codebase while minimizing disruption:

1. Build the new framework in parallel with existing code
2. Create test models that compare outputs between old and new implementations
3. Migrate one model at a time, starting with less critical models
4. For each migration:
   - Implement the new version
   - Run tests to verify equivalence
   - Switch dependencies to the new version
5. Deprecate old macros and models only after all dependencies are migrated