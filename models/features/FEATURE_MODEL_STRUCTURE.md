# Feature Model Structure Blueprint (models/features/)

This document outlines the structured approach for developing and organizing dbt models within the `features` schema. The goal is to create a modular, maintainable, and understandable feature engineering pipeline that progressively builds complex features from foundational data.

We will follow a three-tiered layering system within the `features` schema:

1.  **Base Layer (`models/features/<entity>/base/`)**
2.  **Derived Layer (`models/features/<entity>/derived/`)**
3.  **Aggregates/Profiles Layer (`models/features/<entity>/aggregates/` or `models/features/<entity>/profiles/`)**

This mirrors the broader `staging -> intermediate -> marts` concept but is applied specifically *within* the `features` schema to construct rich, analytical datasets.

---

## 1. Base Layer

**Directory Structure:** `models/features/<entity>/base/`

**Purpose:**
The Base Layer is responsible for creating foundational features directly from `staging` or `intermediate` models. These models perform initial calculations, generate commonly used statistics, and prepare data for more complex derivations. The features created here should be relatively straightforward and often serve as building blocks for multiple downstream models.

**Characteristics:**
*   Directly references `staging` or `intermediate` models (e.g., `ref('stg_...')`, `ref('int_...')`).
*   Calculates fundamental metrics, often time-windowed.
*   Minimal complex business logic; focus is on direct calculations and transformations.
*   Outputs are often granular (e.g., per player per game).

**Examples of Models/Features:**
*   **Rolling Statistics:**
    *   `feat_player__base_traditional_rolling.sql`: Calculating `pts_roll_3g_avg`, `min_roll_10g_avg`, `fg_pct_roll_5g_stddev`.
    *   `feat_team__base_advanced_rolling.sql`: Calculating `team_off_rating_roll_10g_avg`, `team_pace_roll_5g_avg`.
*   **Basic Ratios/Metrics (per event/entity):**
    *   `feat_game__base_event_metrics.sql`: Calculating `points_per_minute` for a specific game (if not already in an intermediate model).
    *   `feat_player__base_game_flags.sql`: Identifying if a player had a double-double in a game.
*   **Lagged Features:**
    *   `feat_player__base_lagged_performance.sql`: Creating `lag_pts_1_game`, `lag_usage_3_games_avg`.

**Materialization:**
*   Typically `incremental` or `view`. If the computations are expensive and used by many downstream models, `table` might be considered.

---

## 2. Derived Layer

**Directory Structure:** `models/features/<entity>/derived/`

**Purpose:**
The Derived Layer builds upon the features created in the Base Layer (or other Derived Layer models). Models in this layer implement more complex business logic, combine multiple base features, create composite scores, or calculate features that require interaction or comparison between entities.

**Characteristics:**
*   References models from the Base Layer (`ref('feat_<entity>__base_...')`) or other Derived Layer models.
*   Involves more complex calculations, conditional logic, and feature engineering techniques.
*   May involve joins between different types of base features (e.g., player rolling stats joined with team rolling stats).
*   Features are more specific and tailored towards particular analytical questions or model requirements.

**Examples of Models/Features:**
*   **Interaction/Comparison Features:**
    *   `feat_player__derived_vs_opponent_base_stats.sql`: Calculating a player's rolling average points vs. an opponent's average points allowed (using Base layer features from both player and opponent).
    *   `feat_team__derived_strength_of_schedule.sql`: Calculating a team's strength of schedule based on the rolling performance of their past opponents (from Base layer).
*   **Composite Scores/Indexes:**
    *   `feat_player__derived_scoring_efficiency_index.sql`: Combining `ts_pct`, `efg_pct`, and `usage_pct` from the Base layer into a single efficiency score.
    *   `feat_team__derived_momentum_score.sql`: Combining recent win percentages, point differentials, and offensive/defensive rating trends.
*   **Contextual Features:**
    *   `feat_player__derived_performance_in_b2b.sql`: Analyzing a player's performance (from Base layer) specifically in back-to-back game scenarios.
    *   `feat_game__derived_pace_delta.sql`: Difference between the home team's average pace and the away team's average pace (from Base layer team stats).
*   **Rate-based metrics requiring normalization:**
    *   `feat_player__derived_points_per_100_poss.sql`: Normalizing player points by team possessions.

**Materialization:**
*   Often `incremental` or `table` as these features can be computationally intensive and are key inputs for the final aggregate/profile models.

---

## 3. Aggregates/Profiles Layer

**Directory Structure:** `models/features/<entity>/aggregates/` or `models/features/<entity>/profiles/`

**Purpose:**
The Aggregates/Profiles Layer is the final stage within the `features` schema. Models in this layer consume features from the Base and/or Derived layers to create comprehensive, often denormalized, datasets. These models are specifically designed to be the direct input for downstream applications, such as ML training models, BI dashboards, or specific analytical reports.

**Characteristics:**
*   References models from the Derived Layer (`ref('feat_<entity>__derived_...')`) and potentially the Base Layer.
*   Aggregates features to the desired granularity (e.g., player-game, player-season-opponent).
*   Often involves joining multiple feature sets together to create a wide table.
*   Represents a "ready-to-use" feature set for a specific purpose.
*   May perform final data type casting, renaming for clarity, or selection of the most relevant features.

**Examples of Models/Features:**
*   **Comprehensive Player vs. Opponent Profile:**
    *   `player_game_matchup_features_v1.sql` (as currently designed): Combines a player's historical performance against an opponent, the opponent's defensive profile against the player's position, recent team context, and the player's own recent form.
*   **Player Pregame Profile for Prediction:**
    *   A model that joins player rolling stats (Base), derived efficiency metrics (Derived), team context (Derived), and opponent profile (Derived/Aggregates) into a single row per player per game, ready for a prediction model.
*   **Team Strength Profile:**
    *   `feat_team__aggregate_strength_profile.sql`: A model summarizing a team's offensive, defensive, and contextual strengths based on various base and derived features, aggregated per team per game date.

**Materialization:**
*   Almost always `table` or `incremental` to ensure performance and stability for downstream consumption. These are the key output tables of your feature engineering process.

---

## General Guidelines:

*   **Naming:** Strive for clear and consistent naming that reflects the layer and purpose (e.g., `feat_player__base_rolling_stats.sql`, `feat_player__derived_efficiency_metrics.sql`, `feat_player__profile_pregame.sql`).
*   **CTEs:** Use CTEs *within* each model to maintain readability and logical flow for that model's specific task.
*   **Modularity:** If a piece of logic is complex and reusable, consider making it its own model in the appropriate layer rather than duplicating it.
*   **Documentation:** Ensure all models and critical columns are documented in `_schema.yml` files within their respective directories.
*   **Testing:** Implement tests at each layer to ensure data quality and correctness.

This layered approach provides a robust framework for developing and managing features, making your dbt project more scalable and easier for team members (and future AI agents!) to understand and contribute to.
