{{ config(
    schema='marts',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['player_id', 'team_id', 'season_year'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['team_id', 'game_date']}
    ],
    tags=['marts', 'team', 'features', 'prediction']
) }}

-- This model aggregates player-specific pre-game context with lagged team performance features
-- to create a feature set for predicting player performance in an upcoming game.
-- All team-based features are lagged to represent the state *before* the game_date in question.

WITH player_games_spine AS (
    -- This CTE defines the universe of player-games for which we are generating features.
    -- It includes pre-game player-specific context and projections.
    -- It is assumed that feat_player__contextual_projections provides data that is inherently pre-game
    -- or has already been appropriately lagged.
    SELECT
        player_game_key,
        player_id,
        player_name,
        team_id,
        game_id,
        game_date,
        season_year,
        home_away,
        position,
        player_offensive_role,
        player_team_style_fit_score,
        pace_impact_on_player,
        usage_opportunity,
        team_form_player_impact,
        team_playstyle_stat_impact,
        pts_in_team_hot_streaks,
        pts_in_team_cold_streaks,
        pts_in_star_dominant_system,
        pts_in_balanced_system,
        pace_multiplier,
        usage_multiplier,
        team_form_multiplier,
        playstyle_stat_multiplier,
        style_fit_multiplier,
        team_adjusted_pts_projection,
        team_adjusted_reb_projection,
        team_adjusted_ast_projection,
        team_context_impact
    FROM {{ ref('feat_player__contextual_projections') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '21 days' FROM {{ this }}) -- Using a slightly larger window for robustness
    {% endif %}
),

team_game_dates AS (
    -- Identifies the previous game date for each team to fetch lagged stats.
    -- This ensures that features for a game on 'game_date' are based on data available *before* that game.
    SELECT
        team_id,
        game_date,
        season_year, -- Important for partitioning LAG correctly and joining to season-based percentiles
        LAG(game_date, 1) OVER (PARTITION BY team_id, season_year ORDER BY game_date) AS previous_game_date
    FROM (
        -- Use distinct team, game_date, season_year combinations from the prediction spine
        -- to ensure we only process relevant dates.
        SELECT DISTINCT
            team_id,
            game_date,
            season_year
        FROM player_games_spine
    )
),

lagged_team_performance AS (
    -- Fetches team performance metrics from the game *prior* to the current game_date.
    SELECT
        tgd.team_id,
        tgd.game_date, -- This is the game_date of the game we are predicting FOR
        tp.last5_wins,
        tp.last10_wins,
        tp.team_strength_tier,
        tp.team_form
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__performance_metrics') }} tp
        ON tgd.team_id = tp.team_id AND tgd.previous_game_date = tp.game_date
    WHERE tgd.previous_game_date IS NOT NULL -- Ensures we only join where a previous game exists
),

-- New CTEs for calculating season averages and fetching advanced rolling stats
team_season_metrics_source AS (
    -- Selects raw metrics needed for season average calculations from the intermediate boxscore model.
    -- Filters data to relevant teams and seasons based on player_games_spine for efficiency.
    SELECT
        box.team_id,
        box.game_date, -- Actual game date from boxscore
        box.season_year,
        box.off_rating,
        box.def_rating
    FROM {{ ref('int_team__combined_boxscore') }} box
    {% if is_incremental() %}
    -- Ensure we have enough historical data for games currently being processed in player_games_spine.
    -- The window function for season averages needs all prior games in that season up to the current game.
    WHERE EXISTS (
        SELECT 1
        FROM player_games_spine pgs_filter
        WHERE box.team_id = pgs_filter.team_id
          AND box.season_year = pgs_filter.season_year
          AND box.game_date <= pgs_filter.game_date -- Include all games up to and including game_dates in pgs
                                                  -- for the window function's '1 PRECEDING' to work correctly.
    )
    {% endif %}
),

calculated_team_season_averages AS (
    -- Calculates lagged season-long averages for offensive and defensive ratings.
    -- The average for a 'game_date' includes all games for that team and season *before* that 'game_date'.
    SELECT
        team_id,
        game_date, -- This game_date is the one FOR WHICH the lagged average is calculated
        season_year,
        AVG(off_rating) OVER (PARTITION BY team_id, season_year ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS team_season_off_rating,
        AVG(def_rating) OVER (PARTITION BY team_id, season_year ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS team_season_def_rating
    FROM team_season_metrics_source
),

lagged_team_advanced_rolling_stats AS (
    -- Fetches L5 rolling team statistics and calculates trends from feat_team__advanced_rolling.
    -- These stats are from the game *prior* to the current game_date.
    SELECT
        tgd.team_id,
        tgd.game_date, -- Game date we are predicting FOR
        tgd.season_year, -- Carry forward for joining season averages and percentile calculations
        adv_roll.off_rating_roll_5g_avg AS team_l5_off_rating,
        adv_roll.def_rating_roll_5g_avg AS team_l5_def_rating,
        adv_roll.pace_roll_5g_avg AS team_l5_pace,
        (adv_roll.off_rating_roll_3g_avg - adv_roll.off_rating_roll_10g_avg) AS team_offense_trend,
        (adv_roll.def_rating_roll_3g_avg - adv_roll.def_rating_roll_10g_avg) AS team_defense_trend
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__advanced_rolling') }} adv_roll
        ON tgd.team_id = adv_roll.team_id AND tgd.previous_game_date = adv_roll.game_date
    WHERE tgd.previous_game_date IS NOT NULL
),

lagged_team_rolling_stats_combined AS (
    -- Combines lagged advanced rolling stats with lagged season averages.
    -- This CTE replaces the old 'lagged_team_rolling_stats_base'.
    SELECT
        ltars.team_id,
        ltars.game_date, -- Game date we are predicting FOR
        ltars.season_year,
        ltars.team_l5_off_rating,
        ltars.team_l5_def_rating,
        ltars.team_l5_pace,
        ctsa.team_season_off_rating, -- From calculated_team_season_averages
        ctsa.team_season_def_rating, -- From calculated_team_season_averages
        ltars.team_offense_trend,
        ltars.team_defense_trend
    FROM lagged_team_advanced_rolling_stats ltars
    JOIN calculated_team_season_averages ctsa
        ON ltars.team_id = ctsa.team_id
        AND ltars.game_date = ctsa.game_date -- Join on the game_date we are predicting FOR
        AND ltars.season_year = ctsa.season_year
),

season_year_percentiles AS (
    -- Calculates season-wide percentiles based on the *lagged* rolling stats.
    -- This ensures percentiles are derived from pre-game data relative to any specific game.
    SELECT
        season_year,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p80,
        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p60,
        PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p40,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p20,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p80,
        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p60,
        PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p40,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p20
    FROM lagged_team_rolling_stats_combined -- Use combined lagged stats as the basis
    GROUP BY season_year
),

lagged_team_rolling_stats_with_percentiles AS (
    -- Joins lagged rolling stats with their corresponding season-year percentiles.
    SELECT
        ltrsc.*, -- Select all from lagged_team_rolling_stats_combined
        syp.pace_p80,
        syp.pace_p60,
        syp.pace_p40,
        syp.pace_p20,
        syp.off_rating_p80,
        syp.off_rating_p60,
        syp.off_rating_p40,
        syp.off_rating_p20
    FROM lagged_team_rolling_stats_combined ltrsc -- Use the new combined CTE
    LEFT JOIN season_year_percentiles syp
        ON ltrsc.season_year = syp.season_year
),

lagged_team_shot_distribution AS (
    -- Fetches team shot distribution metrics from the game *prior* to the current game_date.
    SELECT
        tgd.team_id,
        tgd.game_date, -- Game date we are predicting FOR
        tsd.team_l5_3pt_att_rate,
        tsd.team_l5_paint_scoring_rate,
        tsd.team_l5_fastbreak_rate,
        tsd.team_l5_assisted_rate,
        tsd.team_playstyle -- Playstyle determined based on data prior to current game
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__shot_distribution') }} tsd
        ON tgd.team_id = tsd.team_id AND tgd.previous_game_date = tsd.game_date
    WHERE tgd.previous_game_date IS NOT NULL
),

lagged_team_usage AS (
    -- Fetches team usage pattern metrics from the game *prior* to the current game_date.
    SELECT
        tgd.team_id,
        tgd.game_date, -- Game date we are predicting FOR
        tu.team_l5_max_usage,
        tu.team_l5_top3_usage,
        tu.team_l5_pts_distribution,
        tu.team_offensive_structure -- Offensive structure determined based on data prior to current game
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__usage_distribution') }} tu
        ON tgd.team_id = tu.team_id AND tgd.previous_game_date = tu.game_date
    WHERE tgd.previous_game_date IS NOT NULL
),

final_features AS (
    SELECT
        -- Key identifiers from player_games_spine
        pgs.player_game_key,
        pgs.player_id,
        pgs.player_name,
        pgs.team_id,
        pgs.game_id,
        pgs.game_date,
        pgs.season_year,
        pgs.home_away,
        pgs.position,

        -- Lagged team performance metrics
        ltp.last5_wins,
        ltp.last10_wins,
        ltp.team_strength_tier,
        ltp.team_form,

        -- Lagged team rolling stats with percentiles (fields sourced from ltrwp)
        ltrwp.team_l5_off_rating,
        ltrwp.team_l5_def_rating,
        ltrwp.team_l5_pace,
        ltrwp.team_season_off_rating,
        ltrwp.team_season_def_rating,
        ltrwp.team_offense_trend,
        ltrwp.team_defense_trend,

        -- Lagged team shooting and style
        ltsd.team_l5_3pt_att_rate,
        ltsd.team_l5_paint_scoring_rate,
        ltsd.team_l5_fastbreak_rate,
        ltsd.team_l5_assisted_rate,
        ltsd.team_playstyle,

        -- Lagged team usage patterns
        ltu.team_l5_max_usage,
        ltu.team_l5_top3_usage,
        ltu.team_l5_pts_distribution,
        ltu.team_offensive_structure,

        -- Player-team relationship (from player_games_spine, assumed pre-game)
        pgs.player_offensive_role,
        pgs.player_team_style_fit_score,
        pgs.pace_impact_on_player,
        pgs.usage_opportunity,
        pgs.team_form_player_impact,
        pgs.team_playstyle_stat_impact,
        pgs.pts_in_team_hot_streaks,
        pgs.pts_in_team_cold_streaks,
        pgs.pts_in_star_dominant_system,
        pgs.pts_in_balanced_system,
        pgs.pace_multiplier,
        pgs.usage_multiplier,
        pgs.team_form_multiplier,
        pgs.playstyle_stat_multiplier,
        pgs.style_fit_multiplier,
        pgs.team_adjusted_pts_projection,
        pgs.team_adjusted_reb_projection,
        pgs.team_adjusted_ast_projection,
        pgs.team_context_impact,

        -- Derived features using lagged data and pre-calculated percentiles
        CASE
            WHEN ltrwp.team_l5_pace IS NULL THEN NULL -- Handle cases where lagged data is missing (e.g., first game of season)
            WHEN ltrwp.pace_p80 IS NULL THEN NULL -- Handle cases where percentiles might be null (e.g. very few data points in season_year_percentiles)
            WHEN ltrwp.team_l5_pace >= ltrwp.pace_p80 THEN 5
            WHEN ltrwp.team_l5_pace >= ltrwp.pace_p60 THEN 4
            WHEN ltrwp.team_l5_pace >= ltrwp.pace_p40 THEN 3
            WHEN ltrwp.team_l5_pace >= ltrwp.pace_p20 THEN 2
            ELSE 1
        END AS pace_quintile,

        CASE
            WHEN ltrwp.team_l5_off_rating IS NULL THEN NULL
            WHEN ltrwp.off_rating_p80 IS NULL THEN NULL
            WHEN ltrwp.team_l5_off_rating >= ltrwp.off_rating_p80 THEN 5
            WHEN ltrwp.team_l5_off_rating >= ltrwp.off_rating_p60 THEN 4
            WHEN ltrwp.team_l5_off_rating >= ltrwp.off_rating_p40 THEN 3
            WHEN ltrwp.team_l5_off_rating >= ltrwp.off_rating_p20 THEN 2
            ELSE 1
        END AS offense_quintile,

        -- Relative form impact (ensure pts_in_... are pre-game from pgs)
        CASE
            WHEN pgs.pts_in_team_hot_streaks > 0 AND pgs.pts_in_team_cold_streaks > 0
            THEN pgs.pts_in_team_hot_streaks / NULLIF(pgs.pts_in_team_cold_streaks, 0)
            ELSE NULL -- Return NULL if data is insufficient, rather than assuming 1
        END AS team_form_pts_impact_ratio,

        -- Boolean flags for categorical features (based on lagged team data)
        (ltsd.team_playstyle = 'THREE_POINT_HEAVY')::INT AS is_three_point_heavy_team,
        (ltsd.team_playstyle = 'PAINT_FOCUSED')::INT AS is_paint_focused_team,
        (ltsd.team_playstyle = 'FAST_PACED')::INT AS is_fast_paced_team,
        (ltsd.team_playstyle = 'BALL_MOVEMENT')::INT AS is_ball_movement_team,

        (ltu.team_offensive_structure = 'STAR_DOMINANT')::INT AS is_star_dominant_team,
        (ltu.team_offensive_structure = 'TOP_HEAVY')::INT AS is_top_heavy_team,
        (ltu.team_offensive_structure = 'BALANCED')::INT AS is_balanced_team,

        -- Boolean flags for player roles (from player_games_spine, assumed pre-game)
        (pgs.player_offensive_role = 'PRIMARY_OPTION')::INT AS is_primary_option,
        (pgs.player_offensive_role = 'SECONDARY_OPTION')::INT AS is_secondary_option,
        (pgs.player_offensive_role = 'SUPPORTING_ROLE')::INT AS is_supporting_role

    FROM player_games_spine pgs
    LEFT JOIN lagged_team_performance ltp
        ON pgs.team_id = ltp.team_id AND pgs.game_date = ltp.game_date
    LEFT JOIN lagged_team_rolling_stats_with_percentiles ltrwp
        ON pgs.team_id = ltrwp.team_id AND pgs.game_date = ltrwp.game_date
    LEFT JOIN lagged_team_shot_distribution ltsd
        ON pgs.team_id = ltsd.team_id AND pgs.game_date = ltsd.game_date
    LEFT JOIN lagged_team_usage ltu
        ON pgs.team_id = ltu.team_id AND pgs.game_date = ltu.game_date
)

SELECT
    ff.*,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM final_features ff
WHERE ff.player_game_key IS NOT NULL -- Ensure the primary key for the model is populated