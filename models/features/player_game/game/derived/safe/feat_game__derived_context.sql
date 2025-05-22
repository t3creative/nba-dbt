{{
  config(
    materialized='incremental',
    schema='features',
    tags=['game_context', 'features', 'derived'],
    unique_key='game_team_key',
    incremental_strategy='merge',
    partition_by={
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['season_year', 'game_date', 'team_id']
  )
}}

WITH source_game_context AS (
    SELECT
        game_id,
        game_date,
        season_year,
        home_team_id,
        away_team_id,
        home_team_game_num,
        away_team_game_num,
        home_team_wins,
        home_team_losses,
        away_team_wins,
        away_team_losses,
        home_rest_days,
        away_rest_days,
        home_back_to_back,
        away_back_to_back,
        CASE
            WHEN home_team_wins + home_team_losses = 0 THEN 0.5
            ELSE home_team_wins::NUMERIC / (home_team_wins + home_team_losses)
        END AS home_win_pct_prior,
         CASE
            WHEN away_team_wins + away_team_losses = 0 THEN 0.5
            ELSE away_team_wins::NUMERIC / (away_team_wins + away_team_losses)
        END AS away_win_pct_prior
    FROM {{ ref('int_game__schedules') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

team_game_context AS (
    SELECT
        game_id,
        game_date,
        season_year,
        home_team_id AS team_id,
        away_team_id AS opponent_id,
        home_team_game_num AS team_game_num_in_season,
        home_team_wins AS team_wins_prior_total,
        home_team_losses AS team_losses_prior_total,
        home_win_pct_prior AS team_win_pct_prior_total,
        home_rest_days AS team_rest_days,
        home_back_to_back AS team_back_to_back_flag,
        away_rest_days AS opponent_rest_days,
        away_back_to_back AS opponent_back_to_back_flag,
        TRUE AS is_home,
        {{ dbt_utils.generate_surrogate_key(['game_id', 'home_team_id']) }} AS game_team_key
    FROM source_game_context

    UNION ALL

    SELECT
        game_id,
        game_date,
        season_year,
        away_team_id AS team_id,
        home_team_id AS opponent_id,
        away_team_game_num AS team_game_num_in_season,
        away_team_wins AS team_wins_prior_total,
        away_team_losses AS team_losses_prior_total,
        away_win_pct_prior AS team_win_pct_prior_total,
        away_rest_days AS team_rest_days,
        away_back_to_back AS team_back_to_back_flag,
        home_rest_days AS opponent_rest_days,
        home_back_to_back AS opponent_back_to_back_flag,
        FALSE AS is_home,
         {{ dbt_utils.generate_surrogate_key(['game_id', 'away_team_id']) }} AS game_team_key
    FROM source_game_context
),

prior_game_outcome AS (
    SELECT
        *,
        LAG(team_wins_prior_total, 1, team_wins_prior_total) OVER (PARTITION BY team_id ORDER BY game_date) AS team_wins_lag_1g_total,
        LAG(team_losses_prior_total, 1, team_losses_prior_total) OVER (PARTITION BY team_id ORDER BY game_date) AS team_losses_lag_1g_total
    FROM team_game_context
),

prior_game_outcome_flags AS (
    SELECT
        *,
        CASE
            WHEN team_wins_prior_total > team_wins_lag_1g_total THEN 1
            ELSE 0
        END AS prior_game_was_win,
        CASE
            WHEN team_losses_prior_total > team_losses_lag_1g_total THEN 1
             ELSE 0
        END AS prior_game_was_loss
    FROM prior_game_outcome
),

rolling_stats AS (
    SELECT
        -- Explicitly select columns from prior_game_outcome_flags
        pgof.game_id,
        pgof.game_date,
        pgof.season_year,
        pgof.game_team_key,
        pgof.team_id,
        pgof.opponent_id,
        pgof.is_home,
        pgof.team_game_num_in_season,
        pgof.team_wins_prior_total,
        pgof.team_losses_prior_total,
        pgof.team_win_pct_prior_total,
        pgof.team_rest_days,
        pgof.opponent_rest_days,
        pgof.team_back_to_back_flag,
        pgof.opponent_back_to_back_flag,
        pgof.prior_game_was_win,
        pgof.prior_game_was_loss,
        -- pgof.team_wins_lag_1g_total, -- Not directly needed by base_derived_features from rolling_stats
        -- pgof.team_losses_lag_1g_total, -- Not directly needed by base_derived_features from rolling_stats

        -- Window functions
         SUM(pgof.prior_game_was_win) OVER (
            PARTITION BY pgof.team_id
            ORDER BY pgof.game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
         ) AS team_wins_last_10_count_prior,

         SUM(pgof.prior_game_was_loss) OVER (
            PARTITION BY pgof.team_id
            ORDER BY pgof.game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
         ) AS team_losses_last_10_count_prior,

        AVG(pgof.team_win_pct_prior_total) OVER (
            PARTITION BY pgof.team_id
            ORDER BY pgof.game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS team_win_pct_last_10_avg_prior
    FROM prior_game_outcome_flags pgof
),

base_derived_features AS (
    SELECT
        game_id,
        game_date,
        season_year,
        game_team_key,
        team_id,
        opponent_id,
        is_home,
        team_game_num_in_season,
        team_wins_prior_total,
        team_losses_prior_total,
        team_win_pct_prior_total,
        team_rest_days,
        opponent_rest_days,
        team_back_to_back_flag,
        opponent_back_to_back_flag,
        prior_game_was_win,
        prior_game_was_loss,

        COALESCE(team_wins_last_10_count_prior, 0) AS team_wins_last_10_prior,
        COALESCE(team_losses_last_10_count_prior, 0) AS team_losses_last_10_prior,
        CASE
            WHEN (COALESCE(team_wins_last_10_count_prior, 0) + COALESCE(team_losses_last_10_count_prior, 0)) = 0 THEN 0.5
            ELSE COALESCE(team_wins_last_10_count_prior, 0)::NUMERIC / (COALESCE(team_wins_last_10_count_prior, 0) + COALESCE(team_losses_last_10_count_prior, 0))
        END AS team_win_pct_last_10_calculated,

        COALESCE(team_win_pct_last_10_avg_prior, 0.5) AS team_win_pct_last_10_avg,

        (team_rest_days - opponent_rest_days) AS rest_advantage_days,
        CASE
            WHEN team_rest_days > opponent_rest_days THEN 'TEAM_REST_ADVANTAGE'
            WHEN opponent_rest_days > team_rest_days THEN 'OPPONENT_REST_ADVANTAGE'
            ELSE 'NO_REST_ADVANTAGE'
        END AS rest_advantage_type,

        CASE
             WHEN COALESCE(team_wins_last_10_count_prior, 0) >= 7 THEN 'HOT_STREAK'
             WHEN COALESCE(team_losses_last_10_count_prior, 0) >= 7 THEN 'COLD_STREAK'
             ELSE 'NEUTRAL_STREAK'
        END AS team_form_last_10
    FROM rolling_stats
),

final_derived_features AS (
    SELECT
        *,
        CASE
            WHEN prior_game_was_win = 1 THEN 'WON_LAST_GAME'
            WHEN prior_game_was_loss = 1 THEN 'LOST_LAST_GAME'
            ELSE 'NO_PRIOR_GAME_INFO'
        END AS last_game_outcome,

        COALESCE(team_win_pct_last_10_calculated, 0.5)
         * (1 + (rest_advantage_days) * 0.05)
         * (CASE WHEN team_back_to_back_flag THEN 0.9 ELSE 1.0 END)
         * (CASE WHEN opponent_back_to_back_flag THEN 1.1 ELSE 1.0 END)
         AS home_court_advantage_factor
    FROM base_derived_features
)

SELECT
    game_id,
    game_date,
    season_year,
    game_team_key,
    team_id,
    opponent_id,
    is_home,
    team_game_num_in_season,
    team_wins_prior_total,
    team_losses_prior_total,
    team_win_pct_prior_total,
    team_rest_days,
    opponent_rest_days,
    team_back_to_back_flag,
    opponent_back_to_back_flag,
    team_wins_last_10_prior,
    team_losses_last_10_prior,
    team_win_pct_last_10_calculated,
    team_win_pct_last_10_avg,
    rest_advantage_days,
    rest_advantage_type,
    team_form_last_10,
    last_game_outcome,
    home_court_advantage_factor,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM final_derived_features