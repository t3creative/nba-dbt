{{ config(
    schema='features',
    materialized='incremental',
    tags=['features', 'player_scoring_features_historical'],
    unique_key='player_game_key',
    incremental_strategy='merge',
    indexes=[
            {'columns': ['player_game_key'], 'unique': True},
            {'columns': ['player_id']},
            {'columns': ['game_id']},
            {'columns': ['game_date']}
        ]
    )
}}

WITH source_metrics AS (
    SELECT *
    FROM {{ ref('int_player__current_derived_scoring') }}
    {% if is_incremental() %}
    -- When running incrementally, we need to process a window of data
    -- that is large enough to correctly calculate all rolling features for new games.
    -- This typically means fetching data going back by the largest rolling window period.
    -- For a 20-game rolling window, we might need at least 20 prior games.
    -- The exact logic might need adjustment based on your largest window and data density.
    WHERE game_date >= (
        SELECT LEAST(
                   MAX(game_date) - INTERVAL '{{ var('feature_recalc_window_days', 45) }} days', -- Go back enough days to cover largest rolling window
                   MIN(game_date) -- Ensure we don't go before the earliest date needed for new incremental rows
               )
        FROM {{ this }}
    ) OR game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

lagged_and_rolling_features AS (
    SELECT
        s.player_game_key,
        s.player_id,
        s.game_id,
        s.game_date,
        s.season_year,
        s.player_name, -- Carry over for reference if needed downstream, though usually dropped for ML

        -- Carry over pre-calculated historical/prior features
        s.career_high_pts_prior_game,
        s.career_ppg_prior_game,
        s.career_games_prior_game,
        s.days_since_last_30_plus_game_prior,
        s.days_since_last_40_plus_game_prior,
        s.thirty_plus_games_career_prior,
        s.forty_plus_games_career_prior,
        s.thirty_plus_games_season_prior,
        s.forty_plus_games_season_prior,
        s.thirty_plus_games_prev_season,
        s.forty_plus_games_prev_season,
        s.avg_usage_in_30_plus_career_prior,
        s.avg_usage_in_40_plus_career_prior,
        s.avg_usage_0_to_9_pts_prior,
        s.avg_usage_10_to_19_pts_prior,
        s.avg_usage_20_to_29_pts_prior,

        -- Calculate career average minutes prior to the current game
        COALESCE(
            SUM(s.min) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) /
            NULLIF(SUM(CASE WHEN s.min > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0),
            0
        ) as career_avg_minutes_prior_game,

        -- Lagged version of the "is_new_career_high_flag" (was the *previous* game a new career high?)
        LAG(s.is_new_career_high_flag, 1, 0) OVER (PARTITION BY s.player_id ORDER BY s.game_date) as is_new_career_high_lag1,

        -- Lagged and Rolling features for base stats from intermediate model (which are current game stats there)
        {% for stat_col in [
            'pts', 'min', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct', 'ftm', 'fta', 'ft_pct', 
            'off_reb', 'ts_pct', 'eff_fg_pct', 'usage_pct', 'possessions', 'touches', 
            'pct_unassisted_fgm', 'pct_of_team_fga', 'pct_of_team_ast', 'ast_pct', 'cont_fga', 
            'cont_fgm', 'cont_fg_pct', 'uncont_fg_pct', 'def_at_rim_fga', 'pct_pts_2pt', 
            'pct_pts_3pt', 'pct_pts_ft', 'pct_pts_in_paint', 'pct_fga_2pt', 'pct_fga_3pt', 
            'pct_assisted_fgm', 'matchup_fg_pct', 'fastbreak_pts', 'pts_off_tov', 
            'pace_per_40', 'second_chance_pts'
        ] %}
            LAG(s.{{ stat_col }}, 1) OVER (PARTITION BY s.player_id ORDER BY s.game_date) as {{ stat_col }}_lag1,
            AVG(s.{{ stat_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as {{ stat_col }}_roll3_avg,
            AVG(s.{{ stat_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as {{ stat_col }}_roll5_avg,
            AVG(s.{{ stat_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as {{ stat_col }}_roll10_avg,
            STDDEV(s.{{ stat_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as {{ stat_col }}_roll3_stddev,
            STDDEV(s.{{ stat_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as {{ stat_col }}_roll5_stddev,
            STDDEV(s.{{ stat_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as {{ stat_col }}_roll10_stddev,
        {% endfor %}

        -- Lagged and Rolling features for current game flags from intermediate model
        {% for flag_col in ['is_30_plus_game_current_flag', 'is_40_plus_game_current_flag'] %}
            LAG(s.{{ flag_col }}, 1, 0) OVER (PARTITION BY s.player_id ORDER BY s.game_date) as {{ flag_col | replace('_current_flag', '_lag1') }},
            AVG(s.{{ flag_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as {{ flag_col | replace('_current_flag', '_roll3_avg') }},
            AVG(s.{{ flag_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as {{ flag_col | replace('_current_flag', '_roll5_avg') }},
            AVG(s.{{ flag_col }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as {{ flag_col | replace('_current_flag', '_roll10_avg') }},
        {% endfor %}

        -- Lagged and Rolling features for derived metrics from intermediate model
        {% for derived_metric_col_current in [
            'scoring_efficiency_composite_current',
            'points_opportunity_ratio_current',
            'shot_creation_index_current',
            'defensive_attention_factor_current',
            'scoring_versatility_ratio_current',
            'offensive_role_factor_current',
            'adjusted_usage_with_defense_current',
            'pts_per_half_court_poss_current',
            'usage_weighted_ts_current',
            'three_pt_value_efficiency_index_current',
            'paint_reliance_index_current',
            'assisted_shot_efficiency_current',
            'pace_adjusted_points_per_minute_current',
            'second_chance_conversion_rate_current',
            'contested_vs_uncontested_fg_pct_diff_current',
            'shooting_volume_per_min_current',
            'effective_three_point_contribution_rate_current',
            'free_throw_generation_aggressiveness_current',
            'self_created_scoring_rate_per_min_current',
            'opportunistic_scoring_rate_per_min_current',
            'scoring_focus_ratio_current',
            'contested_fg_makes_per_minute_current',
            'scoring_profile_balance_3pt_vs_paint_current'
        ] %}
            {% set derived_metric_col_base = derived_metric_col_current | replace('_current', '') %}
            LAG(s.{{ derived_metric_col_current }}, 1) OVER (PARTITION BY s.player_id ORDER BY s.game_date) as {{ derived_metric_col_base }}_lag1,
            AVG(s.{{ derived_metric_col_current }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as {{ derived_metric_col_base }}_roll3_avg,
            AVG(s.{{ derived_metric_col_current }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as {{ derived_metric_col_base }}_roll5_avg,
            AVG(s.{{ derived_metric_col_current }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as {{ derived_metric_col_base }}_roll10_avg,
            STDDEV(s.{{ derived_metric_col_current }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as {{ derived_metric_col_base }}_roll3_stddev,
            STDDEV(s.{{ derived_metric_col_current }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as {{ derived_metric_col_base }}_roll5_stddev,
            STDDEV(s.{{ derived_metric_col_current }}) OVER (PARTITION BY s.player_id ORDER BY s.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as {{ derived_metric_col_base }}_roll10_stddev,
        {% endfor %}

        current_timestamp as _dbt_feature_processed_at

    FROM source_metrics s
)

SELECT *
FROM lagged_and_rolling_features