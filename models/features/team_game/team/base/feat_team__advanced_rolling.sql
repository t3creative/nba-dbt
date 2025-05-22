{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'pga', 'team', 'base', 'rolling', 'advanced'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']},
        {'columns': ['season_year']}
    ]
) }}

WITH source_data AS (
    SELECT
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Advanced Stats from int_team__combined_boxscore
        est_off_rating,
        off_rating,
        est_def_rating,
        def_rating,
        est_net_rating,
        net_rating,
        ast_pct,
        ast_to_tov_ratio,
        ast_ratio,
        off_reb_pct,
        def_reb_pct,
        reb_pct,
        est_team_tov_pct,
        tov_ratio,
        eff_fg_pct,
        ts_pct,
        -- usage_pct is always 100% for teams, no need to roll
        est_usage_pct,
        est_pace,
        pace,
        pace_per_40,
        possessions,
        pie,
        updated_at -- Include updated_at from source for lineage
    FROM {{ ref('int_team__combined_boxscore') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Identifiers & Metadata
    team_game_key,
    game_id,
    team_id,
    game_date,
    season_year,

    -- Rolling Advanced Stats (Avg and StdDev for 3, 5, 10 games)
    round(({{ calculate_rolling_avg('est_off_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as est_off_rating_roll_3g_avg,
    round(({{ calculate_rolling_avg('est_off_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as est_off_rating_roll_5g_avg,
    round(({{ calculate_rolling_avg('est_off_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as est_off_rating_roll_10g_avg,
    round(({{ calculate_rolling_stddev('est_off_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as est_off_rating_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('est_off_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as est_off_rating_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('est_off_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as est_off_rating_roll_10g_stddev,

    round(({{ calculate_rolling_avg('off_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as off_rating_roll_3g_avg,
    round(({{ calculate_rolling_avg('off_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as off_rating_roll_5g_avg,
    round(({{ calculate_rolling_avg('off_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as off_rating_roll_10g_avg,
    round(({{ calculate_rolling_stddev('off_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as off_rating_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('off_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as off_rating_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('off_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as off_rating_roll_10g_stddev,

    round(({{ calculate_rolling_avg('est_def_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as est_def_rating_roll_3g_avg,
    round(({{ calculate_rolling_avg('est_def_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as est_def_rating_roll_5g_avg,
    round(({{ calculate_rolling_avg('est_def_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as est_def_rating_roll_10g_avg,
    round(({{ calculate_rolling_stddev('est_def_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as est_def_rating_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('est_def_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as est_def_rating_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('est_def_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as est_def_rating_roll_10g_stddev,

    round(({{ calculate_rolling_avg('def_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as def_rating_roll_3g_avg,
    round(({{ calculate_rolling_avg('def_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as def_rating_roll_5g_avg,
    round(({{ calculate_rolling_avg('def_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as def_rating_roll_10g_avg,
    round(({{ calculate_rolling_stddev('def_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as def_rating_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('def_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as def_rating_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('def_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as def_rating_roll_10g_stddev,

    round(({{ calculate_rolling_avg('est_net_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as est_net_rating_roll_3g_avg,
    round(({{ calculate_rolling_avg('est_net_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as est_net_rating_roll_5g_avg,
    round(({{ calculate_rolling_avg('est_net_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as est_net_rating_roll_10g_avg,
    round(({{ calculate_rolling_stddev('est_net_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as est_net_rating_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('est_net_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as est_net_rating_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('est_net_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as est_net_rating_roll_10g_stddev,

    round(({{ calculate_rolling_avg('net_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as net_rating_roll_3g_avg,
    round(({{ calculate_rolling_avg('net_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as net_rating_roll_5g_avg,
    round(({{ calculate_rolling_avg('net_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as net_rating_roll_10g_avg,
    round(({{ calculate_rolling_stddev('net_rating', 'team_id', 'game_date', 3) }})::numeric, 3) as net_rating_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('net_rating', 'team_id', 'game_date', 5) }})::numeric, 3) as net_rating_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('net_rating', 'team_id', 'game_date', 10) }})::numeric, 3) as net_rating_roll_10g_stddev,

    round(({{ calculate_rolling_avg('ast_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as ast_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('ast_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as ast_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('ast_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as ast_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('ast_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as ast_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('ast_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as ast_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('ast_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as ast_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('ast_to_tov_ratio', 'team_id', 'game_date', 3) }})::numeric, 3) as ast_to_tov_ratio_roll_3g_avg,
    round(({{ calculate_rolling_avg('ast_to_tov_ratio', 'team_id', 'game_date', 5) }})::numeric, 3) as ast_to_tov_ratio_roll_5g_avg,
    round(({{ calculate_rolling_avg('ast_to_tov_ratio', 'team_id', 'game_date', 10) }})::numeric, 3) as ast_to_tov_ratio_roll_10g_avg,
    round(({{ calculate_rolling_stddev('ast_to_tov_ratio', 'team_id', 'game_date', 3) }})::numeric, 3) as ast_to_tov_ratio_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('ast_to_tov_ratio', 'team_id', 'game_date', 5) }})::numeric, 3) as ast_to_tov_ratio_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('ast_to_tov_ratio', 'team_id', 'game_date', 10) }})::numeric, 3) as ast_to_tov_ratio_roll_10g_stddev,

    round(({{ calculate_rolling_avg('ast_ratio', 'team_id', 'game_date', 3) }})::numeric, 3) as ast_ratio_roll_3g_avg,
    round(({{ calculate_rolling_avg('ast_ratio', 'team_id', 'game_date', 5) }})::numeric, 3) as ast_ratio_roll_5g_avg,
    round(({{ calculate_rolling_avg('ast_ratio', 'team_id', 'game_date', 10) }})::numeric, 3) as ast_ratio_roll_10g_avg,
    round(({{ calculate_rolling_stddev('ast_ratio', 'team_id', 'game_date', 3) }})::numeric, 3) as ast_ratio_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('ast_ratio', 'team_id', 'game_date', 5) }})::numeric, 3) as ast_ratio_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('ast_ratio', 'team_id', 'game_date', 10) }})::numeric, 3) as ast_ratio_roll_10g_stddev,

    round(({{ calculate_rolling_avg('off_reb_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as off_reb_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('off_reb_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as off_reb_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('off_reb_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as off_reb_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('off_reb_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as off_reb_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('off_reb_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as off_reb_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('off_reb_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as off_reb_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('def_reb_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as def_reb_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('def_reb_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as def_reb_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('def_reb_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as def_reb_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('def_reb_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as def_reb_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('def_reb_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as def_reb_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('def_reb_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as def_reb_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('reb_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as reb_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('reb_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as reb_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('reb_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as reb_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('reb_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as reb_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('reb_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as reb_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('reb_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as reb_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('est_team_tov_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as est_team_tov_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('est_team_tov_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as est_team_tov_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('est_team_tov_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as est_team_tov_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('est_team_tov_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as est_team_tov_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('est_team_tov_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as est_team_tov_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('est_team_tov_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as est_team_tov_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('tov_ratio', 'team_id', 'game_date', 3) }})::numeric, 3) as tov_ratio_roll_3g_avg,
    round(({{ calculate_rolling_avg('tov_ratio', 'team_id', 'game_date', 5) }})::numeric, 3) as tov_ratio_roll_5g_avg,
    round(({{ calculate_rolling_avg('tov_ratio', 'team_id', 'game_date', 10) }})::numeric, 3) as tov_ratio_roll_10g_avg,
    round(({{ calculate_rolling_stddev('tov_ratio', 'team_id', 'game_date', 3) }})::numeric, 3) as tov_ratio_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('tov_ratio', 'team_id', 'game_date', 5) }})::numeric, 3) as tov_ratio_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('tov_ratio', 'team_id', 'game_date', 10) }})::numeric, 3) as tov_ratio_roll_10g_stddev,

    round(({{ calculate_rolling_avg('eff_fg_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as eff_fg_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('eff_fg_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as eff_fg_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('eff_fg_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as eff_fg_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('eff_fg_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as eff_fg_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('eff_fg_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as eff_fg_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('eff_fg_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as eff_fg_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('ts_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as ts_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('ts_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as ts_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('ts_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as ts_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('ts_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as ts_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('ts_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as ts_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('ts_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as ts_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('est_usage_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as est_usage_pct_roll_3g_avg,
    round(({{ calculate_rolling_avg('est_usage_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as est_usage_pct_roll_5g_avg,
    round(({{ calculate_rolling_avg('est_usage_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as est_usage_pct_roll_10g_avg,
    round(({{ calculate_rolling_stddev('est_usage_pct', 'team_id', 'game_date', 3) }})::numeric, 3) as est_usage_pct_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('est_usage_pct', 'team_id', 'game_date', 5) }})::numeric, 3) as est_usage_pct_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('est_usage_pct', 'team_id', 'game_date', 10) }})::numeric, 3) as est_usage_pct_roll_10g_stddev,

    round(({{ calculate_rolling_avg('est_pace', 'team_id', 'game_date', 3) }})::numeric, 3) as est_pace_roll_3g_avg,
    round(({{ calculate_rolling_avg('est_pace', 'team_id', 'game_date', 5) }})::numeric, 3) as est_pace_roll_5g_avg,
    round(({{ calculate_rolling_avg('est_pace', 'team_id', 'game_date', 10) }})::numeric, 3) as est_pace_roll_10g_avg,
    round(({{ calculate_rolling_stddev('est_pace', 'team_id', 'game_date', 3) }})::numeric, 3) as est_pace_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('est_pace', 'team_id', 'game_date', 5) }})::numeric, 3) as est_pace_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('est_pace', 'team_id', 'game_date', 10) }})::numeric, 3) as est_pace_roll_10g_stddev,

    round(({{ calculate_rolling_avg('pace', 'team_id', 'game_date', 3) }})::numeric, 3) as pace_roll_3g_avg,
    round(({{ calculate_rolling_avg('pace', 'team_id', 'game_date', 5) }})::numeric, 3) as pace_roll_5g_avg,
    round(({{ calculate_rolling_avg('pace', 'team_id', 'game_date', 10) }})::numeric, 3) as pace_roll_10g_avg,
    round(({{ calculate_rolling_stddev('pace', 'team_id', 'game_date', 3) }})::numeric, 3) as pace_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('pace', 'team_id', 'game_date', 5) }})::numeric, 3) as pace_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('pace', 'team_id', 'game_date', 10) }})::numeric, 3) as pace_roll_10g_stddev,

    round(({{ calculate_rolling_avg('pace_per_40', 'team_id', 'game_date', 3) }})::numeric, 3) as pace_per_40_roll_3g_avg,
    round(({{ calculate_rolling_avg('pace_per_40', 'team_id', 'game_date', 5) }})::numeric, 3) as pace_per_40_roll_5g_avg,
    round(({{ calculate_rolling_avg('pace_per_40', 'team_id', 'game_date', 10) }})::numeric, 3) as pace_per_40_roll_10g_avg,
    round(({{ calculate_rolling_stddev('pace_per_40', 'team_id', 'game_date', 3) }})::numeric, 3) as pace_per_40_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('pace_per_40', 'team_id', 'game_date', 5) }})::numeric, 3) as pace_per_40_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('pace_per_40', 'team_id', 'game_date', 10) }})::numeric, 3) as pace_per_40_roll_10g_stddev,

    round(({{ calculate_rolling_avg('possessions', 'team_id', 'game_date', 3) }})::numeric, 3) as possessions_roll_3g_avg,
    round(({{ calculate_rolling_avg('possessions', 'team_id', 'game_date', 5) }})::numeric, 3) as possessions_roll_5g_avg,
    round(({{ calculate_rolling_avg('possessions', 'team_id', 'game_date', 10) }})::numeric, 3) as possessions_roll_10g_avg,
    round(({{ calculate_rolling_stddev('possessions', 'team_id', 'game_date', 3) }})::numeric, 3) as possessions_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('possessions', 'team_id', 'game_date', 5) }})::numeric, 3) as possessions_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('possessions', 'team_id', 'game_date', 10) }})::numeric, 3) as possessions_roll_10g_stddev,

    round(({{ calculate_rolling_avg('pie', 'team_id', 'game_date', 3) }})::numeric, 3) as pie_roll_3g_avg,
    round(({{ calculate_rolling_avg('pie', 'team_id', 'game_date', 5) }})::numeric, 3) as pie_roll_5g_avg,
    round(({{ calculate_rolling_avg('pie', 'team_id', 'game_date', 10) }})::numeric, 3) as pie_roll_10g_avg,
    round(({{ calculate_rolling_stddev('pie', 'team_id', 'game_date', 3) }})::numeric, 3) as pie_roll_3g_stddev,
    round(({{ calculate_rolling_stddev('pie', 'team_id', 'game_date', 5) }})::numeric, 3) as pie_roll_5g_stddev,
    round(({{ calculate_rolling_stddev('pie', 'team_id', 'game_date', 10) }})::numeric, 3) as pie_roll_10g_stddev,

    CURRENT_TIMESTAMP AS created_at,
    source_data.updated_at

FROM source_data
