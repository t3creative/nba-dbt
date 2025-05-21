{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'lagged', 'advanced'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['player_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

with advanced_boxscore_data as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Advanced Boxscore Stats (selected based on int__player_bxsc_advanced.sql)
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
        tov_ratio,
        eff_fg_pct,
        ts_pct,
        usage_pct,
        est_usage_pct,
        est_pace,
        pace,
        pace_per_40,
        possessions,
        pie,

        -- Timestamps for Incremental
        updated_at

    from {{ ref('int_player__combined_boxscore') }}
    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= cast(substring('{{ var('training_start_season_year') }}' from 1 for 4) as integer)
    {% if is_incremental() %}
    -- Look back a fixed number of days to ensure calculations are correct near the incremental boundary.
    -- This window should be large enough to capture the game history needed for the longest lag window.
    and game_date >= (
        select max(game_date) - interval '90 days' -- Adjust interval if longer lags are needed
        from {{ this }}
    )
    {% endif %}
),

final as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Lagged Advanced Boxscore Stats
        {{ calculate_lag('est_off_rating', 'player_id', 'game_date', 1) }} as est_off_rating_lag_1g,
        {{ calculate_lag('est_off_rating', 'player_id', 'game_date', 3) }} as est_off_rating_lag_3g,
        {{ calculate_lag('est_off_rating', 'player_id', 'game_date', 5) }} as est_off_rating_lag_5g,
        {{ calculate_lag('est_off_rating', 'player_id', 'game_date', 7) }} as est_off_rating_lag_7g,

        {{ calculate_lag('off_rating', 'player_id', 'game_date', 1) }} as off_rating_lag_1g,
        {{ calculate_lag('off_rating', 'player_id', 'game_date', 3) }} as off_rating_lag_3g,
        {{ calculate_lag('off_rating', 'player_id', 'game_date', 5) }} as off_rating_lag_5g,
        {{ calculate_lag('off_rating', 'player_id', 'game_date', 7) }} as off_rating_lag_7g,

        {{ calculate_lag('est_def_rating', 'player_id', 'game_date', 1) }} as est_def_rating_lag_1g,
        {{ calculate_lag('est_def_rating', 'player_id', 'game_date', 3) }} as est_def_rating_lag_3g,
        {{ calculate_lag('est_def_rating', 'player_id', 'game_date', 5) }} as est_def_rating_lag_5g,
        {{ calculate_lag('est_def_rating', 'player_id', 'game_date', 7) }} as est_def_rating_lag_7g,

        {{ calculate_lag('def_rating', 'player_id', 'game_date', 1) }} as def_rating_lag_1g,
        {{ calculate_lag('def_rating', 'player_id', 'game_date', 3) }} as def_rating_lag_3g,
        {{ calculate_lag('def_rating', 'player_id', 'game_date', 5) }} as def_rating_lag_5g,
        {{ calculate_lag('def_rating', 'player_id', 'game_date', 7) }} as def_rating_lag_7g,

        {{ calculate_lag('est_net_rating', 'player_id', 'game_date', 1) }} as est_net_rating_lag_1g,
        {{ calculate_lag('est_net_rating', 'player_id', 'game_date', 3) }} as est_net_rating_lag_3g,
        {{ calculate_lag('est_net_rating', 'player_id', 'game_date', 5) }} as est_net_rating_lag_5g,
        {{ calculate_lag('est_net_rating', 'player_id', 'game_date', 7) }} as est_net_rating_lag_7g,

        {{ calculate_lag('net_rating', 'player_id', 'game_date', 1) }} as net_rating_lag_1g,
        {{ calculate_lag('net_rating', 'player_id', 'game_date', 3) }} as net_rating_lag_3g,
        {{ calculate_lag('net_rating', 'player_id', 'game_date', 5) }} as net_rating_lag_5g,
        {{ calculate_lag('net_rating', 'player_id', 'game_date', 7) }} as net_rating_lag_7g,

        {{ calculate_lag('ast_pct', 'player_id', 'game_date', 1) }} as ast_pct_lag_1g,
        {{ calculate_lag('ast_pct', 'player_id', 'game_date', 3) }} as ast_pct_lag_3g,
        {{ calculate_lag('ast_pct', 'player_id', 'game_date', 5) }} as ast_pct_lag_5g,
        {{ calculate_lag('ast_pct', 'player_id', 'game_date', 7) }} as ast_pct_lag_7g,

        {{ calculate_lag('ast_to_tov_ratio', 'player_id', 'game_date', 1) }} as ast_to_tov_ratio_lag_1g,
        {{ calculate_lag('ast_to_tov_ratio', 'player_id', 'game_date', 3) }} as ast_to_tov_ratio_lag_3g,
        {{ calculate_lag('ast_to_tov_ratio', 'player_id', 'game_date', 5) }} as ast_to_tov_ratio_lag_5g,
        {{ calculate_lag('ast_to_tov_ratio', 'player_id', 'game_date', 7) }} as ast_to_tov_ratio_lag_7g,

        {{ calculate_lag('ast_ratio', 'player_id', 'game_date', 1) }} as ast_ratio_lag_1g,
        {{ calculate_lag('ast_ratio', 'player_id', 'game_date', 3) }} as ast_ratio_lag_3g,
        {{ calculate_lag('ast_ratio', 'player_id', 'game_date', 5) }} as ast_ratio_lag_5g,
        {{ calculate_lag('ast_ratio', 'player_id', 'game_date', 7) }} as ast_ratio_lag_7g,

        {{ calculate_lag('off_reb_pct', 'player_id', 'game_date', 1) }} as off_reb_pct_lag_1g,
        {{ calculate_lag('off_reb_pct', 'player_id', 'game_date', 3) }} as off_reb_pct_lag_3g,
        {{ calculate_lag('off_reb_pct', 'player_id', 'game_date', 5) }} as off_reb_pct_lag_5g,
        {{ calculate_lag('off_reb_pct', 'player_id', 'game_date', 7) }} as off_reb_pct_lag_7g,

        {{ calculate_lag('def_reb_pct', 'player_id', 'game_date', 1) }} as def_reb_pct_lag_1g,
        {{ calculate_lag('def_reb_pct', 'player_id', 'game_date', 3) }} as def_reb_pct_lag_3g,
        {{ calculate_lag('def_reb_pct', 'player_id', 'game_date', 5) }} as def_reb_pct_lag_5g,
        {{ calculate_lag('def_reb_pct', 'player_id', 'game_date', 7) }} as def_reb_pct_lag_7g,

        {{ calculate_lag('reb_pct', 'player_id', 'game_date', 1) }} as reb_pct_lag_1g,
        {{ calculate_lag('reb_pct', 'player_id', 'game_date', 3) }} as reb_pct_lag_3g,
        {{ calculate_lag('reb_pct', 'player_id', 'game_date', 5) }} as reb_pct_lag_5g,
        {{ calculate_lag('reb_pct', 'player_id', 'game_date', 7) }} as reb_pct_lag_7g,

        {{ calculate_lag('tov_ratio', 'player_id', 'game_date', 1) }} as tov_ratio_lag_1g,
        {{ calculate_lag('tov_ratio', 'player_id', 'game_date', 3) }} as tov_ratio_lag_3g,
        {{ calculate_lag('tov_ratio', 'player_id', 'game_date', 5) }} as tov_ratio_lag_5g,
        {{ calculate_lag('tov_ratio', 'player_id', 'game_date', 7) }} as tov_ratio_lag_7g,

        {{ calculate_lag('eff_fg_pct', 'player_id', 'game_date', 1) }} as eff_fg_pct_lag_1g,
        {{ calculate_lag('eff_fg_pct', 'player_id', 'game_date', 3) }} as eff_fg_pct_lag_3g,
        {{ calculate_lag('eff_fg_pct', 'player_id', 'game_date', 5) }} as eff_fg_pct_lag_5g,
        {{ calculate_lag('eff_fg_pct', 'player_id', 'game_date', 7) }} as eff_fg_pct_lag_7g,

        {{ calculate_lag('ts_pct', 'player_id', 'game_date', 1) }} as ts_pct_lag_1g,
        {{ calculate_lag('ts_pct', 'player_id', 'game_date', 3) }} as ts_pct_lag_3g,
        {{ calculate_lag('ts_pct', 'player_id', 'game_date', 5) }} as ts_pct_lag_5g,
        {{ calculate_lag('ts_pct', 'player_id', 'game_date', 7) }} as ts_pct_lag_7g,

        {{ calculate_lag('usage_pct', 'player_id', 'game_date', 1) }} as usage_pct_lag_1g,
        {{ calculate_lag('usage_pct', 'player_id', 'game_date', 3) }} as usage_pct_lag_3g,
        {{ calculate_lag('usage_pct', 'player_id', 'game_date', 5) }} as usage_pct_lag_5g,
        {{ calculate_lag('usage_pct', 'player_id', 'game_date', 7) }} as usage_pct_lag_7g,

        {{ calculate_lag('est_usage_pct', 'player_id', 'game_date', 1) }} as est_usage_pct_lag_1g,
        {{ calculate_lag('est_usage_pct', 'player_id', 'game_date', 3) }} as est_usage_pct_lag_3g,
        {{ calculate_lag('est_usage_pct', 'player_id', 'game_date', 5) }} as est_usage_pct_lag_5g,
        {{ calculate_lag('est_usage_pct', 'player_id', 'game_date', 7) }} as est_usage_pct_lag_7g,

        {{ calculate_lag('est_pace', 'player_id', 'game_date', 1) }} as est_pace_lag_1g,
        {{ calculate_lag('est_pace', 'player_id', 'game_date', 3) }} as est_pace_lag_3g,
        {{ calculate_lag('est_pace', 'player_id', 'game_date', 5) }} as est_pace_lag_5g,
        {{ calculate_lag('est_pace', 'player_id', 'game_date', 7) }} as est_pace_lag_7g,

        {{ calculate_lag('pace', 'player_id', 'game_date', 1) }} as pace_lag_1g,
        {{ calculate_lag('pace', 'player_id', 'game_date', 3) }} as pace_lag_3g,
        {{ calculate_lag('pace', 'player_id', 'game_date', 5) }} as pace_lag_5g,
        {{ calculate_lag('pace', 'player_id', 'game_date', 7) }} as pace_lag_7g,

        {{ calculate_lag('pace_per_40', 'player_id', 'game_date', 1) }} as pace_per_40_lag_1g,
        {{ calculate_lag('pace_per_40', 'player_id', 'game_date', 3) }} as pace_per_40_lag_3g,
        {{ calculate_lag('pace_per_40', 'player_id', 'game_date', 5) }} as pace_per_40_lag_5g,
        {{ calculate_lag('pace_per_40', 'player_id', 'game_date', 7) }} as pace_per_40_lag_7g,

        {{ calculate_lag('possessions', 'player_id', 'game_date', 1) }} as possessions_lag_1g,
        {{ calculate_lag('possessions', 'player_id', 'game_date', 3) }} as possessions_lag_3g,
        {{ calculate_lag('possessions', 'player_id', 'game_date', 5) }} as possessions_lag_5g,
        {{ calculate_lag('possessions', 'player_id', 'game_date', 7) }} as possessions_lag_7g,

        {{ calculate_lag('pie', 'player_id', 'game_date', 1) }} as pie_lag_1g,
        {{ calculate_lag('pie', 'player_id', 'game_date', 3) }} as pie_lag_3g,
        {{ calculate_lag('pie', 'player_id', 'game_date', 5) }} as pie_lag_5g,
        {{ calculate_lag('pie', 'player_id', 'game_date', 7) }} as pie_lag_7g,
        
        advanced_boxscore_data.updated_at

    from advanced_boxscore_data
)

select *
from final
