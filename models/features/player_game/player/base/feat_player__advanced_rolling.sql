{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'advanced'],
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

with advanced_boxscore as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Advanced Boxscore Stats
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
    -- Look back a fixed number of days to ensure rolling calculations are correct near the incremental boundary.
    -- This window should be large enough to capture the game history needed for the longest rolling window.
    and game_date >= (
        select max(game_date) - interval '90 days'
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

        -- Advanced Boxscore Stats
        round(({{ calculate_rolling_avg('est_off_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as est_off_rating_roll_3g_avg,
        round(({{ calculate_rolling_avg('est_off_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as est_off_rating_roll_5g_avg,
        round(({{ calculate_rolling_avg('est_off_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as est_off_rating_roll_10g_avg,
        round(({{ calculate_rolling_stddev('est_off_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as est_off_rating_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('est_off_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as est_off_rating_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('est_off_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as est_off_rating_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as off_rating_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as off_rating_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as off_rating_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as off_rating_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as off_rating_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as off_rating_roll_10g_stddev,

        round(({{ calculate_rolling_avg('est_def_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as est_def_rating_roll_3g_avg,
        round(({{ calculate_rolling_avg('est_def_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as est_def_rating_roll_5g_avg,
        round(({{ calculate_rolling_avg('est_def_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as est_def_rating_roll_10g_avg,
        round(({{ calculate_rolling_stddev('est_def_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as est_def_rating_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('est_def_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as est_def_rating_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('est_def_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as est_def_rating_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as def_rating_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as def_rating_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as def_rating_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as def_rating_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as def_rating_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as def_rating_roll_10g_stddev,

        round(({{ calculate_rolling_avg('est_net_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as est_net_rating_roll_3g_avg,
        round(({{ calculate_rolling_avg('est_net_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as est_net_rating_roll_5g_avg,
        round(({{ calculate_rolling_avg('est_net_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as est_net_rating_roll_10g_avg,
        round(({{ calculate_rolling_stddev('est_net_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as est_net_rating_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('est_net_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as est_net_rating_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('est_net_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as est_net_rating_roll_10g_stddev,

        round(({{ calculate_rolling_avg('net_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as net_rating_roll_3g_avg,
        round(({{ calculate_rolling_avg('net_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as net_rating_roll_5g_avg,
        round(({{ calculate_rolling_avg('net_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as net_rating_roll_10g_avg,
        round(({{ calculate_rolling_stddev('net_rating', 'player_id', 'game_date', 3) }})::numeric, 3) as net_rating_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('net_rating', 'player_id', 'game_date', 5) }})::numeric, 3) as net_rating_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('net_rating', 'player_id', 'game_date', 10) }})::numeric, 3) as net_rating_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ast_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('ast_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('ast_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ast_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ast_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ast_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ast_to_tov_ratio', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_to_tov_ratio_roll_3g_avg,
        round(({{ calculate_rolling_avg('ast_to_tov_ratio', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_to_tov_ratio_roll_5g_avg,
        round(({{ calculate_rolling_avg('ast_to_tov_ratio', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_to_tov_ratio_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ast_to_tov_ratio', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_to_tov_ratio_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ast_to_tov_ratio', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_to_tov_ratio_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ast_to_tov_ratio', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_to_tov_ratio_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ast_ratio', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_ratio_roll_3g_avg,
        round(({{ calculate_rolling_avg('ast_ratio', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_ratio_roll_5g_avg,
        round(({{ calculate_rolling_avg('ast_ratio', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_ratio_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ast_ratio', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_ratio_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ast_ratio', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_ratio_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ast_ratio', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_ratio_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_reb_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as off_reb_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_reb_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as off_reb_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_reb_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as off_reb_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_reb_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as off_reb_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_reb_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as off_reb_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_reb_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as off_reb_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_reb_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as def_reb_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_reb_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as def_reb_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_reb_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as def_reb_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_reb_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as def_reb_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_reb_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as def_reb_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_reb_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as def_reb_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('reb_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as reb_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('reb_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as reb_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('reb_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as reb_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('reb_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as reb_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('reb_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as reb_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('reb_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as reb_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('tov_ratio', 'player_id', 'game_date', 3) }})::numeric, 3) as tov_ratio_roll_3g_avg,
        round(({{ calculate_rolling_avg('tov_ratio', 'player_id', 'game_date', 5) }})::numeric, 3) as tov_ratio_roll_5g_avg,
        round(({{ calculate_rolling_avg('tov_ratio', 'player_id', 'game_date', 10) }})::numeric, 3) as tov_ratio_roll_10g_avg,
        round(({{ calculate_rolling_stddev('tov_ratio', 'player_id', 'game_date', 3) }})::numeric, 3) as tov_ratio_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('tov_ratio', 'player_id', 'game_date', 5) }})::numeric, 3) as tov_ratio_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('tov_ratio', 'player_id', 'game_date', 10) }})::numeric, 3) as tov_ratio_roll_10g_stddev,

        round(({{ calculate_rolling_avg('eff_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as eff_fg_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('eff_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as eff_fg_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('eff_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as eff_fg_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('eff_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as eff_fg_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('eff_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as eff_fg_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('eff_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as eff_fg_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ts_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as ts_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('ts_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as ts_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('ts_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as ts_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ts_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as ts_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ts_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as ts_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ts_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as ts_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('usage_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as usage_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('usage_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as usage_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('usage_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as usage_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('usage_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as usage_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('usage_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as usage_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('usage_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as usage_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('est_usage_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as est_usage_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('est_usage_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as est_usage_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('est_usage_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as est_usage_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('est_usage_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as est_usage_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('est_usage_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as est_usage_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('est_usage_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as est_usage_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('est_pace', 'player_id', 'game_date', 3) }})::numeric, 3) as est_pace_roll_3g_avg,
        round(({{ calculate_rolling_avg('est_pace', 'player_id', 'game_date', 5) }})::numeric, 3) as est_pace_roll_5g_avg,
        round(({{ calculate_rolling_avg('est_pace', 'player_id', 'game_date', 10) }})::numeric, 3) as est_pace_roll_10g_avg,
        round(({{ calculate_rolling_stddev('est_pace', 'player_id', 'game_date', 3) }})::numeric, 3) as est_pace_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('est_pace', 'player_id', 'game_date', 5) }})::numeric, 3) as est_pace_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('est_pace', 'player_id', 'game_date', 10) }})::numeric, 3) as est_pace_roll_10g_stddev,
        
        round(({{ calculate_rolling_avg('pace', 'player_id', 'game_date', 3) }})::numeric, 3) as pace_roll_3g_avg,
        round(({{ calculate_rolling_avg('pace', 'player_id', 'game_date', 5) }})::numeric, 3) as pace_roll_5g_avg,
        round(({{ calculate_rolling_avg('pace', 'player_id', 'game_date', 10) }})::numeric, 3) as pace_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pace', 'player_id', 'game_date', 3) }})::numeric, 3) as pace_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pace', 'player_id', 'game_date', 5) }})::numeric, 3) as pace_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pace', 'player_id', 'game_date', 10) }})::numeric, 3) as pace_roll_10g_stddev,
        
        round(({{ calculate_rolling_avg('pace_per_40', 'player_id', 'game_date', 3) }})::numeric, 3) as pace_per_40_roll_3g_avg,
        round(({{ calculate_rolling_avg('pace_per_40', 'player_id', 'game_date', 5) }})::numeric, 3) as pace_per_40_roll_5g_avg,
        round(({{ calculate_rolling_avg('pace_per_40', 'player_id', 'game_date', 10) }})::numeric, 3) as pace_per_40_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pace_per_40', 'player_id', 'game_date', 3) }})::numeric, 3) as pace_per_40_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pace_per_40', 'player_id', 'game_date', 5) }})::numeric, 3) as pace_per_40_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pace_per_40', 'player_id', 'game_date', 10) }})::numeric, 3) as pace_per_40_roll_10g_stddev,
        
        round(({{ calculate_rolling_avg('possessions', 'player_id', 'game_date', 3) }})::numeric, 3) as possessions_roll_3g_avg,
        round(({{ calculate_rolling_avg('possessions', 'player_id', 'game_date', 5) }})::numeric, 3) as possessions_roll_5g_avg,
        round(({{ calculate_rolling_avg('possessions', 'player_id', 'game_date', 10) }})::numeric, 3) as possessions_roll_10g_avg,
        round(({{ calculate_rolling_stddev('possessions', 'player_id', 'game_date', 3) }})::numeric, 3) as possessions_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('possessions', 'player_id', 'game_date', 5) }})::numeric, 3) as possessions_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('possessions', 'player_id', 'game_date', 10) }})::numeric, 3) as possessions_roll_10g_stddev,
        
        round(({{ calculate_rolling_avg('pie', 'player_id', 'game_date', 3) }})::numeric, 3) as pie_roll_3g_avg,
        round(({{ calculate_rolling_avg('pie', 'player_id', 'game_date', 5) }})::numeric, 3) as pie_roll_5g_avg,
        round(({{ calculate_rolling_avg('pie', 'player_id', 'game_date', 10) }})::numeric, 3) as pie_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pie', 'player_id', 'game_date', 3) }})::numeric, 3) as pie_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pie', 'player_id', 'game_date', 5) }})::numeric, 3) as pie_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pie', 'player_id', 'game_date', 10) }})::numeric, 3) as pie_roll_10g_stddev,

        advanced_boxscore.updated_at

    from advanced_boxscore
)

select *
from final 