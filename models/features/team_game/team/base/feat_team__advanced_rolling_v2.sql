{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'advanced'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

with advanced_boxscore as (
    select
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
        est_usage_pct,
        est_pace,
        pace,
        pace_per_40,
        possessions,
        pie,

        -- Season-specific game sequence for progressive rolling logic
        ROW_NUMBER() OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date, game_id
        ) as season_game_sequence,

        updated_at

    from {{ ref('int_team__combined_boxscore') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

progressive_rolling_features as (
    select
        -- Keep all original columns
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,
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
        est_usage_pct,
        est_pace,
        pace,
        pace_per_40,
        possessions,
        pie,
        season_game_sequence,
        updated_at,

        -- Generate advanced rolling features
        {{ generate_stat_features_team('est_off_rating') }},
        {{ generate_stat_features_team('off_rating') }},
        {{ generate_stat_features_team('est_def_rating') }},
        {{ generate_stat_features_team('def_rating') }},
        {{ generate_stat_features_team('est_net_rating') }},
        {{ generate_stat_features_team('net_rating') }},
        {{ generate_stat_features_team('ast_pct') }},
        {{ generate_stat_features_team('ast_to_tov_ratio') }},
        {{ generate_stat_features_team('ast_ratio') }},
        {{ generate_stat_features_team('off_reb_pct') }},
        {{ generate_stat_features_team('def_reb_pct') }},
        {{ generate_stat_features_team('reb_pct') }},
        {{ generate_stat_features_team('est_team_tov_pct') }},
        {{ generate_stat_features_team('tov_ratio') }},
        {{ generate_stat_features_team('eff_fg_pct') }},
        {{ generate_stat_features_team('ts_pct') }},
        {{ generate_stat_features_team('est_usage_pct') }},
        {{ generate_stat_features_team('est_pace') }},
        {{ generate_stat_features_team('pace') }},
        {{ generate_stat_features_team('pace_per_40') }},
        {{ generate_stat_features_team('possessions') }},
        {{ generate_stat_features_team('pie') }}

    from advanced_boxscore
)

select *
from progressive_rolling_features
