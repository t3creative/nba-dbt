{{ config(
    schema='features',
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
        player_name,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Advanced Boxscore Stats (Core efficiency metrics)
        off_rating,
        def_rating,
        net_rating,
        pie,
        ts_pct,
        eff_fg_pct,
        usage_pct,
        
        -- Basketball IQ & Flow metrics
        ast_pct,
        ast_ratio,
        ast_to_tov_ratio,
        tov_ratio,
        
        -- Rebounding impact (position-agnostic)
        reb_pct,
        off_reb_pct,
        def_reb_pct,
        
        -- Opportunity volume (individual predictive value)
        possessions,
        pace,
        pace_per_40,
        
        -- Season-specific game sequence for progressive rolling logic
        ROW_NUMBER() OVER (
            PARTITION BY player_id, season_year
            ORDER BY game_date, game_id
        ) as season_game_sequence,

        updated_at

    from {{ ref('int_player__combined_boxscore') }}
),

progressive_rolling_features as (
    select 
        -- Keep all original columns
        player_game_key,
        game_id,
        player_name,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,
        season_game_sequence,
        updated_at,

        -- Generate advanced rolling features (9 features × 14 stats = 126 features)
        {{ generate_stat_features('off_rating') }},
        {{ generate_stat_features('def_rating') }},
        {{ generate_stat_features('net_rating') }},
        {{ generate_stat_features('pie') }},
        {{ generate_stat_features('ts_pct') }},
        {{ generate_stat_features('eff_fg_pct') }},
        {{ generate_stat_features('usage_pct') }},
        {{ generate_stat_features('ast_pct') }},
        {{ generate_stat_features('ast_ratio') }},
        {{ generate_stat_features('ast_to_tov_ratio') }},
        {{ generate_stat_features('tov_ratio') }},
        {{ generate_stat_features('reb_pct') }},
        {{ generate_stat_features('off_reb_pct') }},
        {{ generate_stat_features('def_reb_pct') }},
        {{ generate_stat_features('possessions') }},
        {{ generate_stat_features('pace_per_40') }},
        {{ generate_stat_features('pace') }}


    from advanced_boxscore
)

select *
from progressive_rolling_features