{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'traditional'],
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

with traditional_boxscore as (
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

        -- Traditional Boxscore Stats
        min, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct, reb, off_reb, def_reb, ast,
        stl, blk, tov, pf, plus_minus,

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
        pts,

        -- Generate features for each stat (AVG, STDDEV, MAX, MIN across 3/5/10 games)
        {{ generate_stat_features('min') }},
        {{ generate_stat_features('pts') }},
        {{ generate_stat_features('fgm') }},
        {{ generate_stat_features('fga') }},
        {{ generate_stat_features('fg_pct') }},
        {{ generate_stat_features('fg3m') }},
        {{ generate_stat_features('fg3a') }},
        {{ generate_stat_features('fg3_pct') }},
        {{ generate_stat_features('ftm') }},
        {{ generate_stat_features('fta') }},
        {{ generate_stat_features('ft_pct') }},
        {{ generate_stat_features('reb') }},
        {{ generate_stat_features('off_reb') }},
        {{ generate_stat_features('def_reb') }},
        {{ generate_stat_features('ast') }},
        {{ generate_stat_features('stl') }},
        {{ generate_stat_features('blk') }},
        {{ generate_stat_features('tov') }},
        {{ generate_stat_features('pf') }},
        {{ generate_stat_features('plus_minus') }}

    from traditional_boxscore
)

select *
from progressive_rolling_features