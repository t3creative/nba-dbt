{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'traditional'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

WITH traditional_boxscore AS (
    SELECT
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,
        -- Traditional Stats
        fgm,
        fga,
        fg_pct,
        fg3m,
        fg3a,
        fg3_pct,
        ftm,
        fta,
        ft_pct,
        off_reb,
        def_reb,
        reb,
        ast,
        stl,
        blk,
        tov,
        pf,
        pts,
        plus_minus,

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
    SELECT
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,
        season_game_sequence,
        updated_at,

        {{ generate_stat_features_team('fgm') }},
        {{ generate_stat_features_team('fga') }},
        {{ generate_stat_features_team('fg_pct') }},
        {{ generate_stat_features_team('fg3m') }},
        {{ generate_stat_features_team('fg3a') }},
        {{ generate_stat_features_team('fg3_pct') }},
        {{ generate_stat_features_team('ftm') }},
        {{ generate_stat_features_team('fta') }},
        {{ generate_stat_features_team('ft_pct') }},
        {{ generate_stat_features_team('off_reb') }},
        {{ generate_stat_features_team('def_reb') }},
        {{ generate_stat_features_team('reb') }},
        {{ generate_stat_features_team('ast') }},
        {{ generate_stat_features_team('stl') }},
        {{ generate_stat_features_team('blk') }},
        {{ generate_stat_features_team('tov') }},
        {{ generate_stat_features_team('pf') }},
        {{ generate_stat_features_team('pts') }},
        {{ generate_stat_features_team('plus_minus') }}
        
    from traditional_boxscore
)

select *
from progressive_rolling_features
