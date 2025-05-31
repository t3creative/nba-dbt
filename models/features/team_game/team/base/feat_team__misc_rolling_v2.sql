{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'misc'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

WITH misc_boxscore AS (
    SELECT
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Misc Stats from int_team__combined_boxscore
        pts_off_tov,
        second_chance_pts,
        fastbreak_pts,
        pts_in_paint,
        opp_pts_off_tov,        -- Points team's defense allowed off opponent turnovers
        opp_second_chance_pts,  -- Points team's defense allowed on opponent second chances
        opp_fastbreak_pts,      -- Points team's defense allowed on opponent fastbreaks
        opp_pts_in_paint,       -- Points team's defense allowed in the paint by opponent

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
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,
        season_game_sequence,
        updated_at,

        {{ generate_stat_features_team('pts_off_tov') }},
        {{ generate_stat_features_team('second_chance_pts') }},
        {{ generate_stat_features_team('fastbreak_pts') }},
        {{ generate_stat_features_team('pts_in_paint') }},
        {{ generate_stat_features_team('opp_pts_off_tov') }},
        {{ generate_stat_features_team('opp_second_chance_pts') }},
        {{ generate_stat_features_team('opp_fastbreak_pts') }},
        {{ generate_stat_features_team('opp_pts_in_paint') }}

    from misc_boxscore
)

select *
from progressive_rolling_features
