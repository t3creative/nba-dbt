{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'scoring'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

WITH scoring_boxscore AS (
    SELECT
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Scoring Stats from int_team__combined_boxscore
        pct_fga_2pt,
        pct_fga_3pt,
        pct_pts_2pt,
        pct_pts_midrange_2pt,
        pct_pts_3pt,
        pct_pts_fastbreak,
        pct_pts_ft,
        pct_pts_off_tov,
        pct_pts_in_paint,
        pct_assisted_2pt,
        pct_unassisted_2pt,
        pct_assisted_3pt,
        pct_unassisted_3pt,
        pct_assisted_fgm,
        pct_unassisted_fgm,

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

        -- Generate advanced rolling features
        {{ generate_stat_features_team('pct_fga_2pt') }},
        {{ generate_stat_features_team('pct_fga_3pt') }},
        {{ generate_stat_features_team('pct_pts_2pt') }},
        {{ generate_stat_features_team('pct_pts_midrange_2pt') }},
        {{ generate_stat_features_team('pct_pts_3pt') }},
        {{ generate_stat_features_team('pct_pts_fastbreak') }},
        {{ generate_stat_features_team('pct_pts_ft') }},
        {{ generate_stat_features_team('pct_pts_off_tov') }},
        {{ generate_stat_features_team('pct_pts_in_paint') }},
        {{ generate_stat_features_team('pct_assisted_2pt') }},
        {{ generate_stat_features_team('pct_unassisted_2pt') }},
        {{ generate_stat_features_team('pct_assisted_3pt') }},
        {{ generate_stat_features_team('pct_unassisted_3pt') }},
        {{ generate_stat_features_team('pct_assisted_fgm') }},
        {{ generate_stat_features_team('pct_unassisted_fgm') }}

    from scoring_boxscore
)

select *
from progressive_rolling_features
