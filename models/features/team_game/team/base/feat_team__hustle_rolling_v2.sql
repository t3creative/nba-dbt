{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'hustle'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

with hustle_boxscore as ( -- Renamed CTE
    select
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Hustle Stats from int_team__combined_boxscore
        cont_shots,
        cont_2pt,
        cont_3pt,
        deflections,
        charges_drawn,
        screen_ast,
        screen_ast_pts,
        off_loose_balls_rec,
        def_loose_balls_rec,
        tot_loose_balls_rec,
        off_box_outs,
        def_box_outs,
        box_out_team_reb,
        box_out_player_reb,
        tot_box_outs,

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
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,
        season_game_sequence,
        updated_at,

        {{ generate_stat_features_team('cont_shots') }},
        {{ generate_stat_features_team('cont_2pt') }},
        {{ generate_stat_features_team('cont_3pt') }},
        {{ generate_stat_features_team('deflections') }},
        {{ generate_stat_features_team('charges_drawn') }},
        {{ generate_stat_features_team('screen_ast') }},
        {{ generate_stat_features_team('screen_ast_pts') }},
        {{ generate_stat_features_team('off_loose_balls_rec') }},
        {{ generate_stat_features_team('def_loose_balls_rec') }},
        {{ generate_stat_features_team('tot_loose_balls_rec') }},
        {{ generate_stat_features_team('off_box_outs') }},
        {{ generate_stat_features_team('def_box_outs') }},
        {{ generate_stat_features_team('box_out_team_reb') }},
        {{ generate_stat_features_team('box_out_player_reb') }},
        {{ generate_stat_features_team('tot_box_outs') }}

    from hustle_boxscore
)

select *
from progressive_rolling_features
