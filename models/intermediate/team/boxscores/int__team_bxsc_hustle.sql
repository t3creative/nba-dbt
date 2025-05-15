{{
    config(
        enabled=true,
        schema='intermediate',
        materialized='incremental',
        unique_key='team_game_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['team_game_key'], 'unique': true},
            {'columns': ['game_id']},
            {'columns': ['team_id']},
            {'columns': ['game_date']},
            {'columns': ['opponent_id']}
        ]
    )
}}

with team_hustle as (
    select * from {{ ref('stg__team_hustle_bxsc') }}
    {% if is_incremental() %}
    where game_id in (
        select distinct game_id 
        from {{ ref('int_opp__game_opponents') }} 
        where game_date > (select max(game_date) from {{ this }}) 
    )
    {% endif %}
),

game_opponents as (
    select 
        game_id,
        team_id,
        opponent_id,
        game_date, 
        season_year,
        home_away
    from {{ ref('int_opp__game_opponents') }}
),

final as (
    select distinct on (th.team_game_key)
        -- Primary Keys and Foreign Keys
        th.team_game_key,
        th.game_id,
        th.team_id,
        gopp.opponent_id,

        -- Game Context from game_opponents map
        gopp.game_date,
        gopp.season_year,
        gopp.home_away,

        -- Team Identifiers
        th.team_city,
        th.team_name,
        th.team_tricode,

        -- Game Stats - Contested Shots
        th.cont_shots,
        th.cont_2pt,
        th.cont_3pt,

        -- Game Stats - Hustle Plays
        th.deflections,
        th.charges_drawn,
        th.screen_ast,
        th.screen_ast_pts,

        -- Game Stats - Loose Balls
        th.off_loose_balls_rec,
        th.def_loose_balls_rec,
        th.tot_loose_balls_rec,

        -- Game Stats - Box Outs
        th.off_box_outs,
        th.def_box_outs,
        th.box_out_team_reb,
        th.box_out_player_reb,
        th.tot_box_outs,

        -- Metadata
        th.created_at,
        th.updated_at
    from team_hustle th
    left join game_opponents gopp 
        on th.game_id = gopp.game_id 
        and th.team_id = gopp.team_id
    order by th.team_game_key, gopp.game_date desc
)

select * from final 