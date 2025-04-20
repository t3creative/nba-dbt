{{
    config(
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

with team_misc as (
    select * from {{ ref('stg__team_misc_bxsc') }}
    {% if is_incremental() %}
    where game_id in (
        select distinct game_id 
        from {{ ref('int__game_opponents') }} 
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
    from {{ ref('int__game_opponents') }}
),

final as (
    select
        -- Primary Keys and Foreign Keys
        tm.team_game_key,
        tm.game_id,
        tm.team_id,
        gopp.opponent_id,

        -- Game Context from game_opponents map
        gopp.game_date,
        gopp.season_year,
        gopp.home_away,

        -- Team Identifiers
        tm.team_city,
        tm.team_name,
        tm.team_tricode,

        -- Game Stats - Time
        tm.min,

        -- Game Stats - Points by Type
        tm.pts_off_tov,
        tm.second_chance_pts,
        tm.fastbreak_pts,
        tm.pts_in_paint,

        -- Game Stats - Opponent Points
        tm.opp_pts_off_tov,
        tm.opp_second_chance_pts,
        tm.opp_fastbreak_pts,
        tm.opp_pts_in_paint,

        -- Game Stats - Blocks and Fouls
        tm.blk,
        tm.blk_against,
        tm.pf,
        tm.fouls_drawn,

        -- Metadata
        tm.created_at,
        tm.updated_at
    from team_misc tm
    left join game_opponents gopp 
        on tm.game_id = gopp.game_id 
        and tm.team_id = gopp.team_id
)

select * from final 