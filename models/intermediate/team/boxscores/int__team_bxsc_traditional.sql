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

with team_traditional as (
    select * from {{ ref('stg__team_traditional_bxsc') }}
    {% if is_incremental() %}
    where game_id in (
        select distinct game_id 
        from {{ ref('feat_opp__game_opponents') }} 
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
    from {{ ref('feat_opp__game_opponents') }}
),

final as (
    select distinct on (tt.team_game_key)
        -- Primary Keys and Foreign Keys
        tt.team_game_key,
        tt.game_id,
        tt.team_id,
        gopp.opponent_id,

        -- Game Context from game_opponents map
        gopp.game_date,
        gopp.season_year,
        gopp.home_away,

        -- Team Identifiers
        tt.team_city,
        tt.team_name,
        tt.team_tricode,

        -- Game Stats - Time
        tt.min,

        -- Game Stats - Shooting
        tt.fgm,
        tt.fga,
        tt.fg_pct,
        tt.fg3m,
        tt.fg3a,
        tt.fg3_pct,
        tt.ftm,
        tt.fta,
        tt.ft_pct,

        -- Game Stats - Rebounds
        tt.off_reb,
        tt.def_reb,
        tt.reb,

        -- Game Stats - Other
        tt.ast,
        tt.stl,
        tt.blk,
        tt.tov,
        tt.pf,
        tt.pts,
        tt.plus_minus,

        -- Metadata
        tt.created_at,
        tt.updated_at
    from team_traditional tt
    left join game_opponents gopp
        on tt.game_id = gopp.game_id 
        and tt.team_id = gopp.team_id
    order by tt.team_game_key, gopp.game_date desc
)

select * from final 