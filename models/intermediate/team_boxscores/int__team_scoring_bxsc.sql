{{
    config(
        enabled=false,
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

with team_scoring as (
    select * from {{ ref('stg__team_scoring_bxsc') }}
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
        ts.team_game_key,
        ts.game_id,
        ts.team_id,
        gopp.opponent_id,

        -- Game Context from game_opponents map
        gopp.game_date,
        gopp.season_year,
        gopp.home_away,

        -- Team Identifiers
        ts.team_city,
        ts.team_name,
        ts.team_tricode,

        -- Game Stats - Time
        ts.min,

        -- Game Stats - Shot Distribution
        ts.pct_fga_2pt,
        ts.pct_fga_3pt,

        -- Game Stats - Points Distribution
        ts.pct_pts_2pt,
        ts.pct_pts_midrange_2pt,
        ts.pct_pts_3pt,
        ts.pct_pts_fastbreak,
        ts.pct_pts_ft,
        ts.pct_pts_off_tov,
        ts.pct_pts_in_paint,

        -- Game Stats - Assisted vs Unassisted
        ts.pct_assisted_2pt,
        ts.pct_unassisted_2pt,
        ts.pct_assisted_3pt,
        ts.pct_unassisted_3pt,
        ts.pct_assisted_fgm,
        ts.pct_unassisted_fgm,

        -- Metadata
        ts.created_at,
        ts.updated_at
    from team_scoring ts
    left join game_opponents gopp 
        on ts.game_id = gopp.game_id 
        and ts.team_id = gopp.team_id
)

select * from final 