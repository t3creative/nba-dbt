{{
    config(
        enabled=true,   
        schema='intermediate',
        materialized='incremental',
        unique_key='team_game_key',
        on_schema_change='delete+insert',
        indexes=[
            {'columns': ['team_game_key'], 'unique': true},
            {'columns': ['game_id']},
            {'columns': ['team_id']},
            {'columns': ['game_date']},
            {'columns': ['opponent_id']}
        ]
    )
}}

with team_advanced as (
    select * from {{ ref('stg__team_advanced_bxsc') }}
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
    select distinct on (ta.team_game_key)
        -- Primary Keys and Foreign Keys
        ta.team_game_key,
        ta.game_id,
        ta.team_id,
        gopp.opponent_id,

        -- Game Context from game_opponents map
        gopp.game_date,
        gopp.season_year,
        gopp.home_away,

        -- Team Identifiers
        ta.team_city,
        ta.team_name,
        ta.team_tricode,

        -- Game Stats - Time
        ta.min,

        -- Game Stats - Ratings
        ta.est_off_rating,
        ta.off_rating,
        ta.est_def_rating,
        ta.def_rating,
        ta.est_net_rating,
        ta.net_rating,

        -- Game Stats - Advanced Metrics
        ta.ast_pct,
        ta.ast_to_tov_ratio,
        ta.ast_ratio,
        ta.off_reb_pct,
        ta.def_reb_pct,
        ta.reb_pct,
        ta.est_team_tov_pct,
        ta.tov_ratio,

        -- Game Stats - Shooting Efficiency
        ta.eff_fg_pct,
        ta.ts_pct,

        -- Game Stats - Usage and Pace
        ta.usage_pct,
        ta.est_usage_pct,
        ta.est_pace,
        ta.pace,
        ta.pace_per_40,
        ta.possessions,
        ta.pie,

        -- Metadata
        ta.created_at,
        ta.updated_at
    from team_advanced ta
    left join game_opponents gopp 
        on ta.game_id = gopp.game_id 
        and ta.team_id = gopp.team_id
    order by ta.team_game_key, gopp.game_date desc
)

select * from final 