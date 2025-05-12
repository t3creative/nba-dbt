{{
    config(
        schema='staging',
        materialized='view'
    )
}}

-- team_traditional_bxsc.sql
with source as (
    select * from {{ source('nba_api', 'team_boxscore_traditional_v3') }}
),

final as (
    select
        -- Primary Keys and Foreign Keys
        {{ dbt_utils.generate_surrogate_key(['"gameId"', '"teamId"']) }} as team_game_key,
        "gameId"::varchar as game_id,
        "teamId"::integer as team_id,

        -- Team Identifiers
        "teamCity"::varchar as team_city,
        "teamName"::varchar as team_name,
        "teamTricode"::varchar as team_tricode,
        "teamSlug"::varchar as team_slug,

        -- Game Stats - Time
        {{ extract_minutes("minutes") }}::decimal(6,1) as min,

        -- Game Stats - Shooting
        "fieldGoalsMade"::integer as fgm,
        "fieldGoalsAttempted"::integer as fga,
        "fieldGoalsPercentage"::decimal(5,3) as fg_pct,
        "threePointersMade"::integer as fg3m,
        "threePointersAttempted"::integer as fg3a,
        "threePointersPercentage"::decimal(5,3) as fg3_pct,
        "freeThrowsMade"::integer as ftm,
        "freeThrowsAttempted"::integer as fta,
        "freeThrowsPercentage"::decimal(5,3) as ft_pct,

        -- Game Stats - Rebounds
        "reboundsOffensive"::integer as off_reb,
        "reboundsDefensive"::integer as def_reb,
        "reboundsTotal"::integer as reb,

        -- Game Stats - Other
        "assists"::integer as ast,
        "steals"::integer as stl,
        "blocks"::integer as blk,
        "turnovers"::integer as tov,
        "foulsPersonal"::integer as pf,
        "points"::integer as pts,
        "plusMinusPoints"::integer as plus_minus,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    where "gameId" is not null 
      and "teamId" is not null
)

select * from final