{{
    config(
        schema='staging',
        materialized='view'
    )
}}

-- team_misc_bxsc.sql
with source as (
    select * from {{ source('nba_api', 'team_boxscore_misc_v3') }}
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

        -- Game Stats - Points by Type
        "pointsOffTurnovers"::integer as pts_off_tov,
        "pointsSecondChance"::integer as second_chance_pts,
        "pointsFastBreak"::integer as fastbreak_pts,
        "pointsPaint"::integer as pts_in_paint,

        -- Game Stats - Opponent Points
        "oppPointsOffTurnovers"::integer as opp_pts_off_tov,
        "oppPointsSecondChance"::integer as opp_second_chance_pts,
        "oppPointsFastBreak"::integer as opp_fastbreak_pts,
        "oppPointsPaint"::integer as opp_pts_in_paint,

        -- Game Stats - Blocks and Fouls
        "blocks"::integer as blk,
        "blocksAgainst"::integer as blk_against,
        "foulsPersonal"::integer as pf,
        "foulsDrawn"::integer as fouls_drawn,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    where "gameId" is not null 
      and "teamId" is not null
)

select * from final