{{
    config(
        schema='staging',
        materialized='view'
    )
}}

-- team_hustle_bxsc.sql
with source as (
    select * from {{ source('nba_api', 'team_boxscore_hustle_v2') }}
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

        -- Game Stats - Basic
        "points"::integer as pts,

        -- Game Stats - Contested Shots
        "contestedShots"::integer as cont_shots,
        "contestedShots2pt"::integer as cont_2pt,
        "contestedShots3pt"::integer as cont_3pt,

        -- Game Stats - Hustle Plays
        "deflections"::integer as deflections,
        "chargesDrawn"::integer as charges_drawn,
        "screenAssists"::integer as screen_ast,
        "screenAssistPoints"::integer as screen_ast_pts,

        -- Game Stats - Loose Balls
        "looseBallsRecoveredOffensive"::integer as off_loose_balls_rec,
        "looseBallsRecoveredDefensive"::integer as def_loose_balls_rec,
        "looseBallsRecoveredTotal"::integer as tot_loose_balls_rec,

        -- Game Stats - Box Outs
        "offensiveBoxOuts"::integer as off_box_outs,
        "defensiveBoxOuts"::integer as def_box_outs,
        "boxOutPlayerTeamRebounds"::integer as box_out_team_reb,
        "boxOutPlayerRebounds"::integer as box_out_player_reb,
        "boxOuts"::integer as tot_box_outs,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    where "gameId" is not null 
      and "teamId" is not null
)

select * from final
