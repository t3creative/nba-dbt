{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_hustle_v2') }}
),

final as (
    select
        -- Primary Keys and Foreign Keys
        {{ dbt_utils.generate_surrogate_key(['"gameId"', '"personId"', '"teamId"']) }} as player_game_key,
        "gameId"::varchar as game_id,
        "personId"::integer as player_id,
        "teamId"::integer as team_id,

        -- Team Identifiers
        "teamCity"::varchar as team_city,
        "teamName"::varchar as team_name,
        "teamTricode"::varchar as team_tricode,
        "teamSlug"::varchar as team_slug,

        -- Player Identifiers
        "firstName"::varchar as first_name,
        "familyName"::varchar as family_name,
        "nameI"::varchar as name_initial,
        "playerSlug"::varchar as player_slug,
        "position"::varchar as position,
        "comment"::varchar as comment,
        "jerseyNum"::varchar as jersey_num,

        -- Game Stats - Time
        CASE
            -- Handle NULL, empty string, or missing ':' for the minutes column
            WHEN "minutes" IS NULL OR "minutes" = '' OR POSITION(':' IN "minutes") = 0 THEN NULL
            ELSE
                (
                    -- Convert the minutes part to numeric, treating empty string as 0
                    COALESCE(NULLIF(split_part("minutes", ':', 1), '')::numeric, 0)
                    +
                    -- Convert the seconds part to numeric, treating empty string as 0, then divide by 60
                    COALESCE(NULLIF(split_part("minutes", ':', 2), '')::numeric, 0) / 60.0
                )::decimal(10, 2) -- Cast the final result to decimal
        END AS min,

        -- Game Stats - Hustle Stats
        "points"::integer as pts,
        "contestedShots"::integer as cont_shots,
        "contestedShots2pt"::integer as cont_2pt,
        "contestedShots3pt"::integer as cont_3pt,
        "deflections"::integer as deflections,
        "chargesDrawn"::integer as charges_drawn,
        "screenAssists"::integer as screen_ast,
        "screenAssistPoints"::integer as screen_ast_pts,
        "looseBallsRecoveredOffensive"::integer as off_loose_balls_rec,
        "looseBallsRecoveredDefensive"::integer as def_loose_balls_rec,
        "looseBallsRecoveredTotal"::integer as tot_loose_balls_rec,
        "offensiveBoxOuts"::integer as off_box_outs,
        "defensiveBoxOuts"::integer as def_box_outs,
        "boxOutPlayerTeamRebounds"::integer as box_out_team_reb,
        "boxOutPlayerRebounds"::integer as box_out_player_reb,
        "boxOuts"::integer as tot_box_outs,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personId" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final