{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_defensive_v2') }}
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
            WHEN "matchupMinutes" IS NULL OR "matchupMinutes" = '' OR POSITION(':' IN "matchupMinutes") = 0 THEN NULL
            ELSE
                (
                    -- Convert the minutes part to numeric, treating empty string as 0
                    COALESCE(NULLIF(split_part("matchupMinutes", ':', 1), '')::numeric, 0)
                    +
                    -- Convert the seconds part to numeric, treating empty string as 0, then divide by 60
                    COALESCE(NULLIF(split_part("matchupMinutes", ':', 2), '')::numeric, 0) / 60.0
                )::decimal(10, 2) -- Cast the final result to decimal
        END AS matchup_min,
        "partialPossessions"::decimal(10,2) as partial_poss,
        "switchesOn"::integer as def_switches,

        -- Game Stats - Defense
        "playerPoints"::integer as pts_allowed,
        "matchupAssists"::integer as ast_allowed,
        "matchupTurnovers"::integer as tov_forced,

        -- Game Stats - Opponent Shooting
        "matchupFieldGoalsMade"::integer as matchup_fgm,
        "matchupFieldGoalsAttempted"::integer as matchup_fga,
        "matchupFieldGoalPercentage"::decimal(5,3) as matchup_fg_pct,
        "matchupThreePointersMade"::integer as matchup_fg3m,
        "matchupThreePointersAttempted"::integer as matchup_fg3a,
        "matchupThreePointerPercentage"::decimal(5,3) as matchup_fg3_pct,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personId" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final