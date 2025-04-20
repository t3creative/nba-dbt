{{
    config(
        schema='staging',
        materialized='view',
        unique_key='player_matchup_key'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_matchups_v3') }}
),

final as (
    select
        -- Primary Keys and Foreign Keys
        {{ dbt_utils.generate_surrogate_key(['"gameId"', '"personIdOff"', '"personIdDef"', '"teamId"']) }} as player_matchup_key,
        "gameId"::varchar as game_id,
        "personIdOff"::integer as off_player_id,
        "personIdDef"::integer as def_player_id,
        "teamId"::integer as team_id,

        -- Team Identifiers
        "teamCity"::varchar as team_city,
        "teamName"::varchar as team_name,
        "teamTricode"::varchar as team_tricode,
        "teamSlug"::varchar as team_slug,

        -- Offensive Player Identifiers
        "firstNameOff"::varchar as off_first_name,
        "familyNameOff"::varchar as off_family_name,
        "nameIOff"::varchar as off_name_initial,
        "playerSlugOff"::varchar as off_player_slug,
        "positionOff"::varchar as off_position,
        "commentOff"::varchar as off_comment,
        "jerseyNumOff"::varchar as off_jersey_num,

        -- Defensive Player Identifiers
        "firstNameDef"::varchar as def_first_name,
        "familyNameDef"::varchar as def_family_name,
        "nameIDef"::varchar as def_name_initial,
        "playerSlugDef"::varchar as def_player_slug,
        "jerseyNumDef"::varchar as def_jersey_num,

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
        "matchupMinutesSort"::decimal(10,2) as matchup_min_sort,
        "partialPossessions"::decimal(10,2) as partial_poss,
        "percentageDefenderTotalTime"::decimal(5,3) as def_time_pct,
        "percentageOffensiveTotalTime"::decimal(5,3) as off_time_pct,
        "percentageTotalTimeBothOn"::decimal(5,3) as both_on_time_pct,
        "switchesOn"::integer as switches_on,

        -- Game Stats - Offensive
        "playerPoints"::integer as off_player_pts,
        "teamPoints"::integer as off_team_pts,
        "matchupAssists"::integer as off_matchup_ast,
        "matchupPotentialAssists"::integer as off_matchup_pot_ast,
        "matchupTurnovers"::integer as off_matchup_tov,

        -- Game Stats - Shooting
        "matchupFieldGoalsMade"::integer as off_matchup_fgm,
        "matchupFieldGoalsAttempted"::integer as off_matchup_fga,
        "matchupFieldGoalsPercentage"::decimal(5,3) as off_matchup_fg_pct,
        "matchupThreePointersMade"::integer as off_matchup_fg3m,
        "matchupThreePointersAttempted"::integer as off_matchup_fg3a,
        "matchupThreePointersPercentage"::decimal(5,3) as off_matchup_fg3_pct,
        "matchupFreeThrowsMade"::integer as off_matchup_ftm,
        "matchupFreeThrowsAttempted"::integer as off_matchup_fta,

        -- Game Stats - Defense
        "matchupBlocks"::integer as def_matchup_blk,
        "helpBlocks"::integer as def_help_blk,
        "helpFieldGoalsMade"::integer as def_help_fgm,
        "helpFieldGoalsAttempted"::integer as def_help_fga,
        "helpFieldGoalsPercentage"::decimal(5,3) as def_help_fg_pct,
        "shootingFouls"::integer as def_shooting_fouls,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personIdOff" IS NOT NULL
      AND "personIdDef" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final