{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_usage_v3') }}
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

        -- Game Stats - Field Goals
        "percentageFieldGoalsMade"::decimal(5,3) as pct_of_team_fgm,
        "percentageFieldGoalsAttempted"::decimal(5,3) as pct_of_team_fga,

        -- Game Stats - Three Pointers
        "percentageThreePointersMade"::decimal(5,3) as pct_of_team_fg3m,
        "percentageThreePointersAttempted"::decimal(5,3) as pct_of_team_fg3a,

        -- Game Stats - Free Throws
        "percentageFreeThrowsMade"::decimal(5,3) as pct_of_team_ftm,
        "percentageFreeThrowsAttempted"::decimal(5,3) as pct_of_team_fta,

        -- Game Stats - Rebounds
        "percentageReboundsOffensive"::decimal(5,3) as pct_of_team_oreb,
        "percentageReboundsDefensive"::decimal(5,3) as pct_of_team_dreb,
        "percentageReboundsTotal"::decimal(5,3) as pct_of_team_reb,

        -- Game Stats - Other Percentages
        "percentageAssists"::decimal(5,3) as pct_of_team_ast,
        "percentageTurnovers"::decimal(5,3) as pct_of_team_tov,
        "percentageSteals"::decimal(5,3) as pct_of_team_stl,
        "percentageBlocks"::decimal(5,3) as pct_of_team_blk,
        "percentageBlocksAllowed"::decimal(5,3) as pct_of_team_blk_allowed,
        "percentagePersonalFouls"::decimal(5,3) as pct_of_team_pf,
        "percentagePersonalFoulsDrawn"::decimal(5,3) as pct_of_team_pfd,
        "percentagePoints"::decimal(5,3) as pct_of_team_pts,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personId" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final
