{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_player_track_v3') }}
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

        -- Game Stats - Speed and Distance
        "speed"::decimal(10,2) as speed,
        "distance"::decimal(10,2) as distance,

        -- Game Stats - Rebounds
        "reboundChancesDefensive"::integer as def_reb_chances,
        "reboundChancesOffensive"::integer as off_reb_chances,
        "reboundChancesTotal"::integer as reb_chances,
        
        -- Game Stats - Touches
        "touches"::integer as touches,
        "secondaryAssists"::integer as secondary_ast,
        "freeThrowAssists"::integer as ft_ast,
        "passes"::integer as passes,

        -- Contested Field Goals
        "contestedFieldGoalsMade"::integer as cont_fgm,
        "contestedFieldGoalsAttempted"::integer as cont_fga,
        "contestedFieldGoalPercentage"::decimal(5,3) as cont_fg_pct,

        -- Uncontested Field Goals
        "uncontestedFieldGoalsMade"::integer as uncont_fgm,
        "uncontestedFieldGoalsAttempted"::integer as uncont_fga,
        "uncontestedFieldGoalsPercentage"::decimal(5,3) as uncont_fg_pct,

        -- Defended At Rim 
        "defendedAtRimFieldGoalsMade"::integer as def_at_rim_fgm,
        "defendedAtRimFieldGoalsAttempted"::integer as def_at_rim_fga,
        "defendedAtRimFieldGoalPercentage"::decimal(5,3) as def_at_rim_fg_pct,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personId" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final