{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_scoring_v3') }}
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

        -- Game Stats - Shot Distribution
        "percentageFieldGoalsAttempted2pt"::decimal(5,3) as pct_fga_2pt,
        "percentageFieldGoalsAttempted3pt"::decimal(5,3) as pct_fga_3pt,

        -- Game Stats - Scoring Distribution
        "percentagePoints2pt"::decimal(5,3) as pct_pts_2pt,
        "percentagePointsMidrange2pt"::decimal(5,3) as pct_pts_midrange_2pt,
        "percentagePoints3pt"::decimal(5,3) as pct_pts_3pt,
        "percentagePointsFastBreak"::decimal(5,3) as pct_pts_fastbreak,
        "percentagePointsFreeThrow"::decimal(5,3) as pct_pts_ft,
        "percentagePointsOffTurnovers"::decimal(5,3) as pct_pts_off_tov,
        "percentagePointsPaint"::decimal(5,3) as pct_pts_paint,

        -- Game Stats - Assisted vs Unassisted
        "percentageAssisted2pt"::decimal(5,3) as pct_assisted_2pt,
        "percentageUnassisted2pt"::decimal(5,3) as pct_unassisted_2pt,
        "percentageAssisted3pt"::decimal(5,3) as pct_assisted_3pt,
        "percentageUnassisted3pt"::decimal(5,3) as pct_unassisted_3pt,
        "percentageAssistedFGM"::decimal(5,3) as pct_assisted_fgm,
        "percentageUnassistedFGM"::decimal(5,3) as pct_unassisted_fgm,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personId" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final