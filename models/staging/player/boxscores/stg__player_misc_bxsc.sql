{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_misc_v3') }}
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

        -- Game Stats - Misc
        "pointsOffTurnovers"::integer as pts_off_tov,
        "pointsSecondChance"::integer as second_chance_pts,
        "pointsFastBreak"::integer as fastbreak_pts,
        "pointsPaint"::integer as pts_in_paint,
        "oppPointsOffTurnovers"::integer as opp_pts_off_tov_while_on,
        "oppPointsSecondChance"::integer as opp_second_chance_pts_while_on,
        "oppPointsFastBreak"::integer as opp_fastbreak_pts_while_on,
        "oppPointsPaint"::integer as opp_pts_in_paint_while_on,
        "blocksAgainst"::integer as blk_against,
        "foulsDrawn"::integer as fouls_drawn,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personId" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final