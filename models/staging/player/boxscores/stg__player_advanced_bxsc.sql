{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_boxscore_advanced_v3') }}
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

        -- Game Stats - Ratings
        "estimatedOffensiveRating"::decimal(10,2) as est_off_rating,
        "offensiveRating"::decimal(10,2) as off_rating,
        "estimatedDefensiveRating"::decimal(10,2) as est_def_rating,
        "defensiveRating"::decimal(10,2) as def_rating,
        "estimatedNetRating"::decimal(10,2) as est_net_rating,
        "netRating"::decimal(10,2) as net_rating,

        -- Game Stats - Advanced Metrics
        "assistPercentage"::decimal(5,3) as ast_pct,
        "assistToTurnover"::decimal(10,2) as ast_to_tov_ratio,
        "assistRatio"::decimal(10,2) as ast_ratio,
        "offensiveReboundPercentage"::decimal(5,3) as off_reb_pct,
        "defensiveReboundPercentage"::decimal(5,3) as def_reb_pct,
        "reboundPercentage"::decimal(5,3) as reb_pct,
        "turnoverRatio"::decimal(10,2) as tov_ratio,

        -- Game Stats - Shooting Efficiency
        "effectiveFieldGoalPercentage"::decimal(5,3) as eff_fg_pct,
        "trueShootingPercentage"::decimal(5,3) as ts_pct,

        -- Game Stats - Usage and Pace
        "usagePercentage"::decimal(5,3) as usage_pct,
        "estimatedUsagePercentage"::decimal(5,3) as est_usage_pct,
        "estimatedPace"::decimal(10,2) as est_pace,
        "pace"::decimal(10,2) as pace,
        "pacePer40"::decimal(10,2) as pace_per_40,
        "possessions"::integer as possessions,
        "PIE"::decimal(5,3) as pie,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    WHERE "gameId" IS NOT NULL 
      AND "personId" IS NOT NULL
      AND "teamId" IS NOT NULL
)

select * from final