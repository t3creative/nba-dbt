{{
    config(
        schema='staging',
        materialized='view'
    )
}}

-- team_advanced_bxsc.sql
with source as (
    select * from {{ source('nba_api', 'team_boxscore_advanced_v3') }}
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

        -- Game Stats - Ratings
        "estimatedOffensiveRating"::decimal(7,2) as est_off_rating,
        "offensiveRating"::decimal(7,2) as off_rating,
        "estimatedDefensiveRating"::decimal(7,2) as est_def_rating,
        "defensiveRating"::decimal(7,2) as def_rating,
        "estimatedNetRating"::decimal(7,2) as est_net_rating,
        "netRating"::decimal(7,2) as net_rating,

        -- Game Stats - Advanced Metrics
        "assistPercentage"::decimal(5,3) as ast_pct,
        "assistToTurnover"::decimal(5,2) as ast_to_tov_ratio,
        "assistRatio"::decimal(5,3) as ast_ratio,
        "offensiveReboundPercentage"::decimal(5,3) as off_reb_pct,
        "defensiveReboundPercentage"::decimal(5,3) as def_reb_pct,
        "reboundPercentage"::decimal(5,3) as reb_pct,
        "estimatedTeamTurnoverPercentage"::decimal(5,3) as est_team_tov_pct,
        "turnoverRatio"::decimal(5,3) as tov_ratio,

        -- Game Stats - Shooting Efficiency
        "effectiveFieldGoalPercentage"::decimal(5,3) as eff_fg_pct,
        "trueShootingPercentage"::decimal(5,3) as ts_pct,

        -- Game Stats - Usage and Pace
        "usagePercentage"::decimal(5,3) as usage_pct,
        "estimatedUsagePercentage"::decimal(5,3) as est_usage_pct,
        "estimatedPace"::decimal(6,2) as est_pace,
        "pace"::decimal(6,2) as pace,
        "pacePer40"::decimal(6,2) as pace_per_40,
        "possessions"::integer as possessions,
        "PIE"::decimal(5,3) as pie,

        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source
    where "gameId" is not null 
      and "teamId" is not null
)

select * from final