{{
    config(
        schema='staging',
        materialized='view'
    )
}}

-- team_scoring_bxsc.sql
with source as (
    select * from {{ source('nba_api', 'team_boxscore_scoring_v3') }}
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

        -- Game Stats - Shot Distribution
        "percentageFieldGoalsAttempted2pt"::decimal(5,3) as pct_fga_2pt,
        "percentageFieldGoalsAttempted3pt"::decimal(5,3) as pct_fga_3pt,

        -- Game Stats - Points Distribution
        "percentagePoints2pt"::decimal(5,3) as pct_pts_2pt,
        "percentagePointsMidrange2pt"::decimal(5,3) as pct_pts_midrange_2pt,
        "percentagePoints3pt"::decimal(5,3) as pct_pts_3pt,
        "percentagePointsFastBreak"::decimal(5,3) as pct_pts_fastbreak,
        "percentagePointsFreeThrow"::decimal(5,3) as pct_pts_ft,
        "percentagePointsOffTurnovers"::decimal(5,3) as pct_pts_off_tov,
        "percentagePointsPaint"::decimal(5,3) as pct_pts_in_paint,

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
    where "gameId" is not null 
      and "teamId" is not null
)

select * from final