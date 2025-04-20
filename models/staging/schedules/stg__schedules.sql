{{
    config(
        schema='staging',
        materialized='view'
    )
}}

-- stg__schedules.sql
with source as (
    select * from {{ source('nba_api', 'season_games') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['"gameId"', '"gameDate"', '"seasonYear"']) }} as game_date_season_id,
        
        -- Game Information
        "gameId"::varchar as game_id,
        "leagueId"::varchar as league_id,
        "seasonYear"::varchar as season_year,
        to_date("gameDate", 'MM/DD/YYYY HH24:MI:SS') as game_date,
        "gameCode"::varchar as game_code,
        "gameStatus"::integer as game_status,
        "gameStatusText"::varchar as game_status_text,
        "gameSequence"::integer as game_sequence,
        "gameDateEst"::varchar as game_date_est,
        "gameTimeEst"::varchar as game_time_est,
        "gameDateTimeEst"::varchar as game_date_time_est,
        "gameDateUTC"::varchar as game_date_utc,
        "gameTimeUTC"::varchar as game_time_utc,
        "gameDateTimeUTC"::varchar as game_date_time_utc,
        "day"::varchar as day_of_week,
        "monthNum"::integer as month_num,
        "weekNumber"::integer as week_number,
        "weekName"::varchar as week_name,
        
        -- Game Classification
        "ifNecessary"::varchar as if_necessary,
        "seriesGameNumber"::varchar as series_game_number,
        "gameLabel"::varchar as game_label,
        "gameSubLabel"::varchar as game_sub_label,
        "seriesText"::varchar as series_text,
        "gameSubtype"::varchar as game_subtype,
        "isNeutral"::varchar as is_neutral,
        
        -- Location Information
        "arenaName"::varchar as arena_name,
        "arenaCity"::varchar as arena_city,
        "arenaState"::varchar as arena_state,
        "postponedStatus"::varchar as postponed_status,
        "branchLink"::varchar as branch_link,
        
        -- Home Team Information
        "homeTeam_teamId"::integer as home_team_id,
        "homeTeam_teamName"::varchar as home_team_name,
        "homeTeam_teamCity"::varchar as home_team_city,
        "homeTeam_teamTricode"::varchar as home_team_tricode,
        "homeTeam_teamSlug"::varchar as home_team_slug,
        "homeTeam_wins"::integer as home_team_wins,
        "homeTeam_losses"::integer as home_team_losses,
        "homeTeam_score"::integer as home_team_score,
        "homeTeam_seed"::integer as home_team_seed,
        "homeTeamTime"::varchar as home_team_time,
        
        -- Away Team Information
        "awayTeam_teamId"::integer as away_team_id,
        "awayTeam_teamName"::varchar as away_team_name,
        "awayTeam_teamCity"::varchar as away_team_city,
        "awayTeam_teamTricode"::varchar as away_team_tricode,
        "awayTeam_teamSlug"::varchar as away_team_slug,
        "awayTeam_wins"::integer as away_team_wins,
        "awayTeam_losses"::integer as away_team_losses,
        "awayTeam_score"::integer as away_team_score,
        "awayTeam_seed"::integer as away_team_seed,
        "awayTeamTime"::varchar as away_team_time,
        
        -- Broadcast Information
        "nationalBroadcasters_broadcasterScope"::varchar as national_broadcaster_scope,
        "nationalBroadcasters_broadcasterMedia"::varchar as national_broadcaster_media,
        "nationalBroadcasters_broadcasterId"::integer as national_broadcaster_id,
        "nationalBroadcasters_broadcasterDisplay"::varchar as national_broadcaster_display,
        "nationalBroadcasters_broadcasterAbbreviation"::varchar as national_broadcaster_abbreviation,
        
        -- Timestamps
        updated_at as updated_at,
        current_timestamp as created_at
    from source
)

select * from final 