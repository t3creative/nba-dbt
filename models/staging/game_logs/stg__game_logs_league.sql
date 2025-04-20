{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'league_game_logs') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['"SEASON_ID"', '"TEAM_ID"','"GAME_ID"']) }} as season_team_game_id,
        
        -- Game and Season Info
        "GAME_ID"::varchar as game_id,
        "GAME_DATE"::date as game_date,
        "SEASON_ID"::integer as season_id,
        
        -- Team Info
        "TEAM_ID"::integer as team_id,
        "TEAM_ABBREVIATION"::varchar as team_tricode,
        "TEAM_NAME"::varchar as team_name,
        "MATCHUP"::varchar as matchup,
        "WL"::varchar as win_loss,
        
        -- Game Statistics
        NULLIF(regexp_replace("MIN"::text, '[^0-9.]', ''), '')::decimal(10,2) as min,
        "FGM"::integer as fgm,
        "FGA"::integer as fga,
        "FG_PCT"::decimal(5,3) as fg_pct,
        "FG3M"::integer as fg3m,
        "FG3A"::integer as fg3a,
        "FG3_PCT"::decimal(5,3) as fg3_pct,
        "FTM"::integer as ftm,
        "FTA"::integer as fta,
        "FT_PCT"::decimal(5,3) as ft_pct,
        "OREB"::integer as off_reb,
        "DREB"::integer as def_reb,
        "REB"::integer as reb,
        "AST"::integer as ast,
        "STL"::integer as stl,
        "BLK"::integer as blk,
        "TOV"::integer as tov,
        "PF"::integer as pf,
        "PTS"::integer as pts,
        "PLUS_MINUS"::integer as plus_minus,
        
        -- Additional Info
        "VIDEO_AVAILABLE"::integer as video_available,
        
        -- Metadata
        current_timestamp as created_at
    from source
)

select * from final 