{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'player_game_logs') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['"PLAYER_ID"', '"GAME_ID"', '"TEAM_ID"']) }} as player_game_team_id,
        
        -- Game and Season Info
        "GAME_ID"::varchar as game_id,
        "GAME_DATE"::date as game_date,
        "SEASON_YEAR"::varchar as season_year,
        "MATCHUP"::varchar as matchup,
        "WL"::varchar as win_loss,
        
        -- Player Info
        "PLAYER_ID"::integer as player_id,
        "PLAYER_NAME"::varchar as player_name,
        
        -- Team Info
        "TEAM_ID"::integer as team_id,
        "TEAM_ABBREVIATION"::varchar as team_tricode,
        "TEAM_NAME"::varchar as team_name,
        
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
        "TOV"::integer as tov,
        "STL"::integer as stl,
        "BLK"::integer as blk,
        "BLKA"::integer as blk_against,
        "PF"::integer as pf,
        "PFD"::integer as pf_drawn,
        "PTS"::integer as pts,
        "PLUS_MINUS"::integer as plus_minus,
        
        -- Fantasy Stats
        "NBA_FANTASY_PTS"::decimal(10,2) as fantasy_points,
        "DD2"::integer as double_doubles,
        "TD3"::integer as triple_doubles,
        
        -- Statistical Rankings
        "GP_RANK"::integer as gp_rank,
        "W_RANK"::integer as win_rank,
        "L_RANK"::integer as loss_rank,
        "W_PCT_RANK"::integer as win_pct_rank,
        "MIN_RANK"::integer as min_rank,
        "FGM_RANK"::integer as fgm_rank,
        "FGA_RANK"::integer as fga_rank,
        "FG_PCT_RANK"::integer as fg_pct_rank,
        "FG3M_RANK"::integer as fg3m_rank,
        "FG3A_RANK"::integer as fg3a_rank,
        "FG3_PCT_RANK"::integer as fg3_pct_rank,
        "FTM_RANK"::integer as ftm_rank,
        "FTA_RANK"::integer as fta_rank,
        "FT_PCT_RANK"::integer as ft_pct_rank,
        "OREB_RANK"::integer as off_reb_rank,
        "DREB_RANK"::integer as def_reb_rank,
        "REB_RANK"::integer as total_rebounds_rank,
        "AST_RANK"::integer as ast_rank,
        "TOV_RANK"::integer as tov_rank,
        "STL_RANK"::integer as stl_rank,
        "BLK_RANK"::integer as blk_rank,
        "BLKA_RANK"::integer as blk_against_rank,
        "PF_RANK"::integer as pf_rank,
        "PFD_RANK"::integer as pf_drawn_rank,
        "PTS_RANK"::integer as pts_rank,
        "PLUS_MINUS_RANK"::integer as plus_minus_rank,
        "NBA_FANTASY_PTS_RANK"::integer as fantasy_points_rank,
        "DD2_RANK"::integer as double_doubles_rank,
        "TD3_RANK"::integer as triple_doubles_rank,
        
        -- Metadata
        current_timestamp as created_at
    from source
)

select * from final 