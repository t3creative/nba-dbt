{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        indexes=[
            {'columns': ['player_game_key']},
            {'columns': ['game_id', 'player_id']},
            {'columns': ['game_id', 'team_id', 'opponent_id']}
        ]
    )
}}

with box_scores as (
    select * from {{ ref('stg__player_usage_bxsc') }}
    {% if is_incremental() %}
    where game_id in (
        select distinct game_id 
        from {{ ref('int__game_opponents') }} 
        where game_date > (select max(game_date) from {{ this }}) 
    )
    {% endif %}
),

game_opponents as (
    select 
        game_id,
        team_id,
        opponent_id,
        game_date, 
        season_year,
        home_away
    from {{ ref('int__game_opponents') }}
),

team_tricodes as (
    select distinct
        team_id,
        team_tricode
    from {{ ref('stg__game_logs_league') }}
),

final as (
    select distinct on (bs.player_game_key)
        -- Identity and Context
        bs.player_game_key,
        gopp.season_year,
        bs.first_name,
        bs.family_name,
        concat(bs.first_name, ' ', bs.family_name) as player_name,
        tt.team_tricode,
        gopp.game_date,
        gopp.home_away,
        gopp.opponent_id,
        
        -- Usage Stats - Time
        bs.min,

        -- Usage Stats - Field Goals
        bs.pct_of_team_fgm,
        bs.pct_of_team_fga,
        bs.pct_of_team_fg3m,
        bs.pct_of_team_fg3a,
        
        -- Usage Stats - Free Throws
        bs.pct_of_team_ftm,
        bs.pct_of_team_fta,
        
        -- Usage Stats - Rebounds
        bs.pct_of_team_oreb,
        bs.pct_of_team_dreb,
        bs.pct_of_team_reb,

        -- Usage Stats - Other Percentages
        bs.pct_of_team_ast,
        bs.pct_of_team_tov,
        bs.pct_of_team_stl,
        bs.pct_of_team_blk,
        bs.pct_of_team_blk_allowed,
        bs.pct_of_team_pf,
        bs.pct_of_team_pfd,
        bs.pct_of_team_pts,
        
        -- IDs and Metadata
        bs.game_id,
        bs.player_id,
        bs.team_id,
        bs.created_at,
        bs.updated_at
    from box_scores bs
    left join game_opponents gopp on bs.game_id = gopp.game_id and bs.team_id = gopp.team_id
    left join team_tricodes tt on bs.team_id = tt.team_id
    order by bs.player_game_key, gopp.game_date desc
)

select * from final