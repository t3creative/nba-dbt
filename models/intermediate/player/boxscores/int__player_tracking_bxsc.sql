{{
    config(
        enabled=true,
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
    select * from {{ ref('stg__player_tracking_bxsc') }}
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
        
        -- Basic Stats
        bs.min,
        
        -- Tracking Stats - Speed and Distance
        coalesce(bs.speed, 0) as speed,
        coalesce(bs.distance, 0) as distance,
        
        -- Tracking Stats - Rebounds
        coalesce(bs.off_reb_chances, 0) as off_reb_chances,
        coalesce(bs.def_reb_chances, 0) as def_reb_chances,
        coalesce(bs.reb_chances, 0) as reb_chances,
        
        -- Tracking Stats - Ball Movement
        coalesce(bs.touches, 0) as touches,
        coalesce(bs.secondary_ast, 0) as secondary_ast,
        coalesce(bs.ft_ast, 0) as ft_ast,
        coalesce(bs.passes, 0) as passes,
        
        -- Tracking Stats - Shot Quality
        coalesce(bs.cont_fgm, 0) as cont_fgm,
        coalesce(bs.cont_fga, 0) as cont_fga,
        coalesce(bs.cont_fg_pct, 0) as cont_fg_pct,
        coalesce(bs.uncont_fgm, 0) as uncont_fgm,
        coalesce(bs.uncont_fga, 0) as uncont_fga,
        coalesce(bs.uncont_fg_pct, 0) as uncont_fg_pct,
        
        -- Tracking Stats - Rim Protection
        coalesce(bs.def_at_rim_fgm, 0) as def_at_rim_fgm,
        coalesce(bs.def_at_rim_fga, 0) as def_at_rim_fga,
        coalesce(bs.def_at_rim_fg_pct, 0) as def_at_rim_fg_pct,
        
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