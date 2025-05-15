{{
  config(
    materialized='table',
    unique_key='player_id'
  )
}}

with player_logs as (
    select
        player_id,
        player_name,
        team_id,
        team_name,
        team_tricode,
        game_date
    from {{ ref('stg__game_logs_player') }}
    where season_year = '2024-25'
),

active_players as (
    select 
        player_id,
        player_name,
        team_id,
        team_name,
        team_tricode,
        row_number() over (partition by player_id order by game_date desc) as rn
    from player_logs
)   

select
    player_id,
    player_name,
    team_id,
    team_name,
    team_tricode,
    'Active' as roster_status,
    current_timestamp as created_at
from active_players
where rn = 1 