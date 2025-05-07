{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['player_game_key']},
            {'columns': ['game_id', 'player_id']}
        ]
    )
}}

with player_data as (
    select 
        tbs.player_game_key,
        tbs.player_id,
        tbs.player_name,
        tbs.game_id,
        tbs.team_id,
        tbs.season_year,
        tbs.game_date,
        tbs.pts,
        pcs.career_ppg_to_date,
        pcs.career_games_to_date,
        pcs.career_high_pts_to_date,
        pcs.is_new_career_high
    from {{ ref('int__player_traditional_bxsc') }} tbs
    inner join {{ ref('int__player_career_stats') }} pcs 
        on tbs.player_game_key = pcs.player_game_key
    -- Filter based on the starting year extracted from tbs.season_year
    where cast(substring(tbs.season_year from 1 for 4) as integer) >= {{ var('training_start_season_year') }}
    {% if is_incremental() %}
    -- Add standard incremental filter using AND
    and tbs.game_date > (select max(game_date) from {{ this }})
    {% endif %}

)

select
    pd.player_game_key,
    pd.player_id,
    pd.player_name,
    pd.game_id,
    pd.team_id,
    pd.season_year,
    pd.game_date,
    
    -- Current game info
    pd.pts,
    pm.is_30_plus_game,
    pm.is_40_plus_game,
    pd.is_new_career_high,
    
    -- Career stats
    pd.career_high_pts_to_date,
    pd.career_ppg_to_date,
    pd.career_games_to_date,
    
    -- Days since milestone games using macro
    {{ days_since_milestone('pd.game_date', 'pm.latest_30_plus_date') }} as days_since_last_30_plus_game,
    {{ days_since_milestone('pd.game_date', 'pm.latest_40_plus_date') }} as days_since_last_40_plus_game,
    
    -- Career milestone counts
    pm.thirty_plus_games_career,
    pm.forty_plus_games_career,
    
    -- This season milestone counts
    pm.thirty_plus_games_season as thirty_plus_games_this_season,
    pm.forty_plus_games_season as forty_plus_games_this_season,
    
    -- Previous season milestone counts
    pm.thirty_plus_games_prev_season as thirty_plus_games_last_season,
    pm.forty_plus_games_prev_season as forty_plus_games_last_season,
    
    -- Usage stats in high scoring games
    pu.avg_usage_in_30_plus_career,
    pu.avg_usage_in_40_plus_career,
    
    -- Additional usage statistics by scoring range
    pu.avg_usage_0_to_9_pts,
    pu.avg_usage_10_to_19_pts,
    pu.avg_usage_20_to_29_pts
from player_data pd
left join {{ ref('int__player_milestone_games') }} pm
    on pd.player_game_key = pm.player_game_key
left join {{ ref('int__player_usage_patterns') }} pu
    on pd.player_game_key = pu.player_game_key
order by pd.player_id, pd.game_date 