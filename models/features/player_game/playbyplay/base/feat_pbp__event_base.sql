{{ config(
    schema='features',
    materialized='incremental',
    unique_key='event_base_key',
    on_schema_change='sync_all_columns',
    tags=['features', 'base', 'pbp'],
    indexes=[
        {'columns': ['game_id', 'game_date'], 'type': 'btree'},
        {'columns': ['player_id', 'game_date'], 'type': 'btree'},
        {'columns': ['seconds_elapsed_game'], 'type': 'btree'}
    ]
) }}

with participation_events as (
    select * from {{ ref('int_pbp__event_participation') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

base_selection as (
    select
        -- Primary Keys
        event_key as event_base_key,
        game_id,
        period,
        primary_player_id as player_id,
        
        -- Timing (standardized)
        game_clock_seconds_remaining,
        seconds_elapsed_game,
        period as game_period,
        
        -- Event Classification
        primary_event_name as event_type,
        primary_event_detail as event_subtype,
        
        -- Player Context
        primary_player_name as player_name,
        primary_team_id as team_id,
        assisting_player_id,
        defending_player_id,
        rebounding_player_id,
        
        -- Event Outcomes (raw values)
        shot_made_flag,
        points_scored,
        turnover_flag,
        
        -- Game State
        score_margin,
        clutch_time_flag,
        
        -- Shot Details (when applicable)
        case when primary_event_name in ('Made Shot', 'Missed Shot') then true else false end as is_shot_event,
        
        -- Meta
        source_synthesis_type,
        created_at,
        updated_at
    from participation_events
    where primary_player_id is not null  -- Only events with clear player attribution
),

-- Add game date join for proper partitioning
final_base as (
    select
        b.*,
        g.game_date
    from base_selection b
    left join {{ ref('feat_opp__game_opponents_v2') }} g
        on b.game_id = g.game_id
)

select * from final_base