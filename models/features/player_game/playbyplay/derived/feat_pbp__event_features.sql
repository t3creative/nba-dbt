{{ config(
    schema='features',
    materialized='incremental',
    unique_key='event_base_key',
    on_schema_change='sync_all_columns',
    tags=['features', 'derived', 'pbp'],
    indexes=[
        {'columns': ['game_id', 'player_id'], 'type': 'btree'},
        {'columns': ['game_date', 'player_id'], 'type': 'btree'}
    ]
) }}

with base_events as (
    select * from {{ ref('feat_pbp__event_base') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

-- Calculate sequential features with proper ML-safe windowing
sequential_features as (
    select
        *,
        -- Time-based lags (previous event timing)
        lag(seconds_elapsed_game, 1) over (
            partition by game_id, player_id 
            order by seconds_elapsed_game
        ) as prev_event_seconds_elapsed,
        
        -- Rolling shot performance (last 5 shots, excluding current)
        avg(case when shot_made_flag = true then 1.0 else 0.0 end) over (
            partition by game_id, player_id
            order by seconds_elapsed_game
            rows between 5 preceding and 1 preceding
        ) as rolling_shot_pct_last_5,
        
        -- Event sequence patterns
        lag(event_type, 1) over (
            partition by game_id, player_id 
            order by seconds_elapsed_game
        ) as prev_event_type,
        
        -- Consecutive event counting
        row_number() over (
            partition by game_id, player_id 
            order by seconds_elapsed_game
        ) as player_game_event_sequence,
        
        -- Score momentum when event occurred
        lag(score_margin, 1) over (
            partition by game_id 
            order by seconds_elapsed_game
        ) as score_margin_at_prev_event
        
    from base_events
),

-- Calculate time-based derived columns first
time_derived as (
    select
        *,
        -- Time between events
        case 
            when prev_event_seconds_elapsed is not null 
            then seconds_elapsed_game - prev_event_seconds_elapsed
            else null
        end as seconds_since_last_event,
        
        -- Game flow context
        case 
            when game_period <= 2 then 'First Half'
            when game_period <= 4 then 'Second Half'
            else 'Overtime'
        end as game_phase
        
    from sequential_features
),

-- Add all other contextual features using previously calculated columns
final_features as (
    select
        *,
        -- Event intensity flags
        case when seconds_since_last_event <= 24 then true else false end as rapid_sequence_event,
        case when player_game_event_sequence = 1 then true else false end as first_player_event,
        
        -- Shot context enhancement
        case 
            when is_shot_event and prev_event_type = 'Made Shot' then 'Shot After Make'
            when is_shot_event and prev_event_type = 'Missed Shot' then 'Shot After Miss'
            when is_shot_event then 'Shot Other Context'
            else 'Non-Shot Event'
        end as shot_context_flag,
        
        -- Performance streaks (simplified)
        case 
            when rolling_shot_pct_last_5 >= 0.6 then 'Hot'
            when rolling_shot_pct_last_5 <= 0.2 then 'Cold'
            else 'Neutral'
        end as recent_shooting_form
        
    from time_derived
)

select * from final_features