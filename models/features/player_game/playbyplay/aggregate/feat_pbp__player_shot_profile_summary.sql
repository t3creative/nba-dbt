{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_game_shot_profile_key',
        tags=['features', 'pbp', 'shot_profile'],
        indexes=[
            {'columns': ['player_game_shot_profile_key'], 'unique': True},
            {'columns': ['player_id', 'game_id']},
            {'columns': ['game_id']}
        ]
    )
}}

-- NEEDS FIXING

-- The reliance on assisting_player_id in int_pbp__event_participation for assisted/unassisted shots should be verified for accuracy.

with event_participation_base as (
    select
        event_key,
        primary_player_id,
        game_id,
        shot_made_flag,
        period,
        game_clock_seconds_remaining,
        assisting_player_id
    from {{ ref('int_pbp__event_participation') }}
    where primary_event_name in ('Made Shot', 'Missed Shot') -- Filter for actual shot events
),

synthesized_event_details as (
    select
        synthesized_event_key,
        shot_zone_basic,    -- e.g., 'Restricted Area', 'Mid-Range', 'Above the Break 3'
        shot_distance_ft
    from {{ ref('int_pbp__synthesized_events') }}
    where is_shot_attempt = true -- From synthesized_events, ensures these are shot-related
),

source_events as (
    select
        ep.primary_player_id as player_id,
        ep.game_id,
        sed.shot_zone_basic as shot_zone, -- Will be mapped in the next CTE
        ep.shot_made_flag,
        ep.period,
        ep.game_clock_seconds_remaining as seconds_remaining_in_period,
        ep.assisting_player_id,
        sed.shot_distance_ft as shot_distance
    from event_participation_base ep
    join synthesized_event_details sed
        on ep.event_key = sed.synthesized_event_key
),

player_game_shot_aggregates as (
    select
        player_id,
        game_id,
        
        -- Shot location clusters (mapping shot_zone_basic to broader categories)
        count(*) filter(
            where shot_zone in ('Restricted Area', 'In The Paint (Non-RA)') -- Assumed mapping for 'At Rim'
        ) as rim_attempts,
        count(*) filter(
            where shot_zone = 'Mid-Range' -- Assumed mapping for 'Mid Range'
        ) as midrange_attempts,
        count(*) filter(
            where shot_zone in ('Left Corner 3', 'Right Corner 3', 'Above the Break 3') -- Assumed mapping for '3PT'
        ) as three_attempts,
        
        -- Shot success rates by zone
        (
            sum(case when shot_made_flag = true and shot_zone in ('Restricted Area', 'In The Paint (Non-RA)') then 1 else 0 end)::decimal /
            nullif(count(*) filter(where shot_zone in ('Restricted Area', 'In The Paint (Non-RA)')), 0)
        ) as rim_fg_pct,
        
        -- Shot timing patterns
        count(*) filter(where period = 4 and seconds_remaining_in_period < 180) as clutch_shots,
        
        -- Shot creation type
        count(*) filter(where assisting_player_id is not null) as assisted_shots,
        count(*) filter(where assisting_player_id is null) as unassisted_shots,
        
        -- Defender impact
        avg(shot_distance) filter(where shot_made_flag = false) as avg_missed_shot_distance
        
    from source_events
    group by
        player_id,
        game_id
),

final as (
    select
        aggs.player_id,
        aggs.game_id,
        md5(aggs.player_id::text || '-' || aggs.game_id::text) as player_game_shot_profile_key,

        aggs.rim_attempts,
        aggs.midrange_attempts,
        aggs.three_attempts,
        aggs.rim_fg_pct,
        aggs.clutch_shots,
        aggs.assisted_shots,
        aggs.unassisted_shots,
        aggs.avg_missed_shot_distance,
        
        current_timestamp as processed_at

    from player_game_shot_aggregates aggs
)

select * from final 