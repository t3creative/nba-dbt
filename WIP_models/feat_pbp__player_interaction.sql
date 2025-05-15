-- int__player_interaction_features.sql
{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='game_player_other_player_interaction_key',
        tags=['features', 'pbp', 'interaction'],
        indexes=[
            {'columns': ['game_player_other_player_interaction_key'], 'unique': True},
            {'columns': ['game_id', 'player_id']},
            {'columns': ['game_id', 'other_player_id']}]
    )
}}

with event_participation as (
    select * from {{ ref('int_pbp__event_participation') }} -- Assuming this is the refactored event participation model
),

assists_received_pairs as (
    select
        game_id,
        primary_player_id as player_id, -- The scorer
        assisting_player_id as other_player_id, -- The assister
        count(*) as assists_received_from_player_count
    from event_participation
    where
        primary_event_name = 'Made Shot'
        and shot_made_flag = true
        and assisting_player_id is not null
        and primary_player_id != assisting_player_id -- Ensure scorer and assister are different
    group by 1, 2, 3
),

defensive_stops_pairs as (
    select
        game_id,
        offensive_player_id as player_id, -- The offensive player
        defender_id as other_player_id, -- The defender making the stop
        sum(coalesce(blocks_against_offensive_player_by_defender, 0) + coalesce(steals_against_offensive_player_by_defender, 0)) as defensive_stops_by_other_player_count
    from defender_matchups_source
    group by 1, 2, 3
),

-- Note: On-court chemistry (plus_minus_with_player) is omitted due to the requirement of detailed lineup data
-- not available directly in PBP event models. If lineup data becomes available, this can be revisited.

all_player_interactions as (
    select
        game_id,
        player_id,
        other_player_id,
        assists_received_from_player_count,
        null::integer as defensive_stops_by_other_player_count
    from assists_received_pairs

    union all

    select
        game_id,
        player_id,
        other_player_id,
        null::integer as assists_received_from_player_count,
        defensive_stops_by_other_player_count
    from defensive_stops_pairs
),

final_aggregation as (
    select
        game_id,
        player_id,
        other_player_id,
        game_id || '_' || player_id || '_' || other_player_id as game_player_other_player_interaction_key,
        sum(coalesce(assists_received_from_player_count, 0)) as assists_received_from_player,
        sum(coalesce(defensive_stops_by_other_player_count, 0)) as defensive_stops_by_player
    from all_player_interactions
    where player_id is not null and other_player_id is not null
    group by 1, 2, 3, 4
)

select * from final_aggregation