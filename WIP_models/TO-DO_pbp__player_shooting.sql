-- This model is runnging successfully, but is only calculating the total_shots correctly.
{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_game_shooting_key',
        on_schema_change='sync_all_columns'
    )
}}

with event_participation_shooters as (
    select
        event_key,
        game_id,
        primary_player_id as player_id,
        primary_player_name as player_name,
        primary_team_id as team_id,
        shot_made_flag,
        points_scored as points,
        period
    from {{ ref('int_pbp__event_participation') }}
    where primary_event_name in ('Made Shot', 'Missed Shot')
),

synthesized_shot_details as (
    select
        synthesized_event_key as event_key,
        event_number, -- From the raw event, used for joining to player_game_events
        shot_zone_basic as shot_zone,
        shot_distance_ft as shot_distance,
        shot_value_attempted as shot_value,
        shot_x_coordinate as shot_x,
        shot_y_coordinate as shot_y
    from {{ ref('int_pbp__synthesized_events') }}
    where is_shot_attempt = true
),

shooting_events as (
    select
        eps.game_id,
        eps.player_id,
        eps.player_name,
        eps.team_id,
        pge.team_code, -- team_code from player_game_events, specific to the player's involvement in that event
        ssd.shot_zone,
        ssd.shot_distance,
        eps.shot_made_flag,
        ssd.shot_value,
        eps.points,
        eps.period,
        pge.is_clutch_time_end_game,
        pge.is_clutch_time_end_half,
        pge.home_lead,
        pge.away_lead,
        ssd.shot_x,
        ssd.shot_y,
        pge.time_since_last_event
    from event_participation_shooters eps
    join synthesized_shot_details ssd on eps.event_key = ssd.event_key
    left join {{ ref('int_pbp__player_game_events') }} pge on eps.game_id = pge.game_id
                                                        and eps.player_id = pge.player_id
                                                        and eps.period = pge.period
                                                        and ssd.event_number = pge.event_number -- event_number link
),

shot_features as (
    select
        player_id,
        player_name,
        game_id,
        md5(game_id::text || '-' || player_id::text) as player_game_shooting_key,
        
        -- Basic shot counts
        count(*) as total_shots,
        sum(case when shot_made_flag = true then 1 else 0 end) as made_shots,
        sum(coalesce(points, 0)) as total_points,
        sum(case when shot_value = 3 then 1 else 0 end) as three_pt_attempts,
        sum(case when shot_value = 3 and shot_made_flag = true then 1 else 0 end) as three_pt_made,
        sum(case when shot_value = 2 then 1 else 0 end) as two_pt_attempts,
        sum(case when shot_value = 2 and shot_made_flag = true then 1 else 0 end) as two_pt_made,
        sum(case when shot_value = 1 then 1 else 0 end) as ft_attempts,
        sum(case when shot_value = 1 and shot_made_flag = true then 1 else 0 end) as ft_made,
        
        -- Zone-based shooting (shot_zone from synthesized_events will be more granular, e.g. 'Restricted Area', 'Mid-Range')
        -- Mapping to broader categories like 'At Rim', 'Mid Range' might be needed here or downstream if desired.
        sum(case when shot_zone = 'Restricted Area' or shot_zone = 'In The Paint (Non-RA)' then 1 else 0 end) as at_rim_attempts,
        sum(case when (shot_zone = 'Restricted Area' or shot_zone = 'In The Paint (Non-RA)') and shot_made_flag = true then 1 else 0 end) as at_rim_made,
        sum(case when shot_zone = 'Mid-Range' then 1 else 0 end) as mid_range_attempts,
        sum(case when shot_zone = 'Mid-Range' and shot_made_flag = true then 1 else 0 end) as mid_range_made,
        -- The original model had 'Long Mid Range'. synthesized_events might not have this specific category.
        -- This will need adjustment based on available shot_zone_basic values from synthesized_events.
        -- For now, I'll omit 'Long Mid Range' as its source is unclear with the new structure.
        sum(case when shot_zone in ('Left Corner 3', 'Right Corner 3', 'Above the Break 3') then 1 else 0 end) as three_pt_zone_attempts,
        sum(case when shot_zone in ('Left Corner 3', 'Right Corner 3', 'Above the Break 3') and shot_made_flag = true then 1 else 0 end) as three_pt_zone_made,
        
        -- Clutch shooting
        sum(case when is_clutch_time_end_game then 1 else 0 end) as clutch_attempts,
        sum(case when is_clutch_time_end_game and shot_made_flag = true then 1 else 0 end) as clutch_made,
        
        -- Shot distribution
        avg(shot_distance) as avg_shot_distance, -- COALESCE(shot_distance, 0) removed, let AVG handle NULLs naturally
        stddev(shot_distance) as std_shot_distance,
        
        -- Shot timing
        avg(time_since_last_event) as avg_time_between_shots,
        stddev(time_since_last_event) as std_time_between_shots,
        
        -- Advanced metrics
        avg(case when home_lead > 0 then home_lead when away_lead > 0 then away_lead else 0 end) as avg_abs_score_diff_during_shots, -- Modified to be absolute lead
        
        -- Spatial features
        avg(shot_x) as avg_shot_x,
        avg(shot_y) as avg_shot_y,
        stddev(shot_x) as std_shot_x,
        stddev(shot_y) as std_shot_y
        
    from shooting_events
    group by player_id, player_name, game_id
),

shot_efficiencies as (
    select
        *,
        -- Shooting percentages
        case when total_shots > 0 then made_shots / nullif(total_shots::float, 0) else null end as fg_pct,
        case when three_pt_attempts > 0 then three_pt_made / nullif(three_pt_attempts::float, 0) else null end as three_pt_pct,
        case when two_pt_attempts > 0 then two_pt_made / nullif(two_pt_attempts::float, 0) else null end as two_pt_pct,
        case when ft_attempts > 0 then ft_made / nullif(ft_attempts::float, 0) else null end as ft_pct,
        
        -- Zone efficiencies
        case when at_rim_attempts > 0 then at_rim_made / nullif(at_rim_attempts::float, 0) else null end as at_rim_pct,
        case when mid_range_attempts > 0 then mid_range_made / nullif(mid_range_attempts::float, 0) else null end as mid_range_pct,
        -- case when long_mid_attempts > 0 then long_mid_made / nullif(long_mid_attempts::float, 0) else null end as long_mid_pct, -- Omitted for now
        case when three_pt_zone_attempts > 0 then three_pt_zone_made / nullif(three_pt_zone_attempts::float, 0) else null end as three_pt_zone_pct,
        
        -- Clutch efficiency
        case when clutch_attempts > 0 then clutch_made / nullif(clutch_attempts::float, 0) else null end as clutch_fg_pct,
        
        -- Advanced metrics
        (made_shots + 0.5 * three_pt_made) / nullif(total_shots::float, 0) as effective_fg_pct, -- Corrected eFG%
        total_points / nullif((total_shots + 0.44 * ft_attempts)::float, 0) as true_shooting_pct -- Corrected TS%

    from shot_features
)

select * from shot_efficiencies