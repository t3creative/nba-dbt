-- models/features/playbyplay/feat_pbp__player_possessions.sql (Enhanced Version)
{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='game_player_possession_key',
        tags=['features', 'pbp', 'possession']
    )
}}

with event_data as (
    select
        *,
        -- Lagged fields for possession logic
        lag(primary_team_id) over (partition by game_id order by period, seconds_elapsed_game, event_key) as lag_event_team_id,
        lag(primary_event_name) over (partition by game_id order by period, seconds_elapsed_game, event_key) as lag_event_name,
        lag(shot_made_flag) over (partition by game_id order by period, seconds_elapsed_game, event_key) as lag_shot_made_flag,
        -- Rank events within each potential possession for ballhandler logic
        row_number() over (partition by game_id, primary_team_id order by period, seconds_elapsed_game, event_key) as event_rank_in_team_sequence -- Helper for ballhandler
    from {{ ref('int_pbp__event_participation') }} -- Relies on the refactored event participation model
),

possession_end_logic as (
    select
        *,
        (shot_made_flag = true and primary_event_name = 'Made Shot' and primary_event_detail not ilike '%Free Throw%') as is_made_field_goal,
        (turnover_flag = true) as is_turnover,
        (primary_event_detail ilike 'End % Period') as is_end_of_period,
        (
            primary_event_name = 'Rebound' and
            primary_team_id is not null and
            lag_event_team_id is not null and
            primary_team_id != lag_event_team_id and
            lag_event_name ilike '%Shot%' and
            lag_shot_made_flag = false
        ) as is_defensive_rebound,
        (
            primary_event_name = 'Free Throw' and
            shot_made_flag = true and
            (primary_event_detail ilike '%1 of 1%' or primary_event_detail ilike '%2 of 2%' or primary_event_detail ilike '%3 of 3%') and
            not (primary_event_detail ilike '%Technical%')
        ) as is_last_made_ft_set
    from event_data
),

possession_assignments as (
    select
        *,
        (is_made_field_goal or is_turnover or is_end_of_period or is_defensive_rebound or is_last_made_ft_set) as is_possession_ending_event
    from possession_end_logic
),

possession_events_with_lag as (
    select
        *,
        lag(is_possession_ending_event, 1, false) over (partition by game_id order by period, seconds_elapsed_game, event_key) as prev_event_ended_possession
    from possession_assignments
),

game_possessions as (
    select
        *,
        sum(case when prev_event_ended_possession then 1 else 0 end)
            over (partition by game_id order by period, seconds_elapsed_game, event_key) as game_possession_sequence
    from possession_events_with_lag
),

-- Determine primary ballhandler per possession (simplified heuristic)
-- Player with the first non-rebound, non-defensive event for their team in a possession
possession_initiators as (
    select
        game_id,
        game_possession_sequence,
        primary_player_id,
        primary_team_id,
        row_number() over (
            partition by game_id, game_possession_sequence, primary_team_id
            order by period, seconds_elapsed_game, event_key
        ) as team_event_rank_in_possession
    from game_possessions
    where
        primary_player_id is not null
        and primary_event_name not in ('Rebound', 'Block', 'Steal') -- Exclude immediate defensive actions
        and not (primary_event_name = 'Foul' and primary_event_detail ilike '%Offensive%') -- Exclude offensive fouls not ending possession
),

primary_ballhandlers as (
    select
        game_id,
        game_possession_sequence,
        primary_team_id,
        primary_player_id as primary_ballhandler_player_id
    from possession_initiators
    where team_event_rank_in_possession = 1
),

player_possession_stats as (
    select
        gp.game_id,
        gp.primary_player_id,
        gp.game_possession_sequence,
        gp.primary_team_id as team_id,
        pbh.primary_ballhandler_player_id is not null and pbh.primary_ballhandler_player_id = gp.primary_player_id as is_primary_ballhandler_for_event, -- Was this specific event by the primary ballhandler?

        gp.primary_event_name,
        gp.points_scored,
        gp.shot_made_flag,
        gp.assisting_player_id,
        gp.turnover_flag,
        gp.seconds_elapsed_game
    from game_possessions gp
    left join primary_ballhandlers pbh
        on gp.game_id = pbh.game_id
        and gp.game_possession_sequence = pbh.game_possession_sequence
        and gp.primary_team_id = pbh.primary_team_id
    where gp.primary_player_id is not null
),

-- Add this new CTE before final_possession_summary
possession_team_scores as (
    select
        game_id,
        game_possession_sequence,
        primary_team_id as team_id,
        sum(coalesce(points_scored, 0)) as team_total_points_in_possession
    from game_possessions
    where primary_team_id is not null -- ensure team context for scoring
    group by
        game_id,
        game_possession_sequence,
        primary_team_id
),

final_possession_summary as (
    select
        pps.game_id,
        pps.primary_player_id,
        pps.team_id,
        pps.game_possession_sequence,
        pps.game_id || '_' || pps.primary_player_id || '_' || pps.game_possession_sequence as game_player_possession_key,

        count(*) as events_in_possession_by_player,
        sum(case when pps.primary_event_name = 'Made Shot' then pps.points_scored else 0 end) as points_from_possessions_involved,
        max(case when pps.primary_event_name = 'Made Shot' and pps.shot_made_flag = true then 1 else 0 end) as scored_in_possession_by_player,
        max(case when pps.assisting_player_id = pps.primary_player_id then 1 else 0 end) as had_an_assist_on_own_event_in_possession,
        sum(case when pps.turnover_flag = true then 1 else 0 end) as turnovers_in_possession_by_player,

        max(case when pps.is_primary_ballhandler_for_event then 1 else 0 end) as was_primary_ballhandler_in_possession,

        sum(case when pps.primary_event_name in ('Made Shot', 'Missed Shot', 'Turnover') then 1 else 0 end) as usage_events_by_player,
        sum(case when pps.primary_event_name in ('Rebound', 'Steal') then 1 else 0 end) as possession_creation_events_by_player,

        min(pps.seconds_elapsed_game) as player_possession_start_seconds,
        max(pps.seconds_elapsed_game) as player_possession_end_seconds,
        max(pps.seconds_elapsed_game) - min(pps.seconds_elapsed_game) as player_involvement_duration_seconds,
        
        coalesce(max(pts.team_total_points_in_possession), 0) as team_total_points_in_possession

    from player_possession_stats pps
    left join possession_team_scores pts
        on pps.game_id = pts.game_id
        and pps.game_possession_sequence = pts.game_possession_sequence
        and pps.team_id = pts.team_id
    group by
        pps.game_id,
        pps.primary_player_id,
        pps.team_id,
        pps.game_possession_sequence
)
select * from final_possession_summary
order by
    game_id,
    game_possession_sequence,
    primary_player_id