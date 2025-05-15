-- feat_pbp__player_possessions.sql
{{ config(
        schema='features',
        materialized='incremental',
        unique_key='game_player_possession_key',
        tags=['features', 'pbp', 'possession'],
        indexes=[
            {'columns': ['game_player_possession_key'], 'unique': True},
            {'columns': ['game_id', 'game_possession_sequence']},
            {'columns': ['game_id', 'player_id']}]
    )
}}

with event_data as (
    select
        *,
        lag(primary_team_id) over (partition by game_id order by period, seconds_elapsed_game, event_key) as lag_event_team_id,
        lag(primary_event_name) over (partition by game_id order by period, seconds_elapsed_game, event_key) as lag_event_name,
        lag(shot_made_flag) over (partition by game_id order by period, seconds_elapsed_game, event_key) as lag_shot_made_flag
    from {{ ref('int_pbp__event_participation') }}
    -- int_pbp__event_participation should be ordered by game_id, period, seconds_elapsed_game desc, event_key (or similar logical event order)
    -- For this model, we need seconds_elapsed_game or a similar robust ordering field.
),

possession_end_logic as (
    select
        *,
        -- Rule 1: Made field goal (not free throws)
        (shot_made_flag = true and primary_event_name = 'Made Shot' and primary_event_detail not ilike '%Free Throw%') as is_made_field_goal,

        -- Rule 2: Turnover
        (turnover_flag = true) as is_turnover,

        -- Rule 3: End of a period
        (primary_event_detail ilike 'End % Period') as is_end_of_period,

        -- Rule 4: Defensive Rebound (rebound by opponent after a missed shot)
        (
            primary_event_name = 'Rebound' and
            primary_team_id is not null and
            lag_event_team_id is not null and
            primary_team_id != lag_event_team_id and -- Rebounder is on a different team than the previous event's actor
            lag_event_name ilike '%Shot%' and -- Previous event was some kind of shot
            lag_shot_made_flag = false      -- Previous shot was a miss
        ) as is_defensive_rebound,

        -- Rule 5: Last made free throw of a set (excluding technicals, which usually don't change possession)
        (
            primary_event_name = 'Free Throw' and
            shot_made_flag = true and
            (primary_event_detail ilike '%1 of 1%' or primary_event_detail ilike '%2 of 2%' or primary_event_detail ilike '%3 of 3%') and -- Covers most FT completion scenarios
            not (primary_event_detail ilike '%Technical%') -- Exclude technical FTs
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

-- New CTE to pre-calculate team scores per possession
possession_team_scores as (
    select
        game_id,
        game_possession_sequence,
        primary_team_id as team_id,
        sum(coalesce(points_scored, 0)) as team_total_points_in_possession
    from game_possessions
    where primary_team_id is not null -- Important for accurate team scoring
    group by
        game_id,
        game_possession_sequence,
        primary_team_id
),

player_possession_stats as (
    select
        gp.game_id,
        gp.primary_player_id as player_id, -- Player who performed the action
        gp.game_possession_sequence,
        gp.primary_team_id as team_id,

        count(*) as events_in_possession_by_player,
        sum(case when gp.primary_event_name = 'Made Shot' then gp.points_scored else 0 end) as points_in_possession_by_player,
        sum(case when gp.primary_event_name = 'Made Shot' and gp.assisting_player_id = gp.primary_player_id then 1 else 0 end) as self_assisted_shots_in_possession,
        max(case when gp.primary_event_name = 'Made Shot' and gp.shot_made_flag = true then 1 else 0 end) as scored_in_possession_by_player,
        max(case when gp.assisting_player_id = gp.primary_player_id then 1 else 0 end) as assisted_in_possession_by_player, 
        sum(case when gp.turnover_flag = true then 1 else 0 end) as turnovers_in_possession_by_player,
        sum(case when gp.primary_event_name = 'Rebound' then 1 else 0 end) as rebounds_in_possession_by_player,
        sum(case when gp.primary_event_name = 'Block' then 1 else 0 end) as blocks_in_possession_by_player,
        sum(case when gp.primary_event_name = 'Steal' then 1 else 0 end) as steals_in_possession_by_player,
        sum(case when gp.primary_event_name = 'Foul' then 1 else 0 end) as fouls_committed_in_possession_by_player,
        
        min(gp.seconds_elapsed_game) as possession_start_seconds,
        max(gp.seconds_elapsed_game) as possession_end_seconds,
        max(gp.seconds_elapsed_game) - min(gp.seconds_elapsed_game) as possession_duration_seconds_by_player_events,
        
        coalesce(max(pts.team_total_points_in_possession), 0) as team_points_in_possession_sequence_with_player

    from game_possessions gp
    left join possession_team_scores pts
        on gp.game_id = pts.game_id
        and gp.game_possession_sequence = pts.game_possession_sequence
        and gp.primary_team_id = pts.team_id
    where gp.primary_player_id is not null
    group by
        gp.game_id,
        gp.primary_player_id,
        gp.game_possession_sequence,
        gp.primary_team_id
)

select
    game_id,
    player_id,
    team_id,
    game_possession_sequence,
    game_id || '_' || player_id || '_' || game_possession_sequence as game_player_possession_key,
    events_in_possession_by_player,
    points_in_possession_by_player,
    scored_in_possession_by_player,
    assisted_in_possession_by_player as had_an_assist_on_own_event_in_possession, -- Renamed for clarity, or use existing name
    turnovers_in_possession_by_player,
    rebounds_in_possession_by_player,
    blocks_in_possession_by_player,
    steals_in_possession_by_player,
    fouls_committed_in_possession_by_player,
    possession_start_seconds,
    possession_end_seconds,
    possession_duration_seconds_by_player_events,
    team_points_in_possession_sequence_with_player

from player_possession_stats
order by
    game_id,
    game_possession_sequence,
    player_id