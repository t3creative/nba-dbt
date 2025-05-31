-- This model is running successfully, but isn't calculating anything.
{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='game_id',
        tags=['features', 'pbp', 'momentum'],
        indexes=[
            {'columns': ['game_id'], 'unique': True},
            {'columns': ['home_team_id']},
            {'columns': ['away_team_id']}
        ]
    )
}}

with game_opponent_base_info as (
    select
        game_id,
        team_id,
        home_away as type -- 'home_away' field from feat_opp__game_opponents_v2 renamed to 'type'
    from {{ ref('feat_opp__game_opponents_v2') }}
),

team_tricodes_for_game as (
    select distinct -- Ensure one tricode per team per game
        game_id,
        player1_team_id as team_id,
        player1_team_tricode as tricode
    from {{ ref('int_pbp__combined_events') }}
    where player1_team_id is not null and player1_team_tricode is not null
),

game_opponent_info as (
    select
        gobi.game_id,
        gobi.team_id,
        ttg.tricode as team_tricode, -- Correctly sourced tricode
        gobi.type
    from game_opponent_base_info gobi
    join team_tricodes_for_game ttg on gobi.game_id = ttg.game_id and gobi.team_id = ttg.team_id
),

base_scoring_events as (
    select
        pbp.game_id,
        pbp.period,
        pbp.primary_team_id as team_id,
        goi.team_tricode as team_code, -- Using the tricode from game_opponent_info
        pbp.points_scored as points,
        pbp.seconds_elapsed_game
    from {{ ref('int_pbp__event_participation') }} pbp
    join game_opponent_info goi on pbp.game_id = goi.game_id and pbp.primary_team_id = goi.team_id
    where pbp.points_scored > 0
),

event_level_momentum_calcs as (
    select
        *,
        coalesce(
            seconds_elapsed_game - lag(seconds_elapsed_game, 1) over (
                partition by game_id, team_id order by seconds_elapsed_game
            ), 0 -- Set to 0 for the first scoring event of a team in a game
        ) as time_since_last_score_for_team,
        sum(points) over (
            partition by game_id, team_id order by seconds_elapsed_game
            rows between 3 preceding and current row
        ) as recent_points_scored_4_events,
        count(*) over (
            partition by game_id, team_id order by seconds_elapsed_game
            rows between 3 preceding and current row
        ) as recent_scoring_events_4_count
    from base_scoring_events
),

team_game_aggregates as (
    select
        game_id,
        team_id,
        team_code, -- This is the tricode
        sum(case when period = 1 then points else 0 end) as q1_points,
        sum(case when period = 2 then points else 0 end) as q2_points,
        sum(case when period = 3 then points else 0 end) as q3_points,
        sum(case when period = 4 then points else 0 end) as q4_points,
        sum(case when period > 4 then points else 0 end) as ot_points,
        
        max(recent_points_scored_4_events) as game_max_point_run_4_events,
        max(recent_scoring_events_4_count) as game_max_consecutive_scores,
        
        avg(time_since_last_score_for_team) as avg_time_between_team_scores,
        stddev(time_since_last_score_for_team) as stddev_time_between_team_scores,
        
        sum(case when recent_points_scored_4_events >= 6 then 1 else 0 end) as count_event_ended_significant_run
        
    from event_level_momentum_calcs
    group by
        game_id,
        team_id,
        team_code
),

pivoted_game_momentum as (
    select
        home_info.game_id,
        home_info.team_id as home_team_id,
        home_info.team_tricode as home_team_code, -- Tricode from game_opponent_info
        away_info.team_id as away_team_id,
        away_info.team_tricode as away_team_code, -- Tricode from game_opponent_info

        coalesce(h.q1_points, 0) as home_q1_points,
        coalesce(h.q2_points, 0) as home_q2_points,
        coalesce(h.q3_points, 0) as home_q3_points,
        coalesce(h.q4_points, 0) as home_q4_points,
        coalesce(h.ot_points, 0) as home_ot_points,
        coalesce(h.game_max_point_run_4_events, 0) as home_max_point_run,
        coalesce(h.game_max_consecutive_scores, 0) as home_max_consecutive_scores,
        h.avg_time_between_team_scores as home_avg_time_between_scores,
        h.stddev_time_between_team_scores as home_scoring_consistency,
        coalesce(h.count_event_ended_significant_run, 0) as home_significant_runs_count,

        coalesce(a.q1_points, 0) as away_q1_points,
        coalesce(a.q2_points, 0) as away_q2_points,
        coalesce(a.q3_points, 0) as away_q3_points,
        coalesce(a.q4_points, 0) as away_q4_points,
        coalesce(a.ot_points, 0) as away_ot_points,
        coalesce(a.game_max_point_run_4_events, 0) as away_max_point_run,
        coalesce(a.game_max_consecutive_scores, 0) as away_max_consecutive_scores,
        a.avg_time_between_team_scores as away_avg_time_between_scores,
        a.stddev_time_between_team_scores as away_scoring_consistency,
        coalesce(a.count_event_ended_significant_run, 0) as away_significant_runs_count

    from (select * from game_opponent_info where type = 'home') home_info
    join (select * from game_opponent_info where type = 'away') away_info
        on home_info.game_id = away_info.game_id
    left join team_game_aggregates h
        on home_info.game_id = h.game_id and home_info.team_id = h.team_id
    left join team_game_aggregates a
        on away_info.game_id = a.game_id and away_info.team_id = a.team_id
),

final as (
    select
        game_id,
        home_team_id,
        home_team_code,
        away_team_id,
        away_team_code,

        home_q1_points, home_q2_points, home_q3_points, home_q4_points, home_ot_points,
        home_max_point_run, home_max_consecutive_scores,
        home_avg_time_between_scores, home_scoring_consistency, home_significant_runs_count,

        away_q1_points, away_q2_points, away_q3_points, away_q4_points, away_ot_points,
        away_max_point_run, away_max_consecutive_scores,
        away_avg_time_between_scores, away_scoring_consistency, away_significant_runs_count,

        (home_q1_points - away_q1_points) as q1_score_diff,
        (home_q2_points - away_q2_points) as q2_score_diff,
        (home_q3_points - away_q3_points) as q3_score_diff,
        (home_q4_points - away_q4_points) as q4_score_diff,
        (home_ot_points - away_ot_points) as ot_score_diff,
        (home_significant_runs_count - away_significant_runs_count) as momentum_shift_diff,
        
        case 
            when (home_q1_points > away_q1_points and home_q2_points > away_q2_points and
                  home_q3_points > away_q3_points and home_q4_points > away_q4_points) then 'HOME_DOMINATE'
            when (away_q1_points > home_q1_points and away_q2_points > home_q2_points and
                  away_q3_points > home_q3_points and away_q4_points > home_q4_points) then 'AWAY_DOMINATE'
            when abs(home_q1_points - away_q1_points) <= 5 and
                 abs(home_q2_points - away_q2_points) <= 5 and
                 abs(home_q3_points - away_q3_points) <= 5 and
                 abs(home_q4_points - away_q4_points) <= 5 then 'TIGHT_CONTEST'
            when ((home_q1_points + home_q2_points) < (away_q1_points + away_q2_points) - 10 and 
                  (home_q1_points + home_q2_points + home_q3_points + home_q4_points + home_ot_points) > 
                  (away_q1_points + away_q2_points + away_q3_points + away_q4_points + away_ot_points)) then 'HOME_COMEBACK'
            when ((away_q1_points + away_q2_points) < (home_q1_points + home_q2_points) - 10 and
                  (away_q1_points + away_q2_points + away_q3_points + away_q4_points + away_ot_points) >
                  (home_q1_points + home_q2_points + home_q3_points + home_q4_points + home_ot_points)) then 'AWAY_COMEBACK'
            else 'STANDARD_GAME'
        end as game_flow_pattern,
        current_timestamp as processed_at
    from pivoted_game_momentum
)

select * from final