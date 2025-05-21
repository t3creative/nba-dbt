{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_game_performance_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_id",
            "data_type": "text",
            "granularity": "day"
        },
        indexes=[
            {'columns': ['game_id']},
            {'columns': ['player_id']},
            {'columns': ['game_id', 'player_id']}
        ]
    )
}}

with event_participation_base as (
    select
        event_key,
        game_id,
        primary_player_id as player_id,
        primary_player_name as player_name,
        primary_team_id as team_id,
        primary_event_name,
        primary_event_detail, -- e.g., "Free Throw 1 of 2"
        shot_made_flag,
        points_scored as points,
        period,
        seconds_elapsed_game
    from {{ ref('int_pbp__event_participation') }}
    where primary_player_id is not null -- Ensure there's a primary player for the event
),

synthesized_event_link as (
    select
        synthesized_event_key as event_key,
        event_number -- Key for joining to player_game_events for specific event context
    from {{ ref('int_pbp__synthesized_events') }}
),

player_game_event_context as (
    select
        game_id,
        player_id,
        period,
        event_number,
        team_code,
        is_clutch_time_end_game,
        score_margin, -- Can be used for close_game_scores logic
        home_lead,    -- Can be used for blowout_scores logic
        away_lead     -- Can be used for blowout_scores logic
    from {{ ref('int_pbp__player_game_events') }}
),

enriched_player_events as (
    select
        epb.game_id,
        epb.player_id,
        epb.player_name,
        epb.team_id,
        pgec.team_code,
        epb.primary_event_name,
        epb.primary_event_detail,
        epb.shot_made_flag,
        epb.points,
        epb.period,
        epb.seconds_elapsed_game,
        pgec.is_clutch_time_end_game,
        pgec.score_margin,
        pgec.home_lead,
        pgec.away_lead
    from event_participation_base epb
    join synthesized_event_link sel on epb.event_key = sel.event_key
    left join player_game_event_context pgec on epb.game_id = pgec.game_id
                                            and epb.player_id = pgec.player_id
                                            and epb.period = pgec.period
                                            and sel.event_number = pgec.event_number
),

player_stats_aggregated as (
    select
        game_id,
        player_id,
        player_name,
        team_id,
        team_code,
        md5(game_id::text || '-' || player_id::text) as player_game_performance_key,

        -- Field Goal Stats
        sum(case when primary_event_name = 'Made Shot' and primary_event_detail not ilike '%Free Throw%' then 1 else 0 end) as field_goals_made,
        sum(case when (primary_event_name = 'Made Shot' or primary_event_name = 'Missed Shot') and primary_event_detail not ilike '%Free Throw%' then 1 else 0 end) as field_goal_attempts,

        -- Free Throw Stats
        sum(case when primary_event_name = 'Made Shot' and primary_event_detail ilike '%Free Throw%' then 1 else 0 end) as free_throws_made,
        sum(case when (primary_event_name = 'Made Shot' or primary_event_name = 'Missed Shot') and primary_event_detail ilike '%Free Throw%' then 1 else 0 end) as free_throw_attempts,

        sum(case when primary_event_name = 'Rebound' then 1 else 0 end) as rebounds,
        sum(case when primary_event_name = 'Turnover' then 1 else 0 end) as turnovers,
        sum(case when primary_event_name = 'Assist' then 1 else 0 end) as assists, -- Assumes 'Assist' is a primary event for the assister
        sum(case when primary_event_name = 'Steal' then 1 else 0 end) as steals,
        sum(case when primary_event_name = 'Block' then 1 else 0 end) as blocks,
        sum(case when primary_event_name = 'Foul' then 1 else 0 end) as fouls,
        
        sum(coalesce(points, 0)) as total_points,
        
        max(period) as max_period,
        coalesce(max(seconds_elapsed_game) / 60.0, 0) as minutes_played, -- Default to 0 if no events
        
        sum(case when is_clutch_time_end_game and primary_event_name = 'Made Shot' and primary_event_detail not ilike '%Free Throw%' then 1 else 0 end) as clutch_shots_made,
        sum(case when is_clutch_time_end_game and (primary_event_name = 'Made Shot' or primary_event_name = 'Missed Shot') and primary_event_detail not ilike '%Free Throw%' then 1 else 0 end) as clutch_shot_attempts,
        
        sum(case when period = 1 then coalesce(points, 0) else 0 end) as q1_points,
        sum(case when period = 2 then coalesce(points, 0) else 0 end) as q2_points,
        sum(case when period = 3 then coalesce(points, 0) else 0 end) as q3_points,
        sum(case when period = 4 then coalesce(points, 0) else 0 end) as q4_points,
        sum(case when period > 4 then coalesce(points, 0) else 0 end) as ot_points,
        
        sum(case when (coalesce(home_lead, 0) > 10 or coalesce(away_lead, 0) > 10) and coalesce(points, 0) > 0 then points else 0 end) as blowout_points_scored,
        sum(case when abs(coalesce(score_margin, 0)) <= 5 and coalesce(points, 0) > 0 then points else 0 end) as close_game_points_scored
        
    from enriched_player_events
    group by game_id, player_id, player_name, team_id, team_code
),

advanced_stats as (
    select
        *,
        (field_goal_attempts - field_goals_made) as field_goals_missed,
        (free_throw_attempts - free_throws_made) as free_throws_missed,

        field_goals_made / nullif(field_goal_attempts::float, 0) as fg_pct,
        free_throws_made / nullif(free_throw_attempts::float, 0) as ft_pct,
        
        total_points + rebounds + assists + steals + blocks - ( (field_goal_attempts - field_goals_made) + (free_throw_attempts - free_throws_made) + turnovers) as game_score,
        
        total_points / nullif((field_goal_attempts + 0.44 * free_throw_attempts)::float, 0) as true_shooting_pct,
        
        (total_points / nullif(minutes_played, 0)) * 36 as points_per_36, -- Assuming 36 minutes as standard
        (assists / nullif(minutes_played, 0)) * 36 as assists_per_36,
        (rebounds / nullif(minutes_played, 0)) * 36 as rebounds_per_36,
        
        greatest(q1_points, q2_points, q3_points, q4_points, ot_points) - least(q1_points, q2_points, q3_points, q4_points, ot_points) as scoring_variance_across_periods
        
    from player_stats_aggregated
)

select * from advanced_stats