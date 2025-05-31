{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        tags=['features', 'pbp', 'player_summary'],
        indexes=[
            {'columns': ['player_game_key'], 'unique': True},
            {'columns': ['game_id', 'player_id']},
            {'columns': ['player_id']},
            {'columns': ['game_id']}
        ]
    )
}}

-- Validate source tables first to ensure data quality
with event_participation_base as (
    select
        event_key,
        game_id,
        primary_player_id as player_id,
        primary_player_name as player_name,
        primary_team_id as team_id,
        primary_event_name,
        primary_event_detail,
        shot_made_flag,
        points_scored as points,
        period,
        seconds_elapsed_game,
        score_margin as game_score_margin -- home_score - away_score
    from {{ ref('int_pbp__event_participation') }}
    -- Filter out NULL or 0 player IDs - these are invalid
    where primary_player_id is not null 
      and primary_player_id > 0
      and primary_team_id is not null
),

-- Get team home/away status for score perspective calculations
game_team_lu as (
    select distinct
        game_id,
        team_id,
        home_away as type -- 'home' or 'away'
    from {{ ref('feat_opp__game_opponents_v2') }}
),

-- Get shot values from synthesized events
synthesized_event_link as (
    select distinct
        synthesized_event_key as event_key,
        event_number,
        shot_value_attempted as shot_value -- 1 for FT, 2 for 2PT, 3 for 3PT
    from {{ ref('int_pbp__synthesized_events') }}
    where shot_value_attempted is not null
),

-- Get clutch time info and team codes
player_game_event_context as (
    select distinct
        game_id,
        player_id,
        period,
        event_number,
        team_code,
        is_clutch_time_end_game
    from {{ ref('int_pbp__player_game_events') }}
    where player_id is not null
      and player_id > 0
      and game_id is not null
),

-- Base event data with enrichments
-- Do LEFT JOINs carefully to avoid losing good data
enriched_player_events as (
    select
        epb.event_key,
        epb.game_id,
        epb.player_id,
        epb.player_name,
        epb.team_id,
        pgec.team_code,
        epb.primary_event_name,
        epb.primary_event_detail,
        epb.shot_made_flag,
        epb.points,
        coalesce(sel.shot_value, 0) as shot_value, -- Default to 0 if not available
        epb.period,
        epb.seconds_elapsed_game,
        coalesce(pgec.is_clutch_time_end_game, false) as is_clutch_time_end_game,
        gtlu_player.type as player_team_type, -- 'home' or 'away'
        epb.game_score_margin, -- home_team_score - away_team_score
        case
            when gtlu_player.type = 'home' then epb.game_score_margin
            when gtlu_player.type = 'away' then -epb.game_score_margin
            else null -- Should not happen if joins are correct
        end as player_perspective_score_margin
    from event_participation_base epb
    left join synthesized_event_link sel 
        on epb.event_key = sel.event_key
    left join player_game_event_context pgec 
        on epb.game_id = pgec.game_id
        and epb.player_id = pgec.player_id
        and epb.period = pgec.period
        and sel.event_number = pgec.event_number
    left join game_team_lu gtlu_player 
        on epb.game_id = gtlu_player.game_id 
        and epb.team_id = gtlu_player.team_id
),

-- Create a player-game level dataset with one row per player-game
player_game_identity as (
    select distinct
        game_id,
        player_id,
        player_name,
        team_id,
        min(team_code) as team_code -- Get a single team code per player-game
    from enriched_player_events
    where player_id is not null
      and player_id > 0  -- Double check no zeros
      and game_id is not null
    group by game_id, player_id, player_name, team_id
),

player_stats_aggregated as (
    select
        pgi.game_id,
        pgi.player_id,
        pgi.player_name,
        pgi.team_id,
        pgi.team_code,
        -- Ensure the key is clean by casting to text explicitly
        pgi.game_id::text || '_' || pgi.player_id::text as player_game_key,

        -- Field Goal Stats (excluding free throws)
        sum(case when epe.primary_event_name = 'Made Shot' and epe.shot_value in (2,3) then 1 else 0 end) as field_goals_made,
        sum(case when epe.primary_event_name in ('Made Shot', 'Missed Shot') and epe.shot_value in (2,3) then 1 else 0 end) as field_goal_attempts,
        sum(case when epe.primary_event_name = 'Made Shot' and epe.shot_value = 3 then 1 else 0 end) as three_points_made,
        sum(case when epe.primary_event_name in ('Made Shot', 'Missed Shot') and epe.shot_value = 3 then 1 else 0 end) as three_point_attempts,

        -- Free Throw Stats
        sum(case when epe.primary_event_name = 'Made Shot' and epe.shot_value = 1 then 1 else 0 end) as free_throws_made,
        sum(case when epe.primary_event_name in ('Made Shot', 'Missed Shot') and epe.shot_value = 1 then 1 else 0 end) as free_throw_attempts,

        -- Other Counting Stats
        sum(case when epe.primary_event_name = 'Rebound' then 1 else 0 end) as rebounds_total,
        sum(case when epe.primary_event_name = 'Rebound' and epe.primary_event_detail ilike '%Offensive%' then 1 else 0 end) as offensive_rebounds,
        sum(case when epe.primary_event_name = 'Assist' then 1 else 0 end) as assists,
        sum(case when epe.primary_event_name = 'Steal' then 1 else 0 end) as steals,
        sum(case when epe.primary_event_name = 'Block' then 1 else 0 end) as blocks,
        sum(case when epe.primary_event_name = 'Turnover' then 1 else 0 end) as turnovers,
        sum(case when epe.primary_event_name = 'Foul' and epe.primary_event_detail not ilike '%Technical%' then 1 else 0 end) as personal_fouls,
        
        sum(coalesce(epe.points, 0)) as total_points,
        coalesce(max(epe.seconds_elapsed_game) / 60.0, 0) as minutes_played,
        
        -- Points by Quarter
        sum(case when epe.period = 1 then coalesce(epe.points, 0) else 0 end) as q1_points,
        sum(case when epe.period = 2 then coalesce(epe.points, 0) else 0 end) as q2_points,
        sum(case when epe.period = 3 then coalesce(epe.points, 0) else 0 end) as q3_points,
        sum(case when epe.period = 4 then coalesce(epe.points, 0) else 0 end) as q4_points,
        sum(case when epe.period > 4 then coalesce(epe.points, 0) else 0 end) as ot_points,
        
        -- Game State Scoring
        sum(coalesce(epe.points, 0)) filter(where abs(coalesce(epe.player_perspective_score_margin, 0)) <= 5) as points_in_close_game,
        sum(coalesce(epe.points, 0)) filter(where coalesce(epe.player_perspective_score_margin, 0) < -10) as points_when_trailing_big,
        sum(coalesce(epe.points, 0)) filter(where coalesce(epe.player_perspective_score_margin, 0) > 10) as points_when_leading_big,

        -- Clutch Shooting (Field Goals)
        sum(case when epe.is_clutch_time_end_game and epe.primary_event_name = 'Made Shot' and epe.shot_value in (2,3) then 1 else 0 end) as clutch_fg_made,
        sum(case when epe.is_clutch_time_end_game and epe.primary_event_name in ('Made Shot', 'Missed Shot') and epe.shot_value in (2,3) then 1 else 0 end) as clutch_fg_attempts
        
    from player_game_identity pgi
    left join enriched_player_events epe 
        on pgi.game_id = epe.game_id 
        and pgi.player_id = epe.player_id
    group by pgi.game_id, pgi.player_id, pgi.player_name, pgi.team_id, pgi.team_code
),

-- First create base calculations with defensive rebounds
advanced_stats_base as (
    select
        *,
        (rebounds_total - offensive_rebounds) as defensive_rebounds,
        
        -- Percentages with appropriate nullif to prevent division by zero
        field_goals_made / nullif(field_goal_attempts::float, 0) as fg_pct,
        three_points_made / nullif(three_point_attempts::float, 0) as three_p_pct,
        free_throws_made / nullif(free_throw_attempts::float, 0) as ft_pct,
        
        -- Advanced Shooting Metrics
        (field_goals_made + 0.5 * three_points_made) / nullif(field_goal_attempts::float, 0) as effective_fg_pct,
        total_points / nullif((2 * (field_goal_attempts + 0.44 * free_throw_attempts))::float, 0) as true_shooting_pct
    from player_stats_aggregated
),

-- Then use the defensive_rebounds field in a separate CTE
advanced_calculations as (
    select
        *,
        -- Simplified Game Score (example, can be customized)
        (total_points + (0.7 * offensive_rebounds) + (0.3 * defensive_rebounds) + (0.7 * assists) + steals + (0.7 * blocks) - (0.7 * field_goal_attempts) - (0.4 * (free_throw_attempts - free_throws_made)) - turnovers - (0.4 * personal_fouls)) as game_score_simple,

        -- Per 36 Minutes (assuming minutes_played is accurate)
        (total_points / nullif(minutes_played, 0)) * 36 as points_per_36,
        (rebounds_total / nullif(minutes_played, 0)) * 36 as rebounds_per_36,
        (assists / nullif(minutes_played, 0)) * 36 as assists_per_36
    from advanced_stats_base
),

-- Final validation to ensure we don't have any invalid records
final as (
    select *
    from advanced_calculations
    where player_id is not null
      and player_id > 0
      and game_id is not null
)

select
    player_game_key,
    game_id,
    player_id,
    player_name,
    team_id,
    team_code,
    minutes_played,
    field_goals_made,
    field_goal_attempts,
    fg_pct,
    three_points_made,
    three_point_attempts,
    three_p_pct,
    free_throws_made,
    free_throw_attempts,
    ft_pct,
    offensive_rebounds,
    defensive_rebounds,
    rebounds_total,
    assists,
    steals,
    blocks,
    turnovers,
    personal_fouls,
    total_points,
    effective_fg_pct,
    true_shooting_pct,
    game_score_simple,
    points_per_36,
    rebounds_per_36,
    assists_per_36,
    q1_points,
    q2_points,
    q3_points,
    q4_points,
    ot_points,
    points_in_close_game,
    points_when_trailing_big,
    points_when_leading_big,
    clutch_fg_made,
    clutch_fg_attempts,
    current_timestamp as processed_at
from final
