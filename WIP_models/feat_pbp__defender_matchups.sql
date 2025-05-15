-- feat_pbp__defender_matchups.sql
{{
  config(
    materialized='incremental',
    schema='features',
    unique_key='game_offensive_player_defender_key',
    tags=['feature', 'pbp', 'defense', 'matchups']
  )
}}

with game_dates as (
    select
        game_id,
        game_date
    from {{ ref('stg__schedules') }}
    group by 1, 2
),

{% if is_incremental() %}
-- Calculate the cutoff date from the existing data in the target table
max_date_calc as (
    select max(game_date) as max_date from {{ this }}
),

lookback_date as (
    select 
        max_date - INTERVAL '{{ var('incremental_lookback_days', 3) }} days' as cutoff_date 
    from max_date_calc
),
{% endif %}

event_participation_source as (
    select * from {{ ref('int_pbp__event_participation') }}
),

event_participation_with_dates as (
    select
        eps.*,
        gd.game_date
    from event_participation_source eps
    join game_dates gd on eps.game_id = gd.game_id
    {% if is_incremental() %}
    -- Filter based on game_date from the joined source using the pre-calculated cutoff_date
    where gd.game_date >= (select cutoff_date from lookback_date)
    {% endif %}
),

event_participation_ordered as (
    select
        epwd.*,
        -- Lagged fields from the previous event in the same game
        lag(epwd.primary_player_id, 1) over (partition by epwd.game_id order by epwd.period, epwd.seconds_elapsed_game, epwd.event_key) as prev_event_player1_id,
        lag(epwd.primary_team_id, 1) over (partition by epwd.game_id order by epwd.period, epwd.seconds_elapsed_game, epwd.event_key) as prev_event_team1_id,
        lag(epwd.primary_event_name, 1) over (partition by epwd.game_id order by epwd.period, epwd.seconds_elapsed_game, epwd.event_key) as prev_event_name,
        lag(epwd.shot_made_flag, 1) over (partition by epwd.game_id order by epwd.period, epwd.seconds_elapsed_game, epwd.event_key) as prev_event_shot_made_flag,
        lag(epwd.turnover_flag, 1) over (partition by epwd.game_id order by epwd.period, epwd.seconds_elapsed_game, epwd.event_key) as prev_event_turnover_flag
    from event_participation_with_dates epwd
    {% if is_incremental() %}
    -- No need for additional filtering here as it's done in the CTE above
    {% endif %}
),

block_matchups as (
    select
        game_id,
        prev_event_player1_id as offensive_player_id, -- Player whose shot was blocked
        primary_player_id as defender_id, -- The blocker
        'block' as matchup_type,
        game_date
    from event_participation_ordered
    where
        primary_event_name = 'Block'  -- Current event is a block
        and prev_event_name ilike '%Shot%' -- Previous event was a shot attempt
        and prev_event_player1_id is not null
        and primary_player_id is not null
        and prev_event_player1_id != primary_player_id -- Ensure not self-block (shouldn't happen)
),

steal_matchups as (
    select
        game_id,
        prev_event_player1_id as offensive_player_id, -- Player who committed the turnover
        primary_player_id as defender_id, -- The stealer
        'steal' as matchup_type,
        game_date
    from event_participation_ordered
    where
        primary_event_name = 'Steal' -- Current event is a steal
        and prev_event_turnover_flag = true -- Previous event was a turnover
        and prev_event_player1_id is not null
        and primary_player_id is not null
        and prev_event_player1_id != primary_player_id -- Ensure not stealing from oneself
),

defensive_rebound_matchups as (
    select
        game_id,
        prev_event_player1_id as offensive_player_id, -- The shooter
        primary_player_id as defender_id, -- The rebounder
        'defensive_rebound' as matchup_type,
        game_date
    from event_participation_ordered
    where
        primary_event_name = 'Rebound' -- Current event is a rebound
        and prev_event_name ilike '%Shot%' -- Previous event was a shot
        and prev_event_shot_made_flag = false -- The shot was a miss
        and primary_team_id is not null
        and prev_event_team1_id is not null
        and primary_team_id != prev_event_team1_id -- Rebounder's team is different from shooter's team
        and prev_event_player1_id is not null
        and primary_player_id is not null
),

all_defensive_matchups as (
    select game_id, offensive_player_id, defender_id, matchup_type, game_date from block_matchups
    union all
    select game_id, offensive_player_id, defender_id, matchup_type, game_date from steal_matchups
    union all
    select game_id, offensive_player_id, defender_id, matchup_type, game_date from defensive_rebound_matchups
),

final_aggregation as (
    select
        game_id,
        offensive_player_id,
        defender_id,
        game_id || '_' || offensive_player_id || '_' || defender_id as game_offensive_player_defender_key,
        max(game_date) as game_date, -- Add game_date to the final output
        count(*) filter(where matchup_type = 'block') as blocks_against_offensive_player_by_defender,
        count(*) filter(where matchup_type = 'steal') as steals_against_offensive_player_by_defender,
        count(*) filter(where matchup_type = 'defensive_rebound') as defensive_rebounds_off_miss_by_offensive_player_by_defender
    from all_defensive_matchups
    where offensive_player_id is not null and defender_id is not null -- Ensure valid player IDs for matchup
    group by 1, 2, 3, 4
)

select * from final_aggregation