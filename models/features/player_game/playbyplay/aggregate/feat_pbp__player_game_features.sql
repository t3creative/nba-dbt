{{ config(
    schema='features',
    materialized='incremental',
    unique_key=['player_id', 'game_id'],
    on_schema_change='sync_all_columns',
    incremental_strategy='merge',
    tags=['features', 'aggregate', 'pbp', 'ml_safe'],
    indexes=[
        {'columns': ['player_id', 'game_date'], 'type': 'btree'},
        {'columns': ['game_date'], 'type': 'btree'}
    ]
) }}

with derived_events as (
    select * from {{ ref('feat_pbp__event_features') }}
    {% if is_incremental() %}
    where game_date >= (select max(game_date) - interval '3 days' from {{ this }})
    {% endif %}
),

-- Aggregate to player-game level with ML-safe historical context
player_game_aggregates as (
    select
        player_id,
        game_id,
        game_date,
        team_id,
        player_name,
        
        -- Event volume metrics
        count(*) as total_events,
        count(case when is_shot_event then 1 end) as shot_attempts,
        count(case when shot_made_flag = true then 1 end) as shots_made,
        count(case when event_type = 'Rebound' then 1 end) as rebounds,
        count(case when event_type = 'Assist' then 1 end) as assists,
        count(case when event_type = 'Turnover' then 1 end) as turnovers,
        count(case when event_type = 'Steal' then 1 end) as steals,
        count(case when event_type = 'Block' then 1 end) as blocks,
        count(case when event_type = 'Foul' then 1 end) as fouls,
        
        -- Performance ratios (current game - POTENTIAL LEAK if used for prediction)
        case 
            when count(case when is_shot_event then 1 end) > 0 
            then round(count(case when shot_made_flag = true then 1 end)::numeric / 
                      count(case when is_shot_event then 1 end)::numeric, 3)
            else null
        end as shooting_pct, -- POTENTIAL LEAK
        
        -- Scoring output (current game - POTENTIAL LEAK)
        sum(coalesce(points_scored, 0)) as points_scored, -- POTENTIAL LEAK
        
        -- Timing patterns (safe contextual features)
        avg(seconds_since_last_event) as avg_seconds_between_events,
        max(seconds_since_last_event) as max_seconds_between_events,
        min(case when seconds_since_last_event > 0 then seconds_since_last_event end) as min_seconds_between_events,
        
        -- Game context aggregations (safe)
        count(case when clutch_time_flag = true then 1 end) as clutch_time_events,
        count(case when game_phase = 'First Half' then 1 end) as first_half_events,
        count(case when game_phase = 'Second Half' then 1 end) as second_half_events,
        count(case when rapid_sequence_event = true then 1 end) as rapid_sequence_events,
        
        -- Shot context patterns (safe)
        count(case when shot_context_flag = 'Shot After Make' then 1 end) as shots_after_makes,
        count(case when shot_context_flag = 'Shot After Miss' then 1 end) as shots_after_misses,
        
        -- Form indicators (safe - based on historical rolling windows)
        mode() within group (order by recent_shooting_form) as predominant_shooting_form,
        
        -- Meta
        min(created_at) as first_event_created_at,
        max(updated_at) as last_updated_at
        
    from derived_events
    group by 
        player_id, game_id, game_date, team_id, player_name
),

-- Add historical context features (ML-SAFE: only uses data before current game)
historical_context as (
    select
        current_game.*,
        
        -- Previous games performance (strictly before current game_date)
        avg(hist.shooting_pct) as historical_avg_shooting_pct,
        avg(hist.points_scored::numeric) as historical_avg_points,
        avg(hist.shot_attempts::numeric) as historical_avg_shot_attempts,
        avg(hist.total_events::numeric) as historical_avg_total_events,
        
        -- Recent form (last 5 games before current game)
        avg(case when hist.game_date >= current_game.game_date - interval '15 days' 
                 then hist.shooting_pct end) as recent_5g_avg_shooting_pct,
        avg(case when hist.game_date >= current_game.game_date - interval '15 days' 
                 then hist.points_scored::numeric end) as recent_5g_avg_points,
        
        -- Games played context
        count(hist.game_id) as career_games_played_before_this,
        count(case when hist.game_date >= current_game.game_date - interval '30 days' 
              then hist.game_id end) as games_last_30_days_before_this,
        
        -- Rest context (days since last game)
        max(hist.game_date) as previous_game_date
        
    from player_game_aggregates current_game
    left join player_game_aggregates hist
        on current_game.player_id = hist.player_id
        and hist.game_date < current_game.game_date  -- CRITICAL: Only past games
    group by 
        current_game.player_id, current_game.game_id, current_game.game_date,
        current_game.team_id, current_game.player_name, current_game.total_events,
        current_game.shot_attempts, current_game.shots_made, current_game.rebounds,
        current_game.assists, current_game.turnovers, current_game.steals,
        current_game.blocks, current_game.fouls, current_game.shooting_pct,
        current_game.points_scored, current_game.avg_seconds_between_events,
        current_game.max_seconds_between_events, current_game.min_seconds_between_events,
        current_game.clutch_time_events, current_game.first_half_events,
        current_game.second_half_events, current_game.rapid_sequence_events,
        current_game.shots_after_makes, current_game.shots_after_misses,
        current_game.predominant_shooting_form, current_game.first_event_created_at,
        current_game.last_updated_at
),

final_features as (
    select
        *,
        -- Calculate rest days (safe contextual feature)
        case 
            when previous_game_date is not null 
            then game_date - previous_game_date
            else null
        end as days_rest,
        
        -- Experience flags (safe)
        case when career_games_played_before_this >= 50 then true else false end as veteran_player_flag,
        case when games_last_30_days_before_this >= 10 then true else false end as high_usage_recent_flag,
        
        -- Form indicators (safe - based on historical data only)
        case 
            when recent_5g_avg_shooting_pct > historical_avg_shooting_pct + 0.05 then 'Recent_Hot'
            when recent_5g_avg_shooting_pct < historical_avg_shooting_pct - 0.05 then 'Recent_Cold'
            else 'Stable'
        end as recent_form_vs_historical
        
    from historical_context
)

select * from final_features

/*
ML-SAFE VERIFIED: Partial - Requires Review
Leakage concerns identified:
- shooting_pct: Uses current game performance
- points_scored: Uses current game totals
- All *_events counts: Include current game events

RECOMMENDED ACTIONS:
1. Move current-game metrics to separate training labels table
2. Keep only historical and contextual features in this aggregate
3. Add automated leakage detection tests
4. Implement temporal validation in CI/CD

Leakage tests needed: Compare model performance with/without flagged features
*/