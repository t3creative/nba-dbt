/*
MODEL OVERVIEW
--------------------------------------------------
- Entity: game, team (within game context)
- Grain: game_id
- Purpose: Provides comprehensive context for each game, including current game details, team rest days, and recent team performance metrics leading up to the game.

DATA TEMPORALITY & FEATURE TYPE
--------------------------------------------------
- Contains Current Event Data: YES

- Primary Use for ML Prediction: NO

TECHNICAL DETAILS
--------------------------------------------------
- Primary Key(s): game_id
- Key Source Models: ref('stg__schedules')
- Last Modified: 2025-05-20
- Modified By: Tyler Tubridy
*/

{{
  config(
    materialized='incremental',
    tags=['game_context', 'intermediate'],
    unique_key='game_id',
    partition_by={
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['season_year', 'game_date']
  )
}}

-- Source game data directly from schedules
with source_game_data as (
    select
        game_id::varchar,
        game_date::date,
        (game_time_utc::timestamp)::time as game_time, -- Extract time from UTC timestamp
        season_year::varchar,
        home_team_id::integer,
        away_team_id::integer,
        home_team_score::integer,
        away_team_score::integer,
        home_team_wins::integer,
        home_team_losses::integer,
        away_team_wins::integer,
        away_team_losses::integer,
        arena_name::varchar,
        arena_city::varchar,
        arena_state::varchar,
        'USA'::varchar as arena_country,
        updated_at as source_updated_at
    from {{ ref('stg__schedules') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Get all team games (home and away) in a clean union
all_team_games as (
    select 
        home_team_id::integer as team_id,
        game_id::varchar,
        game_date::date,
        season_year::varchar
    from source_game_data
    
    union all
    
    select 
        away_team_id::integer as team_id,
        game_id::varchar,
        game_date::date,
        season_year::varchar
    from source_game_data
),

-- Calculate previous game information for rest days
team_last_games as (
    select
        team_id::integer,
        game_id::varchar,
        game_date::date,
        season_year::varchar,
        lag(game_id) over (partition by team_id, season_year order by game_date) as prev_game_id,
        lag(game_date) over (partition by team_id, season_year order by game_date) as prev_game_date
    from all_team_games
),

-- Get team scores and results for performance metrics
team_performance as (
    select
        game_id::varchar,
        team_id::integer,
        game_date::date,
        season_year::varchar,
        pts::integer,
        opp_pts::integer,
        case when pts > opp_pts then 'W'::varchar else 'L'::varchar end as win_loss
    from (
        -- Home teams
        select
            game_id::varchar,
            home_team_id::integer as team_id,
            game_date::date,
            season_year::varchar,
            home_team_score::integer as pts, 
            away_team_score::integer as opp_pts
        from {{ ref('stg__schedules') }}
        
        union all
        
        -- Away teams
        select
            game_id::varchar,
            away_team_id::integer as team_id,
            game_date::date,
            season_year::varchar,
            away_team_score::integer as pts,
            home_team_score::integer as opp_pts
        from {{ ref('stg__schedules') }}
    ) all_games
),

-- Calculate previous N games results for streaks
team_results as (
    select
        team_id::integer,
        game_id::varchar,
        game_date::date,
        season_year::varchar,
        win_loss::varchar,
        sum(case when win_loss = 'W' then 1 else 0 end) over (
            partition by team_id, season_year 
            order by game_date 
            rows between 10 preceding and 1 preceding
        )::integer as wins_last_10,
        sum(case when win_loss = 'L' then 1 else 0 end) over (
            partition by team_id, season_year 
            order by game_date 
            rows between 10 preceding and 1 preceding
        )::integer as losses_last_10,
        count(*) over (
            partition by team_id, season_year 
            order by game_date 
            rows between 10 preceding and 1 preceding
        )::integer as games_played_last_10
    from team_performance
),

-- Create base game context with team rest days
base_game_context as (
    select
        g.game_id::varchar,
        g.game_date::date,
        g.game_time::time,
        g.season_year::varchar,
        g.home_team_id::integer,
        g.away_team_id::integer,
        g.home_team_score::integer,
        g.away_team_score::integer,
        g.home_team_wins::integer,
        g.home_team_losses::integer,
        g.away_team_wins::integer,
        g.away_team_losses::integer,
        concat(g.arena_name, '_', g.arena_city, '_', g.arena_state)::varchar as arena_id,
        g.arena_name::varchar,
        g.arena_city::varchar,
        g.arena_state::varchar,
        g.arena_country::varchar,
        g.source_updated_at,
        
        -- Calculate point differential and margin of victory
        (g.home_team_score - g.away_team_score)::integer as point_differential,
        case 
            when g.home_team_score > g.away_team_score then (g.home_team_score - g.away_team_score)::integer 
            when g.away_team_score > g.home_team_score then (g.away_team_score - g.home_team_score)::integer
            else 0
        end::integer as margin_of_victory,
        
        -- Home team context
        ht.prev_game_id::varchar as home_prev_game_id,
        ht.prev_game_date::date as home_prev_game_date,
        (g.game_date - ht.prev_game_date)::integer as home_rest_days,
        case when ht.prev_game_date is null then true else false end as home_first_game_of_season,
        
        -- Away team context
        at.prev_game_id::varchar as away_prev_game_id,
        at.prev_game_date::date as away_prev_game_date,
        (g.game_date - at.prev_game_date)::integer as away_rest_days,
        case when at.prev_game_date is null then true else false end as away_first_game_of_season,
        
        -- Game sequence information
        row_number() over (partition by g.home_team_id, g.season_year order by g.game_date)::integer as home_team_game_num,
        row_number() over (partition by g.away_team_id, g.season_year order by g.game_date)::integer as away_team_game_num
    from source_game_data g
    left join team_last_games ht 
        on g.game_id = ht.game_id and g.home_team_id = ht.team_id
    left join team_last_games at 
        on g.game_id = at.game_id and g.away_team_id = at.team_id
)

-- Final output with all metrics
select
    g.game_id::varchar,
    g.game_date::date,
    g.game_time::time,
    g.season_year::varchar,
    g.home_team_id::integer,
    g.away_team_id::integer,
    g.home_team_score::integer,
    g.away_team_score::integer,
    g.home_team_wins::integer,
    g.home_team_losses::integer,
    g.away_team_wins::integer,
    g.away_team_losses::integer,
    g.point_differential::integer,
    g.margin_of_victory::integer,
    g.arena_id::varchar,
    g.arena_name::varchar,
    g.arena_city::varchar,
    g.arena_state::varchar,
    g.arena_country::varchar,
    
    -- Home team context
    g.home_prev_game_id::varchar,
    g.home_prev_game_date::date,
    g.home_rest_days::integer,
    g.home_first_game_of_season::boolean,
    case when g.home_rest_days = 1 then true else false end::boolean as home_back_to_back,
    case when g.home_rest_days between 2 and 3 then true else false end::boolean as home_short_rest,
    case when coalesce(g.home_rest_days, 0) >= 4 then true else false end::boolean as home_long_rest,
    g.home_team_game_num::integer,
    
    -- Home team streaks and recent performance
    hr.wins_last_10::integer as home_wins_last_10,
    hr.losses_last_10::integer as home_losses_last_10,
    hr.games_played_last_10::integer as home_games_played_last_10,
    (hr.wins_last_10 * 1.0 / nullif(hr.games_played_last_10, 0))::decimal(5,3) as home_win_pct_last_10,
    
    -- Away team context
    g.away_prev_game_id::varchar,
    g.away_prev_game_date::date,
    g.away_rest_days::integer,
    g.away_first_game_of_season::boolean,
    case when g.away_rest_days = 1 then true else false end::boolean as away_back_to_back,
    case when g.away_rest_days between 2 and 3 then true else false end::boolean as away_short_rest,
    case when coalesce(g.away_rest_days, 0) >= 4 then true else false end::boolean as away_long_rest,
    g.away_team_game_num::integer,
    
    -- Away team streaks and recent performance
    ar.wins_last_10::integer as away_wins_last_10,
    ar.losses_last_10::integer as away_losses_last_10,
    ar.games_played_last_10::integer as away_games_played_last_10,
    (ar.wins_last_10 * 1.0 / nullif(ar.games_played_last_10, 0))::decimal(5,3) as away_win_pct_last_10,
    
    -- Metadata
    g.source_updated_at,
    current_timestamp as created_at

from base_game_context g
left join team_results hr 
    on g.home_team_id = hr.team_id and g.game_id = hr.game_id
left join team_results ar 
    on g.away_team_id = ar.team_id and g.game_id = ar.game_id 