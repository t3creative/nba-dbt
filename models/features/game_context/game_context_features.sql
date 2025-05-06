{{
  config(
    materialized='incremental',
    schema='features',
    tags=['game_context', 'features'],
    unique_key='game_id',
    partition_by={
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['season_year', 'game_date']
  )
}}

with base_context as (
    select *
    from {{ ref('int__game_context') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Team statistical averages over the last 10 games
team_stats_home as (
    select
        g.game_id,
        g.home_team_id as team_id,
        avg(g.home_wins_last_10) as home_wins_last_10_avg,
        avg(g.home_losses_last_10) as home_losses_last_10_avg,
        avg(g.home_win_pct_last_10) as home_win_pct_last_10_avg
    from base_context g
    group by 1, 2
),

team_stats_away as (
    select
        g.game_id,
        g.away_team_id as team_id,
        avg(g.away_wins_last_10) as away_wins_last_10_avg,
        avg(g.away_losses_last_10) as away_losses_last_10_avg,
        avg(g.away_win_pct_last_10) as away_win_pct_last_10_avg
    from base_context g
    group by 1, 2
)

select
    g.*,
    
    -- Derived features for ML
    (coalesce(g.home_win_pct_last_10, 0.5) - coalesce(g.away_win_pct_last_10, 0.5)) as win_pct_diff_last_10,
    
    -- Rest advantage
    (g.home_rest_days - g.away_rest_days) as rest_advantage,
    case 
        when g.home_rest_days > g.away_rest_days then 'HOME'
        when g.away_rest_days > g.home_rest_days then 'AWAY'
        else 'NONE'
    end as rest_advantage_team,
    
    -- Back-to-back impact
    g.home_back_to_back as home_back_to_back_flag,
    g.away_back_to_back as away_back_to_back_flag,
    
    -- Streak indicators
    case
        when g.home_wins_last_10 >= 7 then 'Hot Streak'
        when g.home_losses_last_10 >= 7 then 'Cold Streak'
        else 'Neutral'
    end as home_team_streak,
    
    case
        when g.away_wins_last_10 >= 7 then 'Hot Streak'
        when g.away_losses_last_10 >= 7 then 'Cold Streak'
        else 'Neutral'
    end as away_team_streak,
    
    -- Season progress indicator
    case
        when g.season_type = 'Regular Season' and g.home_team_game_num < 20 then 'Early Season'
        when g.season_type = 'Regular Season' and g.home_team_game_num > 60 then 'Late Season'
        when g.season_type = 'Playoffs' then 'Playoffs'
        else 'Mid Season'
    end as season_stage,
    
    -- Home court advantage indicator
    abs(coalesce(g.home_win_pct_last_10, 0.5) - coalesce(g.away_win_pct_last_10, 0.5)) 
        * coalesce(g.home_rest_days, 0) / greatest(coalesce(g.away_rest_days, 1), 1) 
        * (case when g.away_back_to_back then 1.5 else 1.0 end) as home_court_advantage_factor

from base_context g
left join team_stats_home hts on g.game_id = hts.game_id
left join team_stats_away ats on g.game_id = ats.game_id 