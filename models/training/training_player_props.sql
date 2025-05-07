{{
  config(
    materialized='table',
    schema='training',
    tags=['training', 'player_props'],
    unique_key=['game_id', 'player_id', 'prop_stat']
  )
}}

player_props as (
  select
    game_id,
    player_id,
    market as prop_stat,            -- Use normalized market field as prop_stat ('PTS', 'REB', 'AST', etc.)
    line,
    over_odds,
    under_odds,
    over_implied_prob,
    under_implied_prob,
    hold_percentage as vig_percentage,
    game_date
  from {{ ref('int__player_props_normalized') }}
  where market in ('PTS', 'REB', 'AST')
  {% if is_incremental() %}
    and game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

player_boxscores as (
  select
    game_id,
    player_id,
    pts,       -- Actual points scored in game
    reb,       -- Actual rebounds in game
    ast,       -- Actual assists in game
    game_date
  from {{ ref('int__player_traditional_bxsc') }}
  {% if is_incremental() %}
  where game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

player_features as (
  select
    game_id,
    player_id,
    game_date,
    
    -- Rolling averages for each prop stat type
    pts_roll_10g_avg as pts_rolling_10_avg,         -- Points rolling avg
    reb_roll_10g_avg as reb_rolling_10_avg,         -- Rebounds rolling avg
    ast_roll_10g_avg as ast_rolling_10_avg,         -- Assists rolling avg
    
    -- Standard deviations for volatility measurement
    pts_roll_10g_stddev as pts_rolling_10_stddev,   -- Points rolling stddev
    reb_roll_10g_stddev as reb_rolling_10_stddev,   -- Rebounds rolling stddev
    ast_roll_10g_stddev as ast_rolling_10_stddev,   -- Assists rolling stddev
    
    -- Advanced stats
    usage_pct_roll_10g_avg as usage_pct,            -- Usage rate
    ts_pct_roll_10g_avg as ts_pct,                  -- Shooting efficiency
    pct_of_team_pts_roll_10g_avg as pct_of_team_pts,  -- % of team's points
    pct_of_team_reb_roll_10g_avg as pct_of_team_reb,  -- % of team's rebounds
    pct_of_team_ast_roll_10g_avg as pct_of_team_ast   -- % of team's assists
  from {{ ref('player_rolling_stats') }}

  {% if is_incremental() %}
  where game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

player_matchup_features as (
  select
    game_id,
    off_player_id as player_id,
    defensive_field_goal_diff,           -- Defender impact on player's shooting efficiency
    matchup_points_allowed_per_100_poss, -- Scoring volume allowed by defender
    offensive_matchup_advantage,         -- Player's scoring advantage vs this defender
    partial_poss,                        -- Possessions matched up together
    game_date
  from {{ ref('player_matchup_features') }}

  {% if is_incremental() %}
  where game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

team_def_features as (
  select
    team_id,
    season_year,
    avg_def_rating,             -- Team defensive rating
    avg_opp_eff_fg_pct,         -- Opponent effective field goal percentage
    avg_opp_pts_in_paint,       -- Opponent points in paint
    avg_deflections_per_game,   -- Team disruption metric
    avg_cont_2pt_per_game,      -- Shot contesting metric
    avg_cont_3pt_per_game       -- Shot contesting metric
  from {{ ref('team_season_defensive_summary') }}
),

game_context_features as (
  select
    game_id,
    rest_advantage,                     -- Rest day advantage/disadvantage 
    home_court_advantage_factor,        -- Home court advantage strength
    season_stage,                       -- Stage of season (Early/Mid/Late/Playoffs)
    win_pct_diff_last_10,               -- Win percentage differential over last 10 games
    home_team_streak,                   -- Home team form (Hot/Cold/Neutral streak)
    away_team_streak,                   -- Away team form (Hot/Cold/Neutral streak)
    game_date
  from {{ ref('game_context_features') }}

  {% if is_incremental() %}
  where game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

tempo_features as (
  select
    tp.game_id,
    tp.possessions_per_48,         -- Pace metric (possessions per 48 min)
    tp.game_pace_category,         -- Pace category (FAST_PACED, MODERATE, SLOW_PACED, etc.)
    tp.avg_possession_duration,    -- Average possession length
    tp.total_possessions,          -- Total game possessions
    tp.scoring_efficiency,         -- Scoring efficiency
    tp.pace_consistency,           -- Game pace pattern (CONSISTENT, INCREASING, etc.)
    g.game_date                    -- Get game_date from the joined game context
  from {{ ref('pbp__game_pace') }} tp
  -- Join with game context to get the game date
  inner join {{ ref('int__game_context') }} g on tp.game_id = g.game_id
  {% if is_incremental() %}
  where g.game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

-- Additional CTE to join game info with team defensive stats
game_team_info as (
  select
    go.game_id,
    go.team_id,
    go.opponent_id,
    go.season_year,
    go.game_date
  from {{ ref('int__game_opponents') }} go
  
  {% if is_incremental() %}
  where go.game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

final as (
  select

      -- Original columns
      pp.game_id,
      pp.player_id,
      pp.prop_stat,
      pp.game_date,
      
      -- Market features (betting context)
      pp.line,
      pp.over_odds,
      pp.under_odds,
      pp.over_implied_prob,
      pp.under_implied_prob,
      pp.vig_percentage,

      case pp.prop_stat
        when 'PTS' then pf.pts_rolling_10_avg
        when 'REB' then pf.reb_rolling_10_avg
        when 'AST' then pf.ast_rolling_10_avg
        else null
      end as rolling_10_avg,
      
      -- Stat volatility features
      case pp.prop_stat
        when 'PTS' then pf.pts_rolling_10_stddev
        when 'REB' then pf.reb_rolling_10_stddev
        when 'AST' then pf.ast_rolling_10_stddev
        else null
      end as rolling_10_stddev,
      
      -- Volatility ratio (stddev/mean) - higher values indicate more inconsistent performance
      case pp.prop_stat
        when 'PTS' then pf.pts_rolling_10_stddev / nullif(pf.pts_rolling_10_avg, 0)
        when 'REB' then pf.reb_rolling_10_stddev / nullif(pf.reb_rolling_10_avg, 0)
        when 'AST' then pf.ast_rolling_10_stddev / nullif(pf.ast_rolling_10_avg, 0)
        else null
      end as coefficient_of_variation,
      
      -- Player usage features (market-specific)
      case pp.prop_stat
        when 'PTS' then pf.pct_of_team_pts
        when 'REB' then pf.pct_of_team_reb
        when 'AST' then pf.pct_of_team_ast
        else null
      end as pct_of_team_stat,
      
      pf.usage_pct,
      pf.ts_pct,

      -- Matchup features
      pm.defensive_field_goal_diff,
      pm.matchup_points_allowed_per_100_poss,
      pm.offensive_matchup_advantage,
      pm.partial_poss,
      
      -- Team defensive features (opponent team)
      opp_def.avg_def_rating as opp_def_rating,
      opp_def.avg_opp_eff_fg_pct as opp_eff_fg_pct,
      opp_def.avg_opp_pts_in_paint as opp_pts_in_paint_allowed,
      opp_def.avg_deflections_per_game as opp_deflections_per_game,
      opp_def.avg_cont_2pt_per_game as opp_contested_2pt_per_game,
      opp_def.avg_cont_3pt_per_game as opp_contested_3pt_per_game,
      
      -- Game environment features
      gc.rest_advantage,
      gc.home_court_advantage_factor,
      gc.season_stage,
      gc.win_pct_diff_last_10,
      gc.home_team_streak,
      gc.away_team_streak,
      
      -- Pace/style features
      tp.possessions_per_48,
      tp.game_pace_category,
      tp.avg_possession_duration,
      tp.total_possessions,
      tp.scoring_efficiency,
      tp.pace_consistency,
      
      -- Derived features
      case pp.prop_stat
        when 'PTS' then (pp.line - pf.pts_rolling_10_avg)
        when 'REB' then (pp.line - pf.reb_rolling_10_avg)
        when 'AST' then (pp.line - pf.ast_rolling_10_avg)
        else null
      end as line_vs_recent_avg,
      
      -- Line as a percentage of rolling average
      case pp.prop_stat
        when 'PTS' then pp.line / nullif(pf.pts_rolling_10_avg, 0)
        when 'REB' then pp.line / nullif(pf.reb_rolling_10_avg, 0)
        when 'AST' then pp.line / nullif(pf.ast_rolling_10_avg, 0)
        else null
      end as line_pct_of_avg,
      
      -- Z-score of line compared to player's distribution (how many standard deviations from mean)
      case pp.prop_stat
        when 'PTS' then (pp.line - pf.pts_rolling_10_avg) / nullif(pf.pts_rolling_10_stddev, 0)
        when 'REB' then (pp.line - pf.reb_rolling_10_avg) / nullif(pf.reb_rolling_10_stddev, 0)
        when 'AST' then (pp.line - pf.ast_rolling_10_avg) / nullif(pf.ast_rolling_10_stddev, 0)
        else null
      end as line_z_score,
      
      -- Label columns - dynamically set based on prop_stat
      case pp.prop_stat
        when 'PTS' then pb.pts
        when 'REB' then pb.reb
        when 'AST' then pb.ast
        else null
      end as actual_stat,
      
      -- Binary classification target
      case 
        when (pp.prop_stat = 'PTS' and pb.pts > pp.line) or 
             (pp.prop_stat = 'REB' and pb.reb > pp.line) or
             (pp.prop_stat = 'AST' and pb.ast > pp.line)
          then 1
        else 0
      end as beat_line_flag
  from player_props pp
  inner join player_features pf
    on pp.game_id = pf.game_id and pp.player_id = pf.player_id
  inner join player_boxscores pb
    on pp.game_id = pb.game_id and pp.player_id = pb.player_id
  left join player_matchup_features pm
    on pp.game_id = pm.game_id and pp.player_id = pm.player_id
  left join game_team_info gti
    on pp.game_id = gti.game_id
  left join team_def_features opp_def
    on gti.opponent_id = opp_def.team_id and gti.season_year = opp_def.season_year
  left join game_context_features gc
    on pp.game_id = gc.game_id
  left join tempo_features tp
    on pp.game_id = tp.game_id
  where pp.game_date >= '2017-10-01'  -- Filter for 2017-18 season and beyond
)

select * from final