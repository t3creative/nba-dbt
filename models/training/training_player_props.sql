{{
  config(
    materialized='incremental',
    schema='training',
    tags=['training', 'player_props'],
    unique_key=['game_id', 'player_id', 'prop_stat']
  )
}}

with player_boxscore as (
  select
    game_id,
    season_year,
    player_id,
    player_name,
    team_id,
    game_date,
    pts,
    reb,
    ast
  from {{ ref('int__player_traditional_bxsc') }}
  where game_date >= '2017-10-01' and player_id = '2544' -- Start from 2017-18 season
  {% if is_incremental() %}
    and game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

player_features as (
  select
    game_id,
    player_id,
    team_id,
    game_date,
    pts_roll_10g_avg,
    reb_roll_10g_avg,
    ast_roll_10g_avg,
    pts_roll_10g_stddev,
    reb_roll_10g_stddev,
    ast_roll_10g_stddev,
    usage_pct_roll_10g_avg,
    ts_pct_roll_10g_avg,
    pct_of_team_pts_roll_10g_avg,
    pct_of_team_reb_roll_10g_avg,
    pct_of_team_ast_roll_10g_avg
  from {{ ref('player_rolling_stats') }}
  where game_date >= '2017-10-01'  -- Start from 2017-18 season
  {% if is_incremental() %}
    and game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

-- Create a base dataset with all the stats markets we want to analyze
stats_base as (
  select
    pb.game_id,
    pb.season_year,
    pb.player_id,
    pb.player_name,
    pb.game_date,
    'PTS' as prop_stat,
    pb.pts as actual_stat
  from player_boxscore pb
  
  union all
  
  select
    pb.game_id,
    pb.season_year,
    pb.player_id,
    pb.player_name,
    pb.game_date,
    'REB' as prop_stat,
    pb.reb as actual_stat
  from player_boxscore pb
  
  union all
  
  select
    pb.game_id,
    pb.season_year,
    pb.player_id,
    pb.player_name,
    pb.game_date,
    'AST' as prop_stat,
    pb.ast as actual_stat
  from player_boxscore pb
),

player_props as (
  select 
    game_id,
    player_id,
    player_name,
    game_date,
    market_cleaned as prop_stat,
    line,
    -- Consensus specific odds and probabilities
    consensus_over_odds as over_odds,
    consensus_under_odds as under_odds,
    consensus_over_implied_prob as over_implied_prob,
    consensus_under_implied_prob as under_implied_prob,
    consensus_total_implied_prob as total_implied_prob,
    consensus_over_no_vig_prob as over_no_vig_prob,
    consensus_under_no_vig_prob as under_no_vig_prob,
    consensus_hold_percentage as hold_percentage
  from {{ ref('int__player_props_normalized') }}
  where market_cleaned in ('PTS', 'REB', 'AST')
    and (consensus_over_odds is not null or consensus_under_odds is not null)
  {% if is_incremental() %}
    and game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

-- For tables where we expect exactly one row per key already
team_def_features as (
  select
    team_id,
    season_year,
    avg_def_rating,
    avg_opp_eff_fg_pct,
    avg_opp_pts_in_paint,
    avg_deflections_per_game,
    avg_cont_2pt_per_game,
    avg_cont_3pt_per_game
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
    -- Identifiers
    sb.game_id,
    sb.season_year,
    sb.player_id,
    sb.player_name,
    sb.prop_stat,
    sb.game_date,

    -- Betting data (may be null for pre-2021 data)
    pp.over_odds,
    pp.under_odds,
    pp.over_implied_prob,
    pp.under_implied_prob,
    pp.total_implied_prob,
    pp.over_no_vig_prob,
    pp.under_no_vig_prob,
    pp.hold_percentage,
    pp.line,

    -- Features and derived columns based on available data
    -- Add CASE statements for calculations that need betting data
    case sb.prop_stat
    when 'PTS' then pf.pts_roll_10g_avg
    when 'REB' then pf.reb_roll_10g_avg
    when 'AST' then pf.ast_roll_10g_avg
    else null
    end as rolling_10_avg,
    
    -- Stat volatility features
    case sb.prop_stat
    when 'PTS' then pf.pts_roll_10g_stddev
    when 'REB' then pf.reb_roll_10g_stddev
    when 'AST' then pf.ast_roll_10g_stddev
    else null
    end as rolling_10_stddev,
    
    -- Volatility ratio (stddev/mean) - higher values indicate more inconsistent performance
    case sb.prop_stat
    when 'PTS' then pf.pts_roll_10g_stddev / nullif(pf.pts_roll_10g_avg, 0)
    when 'REB' then pf.reb_roll_10g_stddev / nullif(pf.reb_roll_10g_avg, 0)
    when 'AST' then pf.ast_roll_10g_stddev / nullif(pf.ast_roll_10g_avg, 0)
    else null
    end as coefficient_of_variation,
    
    -- Player usage features (market-specific)
    case sb.prop_stat
    when 'PTS' then pf.pct_of_team_pts_roll_10g_avg
    when 'REB' then pf.pct_of_team_reb_roll_10g_avg
    when 'AST' then pf.pct_of_team_ast_roll_10g_avg
    else null
    end as pct_of_team_stat,
    
    pf.usage_pct_roll_10g_avg,
    pf.ts_pct_roll_10g_avg,
    
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
    
    -- Derived features that depend on betting data (with nulls handled)
    case 
      when pp.line is not null then
        case sb.prop_stat
        when 'PTS' then (pp.line - pf.pts_roll_10g_avg)
        when 'REB' then (pp.line - pf.reb_roll_10g_avg)
        when 'AST' then (pp.line - pf.ast_roll_10g_avg)
        else null
        end
      else null
    end as line_vs_recent_avg,
    
    -- Line as a percentage of rolling average
    case sb.prop_stat
    when 'PTS' then pp.line / nullif(pf.pts_roll_10g_avg, 0)
    when 'REB' then pp.line / nullif(pf.reb_roll_10g_avg, 0)
    when 'AST' then pp.line / nullif(pf.ast_roll_10g_avg, 0)
    else null
    end as line_pct_of_avg,
    
    -- Z-score of line compared to player's distribution (how many standard deviations from mean)
    case sb.prop_stat
    when 'PTS' then (pp.line - pf.pts_roll_10g_avg) / nullif(pf.pts_roll_10g_stddev, 0)
    when 'REB' then (pp.line - pf.reb_roll_10g_avg) / nullif(pf.reb_roll_10g_stddev, 0)
    when 'AST' then (pp.line - pf.ast_roll_10g_avg) / nullif(pf.ast_roll_10g_stddev, 0)
    else null
    end as line_z_score,
    
    -- Target variables
    sb.actual_stat,
    
    -- Classification target (will be null when no betting data)
    case 
      when pp.line is not null then
        case 
        when (sb.prop_stat = 'PTS' and sb.actual_stat > pp.line) or 
             (sb.prop_stat = 'REB' and sb.actual_stat > pp.line) or
             (sb.prop_stat = 'AST' and sb.actual_stat > pp.line)
          then 1
        else 0
        end
      else null
    end as beat_line_flag
    
  from stats_base sb
  left join player_props pp
    on sb.game_id = pp.game_id 
    and sb.player_id = pp.player_id
    and sb.prop_stat = pp.prop_stat
  inner join player_features pf
    on sb.game_id = pf.game_id and sb.player_id = pf.player_id
  left join game_team_info gti
    on sb.game_id = gti.game_id and pf.team_id = gti.team_id
  left join team_def_features opp_def
    on gti.opponent_id = opp_def.team_id and gti.season_year = opp_def.season_year
  left join game_context_features gc
    on sb.game_id = gc.game_id
)

select * from final