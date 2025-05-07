{{
  config(
    materialized='table',
    schema='training',
    tags=['training', 'player_props'],
    unique_key=['game_id', 'player_id', 'prop_stat']
  )
}}

with source_props as (
  select 
    game_id,
    player_id,
    player_name,
    game_date,
    market_cleaned,
    -- Consensus specific odds and probabilities
    consensus_over_odds,
    consensus_under_odds,
    consensus_over_implied_prob,
    consensus_under_implied_prob,
    consensus_total_implied_prob,
    consensus_over_no_vig_prob,
    consensus_under_no_vig_prob,
    consensus_hold_percentage,
    line, -- line is still a general field from int__player_props_normalized
    row_number() over(
      partition by game_id, player_id, market_cleaned
      order by game_date desc
    ) as row_num
  from {{ ref('int__player_props_normalized') }}
  where market_cleaned in ('PTS', 'REB', 'AST')
    and (consensus_over_odds is not null or consensus_under_odds is not null) -- Ensure Consensus odds exist
  {% if is_incremental() %}
    and game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

player_props as (
  select
    game_id,
    player_id,
    player_name,
    market_cleaned as prop_stat,
    line,
    -- Aliasing Consensus columns to generic names for downstream compatibility
    consensus_over_odds as over_odds,
    consensus_under_odds as under_odds,
    consensus_over_implied_prob as over_implied_prob,
    consensus_under_implied_prob as under_implied_prob,
    consensus_total_implied_prob as total_implied_prob,
    consensus_over_no_vig_prob as over_no_vig_prob,
    consensus_under_no_vig_prob as under_no_vig_prob,
    consensus_hold_percentage as hold_percentage,

    game_date
  from source_props
  where row_num = 1
),

player_boxscore as (
  select
    game_id,
    player_id,
    pts,
    reb,
    ast
  from {{ ref('int__player_traditional_bxsc') }}

  {% if is_incremental() %}
  where game_date > (select max(game_date) from {{ this }})
  {% endif %}
),

player_features as (
  select
    game_id,
    player_id,
    team_id,
    game_date,
    pts_roll_10g_avg as pts_rolling_10_avg,
    reb_roll_10g_avg as reb_rolling_10_avg,
    ast_roll_10g_avg as ast_rolling_10_avg,
    pts_roll_10g_stddev as pts_rolling_10_stddev,
    reb_roll_10g_stddev as reb_rolling_10_stddev,
    ast_roll_10g_stddev as ast_rolling_10_stddev,
    usage_pct_roll_10g_avg as usage_pct,
    ts_pct_roll_10g_avg as ts_pct,
    pct_of_team_pts_roll_10g_avg as pct_of_team_pts,
    pct_of_team_reb_roll_10g_avg as pct_of_team_reb,
    pct_of_team_ast_roll_10g_avg as pct_of_team_ast
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

        -- Original columns
        pp.game_id,
        pp.player_id,
        pp.player_name,
        pp.prop_stat,
        pp.game_date,

        -- Consensus specific odds and probabilities
        pp.over_odds,
        pp.under_odds,
        pp.over_implied_prob,
        pp.under_implied_prob,
        pp.total_implied_prob,
        pp.over_no_vig_prob,
        pp.under_no_vig_prob,
        pp.hold_percentage,

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
    inner join player_boxscore pb
        on pp.game_id = pb.game_id and pp.player_id = pb.player_id
    left join player_matchup_features pm
        on pp.game_id = pm.game_id and pp.player_id = pm.player_id
    left join game_team_info gti
        on pp.game_id = gti.game_id and pf.team_id = gti.team_id
    left join team_def_features opp_def
        on gti.opponent_id = opp_def.team_id and gti.season_year = opp_def.season_year
    left join game_context_features gc
        on pp.game_id = gc.game_id
    where pp.game_date >= '2017-10-01'  -- Filter for 2017-18 season and beyond
)

select * from final