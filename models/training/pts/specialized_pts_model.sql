-- Example for a specialized PTS model
{{
  config(
    materialized='table',
    schema='training',
    tags=['training', 'player_pts_prediction'],
    unique_key=['game_id', 'player_id']
  )
}}

with player_features as (
    -- Select core player features
    select * from {{ ref('feat_player__rolling_stats') }}
    where game_date >= '2020-10-01'  -- Set appropriate training start date
),Z

matchup_features as (
    -- Join player-defender matchup stats
    select * from {{ ref('feat_player__matchups') }}
),

team_context as (
    -- Add team-level context
    select * from {{ ref('feat_team__performance_metrics') }}
),

market_data as (
    -- Add betting market features
    select * from {{ ref('int_betting__player_props_probabilities') }}
    where market = 'PTS'
),

final as (
    select
        -- Identifiers
        pf.game_id,
        pf.player_id,
        pf.game_date,
        
        -- Target variable
        pf.pts as actual_pts,
        md.line,
        case when pf.pts > md.line then 1 else 0 end as beat_line_flag,
        
        -- Feature Groups
        
        -- 1. Recent Performance (varying windows)
        pf.pts_roll_5g_avg,
        pf.pts_roll_10g_avg,
        pf.pts_roll_20g_avg,
        pf.pts_roll_5g_stddev,
        pf.pts_roll_10g_stddev,
        
        -- 2. Efficiency Metrics
        pf.ts_pct_roll_10g_avg,
        pf.eff_fg_pct_roll_10g_avg,
        
        -- 3. Usage Indicators
        pf.usage_pct_roll_10g_avg,
        pf.pct_of_team_pts_roll_10g_avg,
        pf.pct_of_team_fga_roll_10g_avg,
        
        -- 4. Game Context
        tc.pace_roll_5g,
        tc.off_rating_roll_5g,
        
        -- 5. Matchup Features
        mf.defensive_rating_vs_position,
        mf.avg_pts_allowed_to_position,
        
        -- 6. Market Features
        md.over_implied_prob,
        md.under_implied_prob,
        md.line - pf.pts_roll_5g_avg as line_vs_recent_avg
        
    from player_features pf
    left join matchup_features mf on pf.game_id = mf.game_id and pf.player_id = mf.player_id
    left join team_context tc on pf.game_id = tc.game_id and pf.team_id = tc.team_id
    left join market_data md on pf.game_id = md.game_id and pf.player_id = md.player_id
)

select * from final
