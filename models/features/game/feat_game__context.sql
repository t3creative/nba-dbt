{{ config(
    materialized='table',
    tags=['features', 'game_context', 'final'],
    depends_on=[
        'int__game_pace_metrics',
        'int__game_script_metrics',
        'int__game_matchup_context',
        'int__game_home_away_factors',
        'int__game_situational_importance'
    ]
) }}

WITH combined_features AS (
    SELECT
        gp.game_id,
        gp.team_id,
        gp.opponent_id,
        -- Game identifiers
        gs.game_team_key,
        gm.season_year,
        gha.home_away,
        
        -- Pace & Style Features
        gp.pace_category,
        gp.team_play_style,
        gp.opponent_play_style,
        gp.team_shot_distribution,
        gp.opponent_shot_distribution,
        gp.game_pace,
        gp.game_possessions,
        gp.avg_season_pace,
        gp.team_off_rating_rel_to_avg,
        gp.opponent_def_rating_rel_to_avg,
        gp.team_off_reb_opportunity_rate,
        gp.opponent_def_reb_rate_vs_team,
        gp.paint_points_differential,
        gp.fastbreak_points_differential,
        
        -- Game Script Features
        gs.team_won,
        gs.q1_point_differential,
        gs.q2_point_differential,
        gs.q3_point_differential,
        gs.q4_point_differential,
        gs.halftime_point_differential,
        gs.team_point_differential,
        gs.game_closeness,
        gs.lead_changes,
        gs.times_tied,
        gs.team_largest_lead,
        gs.comeback_win,
        gs.quarter_scoring_volatility,
        gs.game_outcome_type,
        gs.potential_garbage_time,
        gs.went_to_overtime,
        
        -- Matchup Context Features
        gm.team_off_rank,
        gm.team_def_rank,
        gm.opp_off_rank,
        gm.opp_def_rank,
        gm.off_vs_def_rank_diff,
        gm.def_vs_off_rank_diff,
        gm.previous_matchups_count,
        gm.previous_matchups_win_pct,
        gm.offensive_matchup_type,
        gm.defensive_matchup_type,
        gm.offensive_rebounding_matchup,
        gm.won_last_matchup,
        gm.passing_matchup,
        gm.matchup_strength_score,
        
        -- Home/Away Features
        gha.team_avg_pts_in_context,
        gha.team_avg_off_rtg_in_context,
        gha.team_avg_def_rtg_in_context,
        gha.team_win_pct_in_context,
        gha.opponent_avg_pts_in_context,
        gha.opponent_avg_off_rtg_in_context,
        gha.opponent_avg_def_rtg_in_context,
        gha.team_pts_home_away_diff,
        gha.opponent_pts_home_away_diff,
        gha.recent_avg_pts_in_context,
        gha.recent_win_pct_in_context,
        gha.home_advantage_factor,
        gha.location_performance_type,
        gha.context_advantage_score,
        
        -- Situational Importance Features
        gsi.game_num_in_season,
        gsi.win_pct_before_game,
        gsi.opponent_win_pct_before_game,
        gsi.season_segment,
        gsi.days_since_last_game,
        gsi.days_until_next_game,
        gsi.is_back_to_back,
        gsi.team_rest_advantage,
        gsi.current_streak_length,
        gsi.streak_type,
        gsi.opponent_streak_length,
        gsi.opponent_streak_type,
        gsi.last_10_win_pct,
        gsi.opponent_last_10_win_pct,
        gsi.form_comparison,
        gsi.game_importance,
        gsi.is_significant_streak,
        gsi.schedule_situation,
        gsi.team_momentum,
        
        -- Composite ML Features
        -- Pace & matchup combined interaction
        CASE
            WHEN gp.pace_category = 'high_pace' AND gm.offensive_matchup_type = 'strong_off_vs_weak_def' THEN 'high_scoring_potential'
            WHEN gp.pace_category = 'low_pace' AND gm.defensive_matchup_type = 'strong_def_vs_weak_off' THEN 'low_scoring_potential'
            ELSE 'neutral_scoring_potential'
        END AS scoring_potential,
        
        -- Rest & form combined effect
        CASE
            WHEN gsi.is_back_to_back = TRUE AND gsi.team_momentum = 'negative_momentum' THEN 'high_fatigue_risk'
            WHEN gsi.team_rest_advantage = TRUE AND gsi.team_momentum = 'positive_momentum' THEN 'performance_boost_likely'
            ELSE 'neutral_rest_momentum'
        END AS rest_momentum_factor,
        
        -- Home court & matchup strength interaction
        gha.context_advantage_score + gm.matchup_strength_score AS total_advantage_score,
        
        -- Game script expectation
        CASE
            WHEN ABS(gm.matchup_strength_score) > 5 THEN 'potential_blowout'
            WHEN ABS(gha.context_advantage_score) > 0.3 AND ABS(gm.matchup_strength_score) > 3 THEN 'significant_favorite'
            WHEN ABS(gha.context_advantage_score) < 0.1 AND ABS(gm.matchup_strength_score) < 2 THEN 'tight_contest_likely'
            ELSE 'moderate_favorite'
        END AS expected_game_script,
        
        -- Statistical advantages integration
        CASE
            WHEN gm.offensive_matchup_type LIKE '%strong_off%' AND gp.team_play_style = 'transition_heavy' THEN 'transition_scoring_advantage'
            WHEN gm.offensive_rebounding_matchup = 'strong_oreb_advantage' AND gp.team_shot_distribution = 'inside_oriented' THEN 'inside_scoring_advantage'
            WHEN gm.passing_matchup = 'passing_advantage' AND gp.team_off_rating_rel_to_avg > 2 THEN 'playmaking_advantage'
            ELSE 'no_specific_advantage'
        END AS statistical_advantage_area,
        
        -- Overall context score (normalized -1 to 1 scale) weighted combination
        (
            gm.matchup_strength_score / 10 * 0.35 +  -- Matchup component (35% weight)
            gha.context_advantage_score * 0.25 +     -- Home/Away component (25% weight)
            CASE WHEN gsi.team_momentum = 'positive_momentum' THEN 0.2
                 WHEN gsi.team_momentum = 'negative_momentum' THEN -0.2
                 ELSE 0 END +                       -- Momentum component (20% weight)
            CASE WHEN gsi.schedule_situation = 'schedule_advantage' THEN 0.1
                 WHEN gsi.schedule_situation = 'schedule_disadvantage' THEN -0.1
                 ELSE 0 END +                       -- Schedule component (10% weight)
            CASE WHEN gsi.game_importance = 'high_playoff_implications' THEN 0.1
                 WHEN gsi.game_importance = 'low_importance' THEN -0.05
                 ELSE 0 END                         -- Importance component (10% weight)
        ) AS game_context_score
        
    FROM {{ ref('int__game_pace_metrics') }} gp
    JOIN {{ ref('int__game_script_metrics') }} gs 
        ON gp.game_id = gs.game_id AND gp.team_id = gs.team_id
    JOIN {{ ref('int__game_matchup_context') }} gm 
        ON gp.game_id = gm.game_id AND gp.team_id = gm.team_id
    JOIN {{ ref('int__game_home_away_factors') }} gha 
        ON gp.game_id = gha.game_id AND gp.team_id = gha.team_id
    JOIN {{ ref('int__game_situational_importance') }} gsi 
        ON gp.game_id = gsi.game_id AND gp.team_id = gsi.team_id
)

SELECT
    -- Primary Keys and Identifiers
    game_id,
    team_id,
    opponent_id,
    game_team_key,
    season_year,
    home_away,
    
    -- ---------- KEY PREDICTIVE FEATURES ----------
    
    -- Game Context Score (overall integrated advantage metric)
    game_context_score,
    
    -- Pace and Style
    pace_category,
    game_pace,
    game_possessions,
    team_play_style,
    team_shot_distribution,
    team_off_rating_rel_to_avg,
    opponent_def_rating_rel_to_avg,
    
    -- Matchup Context
    matchup_strength_score,
    offensive_matchup_type,
    defensive_matchup_type,
    offensive_rebounding_matchup,
    passing_matchup,
    
    -- Home/Away Factors
    team_win_pct_in_context,
    context_advantage_score,
    location_performance_type,
    
    -- Situational Factors
    season_segment,
    is_back_to_back,
    team_rest_advantage,
    streak_type,
    current_streak_length,
    form_comparison,
    schedule_situation,
    team_momentum,
    
    -- Integrated Features
    scoring_potential,
    rest_momentum_factor,
    total_advantage_score,
    expected_game_script,
    statistical_advantage_area,
    
    -- ---------- ADDITIONAL CONTEXT FEATURES ----------
    
    -- Detailed Pace Metrics
    opponent_play_style,
    opponent_shot_distribution,
    avg_season_pace,
    team_off_reb_opportunity_rate,
    opponent_def_reb_rate_vs_team,
    paint_points_differential,
    fastbreak_points_differential,
    
    -- Game Script Details
    game_closeness,
    lead_changes,
    times_tied,
    team_largest_lead,
    q1_point_differential,
    q2_point_differential,
    q3_point_differential,
    q4_point_differential,
    halftime_point_differential,
    team_point_differential,
    comeback_win,
    quarter_scoring_volatility,
    game_outcome_type,
    potential_garbage_time,
    went_to_overtime,
    
    -- Detailed Matchup Metrics
    team_off_rank,
    team_def_rank,
    opp_off_rank,
    opp_def_rank,
    off_vs_def_rank_diff,
    def_vs_off_rank_diff,
    previous_matchups_count,
    previous_matchups_win_pct,
    won_last_matchup,
    
    -- Detailed Home/Away Metrics
    team_avg_pts_in_context,
    team_avg_off_rtg_in_context,
    team_avg_def_rtg_in_context,
    opponent_avg_pts_in_context,
    opponent_avg_off_rtg_in_context,
    opponent_avg_def_rtg_in_context,
    team_pts_home_away_diff,
    opponent_pts_home_away_diff,
    recent_avg_pts_in_context,
    recent_win_pct_in_context,
    home_advantage_factor,
    
    -- Detailed Situational Metrics
    game_num_in_season,
    win_pct_before_game,
    opponent_win_pct_before_game,
    days_since_last_game,
    days_until_next_game,
    opponent_streak_length,
    opponent_streak_type,
    last_10_win_pct,
    opponent_last_10_win_pct,
    game_importance,
    is_significant_streak,
    
    -- Ground Truth (for training models)
    team_won,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS created_at
FROM combined_features