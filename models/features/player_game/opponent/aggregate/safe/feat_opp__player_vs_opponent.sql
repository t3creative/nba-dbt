{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_key',
    tags=['derived', 'features', 'opponent', 'prediction']
) }}

WITH base_matchups AS (
    SELECT
        MD5(CONCAT(pb.game_id, '-', pb.player_id)) AS player_game_key,
        pb.game_id,
        pb.player_id,
        pb.team_id,
        go.opponent_id,
        pb.game_date,
        pb.season_year,
        go.home_away,
        pb.player_name,
        pb.position,
        go.team_game_key
    FROM {{ ref('int_player__combined_boxscore') }} pb
    JOIN {{ ref('feat_opp__game_opponents') }} go
        ON pb.game_id = go.game_id 
        AND pb.team_id = go.team_id
    {% if is_incremental() %}
    WHERE pb.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
)

SELECT
    bm.player_game_key,
    bm.game_id,
    bm.player_id,
    bm.team_id,
    bm.opponent_id,
    bm.game_date,
    bm.season_year,
    bm.home_away,
    bm.player_name,
    bm.position,
    
    -- Player form features
    COALESCE(pf.last5_avg_pts, pf.season_avg_pts, 0) AS player_recent_pts,
    COALESCE(pf.last5_avg_reb, pf.season_avg_reb, 0) AS player_recent_reb,
    COALESCE(pf.last5_avg_ast, pf.season_avg_ast, 0) AS player_recent_ast,
    COALESCE(pf.last5_avg_min, 0) AS player_recent_min,
    COALESCE(pf.pts_form_indicator, 'NEUTRAL') AS player_form,
    COALESCE(pf.days_rest, 2) AS player_rest_days,
    
    -- Opponent team features
    COALESCE(opp.opp_l10_def_rating_prior, 0) AS opponent_defensive_rating,
    COALESCE(opp.opp_l10_pace_prior, 0) AS opponent_pace,
    COALESCE(opp.opp_adjusted_def_rating_prior, 0) AS opponent_adjusted_def_rating,
    
    -- Position matchup features
    COALESCE(pm.avg_pts_allowed_to_position, 0) AS avg_pts_allowed_to_position,
    COALESCE(pm.pts_vs_league_avg, 0) AS pts_vs_league_avg,
    COALESCE(pm.pts_matchup_label, 'Unknown') AS pts_matchup_label,
    COALESCE(pm.reb_matchup_label, 'Unknown') AS reb_matchup_label,
    COALESCE(pm.ast_matchup_label, 'Unknown') AS ast_matchup_label,
    -- pm.matchup_score, -- This column does not exist in the source model
    
    -- Historical matchup features
    COALESCE(pvo.avg_pts_vs_opponent_prior, 0) AS hist_avg_pts_vs_opp,
    COALESCE(pvo.avg_reb_vs_opponent_prior, 0) AS hist_avg_reb_vs_opp,
    COALESCE(pvo.avg_ast_vs_opponent_prior, 0) AS hist_avg_ast_vs_opp,
    COALESCE(pvo.prior_games_vs_opponent, 0) AS hist_games_vs_opp,
    COALESCE(pvo.historical_performance_flag_prior, 'NO_HISTORY') AS hist_performance_flag,
    COALESCE(pvo.sample_confidence_prior, 1) AS hist_confidence,
    COALESCE(pvo.recency_weighted_pts_prior, 0) AS hist_recent_pts_vs_opp,
    
    -- Team context features
    COALESCE(tc.team_last5_off_rating, 0) AS team_recent_off_rating,
    COALESCE(tc.missing_key_player, FALSE) AS is_missing_teammate,
    COALESCE(tc.is_back_to_back, FALSE) AS is_back_to_back,
    
    -- Game context
    CASE WHEN bm.home_away = 'HOME' THEN 1 ELSE 0 END AS is_home,
    
    -- Blended prediction features
    -- Points prediction features (blend of recent performance and matchup quality)
    CASE
        WHEN COALESCE(pvo.sample_confidence_prior, 0) >= 4 THEN
            (COALESCE(pf.last5_avg_pts, 0) * 0.4) + 
            (COALESCE(pvo.recency_weighted_pts_prior, 0) * 0.4) + 
            (COALESCE(pm.avg_pts_allowed_to_position, 0) * 0.2)
        WHEN COALESCE(pvo.sample_confidence_prior, 0) >= 2 THEN
            (COALESCE(pf.last5_avg_pts, 0) * 0.6) + 
            (COALESCE(pvo.avg_pts_vs_opponent_prior, 0) * 0.2) + 
            (COALESCE(pm.avg_pts_allowed_to_position, 0) * 0.2)
        ELSE
            (COALESCE(pf.last5_avg_pts, 0) * 0.7) + 
            (COALESCE(pm.avg_pts_allowed_to_position, 0) * 0.3)
    END AS blended_pts_projection,
    
    -- Rebound prediction features
    CASE
        WHEN COALESCE(pvo.sample_confidence_prior, 0) >= 4 THEN
            (COALESCE(pf.last5_avg_reb, 0) * 0.4) + 
            (COALESCE(pvo.avg_reb_vs_opponent_prior, 0) * 0.4) + 
            (COALESCE(pm.avg_reb_allowed_to_position, 0) * 0.2)
        WHEN COALESCE(pvo.sample_confidence_prior, 0) >= 2 THEN
            (COALESCE(pf.last5_avg_reb, 0) * 0.6) + 
            (COALESCE(pvo.avg_reb_vs_opponent_prior, 0) * 0.2) + 
            (COALESCE(pm.avg_reb_allowed_to_position, 0) * 0.2)
        ELSE
            (COALESCE(pf.last5_avg_reb, 0) * 0.7) + 
            (COALESCE(pm.avg_reb_allowed_to_position, 0) * 0.3)
    END AS blended_reb_projection,
    
    -- Assist prediction features
    CASE
        WHEN COALESCE(pvo.sample_confidence_prior, 0) >= 4 THEN
            (COALESCE(pf.last5_avg_ast, 0) * 0.4) + 
            (COALESCE(pvo.avg_ast_vs_opponent_prior, 0) * 0.4) + 
            (COALESCE(pm.avg_ast_allowed_to_position, 0) * 0.2)
        WHEN COALESCE(pvo.sample_confidence_prior, 0) >= 2 THEN
            (COALESCE(pf.last5_avg_ast, 0) * 0.6) + 
            (COALESCE(pvo.avg_ast_vs_opponent_prior, 0) * 0.2) + 
            (COALESCE(pm.avg_ast_allowed_to_position, 0) * 0.2)
        ELSE
            (COALESCE(pf.last5_avg_ast, 0) * 0.7) + 
            (COALESCE(pm.avg_ast_allowed_to_position, 0) * 0.3)
    END AS blended_ast_projection,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM base_matchups bm
LEFT JOIN {{ ref('opponent_pregame_profile_features_v1') }} opp
    ON bm.team_game_key = opp.team_game_key
LEFT JOIN {{ ref('feat_opp__player_position_matchup') }} pm
    ON bm.game_id = pm.game_id
    AND bm.player_id = pm.player_id
    AND bm.opponent_id = pm.opponent_id
    AND bm.game_date = pm.game_date
LEFT JOIN {{ ref('feat_opp__player_vs_opponent_history') }} pvo
    ON bm.player_id = pvo.player_id 
    AND bm.opponent_id = pvo.opponent_id
    AND bm.game_date = pvo.game_date
LEFT JOIN {{ ref('feat_player__recent_form') }} pf
    ON bm.player_id = pf.player_id
    AND bm.game_date = pf.game_date
LEFT JOIN {{ ref('int_team__recent_context') }} tc
    ON bm.team_id = tc.team_id
    AND bm.game_date = tc.game_date