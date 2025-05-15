-- fct__player_game_prediction_features.sql
SELECT
    pge.player_id,
    pge.game_id,
    
    -- Volume features
    COUNT(DISTINCT CASE WHEN pge.action_type = 'SHOT' THEN pge.event_number END) AS shot_attempts,
    SUM(CASE WHEN pge.action_type = 'SHOT' THEN pge.points ELSE 0 END) AS points,
    COUNT(DISTINCT CASE WHEN pge.action_type = 'REBOUND' THEN pge.event_number END) AS rebounds,
    COUNT(DISTINCT CASE WHEN pge.action_type = 'ASSIST' THEN pge.event_number END) AS assists,
    
    -- Shot creation features
    sc.shots AS total_shots,
    sc.shots FILTER(WHERE sc.shot_creation_type = 'Self Created') AS self_created_shots,
    sc.shots FILTER(WHERE sc.shot_creation_type = 'Assisted') AS assisted_shots,
    
    -- Possession involvement
    pp.events_in_possession AS possession_events,
    pp.scored_in_possession AS scoring_possessions,
    
    -- Trend features
    tf.last_3_avg_points,
    tf.last_3_avg_rebounds,
    tf.point_trend
    
FROM {{ ref('int_pbp__player_game_events') }} pge
LEFT JOIN {{ ref('feat_pbp__player_shot_creation') }} sc 
  ON pge.player_id = sc.player_id AND pge.game_id = sc.game_id
LEFT JOIN {{ ref('feat_pbp__player_possessions') }} pp 
  ON pge.player_id = pp.player_id AND pge.game_id = pp.game_id
LEFT JOIN {{ ref('feat_pbp__player_trends') }} tf 
  ON pge.player_id = tf.player_id AND pge.game_id = tf.game_id
GROUP BY 1, 2