{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    tags=['marts', 'prediction', 'features'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['player_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']}
    ]
) }}

WITH base_matchups AS (
    SELECT
        pm.player_game_key,
        pm.game_id,
        pm.player_id,
        pm.team_id,
        pm.opponent_id,
        pm.game_date,
        pm.season_year,
        pm.home_away,
        pm.player_name,
        pm.position
    FROM {{ ref('int__player_position_matchup') }} pm
    {% if is_incremental() %}
    WHERE pm.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

-- Player's own recent performance
player_form AS (
    SELECT
        player_id,
        
        -- Last 5 game stats
        AVG(pts) OVER(
            PARTITION BY player_id 
            ORDER BY game_date DESC 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last5_avg_pts,
        
        AVG(reb) OVER(
            PARTITION BY player_id 
            ORDER BY game_date DESC 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last5_avg_reb,
        
        AVG(ast) OVER(
            PARTITION BY player_id 
            ORDER BY game_date DESC 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last5_avg_ast,
        
        AVG(min) OVER(
            PARTITION BY player_id 
            ORDER BY game_date DESC 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last5_avg_min,
        
        AVG(ts_pct) OVER(
            PARTITION BY player_id 
            ORDER BY game_date DESC 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last5_avg_ts_pct,
        
        -- Season averages
        AVG(pts) OVER(
            PARTITION BY player_id, season_year
            ORDER BY game_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_pts,
        
        AVG(reb) OVER(
            PARTITION BY player_id, season_year
            ORDER BY game_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_reb,
        
        AVG(ast) OVER(
            PARTITION BY player_id, season_year
            ORDER BY game_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_ast,
        
        -- Last game
        FIRST_VALUE(pts) OVER(
            PARTITION BY player_id 
            ORDER BY game_date DESC
        ) AS last_game_pts,
        
        -- Form indicators
        CASE
            WHEN AVG(pts) OVER(
                PARTITION BY player_id 
                ORDER BY game_date DESC 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) > 
            AVG(pts) OVER(
                PARTITION BY player_id, season_year
                ORDER BY game_date DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) * 1.15
            THEN 'HOT'
            WHEN AVG(pts) OVER(
                PARTITION BY player_id 
                ORDER BY game_date DESC 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) < 
            AVG(pts) OVER(
                PARTITION BY player_id, season_year
                ORDER BY game_date DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) * 0.85
            THEN 'COLD'
            ELSE 'NEUTRAL'
        END AS pts_form_indicator,
        
        -- Rest days
        DATE_PART('day', game_date - LAG(game_date) OVER(
            PARTITION BY player_id
            ORDER BY game_date
        )) AS days_rest,
        
        game_date
    FROM {{ ref('int__combined_player_boxscore') }}
    WHERE min > 0
),

-- Team contextual factors
team_context AS (
    SELECT
        tb.team_id,
        tb.game_date,
        
        -- Team's recent performance
        AVG(tb.off_rating) OVER(
            PARTITION BY tb.team_id 
            ORDER BY tb.game_date 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS team_last5_off_rating,
        
        -- Team injuries impact (placeholder - would join to injury data)
        FALSE AS missing_key_player,
        
        -- Back-to-back indicator
        CASE 
            WHEN DATE_PART('day', tb.game_date - LAG(tb.game_date) OVER(
                PARTITION BY tb.team_id
                ORDER BY tb.game_date
            )) = 1 
            THEN TRUE 
            ELSE FALSE 
        END AS is_back_to_back
    FROM {{ ref('int__combined_team_boxscore') }} tb
),

-- Final integrated feature set
final AS (
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
        opp.opp_l10_def_rating AS opponent_defense_rating,
        opp.opp_l10_pace AS opponent_pace,
        opp.opp_adjusted_def_rating AS opponent_adjusted_def_rating,
        
        -- Position matchup features
        pm.avg_pts_allowed_to_position,
        pm.pts_vs_league_avg,
        pm.pts_matchup_quality,
        pm.reb_matchup_quality,
        pm.ast_matchup_quality,
        pm.matchup_score,
        
        -- Historical matchup features
        COALESCE(pvo.avg_pts_vs_opponent, 0) AS hist_avg_pts_vs_opp,
        COALESCE(pvo.avg_reb_vs_opponent, 0) AS hist_avg_reb_vs_opp,
        COALESCE(pvo.avg_ast_vs_opponent, 0) AS hist_avg_ast_vs_opp,
        COALESCE(pvo.games_vs_opponent, 0) AS hist_games_vs_opp,
        COALESCE(pvo.historical_performance_flag, 'NO_HISTORY') AS hist_performance_flag,
        COALESCE(pvo.sample_confidence, 1) AS hist_confidence,
        COALESCE(pvo.recency_weighted_pts, 0) AS hist_recent_pts_vs_opp,
        
        -- Team context features
        COALESCE(tc.team_last5_off_rating, 0) AS team_recent_off_rating,
        COALESCE(tc.missing_key_player, FALSE) AS is_missing_teammate,
        COALESCE(tc.is_back_to_back, FALSE) AS is_back_to_back,
        
        -- Game context
        CASE WHEN bm.home_away = 'HOME' THEN 1 ELSE 0 END AS is_home,
        
        -- Blended prediction features
        -- Points prediction features (blend of recent performance and matchup quality)
        CASE
            WHEN pvo.sample_confidence >= 4 THEN
                (pf.last5_avg_pts * 0.4) + 
                (pvo.recency_weighted_pts * 0.4) + 
                (pm.avg_pts_allowed_to_position * 0.2)
            WHEN pvo.sample_confidence >= 2 THEN
                (pf.last5_avg_pts * 0.6) + 
                (pvo.avg_pts_vs_opponent * 0.2) + 
                (pm.avg_pts_allowed_to_position * 0.2)
            ELSE
                (pf.last5_avg_pts * 0.7) + 
                (pm.avg_pts_allowed_to_position * 0.3)
        END AS blended_pts_projection,
        
        -- Rebound prediction features
        CASE
            WHEN pvo.sample_confidence >= 4 THEN
                (pf.last5_avg_reb * 0.4) + 
                (pvo.avg_reb_vs_opponent * 0.4) + 
                (pm.avg_reb_allowed_to_position * 0.2)
            WHEN pvo.sample_confidence >= 2 THEN
                (pf.last5_avg_reb * 0.6) + 
                (pvo.avg_reb_vs_opponent * 0.2) + 
                (pm.avg_reb_allowed_to_position * 0.2)
            ELSE
                (pf.last5_avg_reb * 0.7) + 
                (pm.avg_reb_allowed_to_position * 0.3)
        END AS blended_reb_projection,
        
        -- Assist prediction features
        CASE
            WHEN pvo.sample_confidence >= 4 THEN
                (pf.last5_avg_ast * 0.4) + 
                (pvo.avg_ast_vs_opponent * 0.4) + 
                (pm.avg_ast_allowed_to_position * 0.2)
            WHEN pvo.sample_confidence >= 2 THEN
                (pf.last5_avg_ast * 0.6) + 
                (pvo.avg_ast_vs_opponent * 0.2) + 
                (pm.avg_ast_allowed_to_position * 0.2)
            ELSE
                (pf.last5_avg_ast * 0.7) + 
                (pm.avg_ast_allowed_to_position * 0.3)
        END AS blended_ast_projection,
        
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM base_matchups bm
    LEFT JOIN {{ ref('int__opponent_pregame_profile') }} opp
        ON bm.game_id = opp.game_id 
        AND bm.team_id = opp.team_id
    LEFT JOIN {{ ref('int__player_position_matchup') }} pm
        ON bm.player_game_key = pm.player_game_key
    LEFT JOIN {{ ref('int__player_vs_opponent_history') }} pvo
        ON bm.player_id = pvo.player_id 
        AND bm.opponent_id = pvo.opponent_id
    LEFT JOIN player_form pf
        ON bm.player_id = pf.player_id
        AND bm.game_date > pf.game_date
    LEFT JOIN team_context tc
        ON bm.team_id = tc.team_id
        AND bm.game_date = tc.game_date
)

SELECT * FROM final