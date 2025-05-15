{{ config(
    materialized='incremental',
    unique_key='game_team_key',
    tags=['features', 'game_context', 'matchups'],
    depends_on=['int_team__combined_boxscore', 'int__game_summary', 'int_opp__game_opponents']
) }}

WITH season_team_stats AS (
    SELECT
        team_id,
        season_year,
        COUNT(*) AS games_played,
        AVG(pts) AS avg_pts,
        AVG(def_rating) AS avg_def_rating,
        AVG(off_rating) AS avg_off_rating,
        AVG(off_reb_pct) AS avg_off_reb_pct,
        AVG(def_reb_pct) AS avg_def_reb_pct,
        AVG(ast) AS avg_ast,
        AVG(tov) AS avg_tov,
        AVG(stl) AS avg_stl,
        AVG(blk) AS avg_blk
    FROM {{ ref('int_team__combined_boxscore') }}
    GROUP BY team_id, season_year
),

season_ranking AS (
    SELECT
        team_id,
        season_year,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_off_rating DESC) AS off_rating_rank,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_def_rating) AS def_rating_rank,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_pts DESC) AS pts_rank,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_off_reb_pct DESC) AS off_reb_rank,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_def_reb_pct DESC) AS def_reb_rank,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_ast DESC) AS ast_rank,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_stl DESC) AS stl_rank,
        RANK() OVER(PARTITION BY season_year ORDER BY avg_blk DESC) AS blk_rank
    FROM season_team_stats
),

previous_matchups AS (
    SELECT
        go.game_id,
        go.team_id,
        go.opponent_id,
        go.season_year,
        tb.pts AS team_pts,
        opp_tb.pts AS opp_pts,
        CASE WHEN tb.pts > opp_tb.pts THEN 1 ELSE 0 END AS won_previous,
        ROW_NUMBER() OVER(PARTITION BY go.team_id, go.opponent_id, go.season_year 
                         ORDER BY go.game_date DESC) AS matchup_num_reverse
    FROM {{ ref('int_opp__game_opponents') }} go
    JOIN {{ ref('int_team__combined_boxscore') }} tb 
        ON go.game_id = tb.game_id AND go.team_id = tb.team_id
    JOIN {{ ref('int_team__combined_boxscore') }} opp_tb 
        ON go.game_id = opp_tb.game_id AND go.opponent_id = opp_tb.team_id
),

prev_matchup_agg AS (
    SELECT
        team_id,
        opponent_id,
        season_year,
        COUNT(*) AS previous_matchups_count,
        SUM(won_previous) AS previous_matchups_won,
        AVG(team_pts) AS avg_pts_vs_opponent,
        AVG(opp_pts) AS avg_pts_allowed_vs_opponent
    FROM previous_matchups
    GROUP BY team_id, opponent_id, season_year
),

game_matchup_context AS (
    SELECT
        go.game_id,
        go.team_id,
        go.opponent_id,
        -- Create a unique key for the game-team combination
        MD5(go.game_id::TEXT || '-' || go.team_id::TEXT) AS game_team_key,
        go.season_year,
        go.home_away,
        -- Team season ranks
        tr.off_rating_rank AS team_off_rank,
        tr.def_rating_rank AS team_def_rank,
        opp_tr.off_rating_rank AS opp_off_rank,
        opp_tr.def_rating_rank AS opp_def_rank,
        -- Rankings differential (positive values = team advantage)
        opp_tr.def_rating_rank - tr.off_rating_rank AS off_vs_def_rank_diff,
        opp_tr.off_rating_rank - tr.def_rating_rank AS def_vs_off_rank_diff,
        -- Previous matchup statistics
        COALESCE(pma.previous_matchups_count, 0) AS previous_matchups_count,
        COALESCE(pma.previous_matchups_won, 0) AS previous_matchups_won,
        CASE 
            WHEN COALESCE(pma.previous_matchups_count, 0) > 0 
            THEN COALESCE(pma.previous_matchups_won, 0)::FLOAT / pma.previous_matchups_count 
            ELSE NULL 
        END AS previous_matchups_win_pct,
        COALESCE(pma.avg_pts_vs_opponent, ts.avg_pts) AS avg_pts_vs_opponent,
        COALESCE(pma.avg_pts_allowed_vs_opponent, opp_ts.avg_pts) AS avg_pts_allowed_vs_opponent,
        -- Defensive/offensive matchup strengths
        CASE
            WHEN tr.off_rating_rank <= 10 AND opp_tr.def_rating_rank >= 20 THEN 'strong_off_vs_weak_def'
            WHEN tr.off_rating_rank >= 20 AND opp_tr.def_rating_rank <= 10 THEN 'weak_off_vs_strong_def'
            WHEN ABS(tr.off_rating_rank - opp_tr.def_rating_rank) <= 5 THEN 'balanced_off_def_matchup'
            ELSE 'mixed_off_def_matchup'
        END AS offensive_matchup_type,
        CASE
            WHEN tr.def_rating_rank <= 10 AND opp_tr.off_rating_rank >= 20 THEN 'strong_def_vs_weak_off'
            WHEN tr.def_rating_rank >= 20 AND opp_tr.off_rating_rank <= 10 THEN 'weak_def_vs_strong_off'
            WHEN ABS(tr.def_rating_rank - opp_tr.off_rating_rank) <= 5 THEN 'balanced_def_off_matchup'
            ELSE 'mixed_def_off_matchup'
        END AS defensive_matchup_type,
        -- Rebounding matchup advantage
        CASE
            WHEN tr.off_reb_rank <= 10 AND opp_tr.def_reb_rank >= 20 THEN 'strong_oreb_advantage'
            WHEN opp_tr.def_reb_rank <= 10 AND tr.off_reb_rank >= 20 THEN 'strong_oreb_disadvantage'
            ELSE 'neutral_reb_matchup'
        END AS offensive_rebounding_matchup,
        -- Last game head-to-head result
        COALESCE(last_game.won_previous, -1) AS won_last_matchup,
        -- Statistical strengths vs opponent weaknesses
        CASE
            WHEN tr.ast_rank <= 10 AND opp_tr.stl_rank >= 20 THEN 'passing_advantage'
            WHEN tr.ast_rank >= 20 AND opp_tr.stl_rank <= 10 THEN 'passing_disadvantage'
            ELSE 'neutral_passing_matchup'
        END AS passing_matchup
    FROM {{ ref('int_opp__game_opponents') }} go
    JOIN season_ranking tr 
        ON go.team_id = tr.team_id AND go.season_year = tr.season_year
    JOIN season_ranking opp_tr 
        ON go.opponent_id = opp_tr.team_id AND go.season_year = opp_tr.season_year
    JOIN season_team_stats ts 
        ON go.team_id = ts.team_id AND go.season_year = ts.season_year
    JOIN season_team_stats opp_ts 
        ON go.opponent_id = opp_ts.team_id AND go.season_year = opp_ts.season_year
    LEFT JOIN prev_matchup_agg pma 
        ON go.team_id = pma.team_id 
        AND go.opponent_id = pma.opponent_id 
        AND go.season_year = pma.season_year
    LEFT JOIN (
        SELECT * FROM previous_matchups WHERE matchup_num_reverse = 1
    ) last_game
        ON go.team_id = last_game.team_id 
        AND go.opponent_id = last_game.opponent_id 
        AND go.season_year = last_game.season_year
)

SELECT
    game_id,
    team_id,
    opponent_id,
    game_team_key,
    season_year,
    home_away,
    team_off_rank,
    team_def_rank,
    opp_off_rank,
    opp_def_rank,
    off_vs_def_rank_diff,
    def_vs_off_rank_diff,
    previous_matchups_count,
    previous_matchups_won,
    previous_matchups_win_pct,
    avg_pts_vs_opponent,
    avg_pts_allowed_vs_opponent,
    offensive_matchup_type,
    defensive_matchup_type,
    offensive_rebounding_matchup,
    won_last_matchup,
    passing_matchup,
    -- Create simple matchup strength score (-10 to +10)
    (
        CASE WHEN off_vs_def_rank_diff < 0 THEN 1 ELSE -1 END * 
        LEAST(ABS(off_vs_def_rank_diff) / 3, 5)
    ) + 
    (
        CASE WHEN def_vs_off_rank_diff < 0 THEN 1 ELSE -1 END * 
        LEAST(ABS(def_vs_off_rank_diff) / 3, 5)
    ) AS matchup_strength_score,
    CURRENT_TIMESTAMP AS updated_at
FROM game_matchup_context
{% if is_incremental() %}
WHERE game_team_key NOT IN (SELECT game_team_key FROM {{ this }})
{% endif %}