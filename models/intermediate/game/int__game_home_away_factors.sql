{{ config(
    materialized='incremental',
    unique_key='game_team_key',
    tags=['features', 'game_context', 'home_away'],
    depends_on=['int_team__combined_boxscore', 'int_opp__game_opponents']
) }}

WITH team_home_away_performance AS (
    SELECT
        team_id,
        season_year,
        home_away,
        COUNT(*) AS games_played,
        SUM(CASE WHEN pts > opponent_pts THEN 1 ELSE 0 END) AS wins,
        AVG(pts) AS avg_pts,
        AVG(opponent_pts) AS avg_opponent_pts,
        AVG(pts - opponent_pts) AS avg_point_differential,
        AVG(off_rating) AS avg_off_rating,
        AVG(def_rating) AS avg_def_rating,
        AVG(pace) AS avg_pace
    FROM (
        SELECT
            tb.team_id,
            tb.season_year,
            tb.home_away,
            tb.pts,
            opp_tb.pts AS opponent_pts,
            tb.off_rating,
            tb.def_rating,
            tb.pace
        FROM {{ ref('int_team__combined_boxscore') }} tb
        JOIN {{ ref('int_team__combined_boxscore') }} opp_tb 
            ON tb.game_id = opp_tb.game_id AND tb.team_id != opp_tb.team_id
    ) team_games
    GROUP BY team_id, season_year, home_away
),

team_home_away_differentials AS (
    SELECT
        h.team_id,
        h.season_year,
        h.avg_pts AS home_avg_pts,
        a.avg_pts AS away_avg_pts,
        h.avg_pts - a.avg_pts AS pts_home_away_differential,
        h.avg_opponent_pts AS home_avg_opponent_pts,
        a.avg_opponent_pts AS away_avg_opponent_pts,
        h.avg_opponent_pts - a.avg_opponent_pts AS allowed_pts_home_away_differential,
        h.avg_off_rating AS home_avg_off_rating,
        a.avg_off_rating AS away_avg_off_rating,
        h.avg_off_rating - a.avg_off_rating AS off_rtg_home_away_differential,
        h.avg_def_rating AS home_avg_def_rating,
        a.avg_def_rating AS away_avg_def_rating,
        h.avg_def_rating - a.avg_def_rating AS def_rtg_home_away_differential,
        h.wins::FLOAT / NULLIF(h.games_played, 0) AS home_win_pct,
        a.wins::FLOAT / NULLIF(a.games_played, 0) AS away_win_pct,
        (h.wins::FLOAT / NULLIF(h.games_played, 0)) - 
        (a.wins::FLOAT / NULLIF(a.games_played, 0)) AS win_pct_home_away_differential
    FROM team_home_away_performance h
    JOIN team_home_away_performance a 
        ON h.team_id = a.team_id 
        AND h.season_year = a.season_year 
        AND h.home_away = 'HOME' 
        AND a.home_away = 'AWAY'
),

last_n_games AS (
    SELECT
        tb.team_id,
        tb.game_id,
        tb.game_date,
        tb.home_away,
        tb.pts,
        tb.opponent_id,
        opp_tb.pts AS opponent_pts,
        CASE WHEN tb.pts > opp_tb.pts THEN 1 ELSE 0 END AS won_game,
        ROW_NUMBER() OVER(PARTITION BY tb.team_id, tb.home_away 
                         ORDER BY tb.game_date DESC) AS recency_rank
    FROM {{ ref('int_team__combined_boxscore') }} tb
    JOIN {{ ref('int_team__combined_boxscore') }} opp_tb 
        ON tb.game_id = opp_tb.game_id AND tb.opponent_id = opp_tb.team_id
),

recent_home_away_performance AS (
    SELECT
        team_id,
        home_away,
        COUNT(*) AS recent_games_count,
        SUM(won_game) AS recent_wins,
        AVG(pts) AS recent_avg_pts,
        AVG(opponent_pts) AS recent_avg_opponent_pts,
        AVG(pts - opponent_pts) AS recent_avg_point_differential
    FROM last_n_games
    WHERE recency_rank <= 5  -- Last 5 games in each home/away context
    GROUP BY team_id, home_away
),

game_home_away_factors AS (
    SELECT
        go.game_id,
        go.team_id,
        go.opponent_id,
        -- Create a unique key for the game-team combination
        MD5(go.game_id::TEXT || '-' || go.team_id::TEXT) AS game_team_key,
        go.season_year,
        go.home_away,
        -- Season home/away performance
        CASE 
            WHEN go.home_away = 'HOME' THEN tds.home_avg_pts
            ELSE tds.away_avg_pts
        END AS team_avg_pts_in_context,
        CASE 
            WHEN go.home_away = 'HOME' THEN tds.home_avg_off_rating
            ELSE tds.away_avg_off_rating
        END AS team_avg_off_rtg_in_context,
        CASE 
            WHEN go.home_away = 'HOME' THEN tds.home_avg_def_rating
            ELSE tds.away_avg_def_rating
        END AS team_avg_def_rtg_in_context,
        CASE 
            WHEN go.home_away = 'HOME' THEN tds.home_win_pct
            ELSE tds.away_win_pct
        END AS team_win_pct_in_context,
        -- Opponent home/away context
        CASE 
            WHEN go.home_away = 'HOME' THEN 'AWAY'
            ELSE 'HOME'
        END AS opponent_context,
        CASE 
            WHEN go.home_away = 'HOME' THEN opp_tds.away_avg_pts
            ELSE opp_tds.home_avg_pts
        END AS opponent_avg_pts_in_context,
        CASE 
            WHEN go.home_away = 'HOME' THEN opp_tds.away_avg_off_rating
            ELSE opp_tds.home_avg_off_rating
        END AS opponent_avg_off_rtg_in_context,
        CASE 
            WHEN go.home_away = 'HOME' THEN opp_tds.away_avg_def_rating
            ELSE opp_tds.home_avg_def_rating
        END AS opponent_avg_def_rtg_in_context,
        CASE 
            WHEN go.home_away = 'HOME' THEN opp_tds.away_win_pct
            ELSE opp_tds.home_win_pct
        END AS opponent_win_pct_in_context,
        -- Home/away differentials (how much better/worse team plays at home vs away)
        tds.pts_home_away_differential AS team_pts_home_away_diff,
        opp_tds.pts_home_away_differential AS opponent_pts_home_away_diff,
        tds.off_rtg_home_away_differential AS team_off_rtg_home_away_diff,
        opp_tds.off_rtg_home_away_differential AS opponent_off_rtg_home_away_diff,
        tds.def_rtg_home_away_differential AS team_def_rtg_home_away_diff,
        opp_tds.def_rtg_home_away_differential AS opponent_def_rtg_home_away_diff,
        tds.win_pct_home_away_differential AS team_win_pct_home_away_diff,
        opp_tds.win_pct_home_away_differential AS opponent_win_pct_home_away_diff,
        -- Recent performance in context
        COALESCE(rhap.recent_avg_pts, 
                CASE WHEN go.home_away = 'HOME' THEN tds.home_avg_pts ELSE tds.away_avg_pts END
        ) AS recent_avg_pts_in_context,
        COALESCE(rhap.recent_avg_opponent_pts, 
                CASE WHEN go.home_away = 'HOME' THEN tds.home_avg_opponent_pts ELSE tds.away_avg_opponent_pts END
        ) AS recent_avg_allowed_pts_in_context,
        COALESCE(rhap.recent_wins::FLOAT / NULLIF(rhap.recent_games_count, 0), 
                CASE WHEN go.home_away = 'HOME' THEN tds.home_win_pct ELSE tds.away_win_pct END
        ) AS recent_win_pct_in_context,
        -- Home court advantage factors
        CASE
            WHEN go.home_away = 'HOME' AND tds.win_pct_home_away_differential > 0.15 THEN 'strong_home_team'
            WHEN go.home_away = 'AWAY' AND tds.win_pct_home_away_differential > 0.15 THEN 'facing_strong_home_team'
            WHEN go.home_away = 'HOME' AND tds.win_pct_home_away_differential < 0.05 THEN 'weak_home_team'
            WHEN go.home_away = 'AWAY' AND tds.win_pct_home_away_differential < 0.05 THEN 'facing_weak_home_team'
            ELSE 'average_home_advantage'
        END AS home_advantage_factor
    FROM {{ ref('int_opp__game_opponents') }} go
    LEFT JOIN team_home_away_differentials tds 
        ON go.team_id = tds.team_id AND go.season_year = tds.season_year
    LEFT JOIN team_home_away_differentials opp_tds 
        ON go.opponent_id = opp_tds.team_id AND go.season_year = opp_tds.season_year
    LEFT JOIN recent_home_away_performance rhap 
        ON go.team_id = rhap.team_id AND go.home_away = rhap.home_away
)

SELECT
    game_id,
    team_id,
    opponent_id,
    game_team_key,
    season_year,
    home_away,
    opponent_context,
    team_avg_pts_in_context,
    team_avg_off_rtg_in_context,
    team_avg_def_rtg_in_context,
    team_win_pct_in_context,
    opponent_avg_pts_in_context,
    opponent_avg_off_rtg_in_context,
    opponent_avg_def_rtg_in_context,
    opponent_win_pct_in_context,
    team_pts_home_away_diff,
    opponent_pts_home_away_diff,
    team_off_rtg_home_away_diff,
    opponent_off_rtg_home_away_diff,
    team_def_rtg_home_away_diff,
    opponent_def_rtg_home_away_diff,
    team_win_pct_home_away_diff,
    opponent_win_pct_home_away_diff,
    recent_avg_pts_in_context,
    recent_avg_allowed_pts_in_context,
    recent_win_pct_in_context,
    home_advantage_factor,
    -- Home/road performance classification
    CASE
        WHEN home_away = 'HOME' AND team_win_pct_in_context > 0.65 THEN 'strong_home_performer'
        WHEN home_away = 'AWAY' AND team_win_pct_in_context > 0.55 THEN 'strong_road_performer'
        WHEN home_away = 'HOME' AND team_win_pct_in_context < 0.35 THEN 'weak_home_performer'
        WHEN home_away = 'AWAY' AND team_win_pct_in_context < 0.25 THEN 'weak_road_performer'
        ELSE 'average_performer'
    END AS location_performance_type,
    -- Context advantage metric
    CASE
        WHEN home_away = 'HOME' THEN 
            team_win_pct_home_away_diff - opponent_win_pct_home_away_diff
        ELSE 
            -1 * (team_win_pct_home_away_diff - opponent_win_pct_home_away_diff)
    END AS context_advantage_score,
    CURRENT_TIMESTAMP AS updated_at
FROM game_home_away_factors
{% if is_incremental() %}
WHERE game_team_key NOT IN (SELECT game_team_key FROM {{ this }})
{% endif %}