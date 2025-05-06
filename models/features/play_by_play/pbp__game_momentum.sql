WITH events AS (
    SELECT * FROM {{ ref('int__player_game_events') }}
),

scoring_runs AS (
    SELECT
        game_id,
        team_id,
        team_code,
        period,
        seconds_elapsed_game,
        points,
        
        -- Track score progression
        SUM(points) OVER (
            PARTITION BY game_id, team_id
            ORDER BY seconds_elapsed_game
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS team_score_progression,
        
        -- Identify runs
        SUM(CASE WHEN points > 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY game_id, team_id
            ORDER BY seconds_elapsed_game
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS recent_scoring_events,
        
        SUM(points) OVER (
            PARTITION BY game_id, team_id
            ORDER BY seconds_elapsed_game
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS recent_points_scored,
        
        -- Track intervals between scores
        seconds_elapsed_game - LAG(seconds_elapsed_game, 1, seconds_elapsed_game) OVER (
            PARTITION BY game_id, team_id
            ORDER BY seconds_elapsed_game
        ) AS time_since_last_score
        
    FROM events
    WHERE points > 0
),

team_momentum AS (
    SELECT
        game_id,
        team_id,
        team_code,
        period,
        
        -- Scoring runs
        MAX(recent_scoring_events) AS max_consecutive_scores,
        MAX(recent_points_scored) AS max_point_run,
        
        -- Scoring patterns
        AVG(time_since_last_score) AS avg_time_between_scores,
        STDDEV(time_since_last_score) AS scoring_consistency,
        
        -- Quarter scoring
        SUM(CASE WHEN period = 1 THEN points ELSE 0 END) AS q1_points,
        SUM(CASE WHEN period = 2 THEN points ELSE 0 END) AS q2_points,
        SUM(CASE WHEN period = 3 THEN points ELSE 0 END) AS q3_points,
        SUM(CASE WHEN period = 4 THEN points ELSE 0 END) AS q4_points,
        SUM(CASE WHEN period > 4 THEN points ELSE 0 END) AS ot_points,
        
        -- Momentum shifts
        SUM(CASE WHEN recent_points_scored >= 6 THEN 1 ELSE 0 END) AS significant_runs,
        
        -- Scoring by game phase
        SUM(CASE WHEN seconds_elapsed_game <= 720 THEN points ELSE 0 END) AS first_quarter_points,
        SUM(CASE WHEN seconds_elapsed_game > 720 AND seconds_elapsed_game <= 1440 THEN points ELSE 0 END) AS second_quarter_points,
        SUM(CASE WHEN seconds_elapsed_game > 1440 AND seconds_elapsed_game <= 2160 THEN points ELSE 0 END) AS third_quarter_points,
        SUM(CASE WHEN seconds_elapsed_game > 2160 THEN points ELSE 0 END) AS fourth_quarter_plus_points
        
    FROM scoring_runs
    GROUP BY game_id, team_id, team_code, period
),

game_momentum AS (
    SELECT
        home.game_id,
        home.team_id AS home_team_id,
        home.team_code AS home_team_code,
        away.team_id AS away_team_id,
        away.team_code AS away_team_code,
        
        -- Team momentum metrics
        home.max_consecutive_scores AS home_max_consecutive_scores,
        away.max_consecutive_scores AS away_max_consecutive_scores,
        home.max_point_run AS home_max_point_run,
        away.max_point_run AS away_max_point_run,
        
        -- Scoring patterns
        home.avg_time_between_scores AS home_avg_time_between_scores,
        away.avg_time_between_scores AS away_avg_time_between_scores,
        home.scoring_consistency AS home_scoring_consistency,
        away.scoring_consistency AS away_scoring_consistency,
        
        -- Score differential by period
        home.q1_points - away.q1_points AS q1_score_diff,
        home.q2_points - away.q2_points AS q2_score_diff,
        home.q3_points - away.q3_points AS q3_score_diff,
        home.q4_points - away.q4_points AS q4_score_diff,
        home.ot_points - away.ot_points AS ot_score_diff,
        
        -- Momentum shift differentials
        home.significant_runs - away.significant_runs AS momentum_shift_diff,
        
        -- Game flow metrics
        CASE 
            WHEN (home.q1_points > away.q1_points AND home.q2_points > away.q2_points AND
                  home.q3_points > away.q3_points AND home.q4_points > away.q4_points) THEN 'HOME_DOMINATE'
            WHEN (away.q1_points > home.q1_points AND away.q2_points > home.q2_points AND
                  away.q3_points > home.q3_points AND away.q4_points > home.q4_points) THEN 'AWAY_DOMINATE'
            WHEN ABS(home.q1_points - away.q1_points) > 10 AND
                 ABS(home.q2_points - away.q2_points) > 10 AND
                 ABS(home.q3_points - away.q3_points) > 10 AND
                 ABS(home.q4_points - away.q4_points) > 10 THEN 'BLOWOUT'
            WHEN ABS(home.q1_points - away.q1_points) < 5 AND
                 ABS(home.q2_points - away.q2_points) < 5 AND
                 ABS(home.q3_points - away.q3_points) < 5 AND
                 ABS(home.q4_points - away.q4_points) < 5 THEN 'TIGHT_CONTEST'
            WHEN (SIGN(home.q1_points - away.q1_points) != SIGN(home.q4_points - away.q4_points)) THEN 'COMEBACK'
            ELSE 'STANDARD_GAME'
        END AS game_flow_pattern
        
    FROM team_momentum home
    JOIN team_momentum away
        ON home.game_id = away.game_id AND home.period = away.period AND home.team_id != away.team_id
    WHERE home.team_id < away.team_id  -- Avoid duplicates by enforcing ordering
)

SELECT * FROM game_momentum