/*
MODEL OVERVIEW
--------------------------------------------------
- Entity: team, game, opponent
- Grain: team_game
- Purpose: Calculates situational factors for a team leading up to a game, including season records, streaks, recent form, rest, and derived importance/momentum classifications.

DATA TEMPORALITY & FEATURE TYPE
--------------------------------------------------
- Contains Current Event Data: NO

- Historical Data Context (Details if data from prior events is used):
    - Type: Cumulative, Rolling Window
    - Window Size: 10 games
    - Window Basis: Team's previous games within the season

- Primary Use for ML Prediction: YES

TECHNICAL DETAILS
--------------------------------------------------
- Primary Key(s): team_game_key
- Key Source Models: ref('int_team__combined_boxscore'), ref('feat_opp__game_opponents')
- Last Modified: 2025-05-20
- Modified By: Tyler Tubridy
*/

{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    tags=['features', 'game_context', 'situational'],
    depends_on=['int_team__combined_boxscore', 'feat_opp__game_opponents', 'int_game__summary']
) }}

WITH team_season_games AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        pts,
        opponent_id,
        opponent_pts,
        CASE WHEN pts > opponent_pts THEN 1 ELSE 0 END AS win,
        ROW_NUMBER() OVER(PARTITION BY team_id, season_year ORDER BY game_date) AS game_num_in_season
    FROM (
        SELECT
            tb.team_id,
            tb.season_year,
            tb.game_id,
            tb.game_date,
            tb.pts,
            tb.opponent_id,
            opp_tb.pts AS opponent_pts
        FROM {{ ref('int_team__combined_boxscore') }} tb
        JOIN {{ ref('int_team__combined_boxscore') }} opp_tb
            ON tb.game_id = opp_tb.game_id AND tb.opponent_id = opp_tb.team_id
    ) team_games
),

season_records AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        game_num_in_season,
        SUM(win) OVER(PARTITION BY team_id, season_year
                      ORDER BY game_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS wins_before_game,
        SUM(1-win) OVER(PARTITION BY team_id, season_year
                        ORDER BY game_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS losses_before_game,
        COUNT(*) OVER(PARTITION BY team_id, season_year) AS total_season_games,
        -- Season segments
        CASE
            WHEN game_num_in_season <= 20 THEN 'early_season'
            WHEN game_num_in_season <= 60 THEN 'mid_season'
            ELSE 'late_season'
        END AS season_segment,
        -- Game counts
        LAG(game_date, 1) OVER(PARTITION BY team_id, season_year ORDER BY game_date) AS prev_game_date,
        LEAD(game_date, 1) OVER(PARTITION BY team_id, season_year ORDER BY game_date) AS next_game_date
    FROM team_season_games
),

win_streaks AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        win,
        SUM(new_streak) OVER(PARTITION BY team_id, season_year ORDER BY game_date ROWS UNBOUNDED PRECEDING) AS streak_group
    FROM (
        SELECT
            team_id,
            season_year,
            game_id,
            game_date,
            win,
            CASE
                WHEN win = 1 AND LAG(win, 1, -1) OVER(PARTITION BY team_id, season_year ORDER BY game_date) = 0 THEN 1
                WHEN win = 0 AND LAG(win, 1, -1) OVER(PARTITION BY team_id, season_year ORDER BY game_date) = 1 THEN 1
                WHEN LAG(win, 1, -1) OVER(PARTITION BY team_id, season_year ORDER BY game_date) = -1 THEN 1
                ELSE 0
            END AS new_streak
        FROM team_season_games
    ) streaks
),

current_streaks AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        win,
        streak_group,
        COUNT(*) OVER(PARTITION BY team_id, season_year, streak_group ORDER BY game_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS current_streak_length,
        CASE
            WHEN MIN(win) OVER(PARTITION BY team_id, season_year, streak_group) = 1
            THEN 'win_streak'
            ELSE 'lose_streak'
        END AS streak_type
    FROM win_streaks
),

team_recent_records AS (
    SELECT
        team_id,
        season_year,
        game_id,
        game_date,
        SUM(win) OVER(PARTITION BY team_id, season_year
                     ORDER BY game_date
                     ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS last_10_wins,
        COUNT(*) OVER(PARTITION BY team_id, season_year
                     ORDER BY game_date
                     ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS last_10_games_played
    FROM team_season_games
),

game_situational_factors AS (
    SELECT
        go.game_id,
        go.team_id,
        go.opponent_id,
        -- Create a unique key for the game-team combination
        MD5(go.game_id::TEXT || '-' || go.team_id::TEXT) AS team_game_key,
        go.season_year,
        go.home_away,
        -- Season game counts and records
        sr.game_num_in_season,
        sr.wins_before_game,
        sr.losses_before_game,
        sr.wins_before_game::FLOAT / NULLIF(sr.wins_before_game + sr.losses_before_game, 0) AS win_pct_before_game,
        opp_sr.wins_before_game AS opponent_wins_before_game,
        opp_sr.losses_before_game AS opponent_losses_before_game,
        opp_sr.wins_before_game::FLOAT / NULLIF(opp_sr.wins_before_game + opp_sr.losses_before_game, 0) AS opponent_win_pct_before_game,
        -- Season segments
        sr.season_segment,
        opp_sr.season_segment AS opponent_season_segment,
        -- Back-to-back and rest calculations
        CASE
            WHEN sr.prev_game_date IS NULL THEN NULL
            ELSE go.game_date - sr.prev_game_date
        END AS days_since_last_game,
        CASE
            WHEN sr.next_game_date IS NULL THEN NULL
            ELSE sr.next_game_date - go.game_date
        END AS days_until_next_game,
        CASE
            WHEN (go.game_date - sr.prev_game_date) = 1 THEN TRUE
            ELSE FALSE
        END AS is_back_to_back,
        CASE
            WHEN (go.game_date - sr.prev_game_date) >= 3 THEN TRUE
            ELSE FALSE
        END AS has_rest_advantage,
        CASE
            WHEN (opp_sr.prev_game_date IS NOT NULL) AND
                 (go.game_date - sr.prev_game_date) > (go.game_date - opp_sr.prev_game_date)
            THEN TRUE
            ELSE FALSE
        END AS team_rest_advantage,
        -- Win/lose streaks
        COALESCE(cs.current_streak_length, 0) AS current_streak_length,
        COALESCE(cs.streak_type, 'none') AS streak_type,
        COALESCE(opp_cs.current_streak_length, 0) AS opponent_streak_length,
        COALESCE(opp_cs.streak_type, 'none') AS opponent_streak_type,
        -- Last 10 games records
        COALESCE(trr.last_10_wins, 0) AS last_10_wins,
        COALESCE(trr.last_10_games_played, 0) AS last_10_games_played,
        COALESCE(trr.last_10_wins::FLOAT / NULLIF(trr.last_10_games_played, 0), 0) AS last_10_win_pct,
        COALESCE(opp_trr.last_10_wins, 0) AS opponent_last_10_wins,
        COALESCE(opp_trr.last_10_games_played, 0) AS opponent_last_10_games_played,
        COALESCE(opp_trr.last_10_wins::FLOAT / NULLIF(opp_trr.last_10_games_played, 0), 0) AS opponent_last_10_win_pct
    FROM {{ ref('feat_opp__game_opponents') }} go
    LEFT JOIN season_records sr
        ON go.game_id = sr.game_id AND go.team_id = sr.team_id
    LEFT JOIN season_records opp_sr
        ON go.game_id = opp_sr.game_id AND go.opponent_id = opp_sr.team_id
    LEFT JOIN current_streaks cs
        ON go.game_id = cs.game_id AND go.team_id = cs.team_id
    LEFT JOIN current_streaks opp_cs
        ON go.game_id = opp_cs.game_id AND go.opponent_id = opp_cs.team_id
    LEFT JOIN team_recent_records trr
        ON go.game_id = trr.game_id AND go.team_id = trr.team_id
    LEFT JOIN team_recent_records opp_trr
        ON go.game_id = opp_trr.game_id AND go.opponent_id = opp_trr.team_id
)

SELECT
    game_id,
    team_id,
    opponent_id,
    team_game_key,
    season_year,
    home_away,
    game_num_in_season,
    wins_before_game,
    losses_before_game,
    win_pct_before_game,
    opponent_wins_before_game,
    opponent_losses_before_game,
    opponent_win_pct_before_game,
    season_segment,
    opponent_season_segment,
    days_since_last_game,
    days_until_next_game,
    is_back_to_back,
    has_rest_advantage,
    team_rest_advantage,
    current_streak_length,
    streak_type,
    opponent_streak_length,
    opponent_streak_type,
    last_10_wins,
    last_10_games_played,
    last_10_win_pct,
    opponent_last_10_wins,
    opponent_last_10_games_played,
    opponent_last_10_win_pct,
    -- Form comparison
    CASE
        WHEN last_10_win_pct - opponent_last_10_win_pct > 0.3 THEN 'much_better_form'
        WHEN last_10_win_pct - opponent_last_10_win_pct > 0.1 THEN 'better_form'
        WHEN opponent_last_10_win_pct - last_10_win_pct > 0.3 THEN 'much_worse_form'
        WHEN opponent_last_10_win_pct - last_10_win_pct > 0.1 THEN 'worse_form'
        ELSE 'similar_form'
    END AS form_comparison,
    -- Game importance factors
    CASE
        WHEN season_segment = 'late_season' AND ABS(win_pct_before_game - opponent_win_pct_before_game) < 0.1 THEN 'high_playoff_implications'
        WHEN season_segment = 'early_season' THEN 'low_importance'
        ELSE 'medium_importance'
    END AS game_importance,
    -- Streak significance
    CASE
        WHEN streak_type = 'win_streak' AND current_streak_length >= 5 THEN TRUE
        WHEN streak_type = 'lose_streak' AND current_streak_length >= 5 THEN TRUE
        ELSE FALSE
    END AS is_significant_streak,
    -- Schedule situation classification
    CASE
        WHEN is_back_to_back = TRUE AND team_rest_advantage = FALSE THEN 'schedule_disadvantage'
        WHEN has_rest_advantage = TRUE AND team_rest_advantage = TRUE THEN 'schedule_advantage'
        ELSE 'neutral_schedule'
    END AS schedule_situation,
    -- Momentum classification
    CASE
        WHEN streak_type = 'win_streak' AND current_streak_length >= 3 THEN 'positive_momentum'
        WHEN streak_type = 'lose_streak' AND current_streak_length >= 3 THEN 'negative_momentum'
        WHEN last_10_win_pct >= 0.7 THEN 'positive_momentum'
        WHEN last_10_win_pct <= 0.3 THEN 'negative_momentum'
        ELSE 'neutral_momentum'
    END AS team_momentum,
    CURRENT_TIMESTAMP AS updated_at
FROM game_situational_factors
{% if is_incremental() %}
WHERE team_game_key NOT IN (SELECT team_game_key FROM {{ this }})
{% endif %}