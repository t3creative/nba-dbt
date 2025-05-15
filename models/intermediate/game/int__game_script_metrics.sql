{{ config(
    materialized='incremental',
    unique_key='game_team_key',
    tags=['features', 'game_context', 'game_script'],
    depends_on=['int_team__combined_boxscore', 'int__game_summary']
) }}

WITH game_summary AS (
    SELECT
        gs.game_id,
        gs.home_team_id,
        gs.away_team_id,
        gs.home_pts,
        gs.away_pts,
        gs.home_pts_qtr1,
        gs.home_pts_qtr2,
        gs.home_pts_qtr3,
        gs.home_pts_qtr4,
        gs.away_pts_qtr1,
        gs.away_pts_qtr2,
        gs.away_pts_qtr3,
        gs.away_pts_qtr4,
        gs.lead_changes,
        gs.times_tied,
        gs.home_largest_lead,
        gs.away_largest_lead,
        CASE WHEN home_pts_ot1 IS NOT NULL OR away_pts_ot1 IS NOT NULL THEN TRUE ELSE FALSE END AS went_to_overtime,
        ABS(gs.home_pts - gs.away_pts) AS final_point_differential
    FROM {{ ref('int__game_summary') }} gs
),

-- Process Home Team
home_team_script AS (
    SELECT
        gs.game_id,
        gs.home_team_id AS team_id,
        MD5(gs.game_id::TEXT || '-' || gs.home_team_id::TEXT) AS game_team_key,
        gs.home_pts > gs.away_pts AS team_won,
        gs.home_pts_qtr1 - gs.away_pts_qtr1 AS q1_point_differential,
        gs.home_pts_qtr2 - gs.away_pts_qtr2 AS q2_point_differential,
        gs.home_pts_qtr3 - gs.away_pts_qtr3 AS q3_point_differential,
        gs.home_pts_qtr4 - gs.away_pts_qtr4 AS q4_point_differential,
        (gs.home_pts_qtr1 + gs.home_pts_qtr2) - (gs.away_pts_qtr1 + gs.away_pts_qtr2) AS halftime_point_differential,
        gs.home_pts - gs.away_pts AS team_point_differential,
        gs.final_point_differential,
        gs.went_to_overtime,
        gs.lead_changes,
        gs.times_tied,
        GREATEST(gs.home_largest_lead, 0) AS team_largest_lead
    FROM game_summary gs
),

-- Process Away Team
away_team_script AS (
    SELECT
        gs.game_id,
        gs.away_team_id AS team_id,
        MD5(gs.game_id::TEXT || '-' || gs.away_team_id::TEXT) AS game_team_key,
        gs.away_pts > gs.home_pts AS team_won,
        gs.away_pts_qtr1 - gs.home_pts_qtr1 AS q1_point_differential,
        gs.away_pts_qtr2 - gs.home_pts_qtr2 AS q2_point_differential,
        gs.away_pts_qtr3 - gs.home_pts_qtr3 AS q3_point_differential,
        gs.away_pts_qtr4 - gs.home_pts_qtr4 AS q4_point_differential,
        (gs.away_pts_qtr1 + gs.away_pts_qtr2) - (gs.home_pts_qtr1 + gs.home_pts_qtr2) AS halftime_point_differential,
        gs.away_pts - gs.home_pts AS team_point_differential,
        gs.final_point_differential,
        gs.went_to_overtime,
        gs.lead_changes,
        gs.times_tied,
        GREATEST(gs.away_largest_lead, 0) AS team_largest_lead
    FROM game_summary gs
),

combined_script AS (
    SELECT * FROM home_team_script
    UNION ALL
    SELECT * FROM away_team_script
),

final AS (
    SELECT
        cs.*,
        -- Game closeness categories
        CASE
            WHEN cs.final_point_differential <= 3 THEN 'one_possession'
            WHEN cs.final_point_differential <= 6 THEN 'two_possession'
            WHEN cs.final_point_differential <= 10 THEN 'close_game'
            WHEN cs.final_point_differential <= 20 THEN 'moderate_game'
            ELSE 'blowout'
        END AS game_closeness,
        -- Comeback metrics
        CASE
            WHEN q1_point_differential < -5 AND team_won = TRUE THEN TRUE
            WHEN halftime_point_differential < -10 AND team_won = TRUE THEN TRUE
            WHEN q3_point_differential < -10 AND team_won = TRUE THEN TRUE
            ELSE FALSE
        END AS comeback_win,
        -- Game flow consistency
        (SELECT stddev_pop(val)
         FROM unnest(ARRAY[
             q1_point_differential::double precision,
             q2_point_differential::double precision,
             q3_point_differential::double precision,
             q4_point_differential::double precision
         ]) AS vals(val)
        ) AS quarter_scoring_volatility
    FROM combined_script cs
)

SELECT
    -- Select columns explicitly from final
    game_id,
    team_id,
    game_team_key,
    team_won,
    q1_point_differential,
    q2_point_differential,
    q3_point_differential,
    q4_point_differential,
    halftime_point_differential,
    team_point_differential,
    final_point_differential,
    went_to_overtime,
    game_closeness,
    lead_changes,
    times_tied,
    team_largest_lead,
    comeback_win,
    quarter_scoring_volatility,
    -- Additional game script classifiers
    CASE
        WHEN team_won = TRUE AND team_point_differential >= 15 THEN 'dominant_win'
        WHEN team_won = TRUE AND went_to_overtime = TRUE THEN 'overtime_win'
        WHEN team_won = TRUE AND team_point_differential < 5 THEN 'close_win'
        WHEN team_won = TRUE THEN 'solid_win'
        WHEN team_won = FALSE AND team_point_differential > -5 THEN 'close_loss'
        WHEN team_won = FALSE AND went_to_overtime = TRUE THEN 'overtime_loss'
        WHEN team_won = FALSE AND team_point_differential <= -15 THEN 'blowout_loss'
        ELSE 'solid_loss'
    END AS game_outcome_type,
    -- Garbage time indicator
    CASE
        WHEN ABS(q4_point_differential) > 15 THEN TRUE
        ELSE FALSE
    END AS potential_garbage_time,
    CURRENT_TIMESTAMP AS updated_at
FROM final -- Use final CTE
{% if is_incremental() %}
WHERE game_team_key NOT IN (SELECT game_team_key FROM {{ this }})
{% endif %}