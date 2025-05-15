{{ config(
    materialized='incremental',
    unique_key='game_id',
    tags=['features', 'game_context', 'pace'],
    depends_on=['int_team__combined_boxscore', 'int__game_summary']
) }}

WITH game_teams AS (
    SELECT
        game_id,
        ARRAY_AGG(DISTINCT team_id) AS team_ids
    FROM {{ ref('int_team__combined_boxscore') }}
    GROUP BY game_id
),

team_stats AS (
    SELECT
        tb.game_id,
        tb.team_id,
        tb.fgm,
        tb.fga,
        tb.pace,
        tb.possessions,
        tb.pts,
        tb.ast,
        tb.reb,
        tb.off_reb,
        tb.def_reb,
        tb.pct_pts_fastbreak,
        tb.pct_pts_in_paint,
        tb.pts_in_paint,
        tb.fastbreak_pts,
        tb.second_chance_pts,
        tb.fg3a,
        tb.def_rating,
        tb.off_rating
    FROM {{ ref('int_team__combined_boxscore') }} tb
),

season_avgs AS (
    SELECT
        season_year,
        AVG(pace) AS avg_season_pace,
        AVG(possessions) AS avg_season_possessions,
        AVG(pts) AS avg_season_pts,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pace) AS p25_pace,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pace) AS p75_pace
    FROM {{ ref('int_team__combined_boxscore') }}
    GROUP BY season_year
),

game_pace_calcs AS (
    SELECT
        t1.game_id,
        t1.team_id,
        t2.team_id AS opponent_id,
        t1.pace AS team_pace,
        t2.pace AS opponent_pace,
        (t1.pace + t2.pace) / 2 AS game_pace,
        t1.possessions AS team_possessions,
        t2.possessions AS opponent_possessions,
        (t1.possessions + t2.possessions) / 2 AS game_possessions,
        s.avg_season_pace,
        s.avg_season_possessions,
        s.p25_pace,
        s.p75_pace,
        -- Game pace classifications
        CASE
            WHEN (t1.pace + t2.pace) / 2 > s.p75_pace THEN 'high_pace'
            WHEN (t1.pace + t2.pace) / 2 < s.p25_pace THEN 'low_pace'
            ELSE 'medium_pace'
        END AS pace_category,
        -- Team style classifications
        CASE 
            WHEN t1.pct_pts_fastbreak > 0.15 THEN 'transition_heavy'
            ELSE 'halfcourt_focused'
        END AS team_play_style,
        CASE 
            WHEN t2.pct_pts_fastbreak > 0.15 THEN 'transition_heavy'
            ELSE 'halfcourt_focused'
        END AS opponent_play_style,
        CASE 
            WHEN t1.pct_pts_in_paint > 0.45 THEN 'inside_oriented'
            WHEN t1.fg3a::FLOAT / NULLIF(t1.fga, 0) > 0.4 THEN 'perimeter_oriented'
            ELSE 'balanced'
        END AS team_shot_distribution,
        CASE 
            WHEN t2.pct_pts_in_paint > 0.45 THEN 'inside_oriented'
            WHEN t2.fg3a::FLOAT / NULLIF(t2.fga, 0) > 0.4 THEN 'perimeter_oriented'
            ELSE 'balanced'
        END AS opponent_shot_distribution,
        -- Offensive and defensive style ratings
        t1.off_rating - s.avg_season_pts AS team_off_rating_rel_to_avg,
        t2.def_rating - s.avg_season_pts AS opponent_def_rating_rel_to_avg,
        -- Second-chance opportunity metrics
        t1.off_reb::FLOAT / NULLIF(t1.fga - t1.fgm, 0) AS team_off_reb_opportunity_rate,
        t2.def_reb::FLOAT / NULLIF(t2.fga - t2.fgm, 0) AS opponent_def_reb_rate_vs_team,
        -- Points in the paint differential
        t1.pts_in_paint - t2.pts_in_paint AS paint_points_differential,
        -- Fastbreak points differential
        t1.fastbreak_pts - t2.fastbreak_pts AS fastbreak_points_differential
    FROM team_stats t1
    JOIN game_teams gt ON t1.game_id = gt.game_id
    JOIN team_stats t2 ON t1.game_id = t2.game_id AND t1.team_id != t2.team_id
    JOIN {{ ref('int_team__combined_boxscore') }} tb ON t1.game_id = tb.game_id AND t1.team_id = tb.team_id
    JOIN season_avgs s ON tb.season_year = s.season_year
)

SELECT
    game_id,
    team_id,
    opponent_id,
    team_pace,
    opponent_pace,
    game_pace,
    team_possessions,
    opponent_possessions,
    game_possessions,
    avg_season_pace,
    avg_season_possessions,
    pace_category,
    team_play_style,
    opponent_play_style,
    team_shot_distribution,
    opponent_shot_distribution,
    team_off_rating_rel_to_avg,
    opponent_def_rating_rel_to_avg,
    team_off_reb_opportunity_rate,
    opponent_def_reb_rate_vs_team,
    paint_points_differential,
    fastbreak_points_differential,
    CURRENT_TIMESTAMP AS updated_at
FROM game_pace_calcs
{% if is_incremental() %}
WHERE game_id NOT IN (SELECT game_id FROM {{ this }})
{% endif %}