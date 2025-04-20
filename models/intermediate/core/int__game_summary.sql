{{ config(
    schema='intermediate',
    materialized='incremental',
    unique_key='game_id',
    on_schema_change='sync_all_columns',
    partition_by={
        "field": "game_date_est",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['game_date_est', 'home_team_id', 'visitor_team_id'],
    indexes=[
        {'columns': ['game_id']},
        {'columns': ['game_date_est']},
        {'columns': ['home_team_id']},
        {'columns': ['visitor_team_id']}
    ]
) }}

WITH game_summary AS (
    SELECT *
    FROM {{ ref('stg__game_summary') }}
    {% if is_incremental() %}
        WHERE game_date_est > (SELECT MAX(game_date_est) FROM {{ this }})
    {% endif %}
),

game_info AS (
    SELECT
        game_id,
        game_date,
        attendance,
        game_time
    FROM {{ ref('stg__game_info') }}
),

last_meeting AS (
    SELECT
        game_id,
        last_game_id,
        last_game_date_est,
        last_game_home_team_id,
        last_game_home_team_city,
        last_game_home_team_name,
        last_game_home_team_abbreviation,
        last_game_home_team_points,
        last_game_visitor_team_id,
        last_game_visitor_team_city,
        last_game_visitor_team_name,
        last_game_visitor_team_abbreviation,
        last_game_visitor_team_points
    FROM {{ ref('stg__last_meeting') }}
),

season_series AS (
    SELECT
        game_id,
        home_team_wins,
        home_team_losses,
        series_leader
    FROM {{ ref('stg__season_series') }}
),

line_score AS (
    SELECT
        game_id,
        team_id,
        team_abbreviation,
        team_city_name,
        team_nickname,
        team_wins_losses,
        pts_qtr1,
        pts_qtr2,
        pts_qtr3,
        pts_qtr4,
        pts_ot1,
        pts_ot2,
        pts_ot3,
        pts_ot4,
        pts_ot5,
        pts_ot6,
        pts_ot7,
        pts_ot8,
        pts_ot9,
        pts_ot10,
        pts
    FROM {{ ref('stg__line_score') }}
),

other_stats AS (
    SELECT
        game_id,
        team_id,
        league_id, -- Consider if this is needed or constant
        pts_paint,
        pts_2nd_chance,
        pts_fb,
        largest_lead,
        lead_changes,
        times_tied,
        team_turnovers, -- Note potential redundancy
        total_turnovers,
        team_rebounds,
        pts_off_to
    FROM {{ ref('stg__other_stats') }}
),

inactive_players_agg AS (
    SELECT
        game_id,
        team_id,
        ARRAY_AGG(player_id ORDER BY last_name, first_name) AS inactive_player_ids,
        ARRAY_AGG(first_name || ' ' || last_name ORDER BY last_name, first_name) AS inactive_player_names
    FROM {{ ref('stg__inactive_players') }}
    GROUP BY
        game_id,
        team_id
),

officials_agg AS (
    SELECT
        game_id,
        ARRAY_AGG(official_id ORDER BY last_name, first_name) AS official_ids,
        ARRAY_AGG(first_name || ' ' || last_name ORDER BY last_name, first_name) AS official_names
    FROM {{ ref('stg__game_officials') }}
    GROUP BY
        game_id
),

final AS (
    SELECT
        -- Core Game Summary Info
        gs.game_id,
        gs.game_date_est,
        gs.game_sequence,
        gs.game_status_id,
        gs.game_status_text,
        gs.gamecode,
        gs.home_team_id,
        gs.visitor_team_id,
        gs.season,
        gs.live_period,
        gs.live_pc_time,
        gs.natl_tv_broadcaster_abbreviation,
        gs.live_period_time_bcast,
        gs.wh_status,

        -- Game Info
        gi.game_date, -- Actual date, potentially slightly different from EST
        gi.attendance,
        gi.game_time,

        -- Officials
        off.official_ids,
        off.official_names,

        -- Last Meeting
        lm.last_game_id,
        lm.last_game_date_est,
        lm.last_game_home_team_id,
        lm.last_game_home_team_points,
        lm.last_game_visitor_team_id,
        lm.last_game_visitor_team_points,

        -- Season Series
        ss.home_team_wins AS series_home_wins, -- Renamed for clarity
        ss.home_team_losses AS series_home_losses, -- Renamed for clarity
        ss.series_leader,

        -- Home Team Line Score
        ls_home.team_abbreviation AS home_team_abbreviation,
        ls_home.team_city_name AS home_team_city_name,
        ls_home.team_nickname AS home_team_nickname,
        ls_home.team_wins_losses AS home_team_wins_losses,
        ls_home.pts_qtr1 AS home_pts_qtr1,
        ls_home.pts_qtr2 AS home_pts_qtr2,
        ls_home.pts_qtr3 AS home_pts_qtr3,
        ls_home.pts_qtr4 AS home_pts_qtr4,
        ls_home.pts_ot1 AS home_pts_ot1,
        ls_home.pts_ot2 AS home_pts_ot2,
        ls_home.pts_ot3 AS home_pts_ot3,
        ls_home.pts_ot4 AS home_pts_ot4,
        ls_home.pts_ot5 AS home_pts_ot5,
        ls_home.pts_ot6 AS home_pts_ot6,
        ls_home.pts_ot7 AS home_pts_ot7,
        ls_home.pts_ot8 AS home_pts_ot8,
        ls_home.pts_ot9 AS home_pts_ot9,
        ls_home.pts_ot10 AS home_pts_ot10,
        ls_home.pts AS home_pts,

        -- Visitor Team Line Score
        ls_away.team_abbreviation AS away_team_abbreviation,
        ls_away.team_city_name AS away_team_city_name,
        ls_away.team_nickname AS away_team_nickname,
        ls_away.team_wins_losses AS away_team_wins_losses,
        ls_away.pts_qtr1 AS away_pts_qtr1,
        ls_away.pts_qtr2 AS away_pts_qtr2,
        ls_away.pts_qtr3 AS away_pts_qtr3,
        ls_away.pts_qtr4 AS away_pts_qtr4,
        ls_away.pts_ot1 AS away_pts_ot1,
        ls_away.pts_ot2 AS away_pts_ot2,
        ls_away.pts_ot3 AS away_pts_ot3,
        ls_away.pts_ot4 AS away_pts_ot4,
        ls_away.pts_ot5 AS away_pts_ot5,
        ls_away.pts_ot6 AS away_pts_ot6,
        ls_away.pts_ot7 AS away_pts_ot7,
        ls_away.pts_ot8 AS away_pts_ot8,
        ls_away.pts_ot9 AS away_pts_ot9,
        ls_away.pts_ot10 AS away_pts_ot10,
        ls_away.pts AS away_pts,

        -- Home Team Other Stats
        os_home.pts_paint AS home_pts_paint,
        os_home.pts_2nd_chance AS home_pts_2nd_chance,
        os_home.pts_fb AS home_pts_fb,
        os_home.largest_lead AS home_largest_lead,
        os_home.lead_changes AS home_lead_changes,
        os_home.times_tied AS home_times_tied,
        os_home.team_turnovers AS home_team_turnovers,
        os_home.total_turnovers AS home_total_turnovers,
        os_home.team_rebounds AS home_team_rebounds,
        os_home.pts_off_to AS home_pts_off_to,

        -- Visitor Team Other Stats
        os_away.pts_paint AS away_pts_paint,
        os_away.pts_2nd_chance AS away_pts_2nd_chance,
        os_away.pts_fb AS away_pts_fb,
        os_away.largest_lead AS away_largest_lead,
        os_away.lead_changes AS away_lead_changes,
        os_away.times_tied AS away_times_tied,
        os_away.team_turnovers AS away_team_turnovers,
        os_away.total_turnovers AS away_total_turnovers,
        os_away.team_rebounds AS away_team_rebounds,
        os_away.pts_off_to AS away_pts_off_to,

        -- Inactive Players
        ip_home.inactive_player_ids AS home_inactive_player_ids,
        ip_home.inactive_player_names AS home_inactive_player_names,
        ip_away.inactive_player_ids AS away_inactive_player_ids,
        ip_away.inactive_player_names AS away_inactive_player_names

    FROM game_summary gs
    LEFT JOIN game_info gi ON gs.game_id = gi.game_id
    LEFT JOIN last_meeting lm ON gs.game_id = lm.game_id
    LEFT JOIN season_series ss ON gs.game_id = ss.game_id
    LEFT JOIN officials_agg off ON gs.game_id = off.game_id
    LEFT JOIN line_score ls_home ON gs.game_id = ls_home.game_id AND gs.home_team_id = ls_home.team_id
    LEFT JOIN line_score ls_away ON gs.game_id = ls_away.game_id AND gs.visitor_team_id = ls_away.team_id
    LEFT JOIN other_stats os_home ON gs.game_id = os_home.game_id AND gs.home_team_id = os_home.team_id
    LEFT JOIN other_stats os_away ON gs.game_id = os_away.game_id AND gs.visitor_team_id = os_away.team_id
    LEFT JOIN inactive_players_agg ip_home ON gs.game_id = ip_home.game_id AND gs.home_team_id = ip_home.team_id
    LEFT JOIN inactive_players_agg ip_away ON gs.game_id = ip_away.game_id AND gs.visitor_team_id = ip_away.team_id
    WHERE gs.season >= '2017-18'
)

SELECT *
FROM final 