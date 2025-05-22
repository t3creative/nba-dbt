{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['player_id', 'team_id', 'season_year'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['team_id', 'game_date']}
    ],
    tags=['team', 'player', 'features']
) }}

WITH game_context_attributes AS (
    -- Select pre-game context attributes for each player-game.
    -- The source model 'feat_player__game_context_attributes' is assumed to provide
    -- data that is available *before* the game date.
    SELECT *
    FROM {{ ref('feat_player__game_context_attributes') }}
    {% if is_incremental() %}
    -- Filter for recent data in incremental runs.
    -- The 14-day window ensures we capture recent games for rolling averages
    -- and any potential backfills or late-arriving data.
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}
),

feat_player__rolling_stats AS (
    -- Calculate player's recent performance statistics using window functions.
    -- These stats are based on games *prior* to the current game_date.
    SELECT
        gca.player_game_key,
        gca.player_id,
        gca.team_id,
        gca.game_date,
        gca.game_id,
        gca.season_year,
        gca.home_away,
        gca.position,

        -- Last 5 game averages for player (excluding the current game)
        AVG(gca.pts) OVER(
            PARTITION BY gca.player_id
            ORDER BY gca.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS player_l5_pts,

        AVG(gca.reb) OVER(
            PARTITION BY gca.player_id
            ORDER BY gca.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS player_l5_reb,

        AVG(gca.ast) OVER(
            PARTITION BY gca.player_id
            ORDER BY gca.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS player_l5_ast,

        AVG(gca.usage_pct) OVER(
            PARTITION BY gca.player_id
            ORDER BY gca.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS player_l5_usage,

        AVG(gca.ts_pct) OVER(
            PARTITION BY gca.player_id
            ORDER BY gca.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS player_l5_ts_pct,

        -- Calculate player performance averages in specific team contexts within the season.
        -- These are season-long averages conditional on team context.
        AVG(CASE WHEN gca.team_form_lagged IN ('HOT', 'VERY_HOT') THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_team_hot_streaks,

        AVG(CASE WHEN gca.team_form_lagged IN ('COLD', 'VERY_COLD') THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_team_cold_streaks,

        AVG(CASE WHEN gca.team_offensive_structure_lagged = 'STAR_DOMINANT' THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_star_dominant_system,

        AVG(CASE WHEN gca.team_offensive_structure_lagged = 'BALANCED' THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_balanced_system

    FROM game_context_attributes gca
),

-- Combine rolling stats with current game context attributes and calculate multipliers.
team_context_impact AS (
    SELECT
        prs.*, -- Include all columns from rolling stats
        gca.player_name,
        -- Redundant season_year, home_away, position are already in prs.*, but keeping for clarity/explicit selection
        -- gca.season_year,
        -- gca.home_away,
        -- gca.position,

        -- Team metrics for the current game context (using lagged fields from gca and aliasing them back)
        gca.team_l5_pace_lagged AS team_l5_pace,
        gca.team_l5_off_rating_lagged AS team_l5_off_rating,
        gca.team_form_lagged AS team_form,
        gca.team_playstyle_lagged AS team_playstyle,
        gca.team_offensive_structure_lagged AS team_offensive_structure,

        -- Player-team relationship attributes for the current game context
        gca.player_offensive_role,
        gca.player_team_style_fit_score,
        gca.pace_impact_on_player,
        gca.usage_opportunity,
        gca.team_form_player_impact,
        gca.team_playstyle_stat_impact,

        -- Removed gca.pts, gca.reb, gca.ast, gca.usage_pct as they represent
        -- actual post-game stats, which are not appropriate for pre-game features/projections.

        -- Calculate team context multipliers based on current game context attributes
        CASE
            -- How pace affects scoring
            WHEN gca.pace_impact_on_player = 'HIGH_PACE_BOOST' THEN 1.1
            WHEN gca.pace_impact_on_player = 'LOW_PACE_LIMITATION' THEN 0.9
            ELSE 1.0
        END AS pace_multiplier,

        -- How usage opportunity affects scoring
        CASE
            WHEN gca.usage_opportunity = 'HIGH_USAGE_OPPORTUNITY' THEN 1.15
            WHEN gca.usage_opportunity = 'LIMITED_USAGE_OPPORTUNITY' THEN 0.85
            ELSE 1.0
        END AS usage_multiplier,

        -- How team form affects player role
        CASE
            WHEN gca.team_form_player_impact = 'POSITIVE_TEAM_ENVIRONMENT' THEN 1.05
            WHEN gca.team_form_player_impact = 'HIGHER_RESPONSIBILITY' THEN 1.1
            ELSE 1.0
        END AS team_form_multiplier,

        -- Primary stat boosted by team playstyle, considering player position
        CASE
            WHEN gca.team_playstyle_stat_impact = 'TRANSITION_STATS' AND gca.position IN ('G', 'F') THEN 1.1
            WHEN gca.team_playstyle_stat_impact = '3PT_SHOOTING' AND gca.position IN ('G', 'F') THEN 1.1
            WHEN gca.team_playstyle_stat_impact = 'INSIDE_SCORING_REBOUNDING' AND gca.position = 'C' THEN 1.15
            WHEN gca.team_playstyle_stat_impact = 'ASSIST_OPPORTUNITIES' AND gca.position = 'G' THEN 1.15
            ELSE 1.0
        END AS playstyle_stat_multiplier,

        -- Style fit impact based on score
        CASE
            WHEN gca.player_team_style_fit_score >= 4 THEN 1.1
            WHEN gca.player_team_style_fit_score <= 2 THEN 0.9
            ELSE 1.0
        END AS style_fit_multiplier

    FROM feat_player__rolling_stats prs
    JOIN game_context_attributes gca
        ON prs.player_game_key = gca.player_game_key
)

-- Final selection of features and calculated projections
SELECT
    tci.player_game_key,
    tci.player_id,
    tci.player_name,
    tci.team_id,
    tci.game_id,
    tci.game_date,
    tci.season_year,
    tci.home_away,
    tci.position,

    -- Player recent stats (rolling averages)
    COALESCE(tci.player_l5_pts, 0) AS player_l5_pts,
    COALESCE(tci.player_l5_reb, 0) AS player_l5_reb,
    COALESCE(tci.player_l5_ast, 0) AS player_l5_ast,
    COALESCE(tci.player_l5_usage, 0) AS player_l5_usage,
    COALESCE(tci.player_l5_ts_pct, 0) AS player_l5_ts_pct, -- Added missing L5 TS%

    -- Team context attributes for the game (now correctly referencing aliased lagged fields)
    tci.team_l5_pace,
    tci.team_l5_off_rating,
    tci.team_form,
    tci.team_playstyle,
    tci.team_offensive_structure,

    -- Player-team relationship attributes
    tci.player_offensive_role,
    tci.player_team_style_fit_score,
    tci.pace_impact_on_player,
    tci.usage_opportunity,
    tci.team_form_player_impact,
    tci.team_playstyle_stat_impact,

    -- Team context comparative stats (season averages conditional on context)
    COALESCE(tci.pts_in_team_hot_streaks, 0) AS pts_in_team_hot_streaks,
    COALESCE(tci.pts_in_team_cold_streaks, 0) AS pts_in_team_cold_streaks,
    COALESCE(tci.pts_in_star_dominant_system, 0) AS pts_in_star_dominant_system,
    COALESCE(tci.pts_in_balanced_system, 0) AS pts_in_balanced_system,

    -- Multipliers derived from team context
    tci.pace_multiplier,
    tci.usage_multiplier,
    tci.team_form_multiplier,
    tci.playstyle_stat_multiplier,
    tci.style_fit_multiplier,

    -- Team-adjusted projections based on recent performance and context multipliers
    ROUND(
        COALESCE(tci.player_l5_pts, 0) *
        tci.pace_multiplier *
        tci.usage_multiplier *
        tci.team_form_multiplier *
        tci.playstyle_stat_multiplier *
        tci.style_fit_multiplier,
        1
    ) AS team_adjusted_pts_projection,

    -- Reb and Ast projections use a simplified set of multipliers
    ROUND(
        COALESCE(tci.player_l5_reb, 0) *
        (CASE WHEN tci.team_playstyle_stat_impact = 'INSIDE_SCORING_REBOUNDING' THEN 1.1 ELSE 1.0 END) *
        tci.style_fit_multiplier,
        1
    ) AS team_adjusted_reb_projection,

    ROUND(
        COALESCE(tci.player_l5_ast, 0) *
        (CASE WHEN tci.team_playstyle_stat_impact = 'ASSIST_OPPORTUNITIES' THEN 1.1 ELSE 1.0 END) *
        tci.style_fit_multiplier,
        1
    ) AS team_adjusted_ast_projection,

    -- Indicator for overall team context impact based on the combined multipliers for points
    CASE
        WHEN (tci.pace_multiplier * tci.usage_multiplier * tci.team_form_multiplier *
              tci.playstyle_stat_multiplier * tci.style_fit_multiplier) > 1.2 THEN 'VERY_POSITIVE'
        WHEN (tci.pace_multiplier * tci.usage_multiplier * tci.team_form_multiplier *
              tci.playstyle_stat_multiplier * tci.style_fit_multiplier) > 1.1 THEN 'POSITIVE'
        WHEN (tci.pace_multiplier * tci.usage_multiplier * tci.team_form_multiplier *
              tci.playstyle_stat_multiplier * tci.style_fit_multiplier) < 0.9 THEN 'NEGATIVE'
        WHEN (tci.pace_multiplier * tci.usage_multiplier * tci.team_form_multiplier *
              tci.playstyle_stat_multiplier * tci.style_fit_multiplier) < 0.8 THEN 'VERY_NEGATIVE'
        ELSE 'NEUTRAL'
    END AS team_context_impact,

    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM team_context_impact tci