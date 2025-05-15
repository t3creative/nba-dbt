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
        {'columns': ['player_game_key'], 'unique': true},
        {'columns': ['player_id', 'game_date']},
        {'columns': ['team_id', 'game_date']}
    ],
    tags=['team', 'player', 'features']
) }}

WITH game_context_attributes AS (
    SELECT *
    FROM {{ ref('feat_player__game_context_attributes') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}
),

feat_player__rolling_stats AS (
    SELECT
        gca.player_game_key,
        gca.player_id,
        gca.team_id,
        gca.game_date,
        
        -- Last 5 game averages for player
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
        
        -- Calculate player performance in team wins vs losses
        AVG(CASE WHEN gca.team_form IN ('HOT', 'VERY_HOT') THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_team_hot_streaks,
        
        AVG(CASE WHEN gca.team_form IN ('COLD', 'VERY_COLD') THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_team_cold_streaks,
        
        -- Player stats based on offensive structure
        AVG(CASE WHEN gca.team_offensive_structure = 'STAR_DOMINANT' THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_star_dominant_system,
        
        AVG(CASE WHEN gca.team_offensive_structure = 'BALANCED' THEN gca.pts ELSE NULL END) OVER(
            PARTITION BY gca.player_id, gca.season_year
        ) AS pts_in_balanced_system
        
    FROM game_context_attributes gca
),

-- Calculate how player production is affected by team context
team_context_impact AS (
    SELECT
        prs.*,
        gca.player_name,
        gca.season_year,
        gca.home_away,
        gca.position,
        
        -- Team metrics
        gca.team_l5_pace,
        gca.team_l5_off_rating,
        gca.team_form,
        gca.team_playstyle,
        gca.team_offensive_structure,
        
        -- Player-team relationship
        gca.player_offensive_role,
        gca.player_team_style_fit_score,
        gca.pace_impact_on_player,
        gca.usage_opportunity,
        gca.team_form_player_impact,
        gca.team_playstyle_stat_impact,
        
        -- Current player stats
        gca.pts,
        gca.reb,
        gca.ast,
        gca.usage_pct,
        
        -- Calculate team context multipliers
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
        
        -- Primary stat boosted by team playstyle
        CASE
            WHEN gca.team_playstyle_stat_impact = 'TRANSITION_STATS' AND gca.position IN ('G', 'F') THEN 1.1
            WHEN gca.team_playstyle_stat_impact = '3PT_SHOOTING' AND gca.position IN ('G', 'F') THEN 1.1
            WHEN gca.team_playstyle_stat_impact = 'INSIDE_SCORING_REBOUNDING' AND gca.position = 'C' THEN 1.15
            WHEN gca.team_playstyle_stat_impact = 'ASSIST_OPPORTUNITIES' AND gca.position = 'G' THEN 1.15
            ELSE 1.0
        END AS playstyle_stat_multiplier,
        
        -- Style fit impact
        CASE
            WHEN gca.player_team_style_fit_score >= 4 THEN 1.1
            WHEN gca.player_team_style_fit_score <= 2 THEN 0.9
            ELSE 1.0
        END AS style_fit_multiplier
        
    FROM feat_player__rolling_stats prs
    JOIN game_context_attributes gca
        ON prs.player_game_key = gca.player_game_key
)

SELECT
    tci.player_game_key,
    tci.player_id,
    tci.player_name,
    tci.team_id,
    tci.game_date,
    tci.season_year,
    tci.home_away,
    tci.position,
    
    -- Player recent stats
    COALESCE(tci.player_l5_pts, 0) AS player_l5_pts,
    COALESCE(tci.player_l5_reb, 0) AS player_l5_reb,
    COALESCE(tci.player_l5_ast, 0) AS player_l5_ast,
    COALESCE(tci.player_l5_usage, 0) AS player_l5_usage,
    
    -- Team context
    tci.team_l5_pace,
    tci.team_l5_off_rating,
    tci.team_form,
    tci.team_playstyle,
    tci.team_offensive_structure,
    
    -- Player-team relationship
    tci.player_offensive_role,
    tci.player_team_style_fit_score,
    tci.pace_impact_on_player,
    tci.usage_opportunity,
    tci.team_form_player_impact,
    tci.team_playstyle_stat_impact,
    
    -- Team context comparative stats
    COALESCE(tci.pts_in_team_hot_streaks, 0) AS pts_in_team_hot_streaks,
    COALESCE(tci.pts_in_team_cold_streaks, 0) AS pts_in_team_cold_streaks,
    COALESCE(tci.pts_in_star_dominant_system, 0) AS pts_in_star_dominant_system,
    COALESCE(tci.pts_in_balanced_system, 0) AS pts_in_balanced_system,
    
    -- Multipliers
    tci.pace_multiplier,
    tci.usage_multiplier,
    tci.team_form_multiplier,
    tci.playstyle_stat_multiplier,
    tci.style_fit_multiplier,
    
    -- Team-adjusted predictions
    ROUND(
        COALESCE(tci.player_l5_pts, 0) * 
        tci.pace_multiplier * 
        tci.usage_multiplier * 
        tci.team_form_multiplier * 
        tci.playstyle_stat_multiplier *
        tci.style_fit_multiplier,
        1
    ) AS team_adjusted_pts_projection,
    
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
    
    -- Team context impact indicators
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