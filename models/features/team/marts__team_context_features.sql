{{ config(
    schema='marts',
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
    tags=['marts', 'team', 'features', 'prediction']
) }}

WITH player_team_context AS (
    SELECT *
    FROM {{ ref('feat_player__contextual_projections') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '14 days' FROM {{ this }}) -- Changed DATE_SUB
    {% endif %}
),

team_performance AS (
    SELECT *
    FROM {{ ref('feat_team__performance_metrics') }}
),

team_rolling_base AS ( -- This CTE simply references your rolling metrics model
    SELECT *
    FROM {{ ref('feat_team__rolling_stats') }}
),

season_year_percentiles AS ( -- Calculates percentiles per season_year
    SELECT
        season_year,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p80,
        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p60,
        PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p40,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY team_l5_pace) AS pace_p20,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p80,
        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p60,
        PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p40,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY team_l5_off_rating) AS off_rating_p20
    FROM team_rolling_base -- Uses the output of feat_team__rolling_stats
    GROUP BY season_year
),

team_rolling_with_percentiles AS ( -- Joins rolling metrics with season-wide percentiles
    SELECT
        tr_base.*, -- All columns from feat_team__rolling_stats
        syp.pace_p80,
        syp.pace_p60,
        syp.pace_p40,
        syp.pace_p20,
        syp.off_rating_p80,
        syp.off_rating_p60,
        syp.off_rating_p40,
        syp.off_rating_p20
    FROM team_rolling_base tr_base
    LEFT JOIN season_year_percentiles syp
        ON tr_base.season_year = syp.season_year
),

team_shot_distribution AS (
    SELECT *
    FROM {{ ref('feat_team__shot_distribution') }}
),

team_usage AS (
    SELECT *
    FROM {{ ref('feat_team__usage_distribution') }}
),

final_features AS (
    SELECT
        -- Key identifiers
        ptc.player_game_key,
        ptc.player_id,
        ptc.player_name,
        ptc.team_id,
        ptc.game_date,
        ptc.season_year,
        ptc.home_away,
        ptc.position,
        
        -- Team performance metrics from enriched team_rolling CTE
        tr.team_l5_off_rating,
        tr.team_l5_def_rating,
        tr.team_l5_pace,
        tr.team_season_off_rating,
        tr.team_season_def_rating,
        tr.team_offense_trend,
        tr.team_defense_trend,
        
        -- Team form and record
        tp.last5_wins,
        tp.last10_wins,
        tp.team_strength_tier,
        tp.team_form,
        
        -- Team shooting and style
        tsd.team_l5_3pt_att_rate,
        tsd.team_l5_paint_scoring_rate,
        tsd.team_l5_fastbreak_rate,
        tsd.team_l5_assisted_rate,
        tsd.team_playstyle,
        
        -- Team usage patterns
        tu.team_l5_max_usage,
        tu.team_l5_top3_usage,
        tu.team_l5_pts_distribution,
        tu.team_offensive_structure,
        
        -- Player-team relationship
        ptc.player_offensive_role,
        ptc.player_team_style_fit_score,
        ptc.pace_impact_on_player,
        ptc.usage_opportunity,
        ptc.team_form_player_impact,
        ptc.team_playstyle_stat_impact,
        
        -- Team context comparative stats
        ptc.pts_in_team_hot_streaks,
        ptc.pts_in_team_cold_streaks,
        ptc.pts_in_star_dominant_system,
        ptc.pts_in_balanced_system,
        
        -- Team impact multipliers
        ptc.pace_multiplier,
        ptc.usage_multiplier,
        ptc.team_form_multiplier,
        ptc.playstyle_stat_multiplier,
        ptc.style_fit_multiplier,
        
        -- Team-adjusted projections
        ptc.team_adjusted_pts_projection,
        ptc.team_adjusted_reb_projection,
        ptc.team_adjusted_ast_projection,
        ptc.team_context_impact,
        
        -- Derived features using pre-calculated percentiles
        CASE
            WHEN tr.team_l5_pace >= tr.pace_p80 THEN 5
            WHEN tr.team_l5_pace >= tr.pace_p60 THEN 4
            WHEN tr.team_l5_pace >= tr.pace_p40 THEN 3
            WHEN tr.team_l5_pace >= tr.pace_p20 THEN 2
            ELSE 1
        END AS pace_quintile,
        
        CASE
            WHEN tr.team_l5_off_rating >= tr.off_rating_p80 THEN 5
            WHEN tr.team_l5_off_rating >= tr.off_rating_p60 THEN 4
            WHEN tr.team_l5_off_rating >= tr.off_rating_p40 THEN 3
            WHEN tr.team_l5_off_rating >= tr.off_rating_p20 THEN 2
            ELSE 1
        END AS offense_quintile,
        
        -- Relative form impact (how much team form boosts player stats)
        CASE 
            WHEN ptc.pts_in_team_hot_streaks > 0 AND ptc.pts_in_team_cold_streaks > 0
            THEN ptc.pts_in_team_hot_streaks / NULLIF(ptc.pts_in_team_cold_streaks, 0)
            ELSE 1
        END AS team_form_pts_impact_ratio,
        
        -- Boolean flags for categorical features
        (ptc.team_playstyle = 'THREE_POINT_HEAVY')::INT AS is_three_point_heavy_team,
        (ptc.team_playstyle = 'PAINT_FOCUSED')::INT AS is_paint_focused_team,
        (ptc.team_playstyle = 'FAST_PACED')::INT AS is_fast_paced_team,
        (ptc.team_playstyle = 'BALL_MOVEMENT')::INT AS is_ball_movement_team,
        
        (ptc.team_offensive_structure = 'STAR_DOMINANT')::INT AS is_star_dominant_team,
        (ptc.team_offensive_structure = 'TOP_HEAVY')::INT AS is_top_heavy_team,
        (ptc.team_offensive_structure = 'BALANCED')::INT AS is_balanced_team,
        
        (ptc.player_offensive_role = 'PRIMARY_OPTION')::INT AS is_primary_option,
        (ptc.player_offensive_role = 'SECONDARY_OPTION')::INT AS is_secondary_option,
        (ptc.player_offensive_role = 'SUPPORTING_ROLE')::INT AS is_supporting_role
        
    FROM player_team_context ptc
    LEFT JOIN team_performance tp
        ON ptc.team_id = tp.team_id AND ptc.game_date = tp.game_date
    LEFT JOIN team_rolling_with_percentiles tr -- This join remains the same
        ON ptc.team_id = tr.team_id AND ptc.game_date = tr.game_date -- Adjust join key if necessary
    LEFT JOIN team_shot_distribution tsd
        ON ptc.team_id = tsd.team_id AND ptc.game_date = tsd.game_date
    LEFT JOIN team_usage tu
        ON ptc.team_id = tu.team_id AND ptc.game_date = tu.game_date
)

SELECT
    ff.*,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM final_features ff