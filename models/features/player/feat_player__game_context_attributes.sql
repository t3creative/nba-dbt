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
    tags=['features', 'player', 'team', 'interaction']
) }}

WITH player_boxscores AS (
    SELECT
        game_id,
        player_id,
        player_name,
        team_id,
        game_date,
        season_year,
        home_away,
        position,
        min,
        pts,
        reb,
        ast,
        usage_pct,
        ts_pct,
        pct_of_team_pts,
        pct_of_team_reb,
        pct_of_team_ast,
        pct_of_team_fga,
        MD5(CONCAT(game_id, '-', player_id)) AS player_game_key
    FROM {{ ref('int_player__combined_boxscore') }}
    WHERE min >= 5  -- Filter out minimal playing time
    {% if is_incremental() %}
    AND game_date > (SELECT MAX(game_date) - INTERVAL '30 days' FROM {{ this }})
    {% endif %}
),

team_metrics AS (
    SELECT *
    FROM {{ ref('feat_team__rolling_stats') }}
),

team_shot_distribution AS (
    SELECT *
    FROM {{ ref('feat_team__shot_distribution') }}
),

team_usage AS (
    SELECT *
    FROM {{ ref('feat_team__usage_distribution') }}
),

-- Calculate 75th percentile of usage_pct per team and game
player_game_percentiles AS (
    SELECT
        team_id,
        game_id,
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY usage_pct) AS p75_usage_pct
    FROM player_boxscores
    GROUP BY team_id, game_id
),

-- Calculate player's deviation from team averages
player_team_deviations AS (
    SELECT
        pb.player_game_key,
        pb.player_id,
        pb.player_name,
        pb.team_id,
        pb.game_id,
        pb.game_date,
        pb.season_year,
        pb.home_away,
        pb.position,
        pb.min,
        pb.pts,
        pb.reb,
        pb.ast,
        pb.usage_pct,
        pb.ts_pct,
        
        -- Player's share of team production
        pb.pct_of_team_pts,
        pb.pct_of_team_reb,
        pb.pct_of_team_ast,
        pb.pct_of_team_fga,
        
        -- Team metrics
        tm.team_l5_pace,
        tm.team_l5_off_rating,
        tm.team_l5_def_rating,
        tm.team_form,
        tm.team_offense_trend,
        
        -- Team shot distribution
        tsd.team_l5_3pt_att_rate,
        tsd.team_l5_paint_scoring_rate,
        tsd.team_l5_fastbreak_rate,
        tsd.team_l5_assisted_rate,
        tsd.team_playstyle,
        
        -- Team usage 
        tu.team_l5_max_usage,
        tu.team_l5_top3_usage,
        tu.team_offensive_structure,
        
        -- Calculate production vs team averages
        CASE 
            WHEN pb.ts_pct > 
                 AVG(pb.ts_pct) OVER(PARTITION BY pb.team_id, pb.game_id) + 0.05
            THEN 'ABOVE_TEAM_AVG'
            WHEN pb.ts_pct < 
                 AVG(pb.ts_pct) OVER(PARTITION BY pb.team_id, pb.game_id) - 0.05
            THEN 'BELOW_TEAM_AVG'
            ELSE 'NEAR_TEAM_AVG'
        END AS efficiency_vs_team_avg,
        
        -- Identify if player is in top 3 usage
        CASE 
            WHEN pb.usage_pct >= tu.team_l5_max_usage * 0.9
            THEN 'PRIMARY_OPTION'
            WHEN pgp.p75_usage_pct IS NOT NULL AND pb.usage_pct >= pgp.p75_usage_pct
            THEN 'SECONDARY_OPTION'
            ELSE 'SUPPORTING_ROLE'
        END AS player_offensive_role,
        
        -- Player-team style fit score (higher = better fit)
        CASE
            -- Guards in fast-paced teams
            WHEN pb.position = 'G' AND tsd.team_playstyle = 'FAST_PACED' THEN 5
            -- Guards in ball movement teams  
            WHEN pb.position = 'G' AND tsd.team_playstyle = 'BALL_MOVEMENT' THEN 5
            -- Forwards in three-point heavy teams
            WHEN pb.position = 'F' AND tsd.team_playstyle = 'THREE_POINT_HEAVY' THEN 5
            -- Centers in paint-focused teams
            WHEN pb.position = 'C' AND tsd.team_playstyle = 'PAINT_FOCUSED' THEN 5
            -- Other good fits
            WHEN pb.position = 'G' AND tsd.team_playstyle = 'THREE_POINT_HEAVY' THEN 4
            WHEN pb.position = 'F' AND tsd.team_playstyle = 'PAINT_FOCUSED' THEN 4
            -- Neutral fits
            WHEN tsd.team_playstyle = 'BALANCED' THEN 3
            -- Suboptimal fits
            WHEN pb.position = 'C' AND tsd.team_playstyle = 'THREE_POINT_HEAVY' THEN 2
            WHEN pb.position = 'G' AND tsd.team_playstyle = 'PAINT_FOCUSED' THEN 2
            ELSE 3
        END AS player_team_style_fit_score
        
    FROM player_boxscores pb
    LEFT JOIN team_metrics tm
        ON pb.team_id = tm.team_id AND pb.game_id = tm.game_id
    LEFT JOIN team_shot_distribution tsd
        ON pb.team_id = tsd.team_id AND pb.game_id = tsd.game_id
    LEFT JOIN team_usage tu
        ON pb.team_id = tu.team_id AND pb.game_id = tu.game_id
    LEFT JOIN player_game_percentiles pgp
        ON pb.team_id = pgp.team_id AND pb.game_id = pgp.game_id
)

SELECT
    ptd.player_game_key,
    ptd.player_id,
    ptd.player_name,
    ptd.team_id,
    ptd.game_id,
    ptd.game_date,
    ptd.season_year,
    ptd.home_away,
    ptd.position,
    
    -- Player stats
    ptd.min,
    ptd.pts,
    ptd.reb,
    ptd.ast,
    ptd.usage_pct,
    ptd.ts_pct,
    
    -- Player's team share
    ptd.pct_of_team_pts,
    ptd.pct_of_team_reb,
    ptd.pct_of_team_ast,
    ptd.pct_of_team_fga,
    
    -- Team context
    ptd.team_l5_pace,
    ptd.team_l5_off_rating, 
    ptd.team_l5_def_rating,
    ptd.team_form,
    ptd.team_offense_trend,
    ptd.team_playstyle,
    ptd.team_offensive_structure,
    
    -- Player-team relationship metrics
    ptd.efficiency_vs_team_avg,
    ptd.player_offensive_role,
    ptd.player_team_style_fit_score,
    
    -- Team composition impact on player stats
    CASE
        -- Pace impact on stats
        WHEN ptd.team_l5_pace > 102 THEN 'HIGH_PACE_BOOST'
        WHEN ptd.team_l5_pace < 96 THEN 'LOW_PACE_LIMITATION'
        ELSE 'NEUTRAL_PACE_IMPACT'
    END AS pace_impact_on_player,
    
    -- Usage competition impact
    CASE
        WHEN ptd.team_offensive_structure = 'STAR_DOMINANT' AND 
             ptd.player_offensive_role = 'PRIMARY_OPTION' THEN 'HIGH_USAGE_OPPORTUNITY'
        WHEN ptd.team_offensive_structure = 'BALANCED' THEN 'MODERATE_USAGE_OPPORTUNITY'
        WHEN ptd.team_offensive_structure = 'TOP_HEAVY' AND 
             ptd.player_offensive_role = 'SUPPORTING_ROLE' THEN 'LIMITED_USAGE_OPPORTUNITY'
        ELSE 'NORMAL_USAGE_OPPORTUNITY'
    END AS usage_opportunity,
    
    -- Team form impact on role players
    CASE
        WHEN ptd.team_form IN ('HOT', 'VERY_HOT') AND 
             ptd.player_offensive_role = 'SUPPORTING_ROLE' THEN 'POSITIVE_TEAM_ENVIRONMENT'
        WHEN ptd.team_form IN ('COLD', 'VERY_COLD') AND 
             ptd.player_offensive_role = 'PRIMARY_OPTION' THEN 'HIGHER_RESPONSIBILITY'
        ELSE 'NEUTRAL_TEAM_ENVIRONMENT'
    END AS team_form_player_impact,
    
    -- Playstyle stat impact (which stats are boosted by team style)
    CASE
        WHEN ptd.team_playstyle = 'FAST_PACED' THEN 'TRANSITION_STATS'
        WHEN ptd.team_playstyle = 'THREE_POINT_HEAVY' THEN '3PT_SHOOTING' 
        WHEN ptd.team_playstyle = 'PAINT_FOCUSED' THEN 'INSIDE_SCORING_REBOUNDING'
        WHEN ptd.team_playstyle = 'BALL_MOVEMENT' THEN 'ASSIST_OPPORTUNITIES'
        ELSE 'BALANCED_STATS'
    END AS team_playstyle_stat_impact,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM player_team_deviations ptd