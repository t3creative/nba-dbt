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
    tags=['features', 'player', 'team', 'interaction', 'derived']
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

team_game_dates AS (
    -- Identifies the previous game date for each team to fetch lagged stats.
    SELECT
        team_id,
        game_date, -- This is the current game_date from player_boxscores
        season_year,
        LAG(game_date, 1) OVER (PARTITION BY team_id, season_year ORDER BY game_date) AS previous_game_date
    FROM (
        SELECT DISTINCT team_id, game_date, season_year FROM player_boxscores
    )
),

lagged_team_advanced_stats AS (
    SELECT
        tgd.team_id,
        tgd.game_date, -- Current game_date, to join back to player_boxscores
        adv.pace_roll_5g_avg AS team_l5_pace_lagged,
        adv.off_rating_roll_5g_avg AS team_l5_off_rating_lagged,
        adv.def_rating_roll_5g_avg AS team_l5_def_rating_lagged,
        (adv.off_rating_roll_3g_avg - adv.off_rating_roll_10g_avg) AS team_offense_trend_lagged,
        (adv.def_rating_roll_3g_avg - adv.def_rating_roll_10g_avg) AS team_defense_trend_lagged -- Added for completeness if needed
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__advanced_rolling') }} adv
        ON tgd.team_id = adv.team_id AND tgd.previous_game_date = adv.game_date
    WHERE tgd.previous_game_date IS NOT NULL
),

lagged_team_performance AS (
    SELECT
        tgd.team_id,
        tgd.game_date, -- Current game_date
        perf.team_form AS team_form_lagged
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__performance_metrics') }} perf
        ON tgd.team_id = perf.team_id AND tgd.previous_game_date = perf.game_date
    WHERE tgd.previous_game_date IS NOT NULL
),

lagged_team_shot_distribution_features AS (
    SELECT
        tgd.team_id,
        tgd.game_date, -- Current game_date
        sd.team_l5_3pt_att_rate AS team_l5_3pt_att_rate_lagged,
        sd.team_l5_paint_scoring_rate AS team_l5_paint_scoring_rate_lagged,
        sd.team_l5_fastbreak_rate AS team_l5_fastbreak_rate_lagged,
        sd.team_l5_assisted_rate AS team_l5_assisted_rate_lagged,
        sd.team_playstyle AS team_playstyle_lagged
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__shot_distribution') }} sd
        ON tgd.team_id = sd.team_id AND tgd.previous_game_date = sd.game_date
    WHERE tgd.previous_game_date IS NOT NULL
),

lagged_team_usage_features AS (
    SELECT
        tgd.team_id,
        tgd.game_date, -- Current game_date
        tu.team_l5_max_usage AS team_l5_max_usage_lagged,
        tu.team_offensive_structure AS team_offensive_structure_lagged
    FROM team_game_dates tgd
    JOIN {{ ref('feat_team__usage_distribution') }} tu
        ON tgd.team_id = tu.team_id AND tgd.previous_game_date = tu.game_date
    WHERE tgd.previous_game_date IS NOT NULL
),

-- Calculate 75th percentile of usage_pct per team and game (based on current game player stats)
player_game_percentiles AS (
    SELECT
        team_id,
        game_id,
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY usage_pct) AS p75_usage_pct
    FROM player_boxscores
    GROUP BY team_id, game_id
),

-- Calculate player's deviation from team averages, using lagged team context
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
        
        -- Player's share of team production (current game)
        pb.pct_of_team_pts,
        pb.pct_of_team_reb,
        pb.pct_of_team_ast,
        pb.pct_of_team_fga,
        
        -- Lagged team metrics
        ltas.team_l5_pace_lagged,
        ltas.team_l5_off_rating_lagged,
        ltas.team_l5_def_rating_lagged,
        ltp.team_form_lagged,
        ltas.team_offense_trend_lagged,
        
        -- Lagged team shot distribution
        ltsdf.team_l5_3pt_att_rate_lagged,
        ltsdf.team_l5_paint_scoring_rate_lagged,
        ltsdf.team_l5_fastbreak_rate_lagged,
        ltsdf.team_l5_assisted_rate_lagged,
        ltsdf.team_playstyle_lagged,
        
        -- Lagged team usage 
        ltuf.team_l5_max_usage_lagged,
        ltuf.team_offensive_structure_lagged,
        
        -- Calculate production vs team averages (efficiency uses current game player average)
        CASE 
            WHEN pb.ts_pct > 
                 AVG(pb.ts_pct) OVER(PARTITION BY pb.team_id, pb.game_id) + 0.05
            THEN 'ABOVE_TEAM_AVG'
            WHEN pb.ts_pct < 
                 AVG(pb.ts_pct) OVER(PARTITION BY pb.team_id, pb.game_id) - 0.05
            THEN 'BELOW_TEAM_AVG'
            ELSE 'NEAR_TEAM_AVG'
        END AS efficiency_vs_team_avg,
        
        -- Identify if player is in top 3 usage (uses current game player usage, lagged team max usage, current game team usage percentile)
        CASE 
            WHEN pb.usage_pct >= ltuf.team_l5_max_usage_lagged * 0.9 -- Using lagged team max usage
            THEN 'PRIMARY_OPTION'
            WHEN pgp.p75_usage_pct IS NOT NULL AND pb.usage_pct >= pgp.p75_usage_pct -- Using current game p75 usage
            THEN 'SECONDARY_OPTION'
            ELSE 'SUPPORTING_ROLE'
        END AS player_offensive_role,
        
        -- Player-team style fit score (higher = better fit, uses lagged team playstyle)
        CASE
            WHEN pb.position = 'G' AND ltsdf.team_playstyle_lagged = 'FAST_PACED' THEN 5
            WHEN pb.position = 'G' AND ltsdf.team_playstyle_lagged = 'BALL_MOVEMENT' THEN 5
            WHEN pb.position = 'F' AND ltsdf.team_playstyle_lagged = 'THREE_POINT_HEAVY' THEN 5
            WHEN pb.position = 'C' AND ltsdf.team_playstyle_lagged = 'PAINT_FOCUSED' THEN 5
            WHEN pb.position = 'G' AND ltsdf.team_playstyle_lagged = 'THREE_POINT_HEAVY' THEN 4
            WHEN pb.position = 'F' AND ltsdf.team_playstyle_lagged = 'PAINT_FOCUSED' THEN 4
            WHEN ltsdf.team_playstyle_lagged = 'BALANCED' THEN 3
            WHEN pb.position = 'C' AND ltsdf.team_playstyle_lagged = 'THREE_POINT_HEAVY' THEN 2
            WHEN pb.position = 'G' AND ltsdf.team_playstyle_lagged = 'PAINT_FOCUSED' THEN 2
            ELSE 3
        END AS player_team_style_fit_score
        
    FROM player_boxscores pb
    LEFT JOIN lagged_team_advanced_stats ltas
        ON pb.team_id = ltas.team_id AND pb.game_date = ltas.game_date
    LEFT JOIN lagged_team_performance ltp
        ON pb.team_id = ltp.team_id AND pb.game_date = ltp.game_date
    LEFT JOIN lagged_team_shot_distribution_features ltsdf
        ON pb.team_id = ltsdf.team_id AND pb.game_date = ltsdf.game_date
    LEFT JOIN lagged_team_usage_features ltuf
        ON pb.team_id = ltuf.team_id AND pb.game_date = ltuf.game_date
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
    
    -- Player stats (current game)
    ptd.min,
    ptd.pts,
    ptd.reb,
    ptd.ast,
    ptd.usage_pct,
    ptd.ts_pct,
    
    -- Player's team share (current game)
    ptd.pct_of_team_pts,
    ptd.pct_of_team_reb,
    ptd.pct_of_team_ast,
    ptd.pct_of_team_fga,
    
    -- Lagged team context
    ptd.team_l5_pace_lagged,
    ptd.team_l5_off_rating_lagged, 
    ptd.team_l5_def_rating_lagged,
    ptd.team_form_lagged,
    ptd.team_offense_trend_lagged,
    ptd.team_playstyle_lagged,
    ptd.team_offensive_structure_lagged,
    
    -- Player-team relationship metrics (current player stats/role + lagged team context)
    ptd.efficiency_vs_team_avg,
    ptd.player_offensive_role,
    ptd.player_team_style_fit_score,
    
    -- Team composition impact on player stats (using lagged team context)
    CASE
        WHEN ptd.team_l5_pace_lagged > 102 THEN 'HIGH_PACE_BOOST'
        WHEN ptd.team_l5_pace_lagged < 96 THEN 'LOW_PACE_LIMITATION'
        ELSE 'NEUTRAL_PACE_IMPACT'
    END AS pace_impact_on_player,
    
    CASE
        WHEN ptd.team_offensive_structure_lagged = 'STAR_DOMINANT' AND 
             ptd.player_offensive_role = 'PRIMARY_OPTION' THEN 'HIGH_USAGE_OPPORTUNITY'
        WHEN ptd.team_offensive_structure_lagged = 'BALANCED' THEN 'MODERATE_USAGE_OPPORTUNITY'
        WHEN ptd.team_offensive_structure_lagged = 'TOP_HEAVY' AND 
             ptd.player_offensive_role = 'SUPPORTING_ROLE' THEN 'LIMITED_USAGE_OPPORTUNITY'
        ELSE 'NORMAL_USAGE_OPPORTUNITY'
    END AS usage_opportunity,
    
    CASE
        WHEN ptd.team_form_lagged IN ('HOT', 'VERY_HOT') AND 
             ptd.player_offensive_role = 'SUPPORTING_ROLE' THEN 'POSITIVE_TEAM_ENVIRONMENT'
        WHEN ptd.team_form_lagged IN ('COLD', 'VERY_COLD') AND 
             ptd.player_offensive_role = 'PRIMARY_OPTION' THEN 'HIGHER_RESPONSIBILITY'
        ELSE 'NEUTRAL_TEAM_ENVIRONMENT'
    END AS team_form_player_impact,
    
    CASE
        WHEN ptd.team_playstyle_lagged = 'FAST_PACED' THEN 'TRANSITION_STATS'
        WHEN ptd.team_playstyle_lagged = 'THREE_POINT_HEAVY' THEN '3PT_SHOOTING' 
        WHEN ptd.team_playstyle_lagged = 'PAINT_FOCUSED' THEN 'INSIDE_SCORING_REBOUNDING'
        WHEN ptd.team_playstyle_lagged = 'BALL_MOVEMENT' THEN 'ASSIST_OPPORTUNITIES'
        ELSE 'BALANCED_STATS'
    END AS team_playstyle_stat_impact,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM player_team_deviations ptd