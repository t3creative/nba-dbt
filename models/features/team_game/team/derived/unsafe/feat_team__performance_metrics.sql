{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['team_id', 'season_year'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': true},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']},
        {'columns': ['season_year', 'game_date']}
    ],
    tags=['derived', 'features', 'team_derived_metrics', 'performance_metrics']
) }}

WITH team_boxscores AS (
    SELECT *
    FROM {{ ref('int_team__combined_boxscore') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}
),

-- Add win/loss outcome
game_outcomes AS (
    SELECT
        tb.team_game_key,
        tb.game_id,
        tb.team_id,
        tb.opponent_id,
        tb.game_date,
        tb.season_year,
        tb.home_away,
        tb.pts,
        tb.team_name,
        tb.team_tricode,
        
        -- Determine win/loss
        CASE WHEN tb.pts > opps.pts THEN 1 ELSE 0 END AS is_win,
        
        -- Calculate margin
        (tb.pts - opps.pts) AS point_margin,
        
        -- Game number within season
        ROW_NUMBER() OVER (
            PARTITION BY tb.team_id, tb.season_year 
            ORDER BY tb.game_date
        ) AS game_num_in_season
        
    FROM team_boxscores tb
    JOIN team_boxscores opps 
        ON tb.game_id = opps.game_id AND tb.opponent_id = opps.team_id
),

-- Calculate team streaks and recent record
team_streaks AS (
    SELECT
        go.*,
        
        -- Current streak
        SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS season_wins,
        
        SUM(CASE WHEN is_win = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS season_losses,
        
        -- Last 5 games record
        SUM(is_win) OVER (
            PARTITION BY team_id
            ORDER BY game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS last5_wins,
        
        -- Last 10 games record
        SUM(is_win) OVER (
            PARTITION BY team_id
            ORDER BY game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS last10_wins,
        
        -- Win streak (consecutive wins)
        (SELECT 
            COUNT(*)
         FROM game_outcomes sub
         WHERE sub.team_id = go.team_id
           AND sub.game_date <= go.game_date
           AND sub.game_date > (
               SELECT MAX(game_date)
               FROM game_outcomes loss
               WHERE loss.team_id = go.team_id
                 AND loss.is_win = 0
                 AND loss.game_date < go.game_date
           )
         ) AS current_win_streak,
         
        -- Loss streak (consecutive losses)
        (SELECT 
            COUNT(*)
         FROM game_outcomes sub
         WHERE sub.team_id = go.team_id
           AND sub.game_date <= go.game_date
           AND sub.game_date > (
               SELECT MAX(game_date)
               FROM game_outcomes win
               WHERE win.team_id = go.team_id
                 AND win.is_win = 1
                 AND win.game_date < go.game_date
           )
         ) AS current_loss_streak
         
    FROM game_outcomes go
)

SELECT
    tb.team_game_key,
    tb.game_id,
    tb.team_id,
    tb.opponent_id,
    tb.game_date,
    tb.season_year,
    tb.home_away,
    
    -- Team identifying info
    tb.team_name,
    tb.team_tricode,
    
    -- Core performance metrics
    tb.pts,
    tb.off_rating,
    tb.def_rating,
    tb.net_rating,
    tb.pace,
    tb.eff_fg_pct,
    tb.ts_pct,
    tb.ast_ratio,
    tb.ast_to_tov_ratio,
    tb.ast_pct,
    tb.reb_pct,
    tb.off_reb_pct,
    tb.def_reb_pct,
    
    -- Play style metrics
    tb.pct_pts_in_paint,
    tb.pct_pts_fastbreak,
    tb.pct_pts_3pt,
    tb.pct_assisted_fgm,
    
    -- Game outcomes and streaks
    ts.is_win,
    ts.point_margin,
    ts.game_num_in_season,
    ts.season_wins,
    ts.season_losses,
    COALESCE(ts.last5_wins, 0) AS last5_wins,
    COALESCE(5 - ts.last5_wins, 0) AS last5_losses,
    COALESCE(ts.last10_wins, 0) AS last10_wins,
    COALESCE(10 - ts.last10_wins, 0) AS last10_losses,
    COALESCE(ts.current_win_streak, 0) AS current_win_streak,
    COALESCE(ts.current_loss_streak, 0) AS current_loss_streak,
    
    -- Team quality indicators
    CASE 
        WHEN (ts.season_wins / NULLIF(ts.season_wins + ts.season_losses, 0)) > 0.65 THEN 'ELITE'
        WHEN (ts.season_wins / NULLIF(ts.season_wins + ts.season_losses, 0)) > 0.55 THEN 'GOOD'
        WHEN (ts.season_wins / NULLIF(ts.season_wins + ts.season_losses, 0)) > 0.45 THEN 'AVERAGE'
        WHEN (ts.season_wins / NULLIF(ts.season_wins + ts.season_losses, 0)) > 0.35 THEN 'POOR'
        ELSE 'STRUGGLING'
    END AS team_strength_tier,
    
    -- Current form indicator
    CASE 
        WHEN COALESCE(ts.last10_wins, 0) >= 8 THEN 'VERY_HOT'
        WHEN COALESCE(ts.last10_wins, 0) >= 6 THEN 'HOT'
        WHEN COALESCE(ts.last10_wins, 0) = 5 THEN 'NEUTRAL'
        WHEN COALESCE(ts.last10_wins, 0) >= 3 THEN 'COLD'
        ELSE 'VERY_COLD'
    END AS team_form,
    
    -- Timestamps
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM team_boxscores tb
LEFT JOIN team_streaks ts
    ON tb.team_game_key = ts.team_game_key