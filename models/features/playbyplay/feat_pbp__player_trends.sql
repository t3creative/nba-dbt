-- feat_pbp__player_trends.sql
{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_game_key',
        tags=['predictive', 'time_series', 'trend']
    )
}}

WITH game_dates as (
    select
        game_id,
        game_date 
    from {{ ref('stg__schedules') }} -- This is the crucial part
    group by 1, 2 
),

player_game_events_source AS (
    SELECT *
    FROM {{ ref('int_pbp__player_game_events') }} -- This model does NOT have game_date
),

player_game_stats AS (
    -- Get core stats from player game events aggregated by game
    SELECT
        pge.player_id,
        pge.game_id,
        gd.game_date,
        SUM(CASE WHEN pge.action_type = 'SHOT' AND pge.shot_made_flag = TRUE THEN pge.points ELSE 0 END) AS points,
        COUNT(*) FILTER(WHERE pge.action_type = 'SHOT' AND pge.shot_zone = '3PT' AND pge.shot_made_flag = TRUE) AS threes_made,
        COUNT(*) FILTER(WHERE pge.action_type = 'REBOUND') AS rebounds,
        COUNT(*) FILTER(WHERE pge.action_type = 'ASSIST') AS assists,
        COUNT(*) FILTER(WHERE pge.action_type = 'STEAL') AS steals,
        COUNT(*) FILTER(WHERE pge.action_type = 'BLOCK') AS blocks,
        COUNT(*) FILTER(WHERE pge.action_type = 'TURNOVER') AS turnovers,
        COUNT(*) FILTER(WHERE pge.action_type = 'SHOT' AND pge.shot_made_flag = FALSE) AS missed_shots,
        COUNT(*) FILTER(WHERE pge.action_type = 'SHOT') AS shot_attempts,
        COUNT(*) FILTER(WHERE pge.action_type = 'SHOT' AND pge.shot_zone = '3PT') AS three_attempts,
        COUNT(*) FILTER(WHERE pge.is_clutch_time_end_game = TRUE) AS clutch_plays
    FROM player_game_events_source pge
    JOIN game_dates gd ON pge.game_id = gd.game_id
    GROUP BY pge.player_id, pge.game_id, gd.game_date 
),

player_game_stats_with_sequences as (
    select
        pgs.player_id,
        pgs.game_id,
        pgs.game_date, -- Select the aliased column, can rename back to game_date here for downstream
        pgs.points,
        pgs.threes_made,
        pgs.rebounds,
        pgs.assists,
        pgs.steals,
        pgs.missed_shots,
        pgs.shot_attempts,
        pgs.three_attempts,
        pgs.clutch_plays,
        row_number() over (partition by pgs.player_id order by pgs.game_date) as player_game_sequence,
        lag(pgs.game_date, 1) over (partition by pgs.player_id order by pgs.game_date) as prev_game_date
    from player_game_stats pgs
),

player_season_stats AS (
    -- Get player's current season averages before each game
    SELECT
        pgs_ws.player_id,
        pgs_ws.game_id,
        pgs_ws.game_date, -- This should now correctly refer to 'actual_game_date' renamed as 'game_date' from the CTE above
        -- Compute cumulative season averages excluding current game
        AVG(pgs_ws.points) OVER(
            PARTITION BY pgs_ws.player_id, EXTRACT(YEAR FROM pgs_ws.game_date)
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_points,
        
        AVG(pgs_ws.threes_made) OVER(
            PARTITION BY pgs_ws.player_id, EXTRACT(YEAR FROM pgs_ws.game_date)
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_threes,
        
        AVG(pgs_ws.rebounds) OVER(
            PARTITION BY pgs_ws.player_id, EXTRACT(YEAR FROM pgs_ws.game_date)
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_rebounds,
        
        AVG(pgs_ws.assists) OVER(
            PARTITION BY pgs_ws.player_id, EXTRACT(YEAR FROM pgs_ws.game_date)
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_assists,
        
        AVG(pgs_ws.steals) OVER(
            PARTITION BY pgs_ws.player_id, EXTRACT(YEAR FROM pgs_ws.game_date)
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_avg_steals,
        
        -- Count of games so far this season (for trend stability)
        COUNT(*) OVER(
            PARTITION BY pgs_ws.player_id, EXTRACT(YEAR FROM pgs_ws.game_date)
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS games_played_this_season
    FROM player_game_stats_with_sequences pgs_ws -- Alias here
),

recent_performance AS (
    -- Calculate recent performance metrics
    SELECT
        pgs_ws.player_id, -- Ensure references are to the correct CTE alias
        pgs_ws.game_id,
        pgs_ws.game_date, -- This should be the game_date from player_game_stats_with_sequences
        
        -- Last game stats
        LAG(pgs_ws.points, 1) OVER(PARTITION BY pgs_ws.player_id ORDER BY pgs_ws.game_date) AS last_game_points,
        LAG(pgs_ws.threes_made, 1) OVER(PARTITION BY pgs_ws.player_id ORDER BY pgs_ws.game_date) AS last_game_threes,
        LAG(pgs_ws.rebounds, 1) OVER(PARTITION BY pgs_ws.player_id ORDER BY pgs_ws.game_date) AS last_game_rebounds,
        LAG(pgs_ws.assists, 1) OVER(PARTITION BY pgs_ws.player_id ORDER BY pgs_ws.game_date) AS last_game_assists,
        LAG(pgs_ws.steals, 1) OVER(PARTITION BY pgs_ws.player_id ORDER BY pgs_ws.game_date) AS last_game_steals,
        
        -- 3-game rolling averages
        AVG(pgs_ws.points) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS three_game_avg_points,
        
        AVG(pgs_ws.threes_made) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS three_game_avg_threes,
        
        AVG(pgs_ws.rebounds) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS three_game_avg_rebounds,
        
        AVG(pgs_ws.assists) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS three_game_avg_assists,
        
        AVG(pgs_ws.steals) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS three_game_avg_steals,
        
        -- 7-game rolling averages
        AVG(pgs_ws.points) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS seven_game_avg_points,
        
        -- Shooting form indicators (last 5 games)
        AVG(CASE WHEN pgs_ws.shot_attempts > 0 THEN (pgs_ws.shot_attempts - pgs_ws.missed_shots)::FLOAT / pgs_ws.shot_attempts ELSE NULL END) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS recent_fg_pct,
        
        AVG(CASE WHEN pgs_ws.three_attempts > 0 THEN pgs_ws.threes_made::FLOAT / pgs_ws.three_attempts ELSE NULL END) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS recent_3pt_pct,
        
        -- Momentum indicators - calculate slope of performance line
        REGR_SLOPE(pgs_ws.points, pgs_ws.player_game_sequence) OVER(
            PARTITION BY pgs_ws.player_id 
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS point_trend_slope,
        
        REGR_SLOPE(pgs_ws.assists, pgs_ws.player_game_sequence) OVER(
            PARTITION BY pgs_ws.player_id 
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS assist_trend_slope,
        
        -- Usage trend indicators
        REGR_SLOPE(pgs_ws.shot_attempts, pgs_ws.player_game_sequence) OVER(
            PARTITION BY pgs_ws.player_id 
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS usage_trend_slope,
        
        -- Consistency measures (standard deviation of recent performances)
        STDDEV(pgs_ws.points) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date  
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS points_volatility,
        
        -- Days since last game (rest factor)
        (pgs_ws.game_date - pgs_ws.prev_game_date) AS days_since_last_game,
        
        -- Performance in clutch situations (recent games)
        SUM(pgs_ws.clutch_plays) OVER(
            PARTITION BY pgs_ws.player_id
            ORDER BY pgs_ws.game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS recent_clutch_involvement
    FROM player_game_stats_with_sequences pgs_ws -- Changed to source from the new CTE and aliased
),

final AS (
    SELECT
        rp.player_id,
        rp.game_id,
        MD5(CONCAT(rp.player_id::TEXT, '-', rp.game_id::TEXT)) AS player_game_key,
        
        -- Season context
        ps.season_avg_points,
        ps.season_avg_threes,
        ps.season_avg_rebounds,
        ps.season_avg_assists,
        ps.season_avg_steals,
        ps.games_played_this_season,
        
        -- Recent form
        rp.last_game_points,
        rp.last_game_threes,
        rp.last_game_rebounds,
        rp.last_game_assists,
        rp.last_game_steals,
        
        rp.three_game_avg_points,
        rp.three_game_avg_threes,
        rp.three_game_avg_rebounds,
        rp.three_game_avg_assists,
        rp.three_game_avg_steals,
        
        rp.seven_game_avg_points,
        
        -- Form indicators
        rp.recent_fg_pct,
        rp.recent_3pt_pct,
        
        -- Trend indicators
        rp.point_trend_slope,
        rp.assist_trend_slope,
        rp.usage_trend_slope,
        
        -- Consistency indicators
        rp.points_volatility,
        
        -- Performance variance from season average (recent hot/cold streaks)
        CASE 
            WHEN ps.season_avg_points > 0 THEN rp.three_game_avg_points / NULLIF(ps.season_avg_points, 0) 
            ELSE NULL 
        END AS points_vs_season_avg,
        
        -- Rest factor
        rp.days_since_last_game,
        
        -- Game-to-game consistency (coefficient of variation)
        CASE 
            WHEN rp.three_game_avg_points > 0 THEN rp.points_volatility / NULLIF(rp.three_game_avg_points, 0) 
            ELSE NULL 
        END AS points_consistency_score,
        
        -- Clutch performance indicator
        rp.recent_clutch_involvement,
        
        -- Road/Home game streak indicators
        -- (this would require joining to game data with location info)
        
        -- Current timestamp for incremental processing
        CURRENT_TIMESTAMP AS processed_at
    FROM recent_performance rp
    JOIN player_season_stats ps 
      ON rp.player_id = ps.player_id
     AND rp.game_id = ps.game_id
)

SELECT * FROM final