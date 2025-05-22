{{ config(
    materialized='incremental',
    schema='features',
    unique_key='player_game_opponent_key',
    tags=['derived', 'features', 'opponent', 'position', 'matchup', 'prediction']
) }}

WITH upcoming_games AS (
    SELECT
        go.game_id,
        go.team_id,
        go.opponent_id,
        go.game_date,
        go.season_year,
        go.home_away,
        go.team_game_key
    FROM {{ ref('feat_opp__game_opponents') }} go
    {% if is_incremental() %}
    WHERE go.game_date > (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
),

player_data AS (
    SELECT DISTINCT
        player_id,
        team_id,
        position,
        player_name,
        first_name,
        family_name
    FROM {{ ref('int_player__combined_boxscore') }}
    WHERE season_year = (SELECT MAX(season_year) FROM {{ ref('int_player__combined_boxscore') }})
),

position_defense_data AS (
    SELECT
        pd.*
    FROM {{ ref('opponent_position_defense_features_v1') }} pd
),

final AS (
    SELECT
        -- Key identifiers
        ug.game_id,
        pd.player_id,
        ug.team_id,
        ug.opponent_id,
        ug.game_date,
        ug.season_year,
        ug.home_away,
        {{ dbt_utils.generate_surrogate_key(['ug.game_id', 'pd.player_id', 'ug.opponent_id']) }} AS player_game_opponent_key,
        
        -- Player details
        pd.player_name,
        pd.position,
        
        -- Raw average stats allowed by opponent to player's position
        pdef.avg_pts_allowed_to_position,
        pdef.avg_reb_allowed_to_position,
        pdef.avg_ast_allowed_to_position,
        pdef.avg_stl_allowed_to_position,
        pdef.avg_blk_allowed_to_position,
        pdef.avg_fg3m_allowed_to_position,
        pdef.avg_fg_pct_allowed_to_position,
        pdef.avg_fg3_pct_allowed_to_position,
        pdef.avg_pts_ast_allowed_to_position,
        pdef.avg_pts_reb_allowed_to_position,
        pdef.avg_pts_reb_ast_allowed_to_position,
        pdef.avg_ast_reb_allowed_to_position,
        pdef.l10_pts_allowed_to_position, -- Example of a last N games stat
        
        -- Matchup quality indicators (Opponent_Allowed_Stat_To_Position vs_League_Avg_For_Position)
        pdef.pts_vs_league_avg,
        pdef.reb_vs_league_avg,
        pdef.ast_vs_league_avg,
        pdef.stl_vs_league_avg,
        pdef.blk_vs_league_avg,
        pdef.fg3m_vs_league_avg,
        pdef.fg_pct_vs_league_avg,
        pdef.fg3_pct_vs_league_avg,
        pdef.pts_ast_vs_league_avg,
        pdef.pts_reb_vs_league_avg,
        pdef.pts_reb_ast_vs_league_avg,
        pdef.ast_reb_vs_league_avg,
        
        -- Labeled Matchup Quality Indicators (Categorical)
        CASE
            WHEN COALESCE(pdef.pts_vs_league_avg, 0.0) > 2.0 THEN 'Great'
            WHEN COALESCE(pdef.pts_vs_league_avg, 0.0) > 0.75 THEN 'Good'
            WHEN COALESCE(pdef.pts_vs_league_avg, 0.0) >= -0.75 THEN 'Average'
            WHEN COALESCE(pdef.pts_vs_league_avg, 0.0) >= -2.0 THEN 'Poor'
            ELSE 'Bad'
        END AS pts_matchup_label,

        CASE
            WHEN COALESCE(pdef.reb_vs_league_avg, 0.0) > 2.0 THEN 'Great'
            WHEN COALESCE(pdef.reb_vs_league_avg, 0.0) > 0.75 THEN 'Good'
            WHEN COALESCE(pdef.reb_vs_league_avg, 0.0) >= -0.75 THEN 'Average'
            WHEN COALESCE(pdef.reb_vs_league_avg, 0.0) >= -2.0 THEN 'Poor'
            ELSE 'Bad'
        END AS reb_matchup_label,

        CASE
            WHEN COALESCE(pdef.ast_vs_league_avg, 0.0) > 2.0 THEN 'Great'
            WHEN COALESCE(pdef.ast_vs_league_avg, 0.0) > 0.75 THEN 'Good'
            WHEN COALESCE(pdef.ast_vs_league_avg, 0.0) >= -0.75 THEN 'Average'
            WHEN COALESCE(pdef.ast_vs_league_avg, 0.0) >= -2.0 THEN 'Poor'
            ELSE 'Bad'
        END AS ast_matchup_label,

        CASE
            WHEN COALESCE(pdef.stl_vs_league_avg, 0.0) > 0.5 THEN 'Great' -- Adjusted thresholds for steals (lower volume)
            WHEN COALESCE(pdef.stl_vs_league_avg, 0.0) > 0.2 THEN 'Good'
            WHEN COALESCE(pdef.stl_vs_league_avg, 0.0) >= -0.2 THEN 'Average'
            WHEN COALESCE(pdef.stl_vs_league_avg, 0.0) >= -0.5 THEN 'Poor'
            ELSE 'Bad'
        END AS stl_matchup_label,

        CASE
            WHEN COALESCE(pdef.blk_vs_league_avg, 0.0) > 0.5 THEN 'Great' -- Adjusted thresholds for blocks (lower volume)
            WHEN COALESCE(pdef.blk_vs_league_avg, 0.0) > 0.2 THEN 'Good'
            WHEN COALESCE(pdef.blk_vs_league_avg, 0.0) >= -0.2 THEN 'Average'
            WHEN COALESCE(pdef.blk_vs_league_avg, 0.0) >= -0.5 THEN 'Poor'
            ELSE 'Bad'
        END AS blk_matchup_label,

        CASE
            WHEN COALESCE(pdef.fg3m_vs_league_avg, 0.0) > 1.0 THEN 'Great' -- Adjusted thresholds for 3PM
            WHEN COALESCE(pdef.fg3m_vs_league_avg, 0.0) > 0.4 THEN 'Good'
            WHEN COALESCE(pdef.fg3m_vs_league_avg, 0.0) >= -0.4 THEN 'Average'
            WHEN COALESCE(pdef.fg3m_vs_league_avg, 0.0) >= -1.0 THEN 'Poor'
            ELSE 'Bad'
        END AS fg3m_matchup_label,
        
        CASE
            WHEN COALESCE(pdef.fg_pct_vs_league_avg, 0.0) > 0.025 THEN 'Great'
            WHEN COALESCE(pdef.fg_pct_vs_league_avg, 0.0) > 0.01 THEN 'Good'
            WHEN COALESCE(pdef.fg_pct_vs_league_avg, 0.0) >= -0.01 THEN 'Average'
            WHEN COALESCE(pdef.fg_pct_vs_league_avg, 0.0) >= -0.025 THEN 'Poor'
            ELSE 'Bad'
        END AS fg_pct_matchup_label,

        CASE
            WHEN COALESCE(pdef.fg3_pct_vs_league_avg, 0.0) > 0.025 THEN 'Great'
            WHEN COALESCE(pdef.fg3_pct_vs_league_avg, 0.0) > 0.01 THEN 'Good'
            WHEN COALESCE(pdef.fg3_pct_vs_league_avg, 0.0) >= -0.01 THEN 'Average'
            WHEN COALESCE(pdef.fg3_pct_vs_league_avg, 0.0) >= -0.025 THEN 'Poor'
            ELSE 'Bad'
        END AS fg3_pct_matchup_label,

        CASE
            WHEN COALESCE(pdef.pts_ast_vs_league_avg, 0.0) > 3.0 THEN 'Great' -- Adjusted for combined stats
            WHEN COALESCE(pdef.pts_ast_vs_league_avg, 0.0) > 1.0 THEN 'Good'
            WHEN COALESCE(pdef.pts_ast_vs_league_avg, 0.0) >= -1.0 THEN 'Average'
            WHEN COALESCE(pdef.pts_ast_vs_league_avg, 0.0) >= -3.0 THEN 'Poor'
            ELSE 'Bad'
        END AS pts_ast_matchup_label,

        CASE
            WHEN COALESCE(pdef.pts_reb_vs_league_avg, 0.0) > 3.0 THEN 'Great'
            WHEN COALESCE(pdef.pts_reb_vs_league_avg, 0.0) > 1.0 THEN 'Good'
            WHEN COALESCE(pdef.pts_reb_vs_league_avg, 0.0) >= -1.0 THEN 'Average'
            WHEN COALESCE(pdef.pts_reb_vs_league_avg, 0.0) >= -3.0 THEN 'Poor'
            ELSE 'Bad'
        END AS pts_reb_matchup_label,

        CASE
            WHEN COALESCE(pdef.pts_reb_ast_vs_league_avg, 0.0) > 4.0 THEN 'Great'
            WHEN COALESCE(pdef.pts_reb_ast_vs_league_avg, 0.0) > 1.5 THEN 'Good'
            WHEN COALESCE(pdef.pts_reb_ast_vs_league_avg, 0.0) >= -1.5 THEN 'Average'
            WHEN COALESCE(pdef.pts_reb_ast_vs_league_avg, 0.0) >= -4.0 THEN 'Poor'
            ELSE 'Bad'
        END AS pts_reb_ast_matchup_label,

        CASE
            WHEN COALESCE(pdef.ast_reb_vs_league_avg, 0.0) > 3.0 THEN 'Great'
            WHEN COALESCE(pdef.ast_reb_vs_league_avg, 0.0) > 1.0 THEN 'Good'
            WHEN COALESCE(pdef.ast_reb_vs_league_avg, 0.0) >= -1.0 THEN 'Average'
            WHEN COALESCE(pdef.ast_reb_vs_league_avg, 0.0) >= -3.0 THEN 'Poor'
            ELSE 'Bad'
        END AS ast_reb_matchup_label,
        
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM upcoming_games ug
    JOIN player_data pd
        ON ug.team_id = pd.team_id
    LEFT JOIN position_defense_data pdef -- Renamed CTE for clarity
        ON ug.opponent_id = pdef.opponent_id
        AND pd.position = pdef.position
        AND ug.season_year = pdef.season_year
        AND ug.game_date = pdef.game_date -- Ensuring join is on game_date as well
)

SELECT * FROM final