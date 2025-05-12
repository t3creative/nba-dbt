{{ config(materialized='incremental',
    tags=['core', 'bridge'],
    unique_key=['player_id', 'team_id', 'valid_from'],
    partition_by={
        'field': 'valid_from',
        'data_type': 'timestamp',
        'granularity': 'month'
    }
) }}

-- Use player game logs as the primary source to track player-team relationships
WITH player_games AS (
    SELECT
        player_id,
        team_id,
        game_date,
        season_year,
        -- Additional team info for context
        team_name,
        team_tricode
    FROM {{ ref('stg__game_logs_player') }}
),

-- Get the earliest and latest appearance of a player on each team by season
player_team_by_season AS (
    SELECT
        player_id,
        team_id,
        season_year,
        team_name,
        team_tricode,
        MIN(game_date) AS first_appearance,
        MAX(game_date) AS last_appearance,
        COUNT(*) AS games_played
    FROM player_games
    GROUP BY
        player_id,
        team_id,
        season_year,
        team_name,
        team_tricode
),

-- Create history with valid_from and valid_to dates
player_team_history AS (
    SELECT
        player_id,
        team_id,
        season_year,
        team_name,
        team_tricode,
        first_appearance AS valid_from,
        -- For current team affiliations, leave valid_to as NULL
        -- For historical affiliations, use the day before first appearance on next team
        LEAD(first_appearance, 1, NULL) OVER (
            PARTITION BY player_id 
            ORDER BY first_appearance
        ) - INTERVAL '1 day' AS valid_to,
        games_played,
        -- Flag for whether this is the player's current team
        CASE 
            WHEN LEAD(first_appearance, 1, NULL) OVER (
                PARTITION BY player_id 
                ORDER BY first_appearance
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM player_team_by_season
),

-- Supplement with player_index data for players without game logs
player_index_data AS (
    SELECT
        player_id,
        team_id,
        team_name,
        team_abbreviation AS team_tricode,
        -- Convert from_year to a date (using January 1st as a placeholder)
        TO_DATE(from_year || '-01-01', 'YYYY-MM-DD') AS valid_from,
        -- Convert to_year to a date (using December 31st as a placeholder)
        -- Use current date for active players
        CASE
            WHEN roster_status = 1 THEN NULL  -- Active players
            ELSE TO_DATE(to_year || '-12-31', 'YYYY-MM-DD')
        END AS valid_to,
        roster_status = 1 AS is_current
    FROM {{ ref('stg__player_index') }}
    -- Only include players not in the game logs
    WHERE NOT EXISTS (
        SELECT 1 FROM player_team_history pth
        WHERE pth.player_id = stg__player_index.player_id
    )
),

-- Combine both sources
combined_data AS (
    SELECT
        player_id,
        team_id,
        team_name,
        team_tricode,
        valid_from,
        valid_to,
        is_current,
        'game_logs' AS data_source
    FROM player_team_history
    
    UNION ALL
    
    SELECT
        player_id,
        team_id,
        team_name,
        team_tricode,
        valid_from,
        valid_to,
        is_current,
        'player_index' AS data_source
    FROM player_index_data
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['player_id', 'team_id', 'valid_from']) }} AS player_team_key,
        player_id,
        team_id,
        team_name,
        team_tricode,
        valid_from,
        valid_to,
        is_current,
        data_source
    FROM combined_data
)

SELECT * FROM final
