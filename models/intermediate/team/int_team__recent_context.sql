{{ config(
    materialized='incremental',
    unique_key=['team_id', 'game_date'],
    tags=['intermediate', 'team', 'performance'],
    indexes=[
        {'columns': ['team_id', 'game_date'], 'unique': true}
    ]
) }}

WITH team_games AS (
    SELECT *
    FROM {{ ref('int_team__combined_boxscore') }}
    {% if is_incremental() %}
    WHERE game_date > (SELECT MAX(game_date) - INTERVAL '60 days' FROM {{ this }})
    {% endif %}
),

team_with_ranks AS (
    SELECT
        team_id,
        game_id,
        game_date,
        off_rating,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_date DESC) AS game_rank,
        (game_date - LAG(game_date) OVER(PARTITION BY team_id ORDER BY game_date)) AS days_between_games
    FROM team_games
),

team_last5 AS (
    SELECT
        team_id,
        game_date,
        AVG(off_rating) AS team_last5_off_rating
    FROM team_with_ranks
    WHERE game_rank <= 5
    GROUP BY team_id, game_date
)

SELECT
    tg.team_id,
    tg.game_date,
    COALESCE(tl5.team_last5_off_rating, 0) AS team_last5_off_rating,
    FALSE AS missing_key_player, -- Placeholder
    CASE 
        WHEN tg.days_between_games = 1 THEN TRUE 
        ELSE FALSE 
    END AS is_back_to_back,
    CURRENT_TIMESTAMP AS created_at, 
    CURRENT_TIMESTAMP AS updated_at
FROM team_with_ranks tg
LEFT JOIN team_last5 tl5
    ON tg.team_id = tl5.team_id AND tg.game_date = tl5.game_date
WHERE tg.game_rank = 1 -- Only keep the latest game data for each team
