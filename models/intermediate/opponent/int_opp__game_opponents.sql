WITH source_schedules AS (
  /* Select all needed columns from the source */
  SELECT
    game_id,
    season_year,
    game_date,
    home_team_id,
    away_team_id,
    ROW_NUMBER() OVER (PARTITION BY game_id, home_team_id, away_team_id ORDER BY game_date DESC NULLS LAST) AS rn /* Add a row number partitioned by game_id, ordered by date descending */ /* to pick the most recent record for a game if duplicates exist */
  FROM {{ ref('stg__schedules') }} AS stg__schedules
  /* Ensure necessary IDs are present */
  WHERE
    NOT home_team_id IS NULL AND NOT away_team_id IS NULL
), schedules AS (
  /* Select only the latest record for each game_id */
  SELECT
    game_id,
    season_year,
    game_date,
    home_team_id,
    away_team_id
  FROM source_schedules
  WHERE
    rn = 1
    AND season_year >= '2017-18'
), home_team_perspective /* Create one row for the home team's perspective */ AS (
  SELECT
    game_id,
    season_year,
    game_date,
    home_team_id AS team_id,
    away_team_id AS opponent_id,
    'HOME' AS home_away,
    MD5(
      CAST(COALESCE(CAST(game_id AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' || COALESCE(CAST(home_team_id AS TEXT), '_dbt_utils_surrogate_key_null_') AS TEXT)
    ) AS game_team_key /* Create a unique key for the game-team combination */
  FROM schedules
), away_team_perspective /* Create one row for the away team's perspective */ AS (
  SELECT
    game_id,
    season_year,
    game_date,
    away_team_id AS team_id,
    home_team_id AS opponent_id,
    'AWAY' AS home_away,
    MD5(
      CAST(COALESCE(CAST(game_id AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' || COALESCE(CAST(away_team_id AS TEXT), '_dbt_utils_surrogate_key_null_') AS TEXT)
    ) AS game_team_key /* Create a unique key for the game-team combination */
  FROM schedules
), final /* Combine both perspectives */ AS (
  SELECT
    *
  FROM home_team_perspective
  UNION ALL
  SELECT
    *
  FROM away_team_perspective
)
SELECT
  *
FROM final