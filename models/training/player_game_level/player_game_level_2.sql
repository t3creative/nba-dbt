WITH traditional_boxscores AS (
  SELECT
    *
  FROM {{ ref('int__player_traditional_bxsc') }} AS int__player_traditional_bxsc
), advanced_boxscores AS (
  SELECT
    *
  FROM {{ ref('int__player_advanced_bxsc') }} AS int__player_advanced_bxsc
), hustle_boxscores AS (
  SELECT
    *
  FROM {{ ref('int__player_hustle_bxsc') }} AS int__player_hustle_bxsc
), usage_boxscores AS (
  SELECT
    *
  FROM {{ ref('int__player_usage_bxsc') }} AS int__player_usage_bxsc
), scoring_boxscores AS (
  SELECT
    *
  FROM {{ ref('int__player_scoring_bxsc') }} AS int__player_scoring_bxsc
), team_game_context /* Get team context features from int_game_context */ AS (
  SELECT
    *
  FROM {{ ref('int_game_context') }} AS int_game_context
), player_games /* Get all distinct player-game combinations with dates */ AS (
  SELECT DISTINCT
    player_game_key,
    player_id,
    game_id,
    game_date,
    season_year,
    team_tricode,
    team_id
  FROM traditional_boxscores
  ORDER BY
    player_id NULLS FIRST,
    game_date NULLS FIRST
), game_context_base /* Add game context features (rest days, back-to-backs, etc.) */ AS (
  SELECT
    pg.player_game_key,
    pg.player_id,
    pg.game_id,
    pg.game_date,
    pg.season_year,
    pg.team_tricode,
    pg.team_id,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY pg.player_id, pg.season_year ORDER BY pg.game_date NULLS FIRST) = 1
      THEN 1
      ELSE 0
    END AS is_first_game_of_season, /* Check if first game of season for player */
    CASE
      WHEN LAG(pg.season_year) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST) <> pg.season_year
      THEN NULL /* First game of a new season */
      ELSE pg.game_date - LAG(pg.game_date) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST)
    END AS days_since_last_game, /* Days since last game (NULL if first game of season) */
    CASE
      WHEN LAG(pg.season_year) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST) <> pg.season_year
      THEN 0 /* Different season */
      WHEN (
        pg.game_date - LAG(pg.game_date) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST)
      ) = 1
      THEN 1
      ELSE 0
    END AS is_back_to_back, /* Is back-to-back (played yesterday) */
    CASE
      WHEN LAG(pg.season_year) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST) <> pg.season_year
      THEN 0 /* New season starts new streak */
      WHEN (
        pg.game_date - LAG(pg.game_date) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST)
      ) > 3
      THEN 0 /* Break in streak if more than 3 days between games */
      ELSE 1 /* Continue streak */
    END AS streak_continue_flag, /* Prepare streak calculation data */
    ROW_NUMBER() OVER (PARTITION BY pg.player_id, pg.season_year ORDER BY pg.game_date NULLS FIRST) AS game_number_in_season, /* Game number in season for player */
    COUNT(*) OVER (PARTITION BY pg.player_id, pg.season_year) AS total_games_in_season_so_far, /* Total games in season so far for player */
    (
      CAST(ROW_NUMBER() OVER (PARTITION BY pg.player_id, pg.season_year ORDER BY pg.game_date NULLS FIRST) AS DOUBLE PRECISION) / 82.0
    ) /* Percentage of season completed (based on 82 games standard season) */ * 100 AS pct_of_season_completed
  FROM player_games AS pg
), game_context /* Calculate streak with non-nested window functions */ AS (
  SELECT
    gcb.*,
    SUM(streak_continue_flag) OVER (PARTITION BY player_id, season_year ORDER BY game_date NULLS FIRST) AS consecutive_games_played, /* Apply streak calculation separately to avoid nested window functions */
    COUNT(*) OVER (PARTITION BY player_id, season_year ORDER BY game_date NULLS FIRST range BETWEEN INTERVAL '6 DAYS' preceding AND CURRENT ROW) /* Number of games in last 7 days (excluding current game) */ - 1 AS games_last_7_days,
    0 AS is_home_game /* Home/away game indicator - requires joining to game data */ /* For now using placeholder until we add the proper game detail data */
  FROM game_context_base AS gcb
), traditional_rolling_features /* Calculate traditional box score rolling averages */ AS (
  SELECT
    pg.player_game_key,
    pg.player_id,
    pg.game_id,
    pg.game_date,
    pg.season_year,
    pg.team_tricode,
    pg.team_id,
    gc.is_first_game_of_season,
    gc.days_since_last_game,
    gc.is_back_to_back,
    gc.consecutive_games_played,
    gc.games_last_7_days,
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN 1
      WHEN tgc.away_team_id = pg.team_id
      THEN 0
      ELSE NULL /* This shouldn't happen if data is consistent */
    END AS is_home_game, /* Update is_home_game with correct values (1=home, 0=away) */
    gc.game_number_in_season,
    gc.total_games_in_season_so_far,
    gc.pct_of_season_completed,
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN 'home'
      WHEN tgc.away_team_id = pg.team_id
      THEN 'away'
      ELSE NULL
    END AS home_or_away, /* Team context features from int_game_context */ /* Is this player on the home team or away team? */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_rest_days
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_rest_days
      ELSE NULL
    END AS team_rest_days, /* Team rest days */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_back_to_back
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_back_to_back
      ELSE NULL
    END AS team_back_to_back, /* Team back-to-back indicator */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_short_rest
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_short_rest
      ELSE NULL
    END AS team_short_rest, /* Team short rest indicator */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_long_rest
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_long_rest
      ELSE NULL
    END AS team_long_rest, /* Team long rest indicator */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_team_game_num
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_team_game_num
      ELSE NULL
    END AS team_game_num_season, /* Team game number in season */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_wins_last_10
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_wins_last_10
      ELSE NULL
    END AS team_wins_last_10, /* Team recent performance */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_losses_last_10
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_losses_last_10
      ELSE NULL
    END AS team_losses_last_10,
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.home_win_pct_last_10
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.away_win_pct_last_10
      ELSE NULL
    END AS team_win_pct_last_10,
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.away_team_id
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.home_team_id
      ELSE NULL
    END AS opponent_team_id, /* Opponent team information */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.away_rest_days
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.home_rest_days
      ELSE NULL
    END AS opponent_rest_days, /* Opponent rest days */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.away_back_to_back
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.home_back_to_back
      ELSE NULL
    END AS opponent_back_to_back, /* Opponent back-to-back indicator */
    CASE
      WHEN tgc.home_team_id = pg.team_id
      THEN tgc.away_win_pct_last_10
      WHEN tgc.away_team_id = pg.team_id
      THEN tgc.home_win_pct_last_10
      ELSE NULL
    END AS opponent_win_pct_last_10, /* Opponent recent performance */
    tgc.is_playoff_game,
    tgc.is_late_season,
    tbs.pts,
    tbs.min,
    tbs.fgm,
    tbs.fga,
    tbs.fg_pct,
    tbs.fg3m,
    tbs.fg3a,
    tbs.fg3_pct,
    tbs.ftm,
    tbs.fta,
    tbs.ft_pct,
    tbs.off_reb,
    tbs.def_reb,
    tbs.reb,
    tbs.ast,
    tbs.stl,
    tbs.blk,
    tbs.tov,
    tbs.pf,
    tbs.plus_minus,
    abs.est_off_rating,
    abs.off_rating,
    abs.est_def_rating,
    abs.def_rating,
    abs.est_net_rating,
    abs.net_rating,
    abs.ast_pct,
    abs.ast_to_tov_ratio,
    abs.ast_ratio,
    abs.off_reb_pct,
    abs.def_reb_pct,
    abs.reb_pct,
    abs.tov_ratio,
    abs.eff_fg_pct,
    abs.ts_pct,
    abs.usage_pct,
    abs.est_usage_pct,
    abs.est_pace,
    abs.pace,
    abs.pace_per_40,
    abs.possessions,
    abs.pie,
    hbs.cont_shots,
    hbs.cont_2pt,
    hbs.cont_3pt,
    hbs.deflections,
    hbs.charges_drawn,
    hbs.screen_ast,
    hbs.screen_ast_pts,
    hbs.off_loose_balls_rec,
    hbs.def_loose_balls_rec,
    hbs.tot_loose_balls_rec,
    hbs.off_box_outs,
    hbs.def_box_outs,
    hbs.box_out_team_reb,
    hbs.box_out_player_reb,
    hbs.tot_box_outs,
    ubs.pct_of_team_fgm,
    ubs.pct_of_team_fga,
    ubs.pct_of_team_fg3m,
    ubs.pct_of_team_fg3a,
    ubs.pct_of_team_ftm,
    ubs.pct_of_team_fta,
    ubs.pct_of_team_oreb,
    ubs.pct_of_team_dreb,
    ubs.pct_of_team_reb,
    ubs.pct_of_team_ast,
    ubs.pct_of_team_tov,
    ubs.pct_of_team_stl,
    ubs.pct_of_team_blk,
    ubs.pct_of_team_blk_allowed,
    ubs.pct_of_team_pf,
    ubs.pct_of_team_pfd,
    ubs.pct_of_team_pts,
    sbs.pct_fga_2pt,
    sbs.pct_fga_3pt,
    sbs.pct_pts_2pt,
    sbs.pct_pts_midrange_2pt,
    sbs.pct_pts_3pt,
    sbs.pct_pts_fastbreak,
    sbs.pct_pts_ft,
    sbs.pct_pts_off_tov,
    sbs.pct_pts_in_paint,
    sbs.pct_assisted_2pt,
    sbs.pct_unassisted_2pt,
    sbs.pct_assisted_3pt,
    sbs.pct_unassisted_3pt,
    sbs.pct_assisted_fgm,
    sbs.pct_unassisted_fgm,
    AVG(tbs.pts) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pts_rolling_avg_4, /* Rolling 4-game averages */
    AVG(tbs.min) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS min_rolling_avg_4,
    AVG(tbs.fgm) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS fgm_rolling_avg_4,
    AVG(tbs.fga) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS fga_rolling_avg_4,
    CAST(NULLIF(
      SUM(tbs.fgm) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fga) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding),
      0
    ) AS fg_pct_rolling_avg_4,
    AVG(tbs.fg3m) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS fg3m_rolling_avg_4,
    AVG(tbs.fg3a) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS fg3a_rolling_avg_4,
    CAST(NULLIF(
      SUM(tbs.fg3m) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fg3a) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding),
      0
    ) AS fg3_pct_rolling_avg_4,
    AVG(tbs.ftm) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS ftm_rolling_avg_4,
    AVG(tbs.fta) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS fta_rolling_avg_4,
    CAST(NULLIF(
      SUM(tbs.ftm) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fta) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding),
      0
    ) AS ft_pct_rolling_avg_4,
    AVG(tbs.off_reb) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS off_reb_rolling_avg_4,
    AVG(tbs.def_reb) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS def_reb_rolling_avg_4,
    AVG(tbs.reb) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS reb_rolling_avg_4,
    AVG(tbs.ast) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS ast_rolling_avg_4,
    AVG(tbs.stl) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS stl_rolling_avg_4,
    AVG(tbs.blk) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS blk_rolling_avg_4,
    AVG(tbs.tov) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS tov_rolling_avg_4,
    AVG(tbs.pf) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pf_rolling_avg_4,
    AVG(tbs.plus_minus) OVER (PARTITION BY pg.player_id ORDER BY pg.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS plus_minus_rolling_avg_4,
    AVG(abs.est_off_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS est_off_rating_rolling_avg_4, /* Advanced stats 4-game rolling averages */
    AVG(abs.off_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS off_rating_rolling_avg_4,
    AVG(abs.est_def_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS est_def_rating_rolling_avg_4,
    AVG(abs.def_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS def_rating_rolling_avg_4,
    AVG(abs.est_net_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS est_net_rating_rolling_avg_4,
    AVG(abs.net_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS net_rating_rolling_avg_4,
    AVG(abs.ast_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS ast_pct_rolling_avg_4,
    AVG(abs.ast_to_tov_ratio) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS ast_to_tov_ratio_rolling_avg_4,
    AVG(abs.ast_ratio) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS ast_ratio_rolling_avg_4,
    AVG(abs.off_reb_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS off_reb_pct_rolling_avg_4,
    AVG(abs.def_reb_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS def_reb_pct_rolling_avg_4,
    AVG(abs.reb_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS reb_pct_rolling_avg_4,
    AVG(abs.tov_ratio) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS tov_ratio_rolling_avg_4,
    AVG(abs.eff_fg_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS eff_fg_pct_rolling_avg_4,
    AVG(abs.ts_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS ts_pct_rolling_avg_4,
    AVG(abs.usage_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS usage_pct_rolling_avg_4,
    AVG(abs.est_usage_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS est_usage_pct_rolling_avg_4,
    AVG(abs.est_pace) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS est_pace_rolling_avg_4,
    AVG(abs.pace) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pace_rolling_avg_4,
    AVG(abs.pace_per_40) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pace_per_40_rolling_avg_4,
    AVG(abs.possessions) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS possessions_rolling_avg_4,
    AVG(abs.pie) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pie_rolling_avg_4,
    AVG(hbs.cont_shots) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS cont_shots_rolling_avg_4, /* Hustle stats 4-game rolling averages */
    AVG(hbs.cont_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS cont_2pt_rolling_avg_4,
    AVG(hbs.cont_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS cont_3pt_rolling_avg_4,
    AVG(hbs.deflections) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS deflections_rolling_avg_4,
    AVG(hbs.charges_drawn) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS charges_drawn_rolling_avg_4,
    AVG(hbs.screen_ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS screen_ast_rolling_avg_4,
    AVG(hbs.screen_ast_pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS screen_ast_pts_rolling_avg_4,
    AVG(hbs.off_loose_balls_rec) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS off_loose_balls_rec_rolling_avg_4,
    AVG(hbs.def_loose_balls_rec) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS def_loose_balls_rec_rolling_avg_4,
    AVG(hbs.tot_loose_balls_rec) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS tot_loose_balls_rec_rolling_avg_4,
    AVG(hbs.off_box_outs) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS off_box_outs_rolling_avg_4,
    AVG(hbs.def_box_outs) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS def_box_outs_rolling_avg_4,
    AVG(hbs.box_out_team_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS box_out_team_reb_rolling_avg_4,
    AVG(hbs.box_out_player_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS box_out_player_reb_rolling_avg_4,
    AVG(hbs.tot_box_outs) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS tot_box_outs_rolling_avg_4,
    AVG(ubs.pct_of_team_fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_fgm_rolling_avg_4, /* Usage stats 4-game rolling averages */
    AVG(ubs.pct_of_team_fga) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_fga_rolling_avg_4,
    AVG(ubs.pct_of_team_fg3m) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_fg3m_rolling_avg_4,
    AVG(ubs.pct_of_team_fg3a) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_fg3a_rolling_avg_4,
    AVG(ubs.pct_of_team_ftm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_ftm_rolling_avg_4,
    AVG(ubs.pct_of_team_fta) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_fta_rolling_avg_4,
    AVG(ubs.pct_of_team_oreb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_oreb_rolling_avg_4,
    AVG(ubs.pct_of_team_dreb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_dreb_rolling_avg_4,
    AVG(ubs.pct_of_team_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_reb_rolling_avg_4,
    AVG(ubs.pct_of_team_ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_ast_rolling_avg_4,
    AVG(ubs.pct_of_team_tov) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_tov_rolling_avg_4,
    AVG(ubs.pct_of_team_stl) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_stl_rolling_avg_4,
    AVG(ubs.pct_of_team_blk) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_blk_rolling_avg_4,
    AVG(ubs.pct_of_team_blk_allowed) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_blk_allowed_rolling_avg_4,
    AVG(ubs.pct_of_team_pf) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_pf_rolling_avg_4,
    AVG(ubs.pct_of_team_pfd) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_pfd_rolling_avg_4,
    AVG(ubs.pct_of_team_pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_of_team_pts_rolling_avg_4,
    AVG(sbs.pct_fga_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_fga_2pt_rolling_avg_4, /* Scoring stats 4-game rolling averages */
    AVG(sbs.pct_fga_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_fga_3pt_rolling_avg_4,
    AVG(sbs.pct_pts_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_pts_2pt_rolling_avg_4,
    AVG(sbs.pct_pts_midrange_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_pts_midrange_2pt_rolling_avg_4,
    AVG(sbs.pct_pts_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_pts_3pt_rolling_avg_4,
    AVG(sbs.pct_pts_fastbreak) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_pts_fastbreak_rolling_avg_4,
    AVG(sbs.pct_pts_ft) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_pts_ft_rolling_avg_4,
    AVG(sbs.pct_pts_off_tov) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_pts_off_tov_rolling_avg_4,
    AVG(sbs.pct_pts_in_paint) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_pts_in_paint_rolling_avg_4,
    AVG(sbs.pct_assisted_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_assisted_2pt_rolling_avg_4,
    AVG(sbs.pct_unassisted_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_unassisted_2pt_rolling_avg_4,
    AVG(sbs.pct_assisted_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_assisted_3pt_rolling_avg_4,
    AVG(sbs.pct_unassisted_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_unassisted_3pt_rolling_avg_4,
    AVG(sbs.pct_assisted_fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_assisted_fgm_rolling_avg_4,
    AVG(sbs.pct_unassisted_fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) AS pct_unassisted_fgm_rolling_avg_4,
    (
      AVG(tbs.min) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) - AVG(tbs.min) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding)
    ) AS min_delta_4, /* Performance deltas (4-game window vs season) */
    (
      AVG(abs.usage_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) - AVG(abs.usage_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding)
    ) AS usage_pct_delta_4,
    (
      AVG(tbs.pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) - AVG(tbs.pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding)
    ) AS pts_delta_4,
    (
      AVG(tbs.ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) - AVG(tbs.ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding)
    ) AS ast_delta_4,
    (
      AVG(tbs.reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) - AVG(tbs.reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding)
    ) AS reb_delta_4,
    (
      AVG(abs.eff_fg_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) - AVG(abs.eff_fg_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding)
    ) AS eff_fg_pct_delta_4,
    (
      AVG(abs.ts_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 4 preceding AND 1 preceding) - AVG(abs.ts_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding)
    ) AS ts_pct_delta_4,
    'with trend_calc as (
        select 
            *,
            row_number() over (
                partition by tbs.player_id
                order by tbs.game_date
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            tbs.min,
            row_num
        ) over (
            partition by tbs.player_id
            order by tbs.game_date
            rows between 5 preceding and 1 preceding
        ) as trend
    from trend_calc' AS min_trend_6, /* Performance trends (6-game window) */ /* Use CTE approach to avoid nested window functions */ /* This generates SQL like:
    with numbered_data as (
        select 
            tbs.min,
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    'with trend_calc as (
        select 
            *,
            row_number() over (
                partition by tbs.player_id
                order by tbs.game_date
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            abs.usage_pct,
            row_num
        ) over (
            partition by tbs.player_id
            order by tbs.game_date
            rows between 5 preceding and 1 preceding
        ) as trend
    from trend_calc' AS usage_pct_trend_6, /* Use CTE approach to avoid nested window functions */ /* This generates SQL like:
    with numbered_data as (
        select 
            abs.usage_pct,
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    'with trend_calc as (
        select 
            *,
            row_number() over (
                partition by tbs.player_id
                order by tbs.game_date
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            tbs.pts,
            row_num
        ) over (
            partition by tbs.player_id
            order by tbs.game_date
            rows between 5 preceding and 1 preceding
        ) as trend
    from trend_calc' AS pts_trend_6, /* Use CTE approach to avoid nested window functions */ /* This generates SQL like:
    with numbered_data as (
        select 
            tbs.pts,
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    'with trend_calc as (
        select 
            *,
            row_number() over (
                partition by tbs.player_id
                order by tbs.game_date
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            tbs.ast,
            row_num
        ) over (
            partition by tbs.player_id
            order by tbs.game_date
            rows between 5 preceding and 1 preceding
        ) as trend
    from trend_calc' AS ast_trend_6, /* Use CTE approach to avoid nested window functions */ /* This generates SQL like:
    with numbered_data as (
        select 
            tbs.ast,
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    'with trend_calc as (
        select 
            *,
            row_number() over (
                partition by tbs.player_id
                order by tbs.game_date
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            tbs.reb,
            row_num
        ) over (
            partition by tbs.player_id
            order by tbs.game_date
            rows between 5 preceding and 1 preceding
        ) as trend
    from trend_calc' AS reb_trend_6, /* Use CTE approach to avoid nested window functions */ /* This generates SQL like:
    with numbered_data as (
        select 
            tbs.reb,
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    'with trend_calc as (
        select 
            *,
            row_number() over (
                partition by tbs.player_id
                order by tbs.game_date
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            abs.eff_fg_pct,
            row_num
        ) over (
            partition by tbs.player_id
            order by tbs.game_date
            rows between 5 preceding and 1 preceding
        ) as trend
    from trend_calc' AS eff_fg_pct_trend_6, /* Use CTE approach to avoid nested window functions */ /* This generates SQL like:
    with numbered_data as (
        select 
            abs.eff_fg_pct,
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    'with trend_calc as (
        select 
            *,
            row_number() over (
                partition by tbs.player_id
                order by tbs.game_date
            ) as row_num
        from __dbt__CTE__src_data
    )
    
    select 
        regr_slope(
            abs.ts_pct,
            row_num
        ) over (
            partition by tbs.player_id
            order by tbs.game_date
            rows between 5 preceding and 1 preceding
        ) as trend
    from trend_calc' AS ts_pct_trend_6, /* Use CTE approach to avoid nested window functions */ /* This generates SQL like:
    with numbered_data as (
        select 
            abs.ts_pct,
            row_number() over(...) as row_num
        from ...
    )
    select regr_slope(metric_col, row_num) over(...) from numbered_data
    */
    AVG(tbs.pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pts_rolling_avg_8, /* Rolling 8-game averages */
    AVG(tbs.min) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS min_rolling_avg_8,
    AVG(tbs.fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS fgm_rolling_avg_8,
    AVG(tbs.fga) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS fga_rolling_avg_8,
    CAST(NULLIF(
      SUM(tbs.fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fga) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding),
      0
    ) AS fg_pct_rolling_avg_8,
    AVG(tbs.fg3m) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS fg3m_rolling_avg_8,
    AVG(tbs.fg3a) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS fg3a_rolling_avg_8,
    CAST(NULLIF(
      SUM(tbs.fg3m) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fg3a) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding),
      0
    ) AS fg3_pct_rolling_avg_8,
    AVG(tbs.ftm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ftm_rolling_avg_8,
    AVG(tbs.fta) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS fta_rolling_avg_8,
    CAST(NULLIF(
      SUM(tbs.ftm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fta) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding),
      0
    ) AS ft_pct_rolling_avg_8,
    AVG(tbs.off_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS off_reb_rolling_avg_8,
    AVG(tbs.def_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS def_reb_rolling_avg_8,
    AVG(tbs.reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS reb_rolling_avg_8,
    AVG(tbs.ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ast_rolling_avg_8,
    AVG(tbs.stl) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS stl_rolling_avg_8,
    AVG(tbs.blk) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS blk_rolling_avg_8,
    AVG(tbs.tov) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS tov_rolling_avg_8,
    AVG(tbs.pf) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pf_rolling_avg_8,
    AVG(tbs.plus_minus) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS plus_minus_rolling_avg_8,
    AVG(abs.est_off_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS est_off_rating_rolling_avg_8, /* Advanced stats 8-game rolling averages */
    AVG(abs.off_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS off_rating_rolling_avg_8,
    AVG(abs.est_def_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS est_def_rating_rolling_avg_8,
    AVG(abs.def_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS def_rating_rolling_avg_8,
    AVG(abs.est_net_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS est_net_rating_rolling_avg_8,
    AVG(abs.net_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS net_rating_rolling_avg_8,
    AVG(abs.ast_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ast_pct_rolling_avg_8,
    AVG(abs.ast_to_tov_ratio) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ast_to_tov_ratio_rolling_avg_8,
    AVG(abs.ast_ratio) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ast_ratio_rolling_avg_8,
    AVG(abs.off_reb_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS off_reb_pct_rolling_avg_8,
    AVG(abs.def_reb_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS def_reb_pct_rolling_avg_8,
    AVG(abs.reb_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS reb_pct_rolling_avg_8,
    AVG(abs.tov_ratio) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS tov_ratio_rolling_avg_8,
    AVG(abs.eff_fg_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS eff_fg_pct_rolling_avg_8,
    AVG(abs.ts_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ts_pct_rolling_avg_8,
    AVG(abs.usage_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS usage_pct_rolling_avg_8,
    AVG(abs.est_usage_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS est_usage_pct_rolling_avg_8,
    AVG(abs.est_pace) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS est_pace_rolling_avg_8,
    AVG(abs.pace) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pace_rolling_avg_8,
    AVG(abs.pace_per_40) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pace_per_40_rolling_avg_8,
    AVG(abs.possessions) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS possessions_rolling_avg_8,
    AVG(abs.pie) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pie_rolling_avg_8,
    AVG(hbs.cont_shots) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS cont_shots_rolling_avg_8, /* Hustle stats 8-game rolling averages */
    AVG(hbs.cont_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS cont_2pt_rolling_avg_8,
    AVG(hbs.cont_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS cont_3pt_rolling_avg_8,
    AVG(hbs.deflections) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS deflections_rolling_avg_8,
    AVG(hbs.charges_drawn) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS charges_drawn_rolling_avg_8,
    AVG(hbs.screen_ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS screen_ast_rolling_avg_8,
    AVG(hbs.screen_ast_pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS screen_ast_pts_rolling_avg_8,
    AVG(hbs.off_loose_balls_rec) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS off_loose_balls_rec_rolling_avg_8,
    AVG(hbs.def_loose_balls_rec) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS def_loose_balls_rec_rolling_avg_8,
    AVG(hbs.tot_loose_balls_rec) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS tot_loose_balls_rec_rolling_avg_8,
    AVG(hbs.off_box_outs) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS off_box_outs_rolling_avg_8,
    AVG(hbs.def_box_outs) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS def_box_outs_rolling_avg_8,
    AVG(hbs.box_out_team_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS box_out_team_reb_rolling_avg_8,
    AVG(hbs.box_out_player_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS box_out_player_reb_rolling_avg_8,
    AVG(hbs.tot_box_outs) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS tot_box_outs_rolling_avg_8,
    AVG(ubs.pct_of_team_fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_fgm_rolling_avg_8, /* Usage stats 8-game rolling averages */
    AVG(ubs.pct_of_team_fga) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_fga_rolling_avg_8,
    AVG(ubs.pct_of_team_fg3m) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_fg3m_rolling_avg_8,
    AVG(ubs.pct_of_team_fg3a) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_fg3a_rolling_avg_8,
    AVG(ubs.pct_of_team_ftm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_ftm_rolling_avg_8,
    AVG(ubs.pct_of_team_fta) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_fta_rolling_avg_8,
    AVG(ubs.pct_of_team_oreb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_oreb_rolling_avg_8,
    AVG(ubs.pct_of_team_dreb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_dreb_rolling_avg_8,
    AVG(ubs.pct_of_team_reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_reb_rolling_avg_8,
    AVG(ubs.pct_of_team_ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_ast_rolling_avg_8,
    AVG(ubs.pct_of_team_tov) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_tov_rolling_avg_8,
    AVG(ubs.pct_of_team_stl) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_stl_rolling_avg_8,
    AVG(ubs.pct_of_team_blk) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_blk_rolling_avg_8,
    AVG(ubs.pct_of_team_blk_allowed) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_blk_allowed_rolling_avg_8,
    AVG(ubs.pct_of_team_pf) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_pf_rolling_avg_8,
    AVG(ubs.pct_of_team_pfd) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_pfd_rolling_avg_8,
    AVG(ubs.pct_of_team_pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_of_team_pts_rolling_avg_8,
    AVG(sbs.pct_fga_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_fga_2pt_rolling_avg_8, /* Scoring stats 8-game rolling averages */
    AVG(sbs.pct_fga_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_fga_3pt_rolling_avg_8,
    AVG(sbs.pct_pts_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_pts_2pt_rolling_avg_8,
    AVG(sbs.pct_pts_midrange_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_pts_midrange_2pt_rolling_avg_8,
    AVG(sbs.pct_pts_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_pts_3pt_rolling_avg_8,
    AVG(sbs.pct_pts_fastbreak) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_pts_fastbreak_rolling_avg_8,
    AVG(sbs.pct_pts_ft) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_pts_ft_rolling_avg_8,
    AVG(sbs.pct_pts_off_tov) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_pts_off_tov_rolling_avg_8,
    AVG(sbs.pct_pts_in_paint) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_pts_in_paint_rolling_avg_8,
    AVG(sbs.pct_assisted_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_assisted_2pt_rolling_avg_8,
    AVG(sbs.pct_unassisted_2pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_unassisted_2pt_rolling_avg_8,
    AVG(sbs.pct_assisted_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_assisted_3pt_rolling_avg_8,
    AVG(sbs.pct_unassisted_3pt) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_unassisted_3pt_rolling_avg_8,
    AVG(sbs.pct_assisted_fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_assisted_fgm_rolling_avg_8,
    AVG(sbs.pct_unassisted_fgm) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pct_unassisted_fgm_rolling_avg_8,
    STDDEV(tbs.pts) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pts_rolling_std_dev_8, /* Rolling 8-game standard deviations */
    STDDEV(tbs.min) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS min_rolling_std_dev_8,
    STDDEV(tbs.reb) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS reb_rolling_std_dev_8,
    STDDEV(tbs.ast) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ast_rolling_std_dev_8,
    STDDEV(tbs.plus_minus) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS plus_minus_rolling_std_dev_8,
    STDDEV(abs.off_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS off_rating_rolling_std_dev_8,
    STDDEV(abs.def_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS def_rating_rolling_std_dev_8,
    STDDEV(abs.net_rating) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS net_rating_rolling_std_dev_8,
    STDDEV(abs.eff_fg_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS eff_fg_pct_rolling_std_dev_8,
    STDDEV(abs.ts_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS ts_pct_rolling_std_dev_8,
    STDDEV(abs.usage_pct) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS usage_pct_rolling_std_dev_8,
    STDDEV(abs.pace) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS pace_rolling_std_dev_8,
    STDDEV(abs.possessions) OVER (PARTITION BY tbs.player_id ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 8 preceding AND 1 preceding) AS possessions_rolling_std_dev_8,
    AVG(tbs.pts) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pts_season_avg, /* Season-to-date averages (partition by player_id and season_year) */
    AVG(tbs.min) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS min_season_avg,
    AVG(tbs.fgm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS fgm_season_avg,
    AVG(tbs.fga) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS fga_season_avg,
    CAST(NULLIF(
      SUM(tbs.fgm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN UNBOUNDED preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fga) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN UNBOUNDED preceding AND 1 preceding),
      0
    ) AS fg_pct_season_avg,
    AVG(tbs.fg3m) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS fg3m_season_avg,
    AVG(tbs.fg3a) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS fg3a_season_avg,
    CAST(NULLIF(
      SUM(tbs.fg3m) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN UNBOUNDED preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fg3a) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN UNBOUNDED preceding AND 1 preceding),
      0
    ) AS fg3_pct_season_avg,
    AVG(tbs.ftm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS ftm_season_avg,
    AVG(tbs.fta) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS fta_season_avg,
    CAST(NULLIF(
      SUM(tbs.ftm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN UNBOUNDED preceding AND 1 preceding),
      0
    ) AS DOUBLE PRECISION) / NULLIF(
      SUM(tbs.fta) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN UNBOUNDED preceding AND 1 preceding),
      0
    ) AS ft_pct_season_avg,
    AVG(tbs.off_reb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS off_reb_season_avg,
    AVG(tbs.def_reb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS def_reb_season_avg,
    AVG(tbs.reb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS reb_season_avg,
    AVG(tbs.ast) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS ast_season_avg,
    AVG(tbs.stl) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS stl_season_avg,
    AVG(tbs.blk) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS blk_season_avg,
    AVG(tbs.tov) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS tov_season_avg,
    AVG(tbs.pf) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pf_season_avg,
    AVG(tbs.plus_minus) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS plus_minus_season_avg,
    AVG(abs.est_off_rating) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS est_off_rating_season_avg, /* Advanced stats season-to-date averages */
    AVG(abs.off_rating) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS off_rating_season_avg,
    AVG(abs.est_def_rating) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS est_def_rating_season_avg,
    AVG(abs.def_rating) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS def_rating_season_avg,
    AVG(abs.est_net_rating) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS est_net_rating_season_avg,
    AVG(abs.net_rating) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS net_rating_season_avg,
    AVG(abs.ast_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS ast_pct_season_avg,
    AVG(abs.ast_to_tov_ratio) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS ast_to_tov_ratio_season_avg,
    AVG(abs.ast_ratio) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS ast_ratio_season_avg,
    AVG(abs.off_reb_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS off_reb_pct_season_avg,
    AVG(abs.def_reb_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS def_reb_pct_season_avg,
    AVG(abs.reb_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS reb_pct_season_avg,
    AVG(abs.tov_ratio) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS tov_ratio_season_avg,
    AVG(abs.eff_fg_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS eff_fg_pct_season_avg,
    AVG(abs.ts_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS ts_pct_season_avg,
    AVG(abs.usage_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS usage_pct_season_avg,
    AVG(abs.est_usage_pct) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS est_usage_pct_season_avg,
    AVG(abs.est_pace) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS est_pace_season_avg,
    AVG(abs.pace) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pace_season_avg,
    AVG(abs.pace_per_40) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pace_per_40_season_avg,
    AVG(abs.possessions) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS possessions_season_avg,
    AVG(abs.pie) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pie_season_avg,
    AVG(hbs.cont_shots) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS cont_shots_season_avg, /* Hustle stats season-to-date averages */
    AVG(hbs.cont_2pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS cont_2pt_season_avg,
    AVG(hbs.cont_3pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS cont_3pt_season_avg,
    AVG(hbs.deflections) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS deflections_season_avg,
    AVG(hbs.charges_drawn) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS charges_drawn_season_avg,
    AVG(hbs.screen_ast) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS screen_ast_season_avg,
    AVG(hbs.screen_ast_pts) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS screen_ast_pts_season_avg,
    AVG(hbs.off_loose_balls_rec) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS off_loose_balls_rec_season_avg,
    AVG(hbs.def_loose_balls_rec) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS def_loose_balls_rec_season_avg,
    AVG(hbs.tot_loose_balls_rec) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS tot_loose_balls_rec_season_avg,
    AVG(hbs.off_box_outs) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS off_box_outs_season_avg,
    AVG(hbs.def_box_outs) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS def_box_outs_season_avg,
    AVG(hbs.box_out_team_reb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS box_out_team_reb_season_avg,
    AVG(hbs.box_out_player_reb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS box_out_player_reb_season_avg,
    AVG(hbs.tot_box_outs) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS tot_box_outs_season_avg,
    AVG(ubs.pct_of_team_fgm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_fgm_season_avg, /* Usage stats season-to-date averages */
    AVG(ubs.pct_of_team_fga) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_fga_season_avg,
    AVG(ubs.pct_of_team_fg3m) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_fg3m_season_avg,
    AVG(ubs.pct_of_team_fg3a) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_fg3a_season_avg,
    AVG(ubs.pct_of_team_ftm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_ftm_season_avg,
    AVG(ubs.pct_of_team_fta) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_fta_season_avg,
    AVG(ubs.pct_of_team_oreb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_oreb_season_avg,
    AVG(ubs.pct_of_team_dreb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_dreb_season_avg,
    AVG(ubs.pct_of_team_reb) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_reb_season_avg,
    AVG(ubs.pct_of_team_ast) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_ast_season_avg,
    AVG(ubs.pct_of_team_tov) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_tov_season_avg,
    AVG(ubs.pct_of_team_stl) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_stl_season_avg,
    AVG(ubs.pct_of_team_blk) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_blk_season_avg,
    AVG(ubs.pct_of_team_blk_allowed) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_blk_allowed_season_avg,
    AVG(ubs.pct_of_team_pf) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_pf_season_avg,
    AVG(ubs.pct_of_team_pfd) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_pfd_season_avg,
    AVG(ubs.pct_of_team_pts) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_of_team_pts_season_avg,
    AVG(sbs.pct_fga_2pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_fga_2pt_season_avg, /* Scoring stats season-to-date averages */
    AVG(sbs.pct_fga_3pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_fga_3pt_season_avg,
    AVG(sbs.pct_pts_2pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_pts_2pt_season_avg,
    AVG(sbs.pct_pts_midrange_2pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_pts_midrange_2pt_season_avg,
    AVG(sbs.pct_pts_3pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_pts_3pt_season_avg,
    AVG(sbs.pct_pts_fastbreak) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_pts_fastbreak_season_avg,
    AVG(sbs.pct_pts_ft) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_pts_ft_season_avg,
    AVG(sbs.pct_pts_off_tov) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_pts_off_tov_season_avg,
    AVG(sbs.pct_pts_in_paint) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_pts_in_paint_season_avg,
    AVG(sbs.pct_assisted_2pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_assisted_2pt_season_avg,
    AVG(sbs.pct_unassisted_2pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_unassisted_2pt_season_avg,
    AVG(sbs.pct_assisted_3pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_assisted_3pt_season_avg,
    AVG(sbs.pct_unassisted_3pt) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_unassisted_3pt_season_avg,
    AVG(sbs.pct_assisted_fgm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_assisted_fgm_season_avg,
    AVG(sbs.pct_unassisted_fgm) OVER (PARTITION BY tbs.player_id, tbs.season_year ORDER BY tbs.game_date NULLS FIRST rows BETWEEN 9999 preceding AND 1 preceding) AS pct_unassisted_fgm_season_avg
  FROM player_games AS pg
  LEFT JOIN traditional_boxscores AS tbs
    ON pg.player_game_key = tbs.player_game_key
  LEFT JOIN advanced_boxscores AS abs
    ON pg.player_game_key = abs.player_game_key
  LEFT JOIN hustle_boxscores AS hbs
    ON pg.player_game_key = hbs.player_game_key
  LEFT JOIN usage_boxscores AS ubs
    ON pg.player_game_key = ubs.player_game_key
  LEFT JOIN scoring_boxscores AS sbs
    ON pg.player_game_key = sbs.player_game_key
  LEFT JOIN game_context AS gc
    ON pg.player_game_key = gc.player_game_key
  LEFT JOIN team_game_context AS tgc
    ON pg.game_id = tgc.game_id
)
SELECT
  *
FROM traditional_rolling_features
-- Safeguard filter to ensure only data from the specified start year onwards is included
WHERE cast(substring(season_year from 1 for 4) as integer) >= {{ var('training_start_season_year') }}
ORDER BY
  player_id NULLS FIRST,
  game_date NULLS FIRST