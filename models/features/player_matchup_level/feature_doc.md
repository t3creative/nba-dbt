# Player Matchup Features Documentation

This document details the features calculated in the `player_matchup_features.sql` model, which focuses on quantifying player-vs-player effectiveness based on matchup data.

---

## 1. `matchup_points_allowed_per_100_poss`

### High-Level Insight
Measures how many points an offensive player scored against a specific defender, normalized per 100 possessions they were matched up against each other. Essentially, it's the defender's points allowed rate in that specific matchup context.

### Real-World Impact
- **Evaluating Individual Defense:** Helps identify which defenders are effective at limiting scoring from specific opponents or player archetypes.
- **Game Planning:** Can inform defensive assignments, suggesting which defenders should guard which offensive stars.
- **Player Development:** Highlights areas where a defender might be struggling against certain types of players.

### Calculation & Feature Engineering
- **Formula:** `(Offensive Player Points in Matchup / Partial Possessions in Matchup) * 100`
- **Sources:**
    - `off_player_pts`: Points scored by the offensive player while guarded by this specific defender (from `int__player_matchups_bxsc`).
    - `partial_poss`: Estimated number of possessions the two players were matched up (from `int__player_matchups_bxsc`).
- **Normalization:** Multiplying by 100 normalizes the rate to a standard "per 100 possessions" basis, making it comparable across matchups with different amounts of time played.
- **Handling Zeros:** Uses `COALESCE` and `SAFE_DIVIDE` to prevent division-by-zero errors and defaults to 0 if possessions are zero.

### Value Interpretation (Approximate Guide)
*Note: "Good" and "Poor" are from the *defender's* perspective. Context (like the quality of the offensive player) is crucial.*

- **Great (Defender):** < 80-90 (Significantly limiting the offensive player below typical league efficiency)
- **Good (Defender):** 90 - 105 (Holding the offensive player below or around average efficiency)
- **Okay (Defender):** 105 - 120 (Allowing slightly above-average to average scoring efficiency)
- **Poor (Defender):** > 120 (Struggling significantly to contain the offensive player)

---

## 2. `defensive_field_goal_diff`

### High-Level Insight
Compares the offensive player's Field Goal Percentage (FG%) when guarded by this specific defender against their overall season average FG%. A positive value means the defender held the offensive player *below* their average FG%.

### Real-World Impact
- **Shot Contesting Effectiveness:** Identifies defenders who force opponents into lower-percentage shots compared to their norm.
- **Matchup Difficulty:** Quantifies how much tougher a specific defender makes it for an offensive player to score efficiently.
- **Scouting:** Highlights defenders who excel at disrupting specific scorers.

### Calculation & Feature Engineering
- **Formula:** `Offensive Player Season Avg FG% - Offensive Player Matchup FG%`
- **Sources:**
    - `Offensive Player Season Avg FG%`: Calculated as `SUM(pts) / SUM(fga)` from `player_season_avg_stats` CTE (derived from `int__player_traditional_bxsc`).
    - `Offensive Player Matchup FG%`: Calculated as `off_matchup_fgm / off_matchup_fga` (from `int__player_matchups_bxsc`).
- **Difference:** A direct subtraction shows the delta. Positive values favor the defender.
- **Handling Zeros:** Uses `COALESCE` and `SAFE_DIVIDE` for both calculations to handle zero attempts and defaults the final difference to 0 if inputs are missing.

### Value Interpretation (Approximate Guide)
*Note: Values represent the percentage points *difference*. Positive is good for the defender.*

- **Great (Defender):** > +5% (Defender forces significantly lower FG% than player's average)
- **Good (Defender):** +1% to +5% (Defender forces slightly lower FG%)
- **Okay (Defender):** -1% to +1% (Defender allows roughly the player's average FG%)
- **Poor (Defender):** < -1% (Defender allows a higher FG% than the player's average)

---

## 3. `offensive_matchup_advantage`

### High-Level Insight
Measures how many more (or fewer) points per 100 possessions an offensive player scored against a specific defender compared to their own season average points per 100 possessions. A positive value means the offensive player performed *better* than their average against this defender.

### Real-World Impact
- **Identifying Favorable Matchups:** Helps offensive players/teams spot defenders they historically score efficiently against.
- **Game Planning:** Can guide offensive strategy to attack specific defenders.
- **Player Evaluation:** Shows which defenders an offensive player struggles or excels against.

### Calculation & Feature Engineering
- **Formula:** `Matchup Points per 100 Poss - Offensive Player Season Avg Points per 100 Poss`
- **Sources:**
    - `Matchup Points per 100 Poss`: Calculated as `(off_player_pts / partial_poss) * 100` (from `int__player_matchups_bxsc`).
    - `Offensive Player Season Avg Points per 100 Poss`: Calculated as `(SUM(pts) * 100) / SUM(possessions)` from `player_season_avg_stats` CTE (derived from `int__player_traditional_bxsc` and `int__player_advanced_bxsc`).
- **Difference:** Direct subtraction highlights the performance difference relative to the player's baseline. Positive values favor the offensive player.
- **Handling Zeros:** Uses `COALESCE` and `SAFE_DIVIDE` for underlying calculations and defaults the final difference to 0.

### Value Interpretation (Approximate Guide)
*Note: "Good" and "Poor" are from the *offensive player's* perspective.*

- **Great (Offensive Player):** > +10 (Scoring significantly more efficiently than average against this defender)
- **Good (Offensive Player):** +2 to +10 (Scoring somewhat more efficiently than average)
- **Okay (Offensive Player):** -2 to +2 (Performing roughly around their season average)
- **Poor (Offensive Player):** < -2 (Scoring less efficiently than average against this defender)

--- 