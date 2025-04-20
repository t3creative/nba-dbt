# NBA Data Model: Feature Categories

This document provides a comprehensive overview of various feature categories used in NBA data modeling and machine learning.

## Table of Contents
- [Categorical Encoding Methods](#categorical-encoding-methods)
- [Aggregations & Grouped Statistics Features](#aggregations--grouped-statistics-features)
- [Interaction Features](#interaction-features)
- [Time-Based Features](#time-based-features)
- [Normalization & Standardization Methods](#normalization--standardization-methods)
- [Outlier Handling & Missing Value Imputation](#outlier-handling--missing-value-imputation)

## Categorical Encoding Methods

| Encoding Type | Best For | Pros | Cons |
|---------------|----------|------|------|
| One-Hot Encoding | Low-cardinality categories (≤10 unique values) | Interpretable, simple | Increases dimensionality |
| Label Encoding | Ordinal categories (e.g., rankings) | Keeps relationships intact | Not for nominal data |
| Target Encoding | High-cardinality, strong correlation with target | Captures relationships | Risk of data leakage |
| Frequency Encoding | High-cardinality categories | Reduces dimensionality | Loses relationship with target |
| Binary Encoding | Moderate-cardinality categories (10-50 values) | Reduces feature space | Less interpretable |
| Embedding Encoding | Deep learning models, very high-cardinality data | Captures complex relationships | Requires training |

## Aggregations & Grouped Statistics Features

### Player/Team Averages

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Player Season Averages | Overall performance baseline | AVG(Points) OVER (SEASON) |
| Player Recent Form Averages | Tracks short-term trends | AVG(Points) OVER (LAST 5 GAMES) |
| Rolling Team Offensive Rating | Team scoring efficiency | Team Points Per 100 Possessions |
| Rolling Team Defensive Rating | Opponent scoring efficiency | Opponent Points Per 100 Possessions |
| Team Pace | Game tempo measure | Team Possessions Per Game |

### Opponent-Based Aggregates

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Player Performance vs. This Opponent | Tracks past performance against a team | AVG(Points) OVER (LAST 3 GAMES VS TEAM X) |
| Opponent Defensive Rating | How strong is the opposing defense? | Opponent Points Allowed Per 100 Possessions |
| Opponent PPG Allowed to Position | Defensive effectiveness vs. player's position | Opponent PPG Allowed to PGs/SGs/SFs/PFs/Cs |
| Opponent Pace | More possessions = more stats | Opponent Possessions Per Game |
| Opponent Foul Draw Rate | Helps predict FT opportunities | Opponent FT Attempts Allowed Per Game |
| Opponent 3PA Allowed Per Game | Measures 3-point defense | Opponent 3PA Allowed Per Game |
| Opponent 3P% Allowed | Measures defensive effectiveness against threes | Opponent 3P% Allowed |

### Team Performance Context

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Player Stats in Close Games | Performance in clutch situations | AVG(Player Stats) WHEN Spread <= 5 |
| Player Stats in Blowouts | Impact of garbage time | AVG(Minutes Played) WHEN Spread > 15 |
| Performance in High-Pace Games | Who benefits from fast tempo? | AVG(Stats) WHEN Team Pace > 100 |
| Performance in Low-Pace Games | Who struggles in slow games? | AVG(Stats) WHEN Team Pace < 95 |

### Strength of Schedule

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Player Stats vs. Bottom 10 Defenses | Performance vs. weak teams | AVG(Stats) WHEN Opponent_Def_Rank > 20 |
| Player Stats vs. Top 10 Defenses | Performance vs. elite defenses | AVG(Stats) WHEN Opponent_Def_Rank <= 10 |

## Interaction Features

### Feature Ratios

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Points per Minute (PPM) | Scoring efficiency per minute | Points / Minutes |
| Assists per Minute (APM) | Playmaking efficiency per minute | Assists / Minutes |
| Rebounds per Minute (RPM) | Rebounding efficiency per minute | Rebounds / Minutes |
| Points per Possession (PPP) | Adjusts scoring for possessions played | Points / Possessions Played |
| Usage Rate (USG%) | Measures offensive involvement | (FGA + 0.44 * FTA + TO) / Team Possessions |
| Assist to Turnover Ratio (AST/TO) | Playmaking efficiency | Assists / Turnovers |
| Rebound Percentage (REB%) | % of available rebounds grabbed | Player REB / (Team REB + Opponent REB) |
| True Shooting Percentage (TS%) | Scoring efficiency considering free throws | PTS / (2 * (FGA + 0.44 * FTA)) |
| Effective FG% (eFG%) | Weights 3PT shots appropriately | (FGM + 0.5 * 3PM) / FGA |

### Multiplicative Features

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Usage Rate * TS% | Efficiency at high usage | USG% * TS% |
| Pace * Points per Possession | Adjusts scoring for game tempo | Team Pace * PPP |
| Minutes * Usage Rate | Expected offensive involvement | Minutes * USG% |
| Assists * Team Pace | Adjusts assists based on possessions | AST * Team Pace |
| Points * Opponent Defensive Rating | Scoring adjustment for opponent quality | Points * Opponent DefRtg |
| Rebounds * Opponent REB% Allowed | Adjusts rebounding based on matchup | REB * Opponent REB% Allowed |
| 3PTA * Opponent 3PA Allowed | Shot volume adjusted for opponent defense | 3PTA * Opponent 3PA Allowed |

### Difference Features

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Points Difference (Last Game vs. Avg) | Short-term overperformance | Points_Lag-1 - RollingAvg(Points, Last 10) |
| Minutes Difference (Last Game vs. Avg) | Detects playing time trends | Minutes_Lag-1 - RollingAvg(Minutes, Last 10) |
| Usage Rate Change (Last 5 Games) | Captures role increase/decrease | USG%_Last5 - USG%_Season |
| TS% Change (Last 5 Games) | Efficiency trend tracking | TS%_Last5 - TS%_Season |
| Home vs. Away Performance Delta | Identifies home-court advantage | Points_Home - Points_Away |
| Opponent Defensive Rating Change | Recent opponent strength tracking | OpponentDefRtg_Last5 - OpponentDefRtg_Season |

### Game Context

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Pace * Spread (Close Game Impact) | Adjusts pace for competitive games | Team Pace * (1 / Absolute(Spread)) |
| Minutes * Rest Days | Captures fatigue effects | Minutes * RestDays |
| 3PTA * Opponent 3PA Allowed | Shot volume adjusted for opponent tendencies | 3PTA * Opponent 3PA Allowed |
| Rebounds * Opponent REB% Allowed | Rebounding adjusted for matchup strength | REB * Opponent REB% Allowed |
| Usage Rate * Star Teammate Missing | Adjusts role based on injuries | USG% * (1 if Star Player Out, else 0) |

## Time-Based Features

### Game-Specific Temporal

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Days Since Last Game (Rest Days) | Tracks how much rest a player has had between games | Game Date - Previous Game Date |
| Back-to-Back Games Indicator | 1 if the player played the previous day, 0 otherwise | IF(Days Rest = 0, 1, 0) |
| 3rd Game in 4 Nights Indicator | 1 if this is the player's third game in four nights | Custom logic based on game schedule |
| Rest Advantage | Difference in rest days between player and opponent | Player Days Rest - Opponent Days Rest |

### Seasonal & Streak

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Rolling Standard Deviation | Measures performance volatility | STDDEV(points) OVER (LAST 5 GAMES) |
| Hot Streak Indicator | 1 if a player has scored above their season average in 3 straight games | IF(AVG(Last 3 Games) > Season Avg, 1, 0) |
| Cold Streak Indicator | 1 if a player has shot under 40% FG for 3 straight games | IF(AVG(Last 3 FG%) < 40, 1, 0) |
| Fatigue Factor | Last 5-game rolling avg of minutes played | AVG(Minutes) OVER (LAST 5 GAMES) |
| Month of the Year | Encodes time-of-season trends | EXTRACT(MONTH FROM Game Date) |

### Opponent-Specific

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Opponent Defensive Rating | Strength of opposing defense | Opponent Points Allowed Per 100 Possessions |
| Opponent Position-Specific Defense | How well does the opponent defend against a player's position? | Opponent PPG Allowed to Position |
| Past Performance vs. This Opponent | Player's last 3-game rolling avg vs. this team | AVG(points) OVER (LAST 3 GAMES VS THIS TEAM) |

### Game Environment

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Game Pace | Measures possessions per game | Team Possessions Per Game |
| Home/Away Splits | Rolling averages and lags based on home/away games | Separate home/away rolling averages |
| Vegas Total & Spread | Projected game total points and spread | Game Total, Point Spread (Vegas Line) |

### Advanced Rolling Trends

| Feature Name | Description | Example Calculation |
|--------------|-------------|---------------------|
| Exponential Weighted Moving Avg (EWMA) | More weight to recent games in rolling avg | EWMA(points, alpha=0.8) |
| Rolling Percent Change | Rate of change compared to previous rolling avg | (Current Game - Rolling Avg) / Rolling Avg |

## Normalization & Standardization Methods

| Method | Description | Best Use Case | Example Calculation |
|--------|-------------|---------------|---------------------|
| Z-Score Standardization | Centers data at mean 0, std 1 | Best for normally distributed features | (X - AVG(X)) / STDDEV(X) |
| Min-Max Scaling | Scales values between 0 and 1 | Useful for bounded features (e.g., percentages) | (X - MIN(X)) / (MAX(X) - MIN(X)) |
| Robust Scaling | Uses median & IQR to reduce outlier influence | Best for datasets with extreme values | (X - MEDIAN(X)) / IQR(X) |
| Log Transformation | Reduces right-skewed distributions | Best for highly skewed data | LOG(X + 1) |
| Decimal Scaling | Moves decimal to bring large numbers into a manageable range | Useful for financial or large-scale data | X / 10^j (where j is chosen to make \|X\| < 1) |
| Unit Vector Scaling | Scales each row to unit norm (L2 = 1) | Used when magnitude of features matters | X / \|\|X\|\| (Euclidean norm) |

## Outlier Handling & Missing Value Imputation

### Outlier Detection

| Method | Description | Example Calculation |
|--------|-------------|---------------------|
| Z-Score Method | Flags values that are k standard deviations from the mean | Z = (X - μ) / σ (If \|Z\| > threshold, it's an outlier) |
| Interquartile Range (IQR) | Flags values that fall outside 1.5x IQR from Q1/Q3 | IQR = Q3 - Q1, Outliers if X < Q1 - 1.5*IQR or X > Q3 + 1.5*IQR |
| Percentile Capping (Winsorizing) | Caps values at the 1st and 99th percentile | CASE WHEN X > P99 THEN P99 ELSE X END |
| Domain-Specific Filtering | Removes values based on sports-specific logic | CASE WHEN Minutes < 5 THEN NULL END (e.g., ignore garbage time stats) |

### Outlier Handling

| Method | Description | Example Calculation |
|--------|-------------|---------------------|
| Remove Outliers | If outliers are errors or anomalies | DELETE FROM table WHERE Z > 3 |
| Cap (Winsorize) Outliers | When extreme values skew model performance | UPDATE table SET X = P99 WHERE X > P99 |
| Transform (Log/Power Transform) | When outliers are skewed but important | LOG(X + 1) |
| Treat Separately (Create Binary Flag) | If outliers contain predictive value | CASE WHEN X > P99 THEN 1 ELSE 0 END |

### Missing Value Imputation

| Method | Description | Example Calculation |
|--------|-------------|---------------------|
| Mean Imputation | Works for normally distributed data | COALESCE(Points, AVG(Points) OVER ()) |
| Median Imputation | Works for skewed data / outliers | COALESCE(Points, PERCENTILE_CONT(0.5) OVER ()) |
| Mode Imputation | Works for categorical features | COALESCE(Position, MODE() OVER ()) |
| Forward Fill (Last Known Value) | Best for time-series (e.g., injuries, rolling averages) | LAG(Points, 1) OVER (PARTITION BY Player ORDER BY Game Date) |
| Interpolation (Linear, Polynomial) | Best for time-series trends | (Next Value + Prev Value) / 2 |
| Regression Imputation | Predict missing values using other variables | Predict missing values using ML model |
| Indicator Variable for Missingness | When missingness itself is predictive | CASE WHEN Points IS NULL THEN 1 ELSE 0 END AS Missing_Flag | 