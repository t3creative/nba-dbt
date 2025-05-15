{% docs opponent_feature_architecture %}

# Opponent Feature Architecture

This documentation explains the architecture of opponent-related features for player performance prediction.

## Model Structure

The architecture follows this hierarchy:

1. `int_opp__game_opponents` - Base model establishing team-opponent relationships
2. `int_opp__opponent_stats` - Enriched opponent stats from boxscores
3. `feat_opp__opponent_pregame_profile` - Rolling window statistics for opponents
4. `feat_opp__position_defense_profile` - Position-specific defensive metrics
5. `feat_opp__player_vs_opponent_history` - Historical player performance vs specific opponents
6. `feat_opp__player_position_matchup` - Combines position and opponent defense profiles
7. `feat_opp___player_vs_opponent` - Consolidated prediction-ready features

## Feature Categories

The prediction features are organized into these categories:

* **Player Form** - Recent performance metrics of the player
* **Opponent Team** - Overall defensive profile of the opponent team
* **Position Matchup** - How the opponent performs against the player's position
* **Historical Matchup** - How this specific player has performed against this opponent
* **Team Context** - Team dynamics that may affect the player's performance
* **Game Context** - Situational factors like home/away, rest days, etc.

## Blended Projections

The `blended_pts_projection`, `blended_reb_projection`, and `blended_ast_projection` fields 
combine multiple feature sets with weighted importance based on the quality of historical data.

{% enddocs %}