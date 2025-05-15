{% docs team_context_features %}

# Team Context Feature Architecture

This documentation explains the architecture of team-related features for player performance prediction.

## Model Structure

The architecture follows this hierarchy:

1. `feat_team__performance_metrics` - Base model for team performance and outcomes
2. `feat_team__rolling_stats` - Rolling window statistics for team metrics
3. `feat_team__shot_distribution` - Shot distribution and playing style analysis
4. `feat_team__usage_distribution` - Usage distribution across team players
5. `feat_player__team_interaction` - Player performance in team context
6. `feat_player__contextual_projections` - Impact of team factors on player stats
7. `marts__team_context_features` - Consolidated team context features for prediction

## Feature Categories

The team context features are organized into these categories:

* **Team Performance** - Overall team quality metrics (ratings, pace, etc.)
* **Team Form** - Recent team performance trends and streaks
* **Team Style** - Shot distribution and playing style characteristics
* **Team Structure** - Usage distribution and offensive hierarchy
* **Player-Team Fit** - How well the player's skills match team needs
* **Context Multipliers** - Quantified team impact on player stats

## Team Impact on Player Stats

The model quantifies how team context affects player stats through several multipliers:

1. **Pace Multiplier** - How team pace affects counting stats
2. **Usage Multiplier** - How team usage distribution affects opportunities
3. **Form Multiplier** - How team winning/losing streaks affect roles
4. **Playstyle Multiplier** - How team style boosts specific stats
5. **Style Fit Multiplier** - How well player skills match team needs

These multipliers combine to create adjusted statistical projections that account for the complete team environment.

{% enddocs %}