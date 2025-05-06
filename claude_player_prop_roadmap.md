## 3. Feature Engineering Formulas

### Odds Conversion Methodologies

**American to Decimal Odds:**
```sql
case 
  when american_odds > 0 then (american_odds / 100.0) + 1
  when american_odds < 0 then (100.0 / abs(american_odds)) + 1
  else null
end as decimal_odds
```

**Decimal to Implied Probability:**
```sql
(1 / decimal_odds) as implied_probability
```

**American to Implied Probability (Direct):**
```sql
case 
  when american_odds > 0 then 100.0 / (american_odds + 100)
  when american_odds < 0 then abs(american_odds) / (abs(american_odds) + 100)
  else null
end as implied_probability
```

**Removing Vig from Market:**
```sql
-- Calculate total implied probability (with vig)
over_implied_prob + under_implied_prob as total_implied_prob,

-- Calculate fair probabilities (removing vig)
over_implied_prob / (over_implied_prob + under_implied_prob) as fair_over_prob,
under_implied_prob / (over_implied_prob + under_implied_prob) as fair_under_prob,

-- Convert back to American odds
case 
  when (over_implied_prob / (over_implied_prob + under_implied_prob)) < 0.5 
  then ((1 / (over_implied_prob / (over_implied_prob + under_implied_prob))) - 1) * 100
  else -100 / ((1 / (over_implied_prob / (over_implied_prob + under_implied_prob))) - 1)
end as fair_over_odds
```

### Line Movement Calculations

**Time-Weighted Line Movement:**
```sql
-- Calculate time-weighted line movement (more recent changes have higher weight)
sum(
  line_change * (1.0 / (1.0 + extract(epoch from (current_timestamp - recorded_at))/86400))
) / 
sum(1.0 / (1.0 + extract(epoch from (current_timestamp - recorded_at))/86400)) 
as time_weighted_line_movement
```

**Market Sentiment Indicator:**
```sql
-- Positive value: market trending toward over
-- Negative value: market trending toward under
sum(case 
  when line_change > 0 then 1
  when line_change < 0 then -1
  else 0
end * (extract(epoch from (recorded_at - lag_recorded_at))/3600)) / 
sum(extract(epoch from (recorded_at - lag_recorded_at))/3600) 
as market_sentiment_indicator
```

### Market Comparison Metrics

**Sportsbook Line Variance:**
```sql
-- Measure of disagreement between sportsbooks
stddev(line) over (partition by prop_key) as line_variance,

-- Distance from consensus
line - avg(line) over (partition by prop_key) as line_vs_consensus
```

**Value Indicator:**
```sql
-- Positive: potential over value
-- Negative: potential under value
(avg_stat_value - line) / stddev_stat_value as standardized_value_indicator
```

### Historical Performance vs. Line

**Performance Edge:**
```sql
-- Positive: player tends to exceed the line
-- Negative: player tends to fall short of the line
avg(stat_value - line) as performance_edge
```

**Consistency Metric:**
```sql
1 - (stddev(stat_value - line) / avg(stat_value)) as consistency_score
```

### Categorical Encoding Strategies

**Team Strength Encoding:**
```sql
-- Calculate team strength rating by category
rank() over (
  order by avg(defensive_rating) 
) as defensive_rank,

ntile(5) over (
  order by avg(defensive_rating)
) as defensive_quintile
```

**Market Type Encoding:**
```sql
-- Map categorical market to numeric feature
case 
  when market = 'Points O/U' then 1
  when market = 'Rebounds O/U' then 2
  when market = 'Assists O/U' then 3
  when market = 'Threes O/U' then 4
  when market = 'Pts+Reb O/U' then 5
  when market = 'Pts+Ast O/U' then 6
  when market = 'Reb+Ast O/U' then 7
  when market = 'Pts+Reb+Ast O/U' then 8
  when market = 'Steals O/U' then 9
  when market = 'Blocks O/U' then 10
  else 99
end as market_type_encoded
```

## 4. Implementation Plan

### Phase 1: Foundation (Weeks 1-2)

**Staging Layer**
- Create `stg__betting__player_props`
- Create `stg__betting__player_props_with_timestamps`
- Add appropriate tests for data quality

**Core Features**
- Implement basic relationship models with player/team entities
- Create foundation schema YML files with documentation

### Phase 2: Market Analysis (Weeks 3-4)

**Intermediate Layer**
- Implement `int__player_props_normalized`
- Implement `int__betting__player_props_daily_snapshot`
- Build `int__betting__player_props_line_movement`

**Analysis Models**
- Create exploratory models for market analysis
- Implement descriptive statistics for different market types

### Phase 3: Feature Engineering (Weeks 5-6)

**Market Features**
- Build `player_prop_market_features`
- Implement `player_prop_market_time_series_features`
- Create `player_prop_market_sportsbook_features`

**Statistical Integration**
- Connect with player performance data
- Implement player form features

### Phase 4: ML Preparation (Weeks 7-8)

**Training Data**
- Build `training__player_prop_prediction`
- Create test datasets for model evaluation
- Implement historical backtesting framework

**Optimization**
- Performance tuning for large datasets
- Implement incremental strategies for performance

## 5. Evaluation Framework

### Cross-Validation Approach

**Time-Series Split:**
```sql
-- Create time-based folds for validation
select
  prop_key,
  player_id,
  market_id,
  game_date,
  ntile(5) over (order by game_date) as fold_number,
  *
from training_data
```

**Feature Importance Assessment:**
```sql
-- Calculate feature importance using correlation with outcome
select
  feature_name,
  corr(feature_value, case when outcome = 'OVER' then 1 else 0 end) as correlation_with_over,
  abs(corr(feature_value, case when outcome = 'OVER' then 1 else 0 end)) as abs_correlation
from model_results
group by feature_name
order by abs_correlation desc
```

### Backtesting Metrics

**ROI Calculation:**
```sql
-- Calculate ROI by market type
select
  market,
  sum(case 
    when prediction = 'OVER' and outcome = 'OVER' then (over_odds/100) 
    when prediction = 'UNDER' and outcome = 'UNDER' then (under_odds/100)
    when prediction = 'OVER' and outcome != 'OVER' then -1
    when prediction = 'UNDER' and outcome != 'UNDER' then -1
    else 0
  end) / count(*) as roi,
  count(*) as sample_size
from backtest_results
group by market
order by roi desc
```

**Edge Detection:**
```sql
-- Calculate edge by prediction confidence level
select
  ntile(10) over (order by prediction_probability) as confidence_decile,
  avg(case when prediction = outcome then 1 else 0 end) as accuracy,
  count(*) as samples
from backtest_results
group by confidence_decile
order by confidence_decile
```

## 6. Performance Benchmarks

### Market-Specific Baselines

| Market Type | Baseline Win Rate | Target Win Rate |
|-------------|-------------------|----------------|
| Points O/U  | 52.4%             | 55.0%+         |
| Rebounds O/U| 53.1%             | 56.0%+         |
| Assists O/U | 53.7%             | 56.5%+         |
| Threes O/U  | 54.2%             | 57.0%+         |
| Combined    | 51.9%             | 54.5%+         |

### Value Identification Metrics

- Minimum 3% edge required for bet recommendation
- Minimum 60% historical win rate in similar scenarios
- Maximum variance threshold for consistency requirement

## 7. Future Enhancements

1. **Real-time Line Movement Tracking**
   - Implement webhook integration for immediate updates
   - Create alert system for significant movement

2. **Player Injury Impact Modeling**
   - Quantify line adjustments based on teammate injuries
   - Model usage rate changes in absence of key players

3. **Advanced Market Integrations**
   - Incorporate alternate lines with adjusted odds
   - Model correlations between related player props

4. **Referee Impact Analysis**
   - Quantify referee tendencies by statistical category
   - Incorporate referee assignments into prediction model

The transformation roadmap provides a comprehensive approach to building an effective player prop betting prediction system. By following this structured implementation plan and leveraging PostgreSQL's advanced features, you can create a robust analytics pipeline that transforms raw betting data into actionable insights for predictive modeling.