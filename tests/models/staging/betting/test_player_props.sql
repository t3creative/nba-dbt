-- Test to ensure implied probabilities are calculated correctly
with test_data as (
    select
        prop_id,
        over_odds,
        under_odds,
        over_implied_probability,
        under_implied_probability
    from {{ ref('stg__player_props') }}
    where over_odds is not null and under_odds is not null
),

calculated_probs as (
    select
        prop_id,
        over_odds,
        under_odds,
        -- Recalculate the implied probabilities
        case 
            when over_odds > 0 then 100.0 / (over_odds + 100)
            when over_odds < 0 then abs(over_odds) / (abs(over_odds) + 100)
            else null
        end as expected_over_implied_probability,
        case 
            when under_odds > 0 then 100.0 / (under_odds + 100)
            when under_odds < 0 then abs(under_odds) / (abs(under_odds) + 100)
            else null
        end as expected_under_implied_probability,
        over_implied_probability,
        under_implied_probability
    from test_data
),

validation as (
    select
        prop_id,
        over_implied_probability,
        expected_over_implied_probability,
        under_implied_probability,
        expected_under_implied_probability,
        -- Check for significant differences in calculated values
        abs(over_implied_probability - expected_over_implied_probability) < 0.0001 as over_prob_matches,
        abs(under_implied_probability - expected_under_implied_probability) < 0.0001 as under_prob_matches
    from calculated_probs
)

-- Return any rows where the calculated probabilities don't match expected values
select * from validation
where not (over_prob_matches and under_prob_matches) 