-- American Odds to Decimal Odds conversion
{% macro american_to_decimal(american_odds) %}
  case
    when {{ american_odds }} is null then null
    when {{ american_odds }} > 0 then ({{ american_odds }} / 100.0) + 1.0
    when {{ american_odds }} < 0 then (100.0 / abs({{ american_odds }})) + 1.0
    else null
  end
{% endmacro %}

-- Decimal Odds to Implied Probability conversion
{% macro decimal_to_probability(decimal_odds) %}
  case
    when {{ decimal_odds }} is null then null
    when {{ decimal_odds }} > 0 then 1.0 / {{ decimal_odds }}
    else null
  end
{% endmacro %}

-- Calculate No-Vig Probability (true probability)
{% macro no_vig_probability(prob1, prob2) %}
  case
    when {{ prob1 }} is null or {{ prob2 }} is null then null
    when ({{ prob1 }} + {{ prob2 }}) > 0 then {{ prob1 }} / ({{ prob1 }} + {{ prob2 }})
    else null
  end
{% endmacro %}

-- Direct conversion from American odds to implied probability
{% macro american_to_probability(american_odds) %}
  case
    when {{ american_odds }} is null then null
    when {{ american_odds }} > 0 then 100.0 / ({{ american_odds }} + 100.0)
    when {{ american_odds }} < 0 then abs({{ american_odds }}) / (abs({{ american_odds }}) + 100.0)
    else null
  end
{% endmacro %} 