{% macro decimal_odds_to_probability(decimal_odds) %}
    case
        when {{ decimal_odds }} is null then null
        when {{ decimal_odds }} > 1.0 then 1.0 / {{ decimal_odds }}
        else null
    end
{% endmacro %} 