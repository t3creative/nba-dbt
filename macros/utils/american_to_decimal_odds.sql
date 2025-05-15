{% macro american_to_decimal_odds(american_odds) %}
    case
        when {{ american_odds }} is null then null
        when {{ american_odds }} > 0 then ({{ american_odds }} / 100.0) + 1.0
        when {{ american_odds }} < 0 then (100.0 / abs({{ american_odds }})) + 1.0
        else null
    end
{% endmacro %} 