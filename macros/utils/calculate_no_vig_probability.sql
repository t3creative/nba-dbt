{% macro calculate_no_vig_probability(over_prob, under_prob) %}
    case
        when {{ over_prob }} is null or {{ under_prob }} is null then null
        when ({{ over_prob }} + {{ under_prob }}) > 0 then {{ over_prob }} / ({{ over_prob }} + {{ under_prob }})
        else null
    end
{% endmacro %} 