{% macro calculate_per_minute_stats(relation_alias, metrics, minutes_col) %}
  {% for metric in metrics %}
    safe_divide({{ relation_alias }}.{{ metric }}, {{ relation_alias }}.{{ minutes_col }}) as {{ metric }}_per_min
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}

{% macro calculate_possessions(fga_col, fta_col, orb_col, tov_col) %}
    ({{ fga_col }} + (0.44 * {{ fta_col }}) - {{ orb_col }} + {{ tov_col }})
{% endmacro %}

{% macro calculate_per_100_poss_stats(relation_alias, metrics, poss_col) %}
  {% for metric in metrics %}
    safe_divide({{ relation_alias }}.{{ metric }} * 100, {{ relation_alias }}.{{ poss_col }}) as {{ metric }}_per_100_poss
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %} 