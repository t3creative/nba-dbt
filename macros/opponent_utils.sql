{% macro get_opponent_ctes(source_model, cte_name, metric_cols) %}
-- Static key columns for team and opponent CTEs
{% set key_cols = ['team_game_key','team_id','season_year','game_date'] %}
-- CTE for team metrics from the given intermediate model
{{ cte_name }} as (
    select
        {{ key_cols | join(', ') }},
        {{ metric_cols | join(', ') }}
    from {{ ref(source_model) }}
),
-- Mirror CTE for opponent metrics using a self-join
{{ cte_name }}_opp as (
    select
        base.team_game_key,
        {% for col in metric_cols %}
        opp.{{ col }} as opp_{{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ ref(source_model) }} base
    join {{ ref(source_model) }} opp
      on base.game_id = opp.game_id
      and base.opponent_id = opp.team_id
)
{% endmacro %} 