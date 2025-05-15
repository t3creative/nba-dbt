{% macro calculate_milestone_flags(points_column, thresholds=[30, 40]) %}
  {% for threshold in thresholds %}
    case when {{ points_column }} >= {{ threshold }} then 1 else 0 end as is_{{ threshold }}_plus_game
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}

{% macro career_stat(player_id, date_column, measure, agg_type='sum') %}
{{ agg_type }}({{ measure }}) over (
    partition by {{ player_id }} 
    order by {{ date_column }}
    rows between unbounded preceding and current row
)
{% endmacro %}

{% macro days_since_milestone(game_date, milestone_date) %}
case 
    when {{ milestone_date }} is not null and {{ game_date }} > {{ milestone_date }} 
    then extract(day from ({{ game_date }}::timestamp - {{ milestone_date }}::timestamp))
    else null 
end
{% endmacro %}

{% macro incremental_date_filter(date_column, lookback_days=0) %}
{% if is_incremental() %}
    {% set table_exists_query %}
        select case when exists (
            select 1 
            from information_schema.tables 
            where table_schema = '{{ this.schema }}'
            and table_name = '{{ this.identifier }}'
        ) then 1 else 0 end as table_exists
    {% endset %}
    
    {% set table_exists = run_query(table_exists_query).columns[0][0] %}
    
    {% if table_exists == 1 %}
        {% set columns_query %}
            select column_name
            from information_schema.columns
            where table_schema = '{{ this.schema }}'
            and table_name = '{{ this.identifier }}'
            and column_name = '{{ date_column.split(".")[-1] }}'
        {% endset %}
        
        {% set columns = run_query(columns_query) %}
        
        {% if columns.columns[0]|length > 0 %}
            {% set max_date_query %}
                select 
                {% if lookback_days > 0 %}
                    max({{ date_column.split(".")[-1] }}) - INTERVAL '{{ lookback_days }} days'
                {% else %}
                    max({{ date_column.split(".")[-1] }})
                {% endif %}
                from {{ this }}
            {% endset %}
            
            {% set max_date = run_query(max_date_query).columns[0][0] %}
            {% if max_date is not none %}
                where {{ date_column }} > '{{ max_date }}'
            {% endif %}
        {% endif %}
    {% endif %}
{% endif %}
{% endmacro %}

{% macro usage_rate(fga, fta, min) %}
({{ fga }} + 0.44 * {{ fta }}) / nullif({{ min }}, 0) * 100
{% endmacro %} 