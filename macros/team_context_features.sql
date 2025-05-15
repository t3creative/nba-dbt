{% macro generate_team_context_features(player_table, team_table, lookback_periods=[5, 10, 'season']) %}
    
WITH player_data AS (
    SELECT * FROM {{ player_table }}
),

team_data AS (
    SELECT * FROM {{ team_table }}
),

player_with_team AS (
    SELECT
        pd.player_game_key,
        pd.player_id,
        pd.team_id,
        pd.game_date,
        td.off_rating,
        td.def_rating,
        td.pace,
        
        {% for period in lookback_periods %}
        -- Generate team rolling metrics for each period
        AVG(td.off_rating) OVER(
            PARTITION BY td.team_id 
            ORDER BY td.game_date 
            {% if period != 'season' %}
            ROWS BETWEEN {{ period }} PRECEDING AND 1 PRECEDING
            {% else %}
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            {% endif %}
        ) AS team_{{ period }}_off_rating,
        
        AVG(td.def_rating) OVER(
            PARTITION BY td.team_id 
            ORDER BY td.game_date 
            {% if period != 'season' %}
            ROWS BETWEEN {{ period }} PRECEDING AND 1 PRECEDING
            {% else %}
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            {% endif %}
        ) AS team_{{ period }}_def_rating,
        
        AVG(td.pace) OVER(
            PARTITION BY td.team_id 
            ORDER BY td.game_date 
            {% if period != 'season' %}
            ROWS BETWEEN {{ period }} PRECEDING AND 1 PRECEDING
            {% else %}
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            {% endif %}
        ) AS team_{{ period }}_pace
        {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM player_data pd
    JOIN team_data td
        ON pd.team_id = td.team_id AND pd.game_date = td.game_date
)

SELECT * FROM player_with_team
    
{% endmacro %}