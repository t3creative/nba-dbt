{% macro test_points_distribution(model, points_column, grouping_column) %}
    /*
    Tests if player/team points distributions fall within reasonable NBA ranges
    */
    select *
    from {{ model }}
    where {{ points_column }} > 150 -- No player has scored over 150 points
       or {{ points_column }} < 0
{% endmacro %}

{% macro test_shooting_percentage_validity(model, percentage_column) %}
    /*
    Tests if shooting percentages are valid (between 0 and 1)
    */
    select *
    from {{ model }}
    where {{ percentage_column }} > 1
       or {{ percentage_column }} < 0
       or {{ percentage_column }} is null
{% endmacro %}

{% macro test_minutes_validity(model, minutes_column) %}
    /*
    Tests if minutes played fall within valid ranges for NBA games
    */
    select *
    from {{ model }}
    where {{ minutes_column }} > 63 -- Max possible with multiple OTs
       or {{ minutes_column }} < 0
{% endmacro %}