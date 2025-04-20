{% macro test_check_home_away_matchup(model) %}
    -- Test that home/away is correctly derived from matchup
    -- Returns rows where is_home_game is null but matchup contains vs. or @
    select *
    from {{ model }}
    where is_home_game is null 
    and (
        matchup like '%vs.%'
        or matchup like '%@%'
    )
{% endmacro %}

{% macro test_check_back_to_back_rest_days(model) %}
    -- Test that back-to-back games have 0 days of rest
    -- Returns rows where is_back_to_back = 1 but days_of_rest > 0
    select *
    from {{ model }}
    where is_back_to_back = 1 
    and days_of_rest > 0
{% endmacro %}

{% macro test_check_season_encoding(model, column_prefix='season_') %}
    -- Test that season encodings are mutually exclusive and complete
    with season_sums as (
        select 
            player_game_id,
            sum(case when column_name like '{{ column_prefix }}%' then column_value else 0 end) as total_encoded
        from {{ model }}
        cross join lateral (
            select column_name, column_value
            from {{ model }} unpivot (column_value for column_name in (*))
            where column_name like '{{ column_prefix }}%'
        ) as unpivoted
        group by player_game_id
    )
    select *
    from season_sums
    where total_encoded != 1
{% endmacro %}

{% macro test_check_team_encoding(model, column_prefix='team_') %}
    -- Test that team encodings are mutually exclusive and complete
    with team_sums as (
        select 
            player_game_id,
            sum(case when column_name like '{{ column_prefix }}%' then column_value else 0 end) as total_encoded
        from {{ model }}
        cross join lateral (
            select column_name, column_value
            from {{ model }} unpivot (column_value for column_name in (*))
            where column_name like '{{ column_prefix }}%'
        ) as unpivoted
        group by player_game_id
    )
    select *
    from team_sums
    where total_encoded != 1
{% endmacro %} 