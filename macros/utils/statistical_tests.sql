{% macro test_statistical_validity(model, column_name, grouping_column, min_samples=5) %}
    /*
    Tests if a metric column has statistical validity by checking if there
    are enough samples per group and the standard deviation isn't extreme
    
    Parameters:
        model: Model to test
        column_name: Column containing the metric to validate
        grouping_column: Column to group by (e.g., player_id)
        min_samples: Minimum number of samples required per group
    */
    
    with grouped_stats as (
        select
            {{ grouping_column }} as grouping_key,
            count(*) as sample_count,
            avg({{ column_name }}) as avg_value,
            stddev({{ column_name }}) as std_value,
            case 
                when avg({{ column_name }}) = 0 then null
                else stddev({{ column_name }}) / nullif(avg({{ column_name }}), 0)
            end as cv_value -- coefficient of variation
        from {{ model }}
        where {{ column_name }} is not null
        group by {{ grouping_column }}
    )
    
    select *
    from grouped_stats
    where sample_count < {{ min_samples }}
      or cv_value > 2.0 -- coefficient of variation threshold
{% endmacro %}