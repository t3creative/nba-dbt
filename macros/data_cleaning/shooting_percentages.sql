{% macro clean_shooting_percentage(pct_column) %}
    /*
    Cleans and validates shooting percentage values
    Parameters:
        pct_column (str): Column containing the shooting percentage
    Returns:
        Normalized percentage value between 0 and 1
    */
    case
        when {{ pct_column }} is null then null
        when {{ pct_column }} > 1 and {{ pct_column }} <= 100 then {{ pct_column }} / 100.0
        when {{ pct_column }} < 0 then 0
        when {{ pct_column }} > 1 then 1
        else {{ pct_column }}
    end
{% endmacro %}