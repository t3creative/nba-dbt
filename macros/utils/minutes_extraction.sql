{% macro extract_minutes(column_name) %}
    /*
    Extracts and converts NBA minutes format (MM:SS) to decimal minutes
    Parameters:
        column_name (str): Column containing minutes in format like '24:30'
    Returns: 
        Decimal minutes (e.g., 24.5 for '24:30')
    */
    case
        when {{ column_name }} is null then 0
        when {{ column_name }} = '' then 0
        when {{ column_name }} ~ '^\d+:\d{2}$' then
            cast(split_part({{ column_name }}::text, ':', 1) as decimal) +
            cast(split_part({{ column_name }}::text, ':', 2) as decimal) / 60
        else 
            case 
                when {{ column_name }} ~ '^\d+(\.\d+)?$' then cast({{ column_name }} as decimal)
                else 0
            end
    end
{% endmacro %}