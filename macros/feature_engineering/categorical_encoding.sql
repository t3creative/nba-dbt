-- =============================================================================
-- CATEGORICAL FEATURE ENGINEERING MACROS
-- These macros help generate features by encoding categorical variables into
-- numerical formats suitable for machine learning models
-- =============================================================================

{# 
    One-Hot Encoding Parameters:
    - column_name: Name of the column to encode
    - category_values: List of possible values for the category
    - prefix: Prefix for the generated column names (optional)
    - exclude_first: Whether to exclude first category to prevent multicollinearity
#}
{% macro one_hot_encode(
    column_name,
    category_values=none,
    prefix=none,
    exclude_first=false
) %}
    {%- set prefix = prefix if prefix else column_name -%}
    
    {%- if category_values is none -%}
        {{ exceptions.raise_compiler_error("category_values must be provided") }}
    {%- endif -%}
    
    {%- set columns = [] -%}
    
    {%- for value in category_values -%}
        {%- if not loop.first or not exclude_first -%}
            {%- set column = prefix ~ '_' ~ value | lower | replace(' ', '_') -%}
            {%- do columns.append(column) -%}
            case when {{ column_name }} = '{{ value }}' then 1 else 0 end as {{ column }}
            {%- if not loop.last -%},{% endif %}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}

{# 
    Binary Encoding Parameters:
    - column_name: Name of the column to encode
    - true_value: Value to encode as 1
    - false_value: Value to encode as 0
    - new_column_name: Name for the new binary column (optional)
#}
{% macro binary_encode(
    column_name,
    true_value,
    false_value,
    new_column_name=none
) %}
    {%- set new_name = new_column_name if new_column_name else column_name ~ '_binary' -%}
    
    case 
        when {{ column_name }} = '{{ true_value }}' then 1
        when {{ column_name }} = '{{ false_value }}' then 0
        else null
    end as {{ new_name }}
{% endmacro %}

{# 
    Create Dummy Variables Parameters:
    - column_name: Name of the column to encode
    - source_relation: Source table/view to get unique values from
    - prefix: Prefix for the generated column names (optional)
    - exclude_first: Whether to exclude first category (default: false)
    - max_categories: Maximum number of categories to encode (default: 50)
#}
{% macro create_dummy_variables(
    column_name,
    source_relation,
    prefix=none,
    exclude_first=false,
    max_categories=50
) %}
    {%- set prefix = prefix if prefix else column_name -%}
    
    {%- set category_query -%}
        select distinct {{ column_name }}
        from {{ source_relation }}
        where {{ column_name }} is not null
        order by {{ column_name }}
        limit {{ max_categories }}
    {%- endset -%}
    
    {%- set results = run_query(category_query) -%}
    
    {%- if execute -%}
        {%- set categories = results.columns[0].values() -%}
        {{ one_hot_encode(column_name, categories, prefix, exclude_first) }}
    {%- endif -%}
{% endmacro %} 