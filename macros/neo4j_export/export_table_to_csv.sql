{% macro export_table_to_csv(model_name, output_path) %}
    {% set query %}
        -- Get the table schema and name
        {% set relation = ref(model_name) %}
        {% set columns = adapter.get_columns_in_relation(relation) %}
        
        -- Build the COPY command with all columns
        COPY (
            SELECT 
                {% for column in columns %}
                "{{ column.name }}"
                {% if not loop.last %},{% endif %}
                {% endfor %}
            FROM {{ relation }}
        )
        TO '{{ output_path }}/{{ model_name }}.csv' 
        WITH CSV HEADER;
    {% endset %}
    
    {% do run_query(query) %}
    
    {{ log("Exported " ~ model_name ~ " to " ~ output_path ~ "/" ~ model_name ~ ".csv", info=True) }}
    
    -- Return success message
    {{ return("Exported " ~ model_name ~ " to CSV file") }}
{% endmacro %} 