-- SQL script to export table schemas
-- This script queries PostgreSQL information_schema to extract details about tables from staging, intermediate, features, and training layers
-- Results are ordered by schema, table, and column position

SELECT 
    table_schema,
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    column_default,
    is_nullable,
    udt_name
FROM 
    information_schema.columns
WHERE 
    table_schema LIKE 'stg%' OR 
    table_name LIKE 'stg_%' OR
    table_schema LIKE 'int%' OR 
    table_name LIKE 'int_%' OR
    table_schema = 'intermediate' OR
    table_schema = 'features' OR
    table_schema = 'training'
ORDER BY 
    table_schema, 
    table_name, 
    ordinal_position; 