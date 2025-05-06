#!/usr/bin/env python
"""
Script to export schema information from PostgreSQL database tables
and write them to structured files in the schema_export directory.
Includes tables from staging, intermediate, features, and training layers.
"""
import os
import psycopg2
import json
from collections import defaultdict
import yaml
from datetime import datetime

# Database connection parameters - replace with your actual connection details
# These should be loaded from environment variables in production
conn_params = {
    "dbname": "nba_data",  # replace with your actual database name
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432")
}

def connect_to_db():
    """Connect to the PostgreSQL database and return connection."""
    try:
        conn = psycopg2.connect(**conn_params)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def get_schemas():
    """Query database for tables and their schema information."""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    query = """
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
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results

def organize_schema_data(schema_data):
    """Organize schema data by schema and table."""
    organized_data = defaultdict(lambda: defaultdict(list))
    
    for row in schema_data:
        schema_name = row[0]
        table_name = row[1]
        column_info = {
            "column_name": row[2],
            "data_type": row[3],
            "character_maximum_length": row[4],
            "column_default": row[5],
            "is_nullable": row[6],
            "udt_name": row[7]
        }
        
        organized_data[schema_name][table_name].append(column_info)
    
    return organized_data

def export_to_yaml(data):
    """Export schema data to YAML files."""
    timestamp = datetime.now().strftime("%Y%m%d")
    
    for schema_name, tables in data.items():
        # Create schema directory if it doesn't exist
        schema_dir = os.path.join("schema_export", schema_name)
        os.makedirs(schema_dir, exist_ok=True)
        
        # Export each table's schema to a YAML file
        for table_name, columns in tables.items():
            filename = f"{table_name}_{timestamp}.yml"
            filepath = os.path.join(schema_dir, filename)
            
            table_schema = {
                "schema": schema_name,
                "table": table_name,
                "columns": columns,
                "export_date": datetime.now().isoformat()
            }
            
            with open(filepath, 'w') as f:
                yaml.dump(table_schema, f, default_flow_style=False, sort_keys=False)
            
            print(f"Exported schema for {schema_name}.{table_name} to {filepath}")

def export_to_json(data):
    """Export schema data to JSON files."""
    timestamp = datetime.now().strftime("%Y%m%d")
    
    for schema_name, tables in data.items():
        # Create schema directory if it doesn't exist
        schema_dir = os.path.join("schema_export", schema_name)
        os.makedirs(schema_dir, exist_ok=True)
        
        # Export each table's schema to a JSON file
        for table_name, columns in tables.items():
            filename = f"{table_name}_{timestamp}.json"
            filepath = os.path.join(schema_dir, filename)
            
            table_schema = {
                "schema": schema_name,
                "table": table_name,
                "columns": columns,
                "export_date": datetime.now().isoformat()
            }
            
            with open(filepath, 'w') as f:
                json.dump(table_schema, f, indent=2)
            
            print(f"Exported schema for {schema_name}.{table_name} to {filepath}")

def main():
    """Main function to run the script."""
    try:
        print("Fetching schema information from database...")
        schema_data = get_schemas()
        
        print("Organizing schema data...")
        organized_data = organize_schema_data(schema_data)
        
        print("Exporting schema data to YAML files...")
        export_to_yaml(organized_data)
        
        print("Exporting schema data to JSON files...")
        export_to_json(organized_data)
        
        print("Schema export completed successfully.")
    except Exception as e:
        print(f"Error exporting schemas: {e}")

if __name__ == "__main__":
    main() 