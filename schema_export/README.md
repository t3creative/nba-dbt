# NBA Database Schema Export

This directory contains scripts to export schema information from the NBA data PostgreSQL database, focusing on the following layers:
- Staging layer (`stg_*`)
- Intermediate layer (`int_*`)
- Features layer
- Training layer

## Files

- `export_schemas.sql`: SQL query for extracting schema information directly from PostgreSQL
- `export_schemas.py`: Python script to export schema information to YAML and JSON files
- `run_export.ps1`: PowerShell script to run the export process automatically

## Requirements

- Python 3.6+
- PostgreSQL database connection
- Required Python packages:
  - psycopg2
  - pyyaml

## Usage

### Option 1: Using PowerShell Script (Recommended)

1. Make sure your database connection details are set in environment variables or in the `.env` file in the project root:
   - `DB_USER`: Database username
   - `DB_PASSWORD`: Database password
   - `DB_HOST`: Database host (default: localhost)
   - `DB_PORT`: Database port (default: 5432)

2. Run the PowerShell script:
   ```
   cd schema_export
   .\run_export.ps1
   ```

The script will:
- Check if Python and required packages are installed
- Load environment variables
- Run the export script

### Option 2: Running Python Script Directly

1. Set the required environment variables:
   ```
   $env:DB_USER = "your_username"
   $env:DB_PASSWORD = "your_password"
   $env:DB_HOST = "localhost"  # optional
   $env:DB_PORT = "5432"  # optional
   ```

2. Run the Python script:
   ```
   cd schema_export
   python export_schemas.py
   ```

### Option 3: Manually Running SQL Query

1. Connect to your database using psql or any PostgreSQL client
2. Run the query in `export_schemas.sql`
3. Save the results as needed

## Output

The scripts will create subdirectories for each schema within the `schema_export` directory, and generate files with the following naming pattern:

- YAML files: `{table_name}_{date}.yml`
- JSON files: `{table_name}_{date}.json`

Each file contains:
- Schema name
- Table name
- Column details (name, data type, length, default, etc.)
- Export timestamp

## Customization

- Modify `export_schemas.py` if you need to change output formats or add additional information
- Adjust the SQL query in both files if you need to capture different schema information 