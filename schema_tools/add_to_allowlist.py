#!/usr/bin/env python
"""
Script to add a new table to the table_allowlist.json file.
This script helps ensure all required information is provided
and validates the table against the schema repository.
"""

import json
import argparse
from datetime import datetime
import sys
try:
    import snowflake.connector
    from snowflake_credentials import get_snowflake_credentials
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

def load_allowlist(file_path="table_allowlist.json"):
    """Load the table allowlist from a JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading allowlist: {e}")
        return []
        
def load_schema_repository(file_path="schema_repository.json"):
    """Load the schema repository from a JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading schema repository: {e}")
        return {"verified_tables": []}

def table_in_repository(table_info, repository):
    """Check if a table exists in the schema repository"""
    for repo_table in repository["verified_tables"]:
        if (repo_table["database"].lower() == table_info["database"].lower() and
            repo_table["schema"].lower() == table_info["schema"].lower() and
            repo_table["table"].lower() == table_info["table"].lower()):
            return True
    return False

def table_in_allowlist(table_info, allowlist):
    """Check if a table already exists in the allowlist"""
    for allowed_table in allowlist:
        database_name = allowed_table.get("database", "EDW")
        if (database_name.lower() == table_info["database"].lower() and
            allowed_table["schema"].lower() == table_info["schema"].lower() and
            allowed_table["table"].lower() == table_info["table"].lower()):
            return True
    return False

def add_table_to_allowlist(table_info, file_path="table_allowlist.json"):
    """Add a table to the allowlist"""
    allowlist = load_allowlist(file_path)
    
    # Check if table already exists in the allowlist
    if table_in_allowlist(table_info, allowlist):
        print(f"Table {table_info['database']}.{table_info['schema']}.{table_info['table']} already exists in the allowlist.")
        return False
    
    # Add the table to the allowlist
    allowlist.append(table_info)
    
    # Write back to the file
    with open(file_path, 'w') as f:
        json.dump(allowlist, f, indent=2)
    
    print(f"Added {table_info['database']}.{table_info['schema']}.{table_info['table']} to the allowlist.")
    return True

def add_table_to_repository(table_info, file_path="schema_repository.json"):
    """Add a table to the schema repository if it doesn't already exist"""
    repository = load_schema_repository(file_path)
    
    # Check if table already exists in the repository
    if table_in_repository(table_info, repository):
        return False
    
    # Add the table to the repository
    repo_entry = {
        "database": table_info["database"],
        "schema": table_info["schema"],
        "table": table_info["table"]
    }
    repository["verified_tables"].append(repo_entry)
    
    # Update the last_updated field
    repository["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    
    # Write back to the file
    with open(file_path, 'w') as f:
        json.dump(repository, f, indent=2)
    
    print(f"Added {table_info['database']}.{table_info['schema']}.{table_info['table']} to the schema repository.")
    return True

def fetch_columns_from_snowflake(database, schema, table):
    """Fetch column information from Snowflake"""
    if not SNOWFLAKE_AVAILABLE:
        print("Error: snowflake.connector package not available. Cannot fetch columns from Snowflake.")
        return None
    
    try:
        params = get_snowflake_credentials()
        with snowflake.connector.connect(**params) as conn:
            query = f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE
            FROM 
                {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
            """
            
            cursor = conn.cursor()
            cursor.execute(query)
            
            columns = {}
            for col_name, data_type in cursor.fetchall():
                columns[col_name] = data_type
                
            return columns
    except Exception as e:
        print(f"Error fetching columns from Snowflake: {e}")
        return None

def main():
    """Main function to add a table to the allowlist"""
    parser = argparse.ArgumentParser(description='Add a table to the table_allowlist.json file.')
    parser.add_argument('--database', default="EDW", help='Database name (default: EDW)')
    parser.add_argument('--schema', required=True, help='Schema name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--tier', type=int, choices=[1, 2, 3, 4], default=2, help='Table tier (1-4) indicating reliability and usage priority (default: 2)')
    parser.add_argument('--description', required=True, help='Description of the table')
    parser.add_argument('--notes', help='Additional notes about the table')
    parser.add_argument('--common-joins', nargs='+', help='List of common tables to join with')
    parser.add_argument('--key-columns', nargs='+', help='List of key columns in the table')
    parser.add_argument('--columns', nargs='+', help='List of column:datatype pairs (e.g., "order_id:VARCHAR" "created_at:TIMESTAMP_NTZ")')
    parser.add_argument('--add-to-repository', action='store_true', help='Also add to schema repository if not already there')
    parser.add_argument('--fetch-columns', action='store_true', help='Fetch column information from Snowflake')
    
    args = parser.parse_args()
    
    # Check if the table exists in the schema repository
    repository = load_schema_repository()
    exists_in_repo = table_in_repository({
        "database": args.database,
        "schema": args.schema,
        "table": args.table
    }, repository)
    
    if not exists_in_repo:
        print(f"Warning: Table {args.database}.{args.schema}.{args.table} does not exist in the schema repository.")
        print("This might indicate the table doesn't exist in the database or hasn't been verified.")
        if args.add_to_repository:
            print("Adding to repository since --add-to-repository flag is set.")
            add_table_to_repository({
                "database": args.database,
                "schema": args.schema,
                "table": args.table
            })
        else:
            confirmation = input("Do you want to continue anyway? (y/n): ")
            if confirmation.lower() != 'y':
                print("Aborted.")
                return
    
    # Create the table entry for the allowlist
    table_entry = {
        "table": args.table,
        "schema": args.schema,
        "database": args.database,
        "tier": args.tier,
        "description": args.description
    }
    
    if args.notes:
        table_entry["notes"] = args.notes
    
    if args.common_joins:
        table_entry["common_joins"] = args.common_joins
    
    if args.key_columns:
        table_entry["key_columns"] = args.key_columns
    
    # Fetch columns from Snowflake if requested
    if args.fetch_columns:
        if not SNOWFLAKE_AVAILABLE:
            print("Warning: Cannot fetch columns. snowflake.connector package is not available.")
        else:
            print(f"Fetching columns for {args.database}.{args.schema}.{args.table}...")
            columns = fetch_columns_from_snowflake(args.database, args.schema, args.table)
            if columns:
                table_entry["columns"] = columns
                print(f"Added {len(columns)} columns to the table entry.")
    
    # Process manually specified columns if provided
    if args.columns and "columns" not in table_entry:
        columns_dict = {}
        for col_def in args.columns:
            if ":" in col_def:
                col_name, col_type = col_def.split(":", 1)
                columns_dict[col_name] = col_type
            else:
                print(f"Warning: Skipping invalid column definition '{col_def}'. Format should be 'column:datatype'")
        
        if columns_dict:
            table_entry["columns"] = columns_dict
    
    # Add the table to the allowlist
    add_table_to_allowlist(table_entry)

if __name__ == "__main__":
    main() 