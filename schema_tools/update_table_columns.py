#!/usr/bin/env python
"""
Script to update existing tables in the allowlist with column information from Snowflake.
This is useful for enriching the allowlist with schema information without re-adding tables.
"""

import json
import argparse
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

def save_allowlist(allowlist, file_path="table_allowlist.json"):
    """Save the table allowlist to a JSON file"""
    try:
        with open(file_path, 'w') as f:
            json.dump(allowlist, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving allowlist: {e}")
        return False

def fetch_columns_from_snowflake(database, schema, table):
    """Fetch column information from Snowflake"""
    if not SNOWFLAKE_AVAILABLE:
        print("Error: snowflake.connector package not available. Cannot fetch columns from Snowflake.")
        return None
    
    try:
        params = get_snowflake_credentials()
        print(f"Connecting to Snowflake with account: {params['account']}, user: {params['user']}, database: {params['database']}")
        
        # Convert to uppercase for Snowflake
        database_upper = database.upper()
        schema_upper = schema.upper()
        table_upper = table.upper()
        
        with snowflake.connector.connect(**params) as conn:
            query = f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE
            FROM 
                {database_upper}.INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = '{schema_upper}'
                AND TABLE_NAME = '{table_upper}'
            ORDER BY ORDINAL_POSITION
            """
            
            print(f"Executing query: {query}")
            cursor = conn.cursor()
            cursor.execute(query)
            
            columns = {}
            result = cursor.fetchall()
            print(f"Found {len(result)} columns")
            
            for col_name, data_type in result:
                columns[col_name] = data_type
                
            return columns
    except Exception as e:
        print(f"Error fetching columns from Snowflake: {e}")
        print(f"Query attempted: SELECT COLUMN_NAME, DATA_TYPE FROM {database_upper}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_upper}' AND TABLE_NAME = '{table_upper}'")
        return None

def update_table_with_columns(table_name=None, schema_name=None, database_name=None, all_tables=False):
    """Update tables in the allowlist with column information"""
    if not SNOWFLAKE_AVAILABLE:
        print("Error: snowflake.connector package not available. Cannot update columns.")
        return False
    
    # Load the allowlist
    allowlist = load_allowlist()
    
    # Track updates
    updated_tables = []
    
    # Process each table in the allowlist
    for i, table_info in enumerate(allowlist):
        table_info_db = table_info.get("database", "EDW")
        table_info_schema = table_info.get("schema", "")
        table_info_name = table_info.get("table", "")
        
        # Skip if not the specified table and not updating all tables
        if not all_tables and (
            (table_name and table_info_name.lower() != table_name.lower()) or
            (schema_name and table_info_schema.lower() != schema_name.lower()) or
            (database_name and table_info_db.lower() != database_name.lower())
        ):
            continue
        
        print(f"Fetching columns for {table_info_db}.{table_info_schema}.{table_info_name}...")
        
        # Fetch columns from Snowflake
        columns = fetch_columns_from_snowflake(table_info_db, table_info_schema, table_info_name)
        
        if columns:
            # Update the table entry with columns
            allowlist[i]["columns"] = columns
            updated_tables.append(f"{table_info_db}.{table_info_schema}.{table_info_name}")
            print(f"  Added {len(columns)} columns")
        else:
            print(f"  Failed to fetch columns")
    
    # Save the updated allowlist
    if updated_tables:
        if save_allowlist(allowlist):
            print(f"\nSuccessfully updated {len(updated_tables)} tables with column information:")
            for table in updated_tables:
                print(f"  {table}")
            return True
        else:
            print("\nFailed to save the updated allowlist.")
            return False
    else:
        print("\nNo tables were updated.")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Update existing tables in the allowlist with column information')
    parser.add_argument('--database', help='Database name to filter tables')
    parser.add_argument('--schema', help='Schema name to filter tables')
    parser.add_argument('--table', help='Table name to update')
    parser.add_argument('--all', action='store_true', help='Update all tables in the allowlist')
    
    args = parser.parse_args()
    
    if not args.all and not (args.table or args.schema or args.database):
        print("Error: You must specify a table, schema, database, or use --all flag.")
        parser.print_help()
        return 1
    
    success = update_table_with_columns(
        table_name=args.table,
        schema_name=args.schema,
        database_name=args.database,
        all_tables=args.all
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 