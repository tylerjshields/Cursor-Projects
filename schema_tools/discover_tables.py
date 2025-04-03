#!/usr/bin/env python
"""
Snowflake Table Discovery Tool

This script helps discover tables available in Snowflake databases and schemas
to assist in building your table allowlist for documentation.
"""

import snowflake.connector
import sys
import json
import os
from snowflake_credentials import get_snowflake_credentials

def connect_to_snowflake():
    """Connect to Snowflake using credentials from environment variables"""
    try:
        # Get credentials from environment
        params = get_snowflake_credentials()
        conn = snowflake.connector.connect(**params)
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        sys.exit(1)

def list_databases(cursor):
    """List all accessible databases"""
    query = """
    SHOW DATABASES;
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n=== Available Databases ===")
        for result in results:
            print(f"- {result[1]}")
        
        return [result[1] for result in results]
    except Exception as e:
        print(f"Error listing databases: {e}")
        return []

def list_schemas(cursor, database):
    """List all schemas in a database"""
    query = f"""
    SHOW SCHEMAS IN DATABASE {database};
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"\n=== Schemas in {database} ===")
        for result in results:
            print(f"- {result[1]}")
        
        return [result[1] for result in results]
    except Exception as e:
        print(f"Error listing schemas in {database}: {e}")
        return []

def list_tables(cursor, database, schema):
    """List all tables in a schema"""
    query = f"""
    SHOW TABLES IN {database}.{schema};
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        if not results:
            print(f"No tables found in {database}.{schema}")
            return []
        
        print(f"\n=== Tables in {database}.{schema} ===")
        for result in results:
            table_name = result[1]
            table_type = result[3]
            created = result[5]
            print(f"- {table_name} ({table_type}, created: {created})")
        
        return [result[1] for result in results]
    except Exception as e:
        print(f"Error listing tables in {database}.{schema}: {e}")
        return []

def search_tables(cursor, search_term):
    """Search for tables matching a search term"""
    query = f"""
    SELECT table_catalog, table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_name LIKE '%{search_term.upper()}%'
    ORDER BY table_catalog, table_schema, table_name;
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        if not results:
            print(f"No tables found matching '{search_term}'")
            return []
        
        print(f"\n=== Tables matching '{search_term}' ===")
        for result in results:
            database = result[0]
            schema = result[1]
            table = result[2]
            print(f"- {database}.{schema}.{table}")
        
        return results
    except Exception as e:
        print(f"Error searching for tables: {e}")
        return []

def add_to_allowlist(tables, allowlist_file="table_allowlist.json"):
    """Add discovered tables to the allowlist file"""
    # Load existing allowlist
    try:
        with open(allowlist_file, 'r') as f:
            allowlist = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        allowlist = []
    
    # Add new tables
    added = 0
    for table_info in tables:
        database, schema, table = table_info
        
        # Check if table already exists in allowlist
        exists = False
        for entry in allowlist:
            if isinstance(entry, dict) and entry.get('table') == table and entry.get('schema') == schema:
                exists = True
                break
        
        if not exists:
            allowlist.append({
                "table": table,
                "schema": schema,
                "description": f"Table from {database}.{schema}"
            })
            added += 1
    
    # Write updated allowlist
    with open(allowlist_file, 'w') as f:
        json.dump(allowlist, f, indent=2)
    
    print(f"\nAdded {added} new tables to {allowlist_file}")
    print(f"Total tables in allowlist: {len(allowlist)}")

def interactive_mode():
    """Run in interactive mode to discover tables"""
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    
    while True:
        print("\n=== Snowflake Table Discovery Tool ===")
        print("1. List databases")
        print("2. List schemas in a database")
        print("3. List tables in a schema")
        print("4. Search for tables by name")
        print("5. Add found tables to allowlist")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ")
        
        if choice == '1':
            list_databases(cursor)
        
        elif choice == '2':
            database = input("Enter database name: ")
            list_schemas(cursor, database)
        
        elif choice == '3':
            database = input("Enter database name: ")
            schema = input("Enter schema name: ")
            list_tables(cursor, database, schema)
        
        elif choice == '4':
            search_term = input("Enter search term: ")
            global search_results
            search_results = search_tables(cursor, search_term)
        
        elif choice == '5':
            if 'search_results' in globals() and search_results:
                add_to_allowlist(search_results)
            else:
                print("No search results available. Please search for tables first.")
        
        elif choice == '6':
            break
        
        else:
            print("Invalid choice. Please try again.")
    
    cursor.close()
    conn.close()
    print("\nThank you for using the Table Discovery Tool.")

def main():
    """Main function"""
    if len(sys.argv) > 1:
        if sys.argv[1] == "search" and len(sys.argv) > 2:
            # Search mode
            conn = connect_to_snowflake()
            cursor = conn.cursor()
            search_results = search_tables(cursor, sys.argv[2])
            
            if len(sys.argv) > 3 and sys.argv[3] == "--add":
                add_to_allowlist(search_results)
            
            cursor.close()
            conn.close()
        elif sys.argv[1] == "help":
            print("Usage:")
            print("  python discover_tables.py               # Interactive mode")
            print("  python discover_tables.py search TERM   # Search for tables")
            print("  python discover_tables.py search TERM --add  # Search and add to allowlist")
            print("  python discover_tables.py help          # Show this help message")
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Run 'python discover_tables.py help' for usage information.")
    else:
        # Interactive mode
        interactive_mode()

if __name__ == "__main__":
    main() 