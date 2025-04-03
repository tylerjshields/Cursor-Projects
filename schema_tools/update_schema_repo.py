#!/usr/bin/env python
"""
Script to update schema_repository.json with verified tables from Snowflake.
This script queries Snowflake to get a list of tables in the specified database
and updates the schema_repository.json file with the results.
"""

import os
import json
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def connect_to_snowflake():
    """Connect to Snowflake using credentials from .env file"""
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT', 'DOORDASH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'PRODDB'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'TEAM_DATA_ANALYTICS_ETL'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'public')
    )
    return conn

def get_tables_from_database(conn, database_name="EDW"):
    """Query Snowflake to get a list of tables in the specified database"""
    tables = []
    
    cursor = conn.cursor()
    try:
        # Get the list of schemas in the database
        cursor.execute(f"SHOW SCHEMAS IN {database_name}")
        schemas_result = cursor.fetchall()
        schemas = [schema[1] for schema in schemas_result]
        
        # For each schema, get the list of tables
        for schema in schemas:
            cursor.execute(f"SHOW TABLES IN {database_name}.{schema}")
            tables_result = cursor.fetchall()
            
            # Add each table to our list
            for table in tables_result:
                table_name = table[1]
                tables.append({
                    "database": database_name,
                    "schema": schema,
                    "table": table_name
                })
                
    except Exception as e:
        print(f"Error querying Snowflake: {e}")
    finally:
        cursor.close()
        
    return tables

def update_schema_repository(tables, output_file="schema_repository.json"):
    """Update the schema_repository.json file with the tables from Snowflake"""
    # Create the repository structure
    repository = {
        "last_updated": datetime.now().strftime("%Y-%m-%d"),
        "verified_tables": tables,
        "note": "This is a lightweight repository of table names that exist in the database. It contains no description or metadata, only verified table names."
    }
    
    # Write to the output file
    with open(output_file, 'w') as f:
        json.dump(repository, f, indent=2)
    
    print(f"Updated {output_file} with {len(tables)} tables from Snowflake")

def main():
    """Main function to update the schema repository"""
    print("Connecting to Snowflake...")
    conn = connect_to_snowflake()
    
    print("Fetching tables from EDW database...")
    tables = get_tables_from_database(conn, "EDW")
    
    print(f"Found {len(tables)} tables in EDW database")
    
    print("Updating schema repository...")
    update_schema_repository(tables)
    
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main() 