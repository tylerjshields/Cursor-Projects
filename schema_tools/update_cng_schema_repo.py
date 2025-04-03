#!/usr/bin/env python
"""
Script to update schema_repository.json with all tables from the edw.cng schema.
This focused approach allows us to build a comprehensive list of available CNG tables
without overwhelming the repository with every table in Snowflake.
"""

import os
import json
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv
from snowflake_credentials import get_snowflake_credentials

load_dotenv()

def connect_to_snowflake():
    """Connect to Snowflake using credentials from snowflake_credentials module"""
    params = get_snowflake_credentials()
    conn = snowflake.connector.connect(**params)
    return conn

def get_cng_tables(conn, database="EDW", schema="CNG"):
    """Query Snowflake to get all tables in the specified schema"""
    tables = []
    
    cursor = conn.cursor()
    try:
        # Get all tables in the specified schema
        query = f"""
        SELECT table_name 
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = '{schema}'
        """
        cursor.execute(query)
        
        # Add each table to our list
        for table in cursor.fetchall():
            table_name = table[0]
            tables.append({
                "database": database,
                "schema": schema,
                "table": table_name
            })
        
        print(f"Found {len(tables)} tables in {database}.{schema}")
                
    except Exception as e:
        print(f"Error querying Snowflake: {e}")
    finally:
        cursor.close()
        
    return tables

def update_schema_repository(tables, output_file="schema_repository.json"):
    """Update the schema_repository.json file with the tables from Snowflake"""
    try:
        # Load existing repository if it exists
        with open(output_file, 'r') as f:
            repository = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Create a new repository if it doesn't exist or is invalid
        repository = {
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "verified_tables": [],
            "note": "This is a lightweight repository of table names that exist in the database. It contains no description or metadata, only verified table names."
        }
    
    # Track the new tables added
    new_tables = 0
    
    # Add each table to the repository if it doesn't already exist
    for table in tables:
        # Check if the table already exists
        table_exists = False
        for repo_table in repository["verified_tables"]:
            if (repo_table["database"].lower() == table["database"].lower() and
                repo_table["schema"].lower() == table["schema"].lower() and
                repo_table["table"].lower() == table["table"].lower()):
                table_exists = True
                break
        
        if not table_exists:
            repository["verified_tables"].append(table)
            new_tables += 1
    
    # Update the last updated date
    repository["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    
    # Write to the output file
    with open(output_file, 'w') as f:
        json.dump(repository, f, indent=2)
    
    print(f"Added {new_tables} new tables to {output_file}")
    print(f"Total tables in repository: {len(repository['verified_tables'])}")

def main():
    """Main function to update the schema repository"""
    print("Connecting to Snowflake...")
    conn = connect_to_snowflake()
    
    print("Fetching tables from EDW.CNG schema...")
    tables = get_cng_tables(conn)
    
    if tables:
        print("Updating schema repository...")
        update_schema_repository(tables)
    else:
        print("No tables found or error occurred.")
    
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main() 