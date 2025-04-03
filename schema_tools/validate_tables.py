#!/usr/bin/env python
"""
Script to validate tables in the allowlist against the database and schema repository.
This script checks:
1. If tables in the allowlist exist in the database
2. If tables in the allowlist exist in the schema repository
3. If there are schema mismatches between the allowlist and the database
"""

import os
import json
import argparse
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

def check_table_exists(conn, database, schema, table):
    """Check if a table exists in the database"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
        SELECT COUNT(*) 
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        """)
        result = cursor.fetchone()
        return result[0] > 0
    except Exception as e:
        print(f"Error checking if table exists: {e}")
        return False
    finally:
        cursor.close()

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

def validate_tables():
    """Validate tables in the allowlist against the database and schema repository"""
    print("Validating tables in the allowlist...")
    
    # Load the allowlist and schema repository
    allowlist = load_allowlist()
    repository = load_schema_repository()
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    
    valid_tables = []
    invalid_tables = []
    missing_from_repo = []
    
    # Check each table in the allowlist
    for table_info in allowlist:
        table_name = table_info["table"]
        schema_name = table_info["schema"]
        database_name = table_info.get("database", "EDW")
        
        # Check if the table exists in the database
        exists_in_db = check_table_exists(conn, database_name, schema_name, table_name)
        
        # Check if the table exists in the schema repository
        exists_in_repo = table_in_repository(
            {"database": database_name, "schema": schema_name, "table": table_name}, 
            repository
        )
        
        if exists_in_db:
            valid_tables.append({
                "database": database_name,
                "schema": schema_name,
                "table": table_name
            })
        else:
            invalid_tables.append({
                "database": database_name,
                "schema": schema_name,
                "table": table_name
            })
            
        if not exists_in_repo and exists_in_db:
            missing_from_repo.append({
                "database": database_name,
                "schema": schema_name,
                "table": table_name
            })
    
    # Print results
    print("\nValidation Results:")
    print(f"Total tables in allowlist: {len(allowlist)}")
    print(f"Valid tables (exist in database): {len(valid_tables)}")
    print(f"Invalid tables (don't exist in database): {len(invalid_tables)}")
    print(f"Tables missing from repository but exist in database: {len(missing_from_repo)}")
    
    if invalid_tables:
        print("\nInvalid tables:")
        for table in invalid_tables:
            print(f"  {table['database']}.{table['schema']}.{table['table']}")
    
    if missing_from_repo:
        print("\nTables missing from repository:")
        for table in missing_from_repo:
            print(f"  {table['database']}.{table['schema']}.{table['table']}")
    
    conn.close()
    
    return valid_tables, invalid_tables, missing_from_repo

def main():
    """Main function to validate tables"""
    parser = argparse.ArgumentParser(description='Validate tables in the allowlist against the database and schema repository.')
    parser.add_argument('--add-missing', action='store_true', help='Add missing tables to the schema repository')
    args = parser.parse_args()
    
    valid_tables, invalid_tables, missing_from_repo = validate_tables()
    
    if args.add_missing and missing_from_repo:
        # Load the current repository
        repository = load_schema_repository()
        
        # Add missing tables to the repository
        for table in missing_from_repo:
            repository["verified_tables"].append(table)
        
        # Update the repository file
        with open("schema_repository.json", 'w') as f:
            json.dump(repository, f, indent=2)
        
        print(f"\nAdded {len(missing_from_repo)} missing tables to the schema repository.")

if __name__ == "__main__":
    main() 