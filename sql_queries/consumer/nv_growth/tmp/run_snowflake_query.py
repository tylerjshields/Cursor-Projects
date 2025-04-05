#!/usr/bin/env python
"""
Script to run a Snowflake query using environment variables for credentials.
"""

import snowflake.connector
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("/Users/tylershields/Cursor Projects/config/.env")

def get_snowflake_connection():
    """Get Snowflake connection using parameters from environment variables"""
    # Get credentials from environment variables
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    database = os.getenv('SNOWFLAKE_DATABASE', 'PRODDB')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'TEAM_DATA_ANALYTICS_ETL')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'public')
    
    # Print connection information (except password)
    print(f"Connecting with:")
    print(f"  User: {user}")
    print(f"  Account: {account}")
    print(f"  Database: {database}")
    print(f"  Warehouse: {warehouse}")
    print(f"  Schema: {schema}")
    
    # Create connection
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

def run_query(query_file):
    """Run a query from a file and return results as a DataFrame"""
    # Read query from file
    with open(query_file, "r") as f:
        query = f.read()
    
    # Split the query into statements
    statements = [stmt.strip() for stmt in query.split(";") if stmt.strip()]
    
    results = []
    
    # Connect to Snowflake and execute each statement
    with get_snowflake_connection() as conn:
        for stmt in statements:
            try:
                print(f"Executing: {stmt[:100]}...")
                cursor = conn.cursor()
                cursor.execute(stmt)
                
                # Fetch results if available
                if cursor.description:
                    df = cursor.fetch_pandas_all()
                    print(f"Results: {len(df)} rows")
                    results.append(df)
                else:
                    print("Statement executed successfully (no results)")
            except Exception as e:
                print(f"Error executing statement: {e}")
    
    return results

if __name__ == "__main__":
    # File path to the query
    query_file = "tmp/test_fsi_query.sql"
    
    # Run the query
    print(f"Running query from file: {query_file}")
    dfs = run_query(query_file)
    
    # Display results
    for i, df in enumerate(dfs):
        print(f"\nResult set {i+1}:")
        print(df)
        
        # Show column names and types
        print("\nColumns:")
        for col in df.columns:
            print(f"- {col} ({df[col].dtype})")
            
            # If it's a possible JSON column, show a sample
            if 'object' in str(df[col].dtype):
                for val in df[col].head(1):
                    print(f"  Sample: {val}") 