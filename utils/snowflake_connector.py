"""
Snowflake Connector for Python

This module provides functions to connect to Snowflake and execute queries.

DEPRECATION NOTICE:
==================
This module is deprecated and will be removed in a future update.
Please transition to the VS Code Snowflake extension for a better experience.
Connection configuration is now managed through ~/.snowflake/connections.toml.
See README.md for migration instructions.

Last planned maintenance date: August 31, 2024

Setup Instructions:
1. Create a virtual environment:
   python -m venv snowflake_env

2. Activate the virtual environment:
   - On macOS/Linux: source snowflake_env/bin/activate
   - On Windows: snowflake_env\\Scripts\\activate

3. Install required packages:
   pip install -r requirements.txt

4. Update the .env file with your Snowflake credentials

Usage:
- Import the functions from this module
- Use get_data() to fetch query results as a pandas DataFrame
- Use execute_query() to run queries without returning results

Example:
    from snowflake_connector import get_data
    
    query = "SELECT * FROM my_table LIMIT 10"
    results = get_data(query)
    print(results)
"""

import snowflake.connector
import pandas as pd
import json
import os
from dotenv import load_dotenv
from snowflake_credentials import get_snowflake_credentials

# Get Snowflake connection parameters
PARAMS = get_snowflake_credentials()

# ------- utils ----------
def get_data_legacy(q):
    """Legacy function to fetch data using pandas read_sql"""
    with snowflake.connector.connect(**PARAMS) as ctx:
        data = pd.read_sql(q, ctx)
        data.columns = [i.lower() for i in data.columns]
        return data

def get_data(q):
    """Fetch data using Snowflake cursor and convert to pandas DataFrame"""
    with snowflake.connector.connect(**PARAMS) as ctx:
        cur = ctx.cursor()
        cur.execute(q)
        data = cur.fetch_pandas_all()
    
    data.columns = [i.lower() for i in data.columns]
    return data

def execute_query(q):
    """Execute a query without returning results"""
    with snowflake.connector.connect(**PARAMS, client_session_keep_alive=True) as ctx:
        cursor = ctx.cursor()
        cursor.execute(q)
        
# Example usage
if __name__ == "__main__":
    # Example query
    query = "SELECT current_warehouse(), current_database(), current_schema()"
    
    print("Testing connection...")
    print(f"Using account: {PARAMS['account']}")
    print(f"Using user: {PARAMS['user']}")
    print(f"Using database: {PARAMS['database']}")
    print(f"Using warehouse: {PARAMS['warehouse']}")
    print(f"Using schema: {PARAMS['schema']}")
    
    try:
        result = get_data(query)
        print("Connection successful!")
        print(result)
    except Exception as e:
        print(f"Connection failed: {e}") 