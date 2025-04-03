"""
Snowflake Compatibility Layer

This module provides a bridge from the old custom implementation to
the new VS Code extension-based approach. It maintains the same interface
as the old snowflake_connector.py but uses the new connection methods.

Usage:
- Replace imports from snowflake_connector with imports from snowflake_compat
- The rest of your code should continue to work the same way

Example:
    # Old code
    from snowflake_connector import get_data
    
    # New code
    from snowflake_compat import get_data
"""

import pandas as pd
import json
import os
import tempfile
import subprocess
import warnings
import toml
import snowflake.connector

# Display deprecation warning
warnings.warn(
    "You are using the compatibility layer for Snowflake connections. "
    "This is a temporary solution during the transition period. "
    "Please migrate to the direct VS Code extension or the new toml-based approach by August 31, 2024.",
    DeprecationWarning,
    stacklevel=2
)

def get_snowflake_connection():
    """Get Snowflake connection using parameters from connections.toml"""
    # Load connection parameters from ~/.snowflake/connections.toml
    with open(os.path.expanduser("~/.snowflake/connections.toml"), "r") as f:
        config = toml.load(f)
    
    # Get parameters for doordash_prod
    params = config["connections"]["doordash_prod"]
    
    # Create connection
    return snowflake.connector.connect(
        user=params["user"],
        password=params["password"],
        account=params["account"],
        warehouse=params["warehouse"],
        database=params["database"],
        schema=params["schema"]
    )

def get_data(query):
    """
    Fetch data from Snowflake and return as a pandas DataFrame.
    This maintains the same interface as the old get_data function.
    """
    with get_snowflake_connection() as conn:
        data = pd.read_sql(query, conn)
        data.columns = [i.lower() for i in data.columns]
        return data

def execute_query(query):
    """
    Execute a query in Snowflake without returning results.
    This maintains the same interface as the old execute_query function.
    """
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

def get_data_vscode(query):
    """
    Alternative implementation using VS Code extension CLI.
    This is more experimental but shows how to leverage the extension.
    """
    # Create temporary files
    with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as query_file:
        query_file.write(query)
        query_path = query_file.name
    
    result_path = query_path + ".json"
    
    # Execute query using VS Code extension CLI
    try:
        subprocess.run([
            "snowflake", "query", 
            "--connection", "doordash_prod",
            "--query-file", query_path,
            "--output", result_path,
            "--format", "json"
        ], check=True)
        
        # Read results
        with open(result_path, "r") as f:
            results = json.load(f)
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        df.columns = [i.lower() for i in df.columns]
        return df
        
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error executing query via VS Code extension: {e}")
    finally:
        # Clean up temporary files
        if os.path.exists(query_path):
            os.remove(query_path)
        if os.path.exists(result_path):
            os.remove(result_path) 