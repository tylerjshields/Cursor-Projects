#!/usr/bin/env python
"""
Example script demonstrating how to run Snowflake queries
using the VS Code extension approach.

This shows the recommended way to execute queries and get results
after the manual implementation is deprecated.
"""

import os
import pandas as pd
import subprocess
import json
import tempfile

def run_snowflake_query(query, connection_name="doordash_prod", output_format="json"):
    """Run a Snowflake query using the VS Code extension CLI"""
    
    # Create temporary files for query and results
    with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as query_file:
        query_file.write(query)
        query_path = query_file.name
    
    result_path = query_path + f".{output_format}"
    
    print(f"Running query: {query[:60]}...")
    print(f"Using connection: {connection_name}")
    
    try:
        # Run the query through the VS Code extension CLI
        cmd = [
            "snowflake", "query", 
            "--connection", connection_name,
            "--query-file", query_path,
            "--output", result_path,
            "--format", output_format
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"Command output: {result.stdout}")
        
        # Process the results based on format
        if output_format == "json":
            with open(result_path, "r") as f:
                data = json.load(f)
                return pd.DataFrame(data)
        else:
            # For CSV or other formats
            return result_path  # Return path to result file
            
    except subprocess.CalledProcessError as e:
        print(f"Error executing query: {e}")
        print(f"Command output: {e.stdout}")
        print(f"Command error: {e.stderr}")
        raise
    finally:
        # Clean up temporary files
        if os.path.exists(query_path):
            os.remove(query_path)
        if output_format != "csv":  # Keep CSV files for reference
            if os.path.exists(result_path):
                os.remove(result_path)

if __name__ == "__main__":
    # Example query
    test_query = """
    SELECT 
        CURRENT_USER() AS current_user,
        CURRENT_ROLE() AS current_role,
        CURRENT_WAREHOUSE() AS current_warehouse,
        CURRENT_DATABASE() AS current_database,
        CURRENT_SCHEMA() AS current_schema,
        CURRENT_TIMESTAMP() AS current_time
    """
    
    try:
        # Run the query and get results as a DataFrame
        results = run_snowflake_query(test_query)
        
        # Display the results
        print("\nQuery Results:")
        print(results)
        
    except Exception as e:
        print(f"Failed to execute query: {e}") 