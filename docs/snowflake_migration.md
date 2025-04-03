# Snowflake Connection Migration Guide

## Overview

We are transitioning from our custom Python-based Snowflake connection implementation to using the official Snowflake VS Code extension. This document provides instructions for migrating existing code and workflows.

## Why We're Migrating

The VS Code Snowflake extension offers several advantages:
- Better integration with our code editor
- SQL syntax highlighting and autocompletion
- Visual query results
- Connection management through the UI
- Maintained by Snowflake with regular updates

## Timeline

- **Immediate**: Both systems will run in parallel
- **August 31, 2024**: Custom implementation will be removed

## Setup Instructions

### 1. Install the Extension

1. Open Cursor (or VS Code)
2. Open the Extensions view (`Cmd+Shift+X` on Mac, `Ctrl+Shift+X` on Windows/Linux)
3. Search for "Snowflake"
4. Install the official "Snowflake" extension by Snowflake Inc.

### 2. Configure Connection

Your connection details have been migrated to `~/.snowflake/connections.toml`. This file should include:

```toml
[connections.doordash_prod]
account = "DOORDASH" 
user = "TYLER.SHIELDS"
password = "****" # Obscured for security
warehouse = "TEAM_DATA_ANALYTICS_ETL"
database = "PRODDB"
schema = "public"
```

### 3. Connect and Run Queries

1. Open a `.sql` file
2. Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
3. Type "Snowflake: Connect" and select your connection
4. Execute queries with "Snowflake: Execute Current Statement"

## Code Migration Guide

### For Scripts Using `get_data()` or `execute_query()`

Instead of:
```python
from snowflake_connector import get_data

query = "SELECT * FROM my_table"
results = get_data(query)
```

Use this approach:
```python
import subprocess
import pandas as pd
import json

def run_snowflake_query(query, output_file="query_results.json"):
    """Run a Snowflake query using the VS Code extension CLI"""
    
    # Save query to temporary file
    with open("temp_query.sql", "w") as f:
        f.write(query)
    
    # Execute query using the Snowflake extension CLI
    subprocess.run([
        "snowflake", "query", 
        "--connection", "doordash_prod",
        "--query-file", "temp_query.sql",
        "--output", output_file,
        "--format", "json"
    ])
    
    # Read results
    with open(output_file, "r") as f:
        results = json.load(f)
    
    # Convert to DataFrame if needed
    return pd.DataFrame(results)
```

### For Schema Tools

For schema tools and other utilities, consider using the Snowflake Python Connector directly with connection parameters from the `.snowflake/connections.toml` file:

```python
import snowflake.connector
import toml

def get_snowflake_connection():
    """Get Snowflake connection using parameters from connections.toml"""
    # Load connection parameters from ~/.snowflake/connections.toml
    with open("/Users/tylershields/.snowflake/connections.toml", "r") as f:
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

# Example usage
with get_snowflake_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT current_version()")
    results = cursor.fetchall()
    print(results)
```

## Need Help?

If you have questions or need assistance with migration, please contact Tyler Shields. 