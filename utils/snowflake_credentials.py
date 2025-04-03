"""
Snowflake Credentials Manager

This module provides secure access to Snowflake credentials from environment variables.

DEPRECATION NOTICE:
==================
This module is deprecated and will be removed in a future update.
Please transition to the VS Code Snowflake extension for a better experience.
Connection configuration is now managed through ~/.snowflake/connections.toml.
See README.md for migration instructions.

Last planned maintenance date: August 31, 2024
"""

import os
from dotenv import load_dotenv
import sys

# Load environment variables from .env file
load_dotenv(override=True)

def get_snowflake_credentials():
    """
    Get Snowflake connection parameters from environment variables.
    
    Returns:
        dict: Connection parameters for Snowflake
        
    Raises:
        ValueError: If required credentials are missing
    """
    # Get credentials from environment variables
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    database = os.getenv('SNOWFLAKE_DATABASE', 'PRODDB')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'TEAM_DATA_ANALYTICS_ETL')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'public')
    
    # Verify required credentials are present
    if not all([user, password, account]):
        missing = []
        if not user: missing.append('SNOWFLAKE_USER')
        if not password: missing.append('SNOWFLAKE_PASSWORD')
        if not account: missing.append('SNOWFLAKE_ACCOUNT')
        
        error_msg = f"Missing required Snowflake credentials: {', '.join(missing)}\n"
        error_msg += "Please check your .env file and ensure all required variables are set."
        raise ValueError(error_msg)
    
    # Return connection parameters
    return {
        'user': user,
        'password': password,
        'account': account,
        'database': database,
        'warehouse': warehouse,
        'schema': schema,
    }

# Standalone test
if __name__ == "__main__":
    try:
        credentials = get_snowflake_credentials()
        
        # Print credentials (masking password)
        print("Snowflake Credentials:")
        for key, value in credentials.items():
            if key == 'password':
                print(f"  {key}: {'*' * len(value)}")
            else:
                print(f"  {key}: {value}")
                
        print("\nAll required credentials found.")
        
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1) 