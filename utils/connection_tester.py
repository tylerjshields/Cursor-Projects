#!/usr/bin/env python
"""
Snowflake Connection Tester
This script will try multiple account formats to connect to Snowflake.
"""

import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection(params):
    """Test connection to Snowflake with given parameters"""
    try:
        print(f"\nTrying connection with account format: {params['account']}")
        
        with snowflake.connector.connect(**params) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT current_user(), current_account(), current_version()")
            result = cursor.fetchone()
            
            print(f"✅ SUCCESS! Connected as: {result[0]}")
            print(f"Account: {result[1]}")
            print(f"Snowflake version: {result[2]}")
            return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

def main():
    """Main function to test different connection formats"""
    # Force reload environment variables
    load_dotenv(override=True)
    
    # Get credentials from environment variables
    user = os.getenv('SNOWFLAKE_USER', '')
    password = os.getenv('SNOWFLAKE_PASSWORD', '')
    
    if not user or not password:
        print("❌ Error: SNOWFLAKE_USER and SNOWFLAKE_PASSWORD must be set in your .env file")
        return
    
    print(f"Testing various connection formats for user: {user}")
    
    # Base connection parameters
    base_params = {
        'user': user,
        'password': password,
        'database': os.getenv('SNOWFLAKE_DATABASE', 'PRODDB'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'TEAM_DATA_ANALYTICS_ETL'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'public')
    }
    
    # Different account formats to try
    account_formats = [
        # Basic formats
        "doordash",
        
        # Common account format patterns
        "doordash.snowflakecomputing.com",  
        
        # Organizational account patterns
        "doordash.us-east-1",
        "doordash.us-east-1.aws",
        
        # DoorDash-specific formats
        "doo25390",
        "doo25390.us-east-1.aws",
        "doo25390.snowflakecomputing.com",
        
        # Try with your username as part of account identifier
        f"doordash-{user.replace('.', '_')}",
        
        # Direct URL formats
        "doordash.snowflakecomputing.com",
        "doo25390.snowflakecomputing.com",
        
        # Alternative regions
        "doordash.us-west-2",
        "doordash.us-west-2.aws"
    ]
    
    success = False
    for account_format in account_formats:
        params = base_params.copy()
        params['account'] = account_format
        
        if test_connection(params):
            print(f"\n✅ SUCCESS FOUND! Use this account format: {account_format}")
            success = True
            break
    
    if not success:
        print("\n❌ All connection attempts failed.")
        print("Troubleshooting steps:")
        print("1. Verify your username and password are correct")
        print("2. Ensure you're connected to any required VPN")
        print("3. Check if your IP is allowed to access Snowflake")
        print("4. Confirm with your admin the correct account identifier/URL")
        print("5. Check if you need to use SSO or other authentication method")

if __name__ == "__main__":
    main() 