"""
Test Snowflake Connection

This script tests the connection to Snowflake using credentials from .env file.

Make sure to:
1. Have your virtual environment activated
2. Have updated your .env file with your credentials
"""

import snowflake.connector
import sys
from snowflake_credentials import get_snowflake_credentials

def test_connection():
    """Test the connection to Snowflake"""
    try:
        # Get credentials from environment
        params = get_snowflake_credentials()
        
        print("Testing connection to Snowflake...")
        print(f"Using account: {params['account']}")
        print(f"Using user: {params['user']}")
        print(f"Using database: {params['database']}")
        print(f"Using warehouse: {params['warehouse']}")
        print(f"Using schema: {params['schema']}")
        
        # Test query to check connection
        query = "SELECT current_user(), current_warehouse(), current_database(), current_schema()"
        
        # Connect directly with parameters
        with snowflake.connector.connect(**params) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
        
        print("\n✅ Connection successful!")
        print("\nConnection Details:")
        print(f"User: {result[0]}")
        print(f"Warehouse: {result[1]}")
        print(f"Database: {result[2]}")
        print(f"Schema: {result[3]}")
        
        return True
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting steps:")
        print("1. Check your credentials in the .env file")
        print("2. Make sure you're connected to the internet and any required VPN")
        print("3. Verify that your IP is whitelisted if required")
        print("4. Ensure your Snowflake account is active")
        
        return False

if __name__ == "__main__":
    print("\n=== Snowflake Connection Test ===\n")
    
    success = test_connection()
    
    print("\n=================================\n")
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1) 