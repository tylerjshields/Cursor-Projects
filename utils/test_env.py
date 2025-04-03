#!/usr/bin/env python

import os
from dotenv import load_dotenv
import sys

print("Test script to verify .env file loading")
print(f"Python version: {sys.version}")
print("Current directory:", os.getcwd())

# Check if .env file exists
env_path = os.path.join(os.getcwd(), '.env')
print(f".env file exists: {os.path.exists(env_path)}")

if os.path.exists(env_path):
    print("Contents of .env file:")
    with open(env_path, 'r') as f:
        print(f.read())

# Try to load .env file
print("\nAttempting to load .env file...")
load_dotenv(override=True)

# Check if variables were loaded
print("\nEnvironment variables after loading .env:")
print(f"SNOWFLAKE_USER: {os.getenv('SNOWFLAKE_USER')}")
if os.getenv('SNOWFLAKE_PASSWORD'):
    print(f"SNOWFLAKE_PASSWORD: {'*' * len(os.getenv('SNOWFLAKE_PASSWORD'))} (masked)")
else:
    print("SNOWFLAKE_PASSWORD: Not set")
print(f"SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
print(f"SNOWFLAKE_DATABASE: {os.getenv('SNOWFLAKE_DATABASE')}")
print(f"SNOWFLAKE_WAREHOUSE: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
print(f"SNOWFLAKE_SCHEMA: {os.getenv('SNOWFLAKE_SCHEMA')}")

# Try directly setting environment variables
print("\nSetting environment variables directly...")
os.environ['SNOWFLAKE_USER_TEST'] = 'test_user'
print(f"SNOWFLAKE_USER_TEST: {os.getenv('SNOWFLAKE_USER_TEST')}") 