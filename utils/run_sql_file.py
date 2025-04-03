#!/usr/bin/env python
"""
Run a SQL File

This script executes SQL files on Snowflake using credentials from .env file.
"""

import snowflake.connector
import os
import sys
import argparse
import time
import re
from datetime import datetime
from snowflake_credentials import get_snowflake_credentials

def print_progress(current, total, file_name, status="Running"):
    """Print a progress bar and status information"""
    width = 40
    percent = current / total
    filled_length = int(width * percent)
    bar = '█' * filled_length + '-' * (width - filled_length)
    timestamp = datetime.now().strftime("%H:%M:%S")
    
    # Create status line with color
    if status == "Success":
        status_str = f"\033[92m[SUCCESS]\033[0m"  # Green
    elif status == "Failed":
        status_str = f"\033[91m[FAILED]\033[0m"   # Red
    else:
        status_str = f"\033[93m[RUNNING]\033[0m"  # Yellow
        
    sys.stdout.write(f"\r[{timestamp}] {current}/{total} {status_str} |{bar}| {os.path.basename(file_name)}")
    sys.stdout.flush()
    
    if status != "Running":
        sys.stdout.write('\n')
        sys.stdout.flush()

def run_sql_file(sql_file, show_statements=True, show_progress=True, stop_on_error=True):
    """Read SQL from file and run on Snowflake"""
    start_time = time.time()
    
    # Check if file exists
    if not os.path.exists(sql_file):
        print(f"\n❌ Error: SQL file '{sql_file}' not found.")
        return False
    
    # Read SQL from file
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
    except Exception as e:
        print(f"\n❌ Error reading SQL file: {e}")
        return False
    
    # Get credentials from environment
    params = get_snowflake_credentials()
    
    print(f"\n📄 Executing file: {sql_file}")
    print(f"Connecting to Snowflake...")
    
    # For tracking create/replace table statements
    table_pattern = re.compile(r'create\s+or\s+replace\s+table\s+(\w+\.\w+\.\w+)', re.IGNORECASE)
    tables_created = []
    
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()
        
        # Split SQL into statements using regex
        # This handles semicolons within quotes correctly
        statements = split_sql_statements(sql_content)
        valid_statements = [s for s in statements if s.strip()]
        
        if not valid_statements:
            print(f"⚠️ No valid SQL statements found in {sql_file}")
            return True
            
        print(f"\n🔍 Found {len(valid_statements)} statements to execute")
        
        # Execute each statement
        success_count = 0
        for i, statement in enumerate(valid_statements):
            # Skip empty statements
            if not statement.strip():
                continue
                
            # Show progress
            if show_progress:
                print_progress(i+1, len(valid_statements), sql_file)
                
            # Check if this is a create/replace table statement
            match = table_pattern.search(statement)
            if match:
                tables_created.append(match.group(1))
            
            # Show statement if requested
            if show_statements:
                print(f"\n=== Executing statement {i+1}/{len(valid_statements)} ===")
                print(statement)
                print("="*40)
            
            statement_start = time.time()
            try:
                cursor.execute(statement)
                success_count += 1
                
                # Process results if the statement returns data
                if cursor.description:
                    # Get column names
                    columns = [desc[0] for desc in cursor.description]
                    
                    # Fetch results
                    results = cursor.fetchall()
                    
                    # Print results
                    if results:
                        # Print header
                        header = " | ".join(columns)
                        print("\nColumns:", ", ".join(columns))
                        print(f"Results ({len(results)} rows):")
                        print("-" * len(header))
                        print(header)
                        print("-" * len(header))
                        
                        # Print up to 20 rows
                        max_rows = min(20, len(results))
                        for row in results[:max_rows]:
                            print(" | ".join(str(val) for val in row))
                            
                        if len(results) > 20:
                            print(f"... and {len(results) - 20} more rows")
                    else:
                        print("Query executed successfully, but no results were returned.")
                else:
                    if show_statements:
                        print("Statement executed successfully.")
                
                if show_progress:
                    print_progress(i+1, len(valid_statements), sql_file, "Success")
                    
            except Exception as e:
                if show_progress:
                    print_progress(i+1, len(valid_statements), sql_file, "Failed")
                print(f"❌ Error executing statement: {e}")
                
                # Stop execution if stop_on_error is True
                if stop_on_error:
                    print("❌ Stopping execution due to error.")
                    cursor.close()
                    conn.close()
                    total_time = time.time() - start_time
                    print(f"\n⛔ SQL execution stopped: {success_count}/{len(valid_statements)} statements successful in {total_time:.2f} seconds")
                    return False
                else:
                    # Continue with the next statement
                    print("Continuing with next statement...\n")
        
        cursor.close()
        conn.close()
        
        # Report on tables created
        if tables_created:
            print(f"\n🏗️ Tables created or replaced ({len(tables_created)}):")
            for table in tables_created:
                print(f"  - {table}")
        
        total_time = time.time() - start_time
        print(f"\n✅ SQL execution completed: {success_count}/{len(valid_statements)} statements successful in {total_time:.2f} seconds")
        return success_count == len(valid_statements)
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False

def split_sql_statements(sql_content):
    """Split SQL content into individual statements correctly handling quoted strings"""
    # First, replace any commented-out semicolons with a placeholder
    # Pattern for single-line comments
    sql_content = re.sub(r'--.*?;', lambda m: m.group(0).replace(';', '@@SEMICOLON@@'), sql_content)
    
    # Regex for splitting SQL statements while respecting quotes
    # This uses a negative lookahead to ensure we don't split on semicolons within quotes
    statements = []
    current_statement = []
    lines = sql_content.split('\n')
    
    for line in lines:
        # Skip empty lines or comments
        if not line.strip() or line.strip().startswith('--'):
            current_statement.append(line)
            continue
            
        # Check if the line ends with a semicolon (outside of quotes)
        if re.search(r';(?=(?:[^\']*\'[^\']*\')*[^\']*$)(?=(?:[^"]*"[^"]*")*[^"]*$)', line):
            # Split by semicolon outside quotes
            parts = re.split(r'(;)(?=(?:[^\']*\'[^\']*\')*[^\']*$)(?=(?:[^"]*"[^"]*")*[^"]*$)', line)
            
            for i in range(0, len(parts), 2):
                if i+1 < len(parts):  # If we have a semicolon part
                    current_statement.append(parts[i] + parts[i+1])  # Add with semicolon
                    statements.append('\n'.join(current_statement))
                    current_statement = []
                else:
                    current_statement.append(parts[i])
        else:
            current_statement.append(line)
    
    # Add the last statement if there's any
    if current_statement:
        statements.append('\n'.join(current_statement))
    
    # If no statements were found, treat the entire content as one statement
    if not statements:
        statements = [sql_content]
    
    # Restore any placeholders back to semicolons
    statements = [stmt.replace('@@SEMICOLON@@', ';') for stmt in statements]
    
    return statements

def run_multiple_files(file_list, show_statements=False, show_progress=True, stop_on_error=True):
    """Run multiple SQL files in sequence"""
    print(f"\n🚀 Executing {len(file_list)} SQL files")
    
    start_time = time.time()
    success_count = 0
    
    for i, sql_file in enumerate(file_list):
        file_start = time.time()
        print(f"\n[{i+1}/{len(file_list)}] Processing {sql_file}")
        success = run_sql_file(sql_file, show_statements, show_progress, stop_on_error)
        
        elapsed = time.time() - file_start
        if success:
            success_count += 1
            print(f"✅ Completed in {elapsed:.2f} seconds")
        else:
            print(f"❌ Failed after {elapsed:.2f} seconds")
            if stop_on_error:
                print(f"⛔ Stopping execution of remaining files due to error.")
                break
        
        print("-" * 80)
    
    total_time = time.time() - start_time
    print(f"\n🏁 All SQL files processed: {success_count}/{len(file_list)} files successful in {total_time:.2f} seconds")
    return success_count == len(file_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SQL files on Snowflake")
    parser.add_argument("files", nargs="+", help="SQL files to execute")
    parser.add_argument("--quiet", action="store_true", help="Don't show individual SQL statements")
    parser.add_argument("--no-progress", action="store_true", help="Don't show progress bars")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue executing statements even if one fails")
    
    args = parser.parse_args()
    
    print("\n=== Snowflake SQL File Runner ===\n")
    
    show_statements = not args.quiet
    show_progress = not args.no_progress
    stop_on_error = not args.continue_on_error
    
    if len(args.files) == 1:
        success = run_sql_file(args.files[0], show_statements, show_progress, stop_on_error)
    else:
        success = run_multiple_files(args.files, show_statements, show_progress, stop_on_error)
    
    sys.exit(0 if success else 1) 