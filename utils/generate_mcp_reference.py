#!/usr/bin/env python
"""
Script to generate MCP (Model Configuration Profile) reference for AI assistants
based on your Snowflake tables, schemas, and allowlist.

This helps AI assistants understand your data structure for better query generation.
"""

import json
import os
import argparse
from snowflake_credentials import get_snowflake_credentials
import snowflake.connector

def connect_to_snowflake():
    """
    Connect to Snowflake using credentials from environment variables.
    """
    credentials = get_snowflake_credentials()
    
    conn = snowflake.connector.connect(
        user=credentials['user'],
        password=credentials['password'],
        account=credentials['account'],
        warehouse=credentials['warehouse'],
        database=credentials['database'],
        schema=credentials['schema'],
    )
    return conn

def load_allowlist(file_path='table_allowlist.json'):
    """
    Load the list of allowlisted tables from the specified JSON file.
    """
    if not os.path.exists(file_path):
        print(f"Warning: Allowlist file '{file_path}' not found.")
        return []
    
    with open(file_path, 'r') as f:
        return json.load(f)

def get_table_metadata(conn, tables):
    """
    Retrieve metadata for the specified tables from Snowflake.
    """
    metadata = {}
    
    for table_entry in tables:
        table_name = table_entry.get('table')
        schema_name = table_entry.get('schema')
        database_name = table_entry.get('database')
        
        if not all([table_name, schema_name, database_name]):
            print(f"Skipping incomplete table entry: {table_entry}")
            continue
        
        # Get column information
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            COMMENT
        FROM
            {database_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = []
            
            for row in cursor:
                columns.append({
                    'name': row[0],
                    'data_type': row[1],
                    'description': row[2] if row[2] else ''
                })
            
            # Add table metadata from allowlist
            full_table_name = f"{database_name}.{schema_name}.{table_name}"
            metadata[full_table_name] = {
                'columns': columns,
                'description': table_entry.get('description', ''),
                'notes': table_entry.get('notes', ''),
                'common_joins': table_entry.get('common_joins', []),
                'key_columns': table_entry.get('key_columns', [])
            }
            
            print(f"Added metadata for {full_table_name}")
            
        except Exception as e:
            print(f"Error retrieving metadata for {database_name}.{schema_name}.{table_name}: {e}")
    
    return metadata

def generate_mcp_reference(metadata, output_file='mcp_reference.json'):
    """
    Generate a structured MCP reference file for AI tools to understand your data.
    """
    mcp_data = {
        "schema_version": "1.0",
        "tables": metadata,
        "common_practices": [
            "Always use double quotes for identifiers as Snowflake is case-sensitive",
            "Prefer to qualify table names with database and schema",
            "Use IFF() instead of binary CASE WHEN statements",
            "Use 'GROUP BY ALL' when appropriate to avoid listing all non-aggregated columns",
            "Filter test data with appropriate flags (e.g., is_filtered_core=TRUE)",
            "Include comments in complex queries to explain business logic"
        ],
        "query_templates": {
            "basic_select": "SELECT {columns} FROM {database}.{schema}.{table} WHERE {condition} LIMIT 100;",
            "join_example": "SELECT a.{col1}, b.{col2} FROM {table1} a JOIN {table2} b ON a.{join_key} = b.{join_key} WHERE {condition};"
        }
    }
    
    with open(output_file, 'w') as f:
        json.dump(mcp_data, f, indent=2)
    
    print(f"MCP reference generated: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate MCP reference for AI assistants')
    parser.add_argument('--allowlist', default='table_allowlist.json', help='Path to allowlist file')
    parser.add_argument('--output', default='mcp_reference.json', help='Output file path')
    args = parser.parse_args()
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    
    # Load allowlist
    tables = load_allowlist(args.allowlist)
    
    if not tables:
        print("No tables found in allowlist. Please run generate_schema_docs.py create-allowlist first.")
        return
    
    # Get metadata for all tables
    metadata = get_table_metadata(conn, tables)
    
    # Generate MCP reference
    generate_mcp_reference(metadata, args.output)
    
    # Close connection
    conn.close()
    
    print(f"Successfully generated MCP reference with metadata for {len(metadata)} tables.")

if __name__ == "__main__":
    main() 