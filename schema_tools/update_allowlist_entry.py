#!/usr/bin/env python
"""
Script to update an existing table entry in the table_allowlist.json file.
"""

import json
import argparse
from datetime import datetime

def load_allowlist(file_path="table_allowlist.json"):
    """Load the table allowlist from a JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading allowlist: {e}")
        return []

def find_table_in_allowlist(table_info, allowlist):
    """Find a table in the allowlist and return its index"""
    for i, table in enumerate(allowlist):
        database_name = table.get("database", "EDW")
        if (database_name.lower() == table_info["database"].lower() and
            table["schema"].lower() == table_info["schema"].lower() and
            table["table"].lower() == table_info["table"].lower()):
            return i
    return -1

def update_table_in_allowlist(table_info, file_path="table_allowlist.json"):
    """Update an existing table entry in the allowlist"""
    allowlist = load_allowlist(file_path)
    
    # Find the table in the allowlist
    idx = find_table_in_allowlist(table_info, allowlist)
    
    if idx == -1:
        print(f"Table {table_info['database']}.{table_info['schema']}.{table_info['table']} not found in the allowlist.")
        return False
    
    # Update the table entry
    current_entry = allowlist[idx]
    
    # Only update fields that are provided
    if "tier" in table_info:
        current_entry["tier"] = table_info["tier"]
    if "description" in table_info:
        current_entry["description"] = table_info["description"]
    if "notes" in table_info:
        current_entry["notes"] = table_info["notes"]
    if "key_columns" in table_info:
        current_entry["key_columns"] = table_info["key_columns"]
    if "common_joins" in table_info:
        current_entry["common_joins"] = table_info["common_joins"]
    if "columns" in table_info:
        current_entry["columns"] = table_info["columns"]
    
    # Write back to the file
    with open(file_path, 'w') as f:
        json.dump(allowlist, f, indent=2)
    
    print(f"Updated {table_info['database']}.{table_info['schema']}.{table_info['table']} in the allowlist.")
    return True

def main():
    """Main function to update a table in the allowlist"""
    parser = argparse.ArgumentParser(description='Update a table entry in the table_allowlist.json file.')
    parser.add_argument('--database', default="EDW", help='Database name (default: EDW)')
    parser.add_argument('--schema', required=True, help='Schema name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--tier', type=int, choices=[1, 2, 3, 4], help='Table tier (1-4) indicating reliability and usage priority')
    parser.add_argument('--description', help='Description of the table')
    parser.add_argument('--notes', help='Additional notes about the table')
    parser.add_argument('--common-joins', nargs='+', help='List of common tables to join with')
    parser.add_argument('--key-columns', nargs='+', help='List of key columns in the table')
    parser.add_argument('--columns', nargs='+', help='List of column:datatype pairs (e.g., "order_id:VARCHAR" "created_at:TIMESTAMP_NTZ")')
    
    args = parser.parse_args()
    
    # Create the table entry for the allowlist
    table_entry = {
        "table": args.table,
        "schema": args.schema,
        "database": args.database,
    }
    
    # Add optional fields if provided
    if args.tier:
        table_entry["tier"] = args.tier
    
    if args.description:
        table_entry["description"] = args.description
    
    if args.notes:
        table_entry["notes"] = args.notes
    
    if args.common_joins:
        table_entry["common_joins"] = args.common_joins
    
    if args.key_columns:
        table_entry["key_columns"] = args.key_columns
    
    if args.columns:
        columns = {}
        for col in args.columns:
            parts = col.split(":")
            if len(parts) == 2:
                columns[parts[0]] = parts[1]
        if columns:
            table_entry["columns"] = columns
    
    # Update the table in the allowlist
    update_table_in_allowlist(table_entry)

if __name__ == "__main__":
    main() 