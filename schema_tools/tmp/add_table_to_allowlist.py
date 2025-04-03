#!/usr/bin/env python
"""
Add fact_delivery_allocation_custom table to the allowlist
"""

import sys
import os
import json

# Add the parent directory to the system path so we can import from schema_tools
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from add_to_allowlist import add_table_to_allowlist, load_allowlist
except ImportError:
    print("Error: Could not import from add_to_allowlist.py")
    print("Make sure you're running this script from the schema_tools directory")
    sys.exit(1)

# Table information
table_info = {
    "table": "fact_delivery_allocation_custom",
    "schema": "finance",
    "database": "edw",
    "tier": 2,  # Specialized table for financial analysis
    "description": "Financial table for delivery allocation with marginal variable profit metrics",
    "notes": "Contains marginal_variable_profit which is a near true-to-transaction version of variable_profit, excluding P2C costs and peanutbuttered costs",
    "key_columns": ["delivery_id", "active_date_utc", "marginal_variable_profit"],
    "columns": {
        "delivery_id": "NUMBER(38,0)",
        "active_date_utc": "DATE",
        "business_id": "NUMBER(38,0)",
        "store_id": "NUMBER(38,0)",
        "mx_selection_investment_payment_to_customers": "NUMBER(38,12)",
        "marginal_variable_profit": "NUMBER(38,12)"
    }
}

def main():
    """Main function to add the table to the allowlist"""
    allowlist_file = "../table_allowlist.json"
    
    # Check if allowlist file exists relative to the script location
    if not os.path.exists(allowlist_file):
        allowlist_file = "table_allowlist.json"
        if not os.path.exists(allowlist_file):
            print(f"Error: Allowlist file not found in either location.")
            print("You might need to provide the full path to the allowlist file.")
            sys.exit(1)
    
    # Add the table to the allowlist
    try:
        add_table_to_allowlist(table_info, allowlist_file)
        print(f"Successfully added {table_info['database']}.{table_info['schema']}.{table_info['table']} to the allowlist.")
    except Exception as e:
        print(f"Error adding table to allowlist: {e}")
        sys.exit(1)
    
    # Verify the table was added
    try:
        allowlist = load_allowlist(allowlist_file)
        found = False
        for entry in allowlist:
            if (entry.get("table") == table_info["table"] and 
                entry.get("schema") == table_info["schema"] and 
                entry.get("database") == table_info["database"]):
                found = True
                print("Table found in allowlist after addition.")
                print(f"Entry: {json.dumps(entry, indent=2)}")
                break
        
        if not found:
            print("Warning: Table was not found in the allowlist after addition.")
    except Exception as e:
        print(f"Error verifying table addition: {e}")

if __name__ == "__main__":
    main() 