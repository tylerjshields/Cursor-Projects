#!/usr/bin/env python
"""
Script to verify the table_allowlist.json against schema_repository.json.
This is a simplified version that doesn't require Snowflake credentials.
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
        
def load_schema_repository(file_path="schema_repository.json"):
    """Load the schema repository from a JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading schema repository: {e}")
        return {"verified_tables": []}

def table_in_repository(table_info, repository):
    """Check if a table exists in the schema repository"""
    for repo_table in repository["verified_tables"]:
        if (repo_table["database"].lower() == table_info["database"].lower() and
            repo_table["schema"].lower() == table_info["schema"].lower() and
            repo_table["table"].lower() == table_info["table"].lower()):
            return True
    return False

def verify_allowlist_against_repository():
    """Verify the tables in the allowlist against the schema repository"""
    print("Verifying tables in the allowlist against the schema repository...")
    
    # Load the allowlist and schema repository
    allowlist = load_allowlist()
    repository = load_schema_repository()
    
    # Track tables for reporting
    verified_tables = []
    unverified_tables = []
    
    # Track tier distribution
    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    
    # Check each table in the allowlist
    for table_info in allowlist:
        table_name = table_info["table"]
        schema_name = table_info["schema"]
        database_name = table_info.get("database", "EDW")
        tier = table_info.get("tier", 2)  # Default to tier 2 if not specified
        
        # Update tier counts
        if tier in tier_counts:
            tier_counts[tier] += 1
        
        # Check if the table exists in the schema repository
        exists_in_repo = table_in_repository(
            {"database": database_name, "schema": schema_name, "table": table_name}, 
            repository
        )
        
        if exists_in_repo:
            verified_tables.append({
                "database": database_name,
                "schema": schema_name,
                "table": table_name,
                "tier": tier
            })
        else:
            unverified_tables.append({
                "database": database_name,
                "schema": schema_name,
                "table": table_name,
                "tier": tier
            })
    
    # Print results
    print("\nVerification Results:")
    print(f"Total tables in allowlist: {len(allowlist)}")
    print(f"Verified tables (exist in repository): {len(verified_tables)}")
    print(f"Unverified tables (not in repository): {len(unverified_tables)}")
    
    # Print tier distribution
    print("\nTier Distribution:")
    for tier, count in tier_counts.items():
        print(f"  Tier {tier}: {count} tables")
    
    if unverified_tables:
        print("\nUnverified tables:")
        for table in unverified_tables:
            print(f"  Tier {table['tier']} - {table['database']}.{table['schema']}.{table['table']}")
    
    # Find tables in repository that aren't in the allowlist
    tables_not_in_allowlist = []
    for repo_table in repository["verified_tables"]:
        found = False
        for table_info in allowlist:
            database_name = table_info.get("database", "EDW")
            if (repo_table["database"].lower() == database_name.lower() and
                repo_table["schema"].lower() == table_info["schema"].lower() and
                repo_table["table"].lower() == table_info["table"].lower()):
                found = True
                break
        
        if not found:
            tables_not_in_allowlist.append(repo_table)
    
    print(f"\nTables in repository not in allowlist: {len(tables_not_in_allowlist)}")
    
    return verified_tables, unverified_tables, tables_not_in_allowlist

def add_tables_to_repository(tables, file_path="schema_repository.json"):
    """Add tables to the schema repository"""
    # Load the current repository
    repository = load_schema_repository(file_path)
    
    # Add the tables
    for table in tables:
        # Check if the table already exists in the repository
        if not table_in_repository(table, repository):
            repository["verified_tables"].append(table)
    
    # Update the last_updated field
    repository["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    
    # Write back to the file
    with open(file_path, 'w') as f:
        json.dump(repository, f, indent=2)
    
    print(f"Updated {file_path} with new tables")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Verify the table_allowlist.json against schema_repository.json')
    parser.add_argument('--add-missing', action='store_true', help='Add unverified tables to the schema repository')
    args = parser.parse_args()
    
    verified_tables, unverified_tables, tables_not_in_allowlist = verify_allowlist_against_repository()
    
    if args.add_missing and unverified_tables:
        add_tables_to_repository(unverified_tables)
        print(f"Added {len(unverified_tables)} tables to the schema repository")

if __name__ == "__main__":
    main() 