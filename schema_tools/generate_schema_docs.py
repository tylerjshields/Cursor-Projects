#!/usr/bin/env python
"""
Schema Documentation Generator

This script generates comprehensive documentation for specified Snowflake tables.
It creates a markdown file with table structures, column details, and sample data.
"""

import snowflake.connector
import os
import sys
import json
from datetime import datetime
from snowflake_credentials import get_snowflake_credentials

def connect_to_snowflake():
    """Connect to Snowflake using credentials from environment variables"""
    try:
        # Get credentials from environment
        params = get_snowflake_credentials()
        conn = snowflake.connector.connect(**params)
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        sys.exit(1)

def get_table_structure(cursor, database, schema, table):
    """Get the structure of a specific table"""
    query = f"""
    SELECT 
        column_name, 
        data_type, 
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable,
        column_default,
        comment
    FROM 
        {database}.information_schema.columns
    WHERE 
        table_schema = '{schema}'
        AND table_name = '{table}'
    ORDER BY 
        ordinal_position
    """
    
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error getting structure for {schema}.{table}: {e}")
        return []

def get_table_comment(cursor, database, schema, table):
    """Get the comment for a specific table"""
    query = f"""
    SELECT 
        comment
    FROM 
        {database}.information_schema.tables
    WHERE 
        table_schema = '{schema}'
        AND table_name = '{table}'
    """
    
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result and result[0] else "No description available"
    except Exception as e:
        print(f"Error getting comment for {schema}.{table}: {e}")
        return "No description available"

def get_sample_data(cursor, database, schema, table, limit=5):
    """Get sample data from a table"""
    query = f"""
    SELECT * FROM {database}.{schema}.{table}
    LIMIT {limit}
    """
    
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return columns, data
    except Exception as e:
        print(f"Error getting sample data for {schema}.{table}: {e}")
        return [], []

def get_table_row_count(cursor, database, schema, table):
    """Get the row count of a table"""
    query = f"""
    SELECT COUNT(*) FROM {database}.{schema}.{table}
    """
    
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        print(f"Error getting row count for {schema}.{table}: {e}")
        return "Unknown"

def write_markdown_doc(allowlist, output_file="schema_documentation.md"):
    """Write the schema documentation to a markdown file"""
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    
    # Get current database and schema
    cursor.execute("SELECT current_database(), current_schema()")
    current_db, current_schema = cursor.fetchone()
    
    with open(output_file, 'w') as f:
        # Write header
        f.write(f"# Snowflake Schema Documentation\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Database: {current_db}\n\n")
        f.write(f"## Tables\n\n")
        
        # Table of contents
        f.write("### Table of Contents\n\n")
        for table_info in allowlist:
            # Handle both string and dict formats
            if isinstance(table_info, str):
                schema = current_schema
                table = table_info
                database = current_db
                description = ""
            else:
                schema = table_info.get('schema', current_schema)
                table = table_info['table']
                database = table_info.get('database', current_db)
                description = table_info.get('description', "")
            
            anchor = f"{database.lower()}_{schema.lower()}_{table.lower()}".replace('.', '_')
            f.write(f"- [{database}.{schema}.{table}](#{anchor}) {description}\n")
        
        f.write("\n---\n\n")
        
        # Process each table
        for table_info in allowlist:
            # Handle both string and dict formats
            if isinstance(table_info, str):
                schema = current_schema
                table = table_info
                database = current_db
                notes = ""
                common_joins = []
                key_columns = []
            else:
                schema = table_info.get('schema', current_schema)
                table = table_info['table']
                database = table_info.get('database', current_db)
                notes = table_info.get('notes', "")
                common_joins = table_info.get('common_joins', [])
                key_columns = table_info.get('key_columns', [])
            
            # Print processing status
            full_table_name = f"{database}.{schema}.{table}"
            print(f"Processing {full_table_name}...")
            
            # Get table comment and other metadata from Snowflake
            table_comment = get_table_comment(cursor, database, schema, table)
            columns = get_table_structure(cursor, database, schema, table)
            row_count = get_table_row_count(cursor, database, schema, table)
            
            # Get sample data if available
            try:
                sample_columns, sample_data = get_sample_data(cursor, database, schema, table)
            except Exception as e:
                print(f"Error getting sample data: {e}")
                sample_columns, sample_data = [], []
            
            # Create anchor for linking
            anchor = f"{database.lower()}_{schema.lower()}_{table.lower()}".replace('.', '_')
            
            # Write table header with full qualification
            f.write(f"## <a id='{anchor}'></a>{database}.{schema}.{table}\n\n")
            
            # Description from allowlist or database comment
            description = table_info.get('description', "") if isinstance(table_info, dict) else ""
            if description:
                f.write(f"**Description**: {description}\n\n")
            if table_comment and table_comment != "No description available":
                f.write(f"**Database Comment**: {table_comment}\n\n")
            
            # Table notes (from the allowlist)
            if notes:
                f.write(f"**Notes**:\n{notes}\n\n")
            
            # Row count
            f.write(f"**Row Count**: {row_count}\n\n")
            
            # Common joins
            if common_joins:
                f.write("**Common Joins**:\n")
                for join in common_joins:
                    # Create a link to the joined table if it's in our allowlist
                    join_parts = join.split('.')
                    if len(join_parts) == 3:  # fully qualified
                        join_db, join_schema, join_table = join_parts
                        join_anchor = f"{join_db.lower()}_{join_schema.lower()}_{join_table.lower()}".replace('.', '_')
                        f.write(f"- [{join}](#{join_anchor})\n")
                    else:
                        f.write(f"- {join}\n")
                f.write("\n")
            
            # Important columns
            if key_columns:
                f.write("**Key Columns**:\n")
                for col in key_columns:
                    # Try to find the column in the columns list for extra details
                    col_details = next((c for c in columns if c[0].upper() == col.upper()), None)
                    if col_details:
                        col_comment = col_details[7] if col_details[7] else ""
                        if col_comment:
                            f.write(f"- `{col}`: {col_comment}\n")
                        else:
                            f.write(f"- `{col}`\n")
                    else:
                        f.write(f"- `{col}`\n")
                f.write("\n")
            
            # Write column details
            f.write("### Columns\n\n")
            f.write("| Column Name | Data Type | Nullable | Default | Description |\n")
            f.write("|------------|-----------|----------|---------|-------------|\n")
            
            for col in columns:
                col_name = col[0]
                
                # Format data type with length/precision/scale if applicable
                data_type = col[1]
                if col[2]:  # character_maximum_length
                    data_type += f"({col[2]})"
                elif col[3] and col[4] is not None:  # numeric_precision and numeric_scale
                    data_type += f"({col[3]},{col[4]})"
                elif col[3]:  # just numeric_precision
                    data_type += f"({col[3]})"
                
                nullable = "YES" if col[5] == "YES" else "NO"
                default = col[6] if col[6] else ""
                comment = col[7] if col[7] else ""
                
                # Highlight key columns
                if key_columns and col_name.upper() in [k.upper() for k in key_columns]:
                    col_name = f"**{col_name}**"
                
                f.write(f"| {col_name} | {data_type} | {nullable} | {default} | {comment} |\n")
            
            f.write("\n")
            
            # Write sample data if available
            if sample_columns and sample_data:
                f.write("### Sample Data\n\n")
                # Write header
                f.write("| " + " | ".join(sample_columns) + " |\n")
                f.write("|" + "---|" * len(sample_columns) + "\n")
                
                # Write data rows
                for row in sample_data:
                    formatted_row = []
                    for val in row:
                        if val is None:
                            formatted_row.append("NULL")
                        elif isinstance(val, str):
                            # Escape pipe characters and format multiline strings
                            formatted_val = str(val).replace("|", "\\|").replace("\n", "<br>")
                            # Truncate long strings
                            if len(formatted_val) > 100:
                                formatted_val = formatted_val[:100] + "..."
                            formatted_row.append(formatted_val)
                        else:
                            formatted_row.append(str(val))
                    f.write("| " + " | ".join(formatted_row) + " |\n")
            else:
                f.write("### Sample Data\n\n")
                f.write("*No sample data available*\n")
            
            f.write("\n---\n\n")
    
    cursor.close()
    conn.close()
    
    print(f"Documentation written to {output_file}")

def load_allowlist(allowlist_file):
    """Load the table allowlist from a JSON file"""
    try:
        with open(allowlist_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Allowlist file '{allowlist_file}' not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error parsing allowlist file '{allowlist_file}'. Not valid JSON.")
        return []

def create_default_allowlist(output_file="table_allowlist.json"):
    """Create a default allowlist file if none exists"""
    if os.path.exists(output_file):
        print(f"Allowlist file '{output_file}' already exists. Not overwriting.")
        return
    
    default_allowlist = [
        {
            "table": "USERS",
            "schema": "PUBLIC",
            "description": "User account information"
        },
        {
            "table": "ORDERS",
            "schema": "PUBLIC",
            "description": "Customer order data"
        }
    ]
    
    with open(output_file, 'w') as f:
        json.dump(default_allowlist, f, indent=2)
    
    print(f"Created default allowlist file: {output_file}")
    print("Please edit this file to include the tables you want to document.")

def main():
    """Main function"""
    allowlist_file = "table_allowlist.json"
    output_file = "schema_documentation.md"
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "create-allowlist":
            create_default_allowlist(allowlist_file)
            return
        elif sys.argv[1] == "help":
            print("Usage:")
            print("  python generate_schema_docs.py                  # Generate documentation using allowlist")
            print("  python generate_schema_docs.py create-allowlist # Create a default allowlist template")
            print("  python generate_schema_docs.py help             # Show this help message")
            return
    
    # Check if allowlist file exists
    if not os.path.exists(allowlist_file):
        print(f"Allowlist file '{allowlist_file}' not found.")
        print("Run 'python generate_schema_docs.py create-allowlist' to create a template.")
        return
    
    # Load allowlist
    allowlist = load_allowlist(allowlist_file)
    
    if not allowlist:
        print("Allowlist is empty. Please add tables to document.")
        return
    
    # Generate documentation
    write_markdown_doc(allowlist, output_file)

if __name__ == "__main__":
    main() 