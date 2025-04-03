#!/usr/bin/env python
"""
Schema Discovery Utility

This script provides utilities to explore Snowflake schema information,
find tables with specific columns, and analyze table relationships.
"""

import os
import json
import argparse
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
import pandas as pd
from snowflake_credentials import get_snowflake_credentials
import snowflake.connector

console = Console()

def connect_to_snowflake():
    """Connect to Snowflake using environment variables."""
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

def list_databases(conn):
    """List all accessible databases."""
    query = """
    SELECT 
        DATABASE_NAME,
        CREATED,
        COMMENT
    FROM INFORMATION_SCHEMA.DATABASES
    ORDER BY DATABASE_NAME
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    table = Table(title="Available Databases")
    table.add_column("Database Name", style="cyan")
    table.add_column("Created", style="green")
    table.add_column("Comment", style="yellow")
    
    for row in cursor:
        table.add_row(row[0], str(row[1]), row[2] or "")
    
    console.print(table)
    cursor.close()

def list_schemas(conn, database=None):
    """List schemas in the specified database or current database."""
    if database:
        conn.cursor().execute(f"USE DATABASE {database}")
    
    query = """
    SELECT 
        SCHEMA_NAME,
        CREATED,
        COMMENT
    FROM INFORMATION_SCHEMA.SCHEMATA
    ORDER BY SCHEMA_NAME
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    current_db = conn.cursor().execute("SELECT CURRENT_DATABASE()").fetchone()[0]
    
    table = Table(title=f"Schemas in {current_db}")
    table.add_column("Schema Name", style="cyan")
    table.add_column("Created", style="green")
    table.add_column("Comment", style="yellow")
    
    for row in cursor:
        table.add_row(row[0], str(row[1]), row[2] or "")
    
    console.print(table)
    cursor.close()

def list_tables(conn, database=None, schema=None, show_size=False):
    """List tables in the specified database and schema."""
    if database:
        conn.cursor().execute(f"USE DATABASE {database}")
    if schema:
        conn.cursor().execute(f"USE SCHEMA {schema}")
    
    size_column = ", BYTES/1024/1024 AS SIZE_MB" if show_size else ""
    
    query = f"""
    SELECT 
        TABLE_SCHEMA,
        TABLE_NAME,
        ROW_COUNT
        {size_column}
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    current_db = conn.cursor().execute("SELECT CURRENT_DATABASE()").fetchone()[0]
    current_schema = conn.cursor().execute("SELECT CURRENT_SCHEMA()").fetchone()[0]
    
    table = Table(title=f"Tables in {current_db}.{current_schema}")
    table.add_column("Schema", style="blue")
    table.add_column("Table Name", style="cyan")
    table.add_column("Row Count", style="green")
    
    if show_size:
        table.add_column("Size (MB)", style="yellow")
    
    for row in cursor:
        if show_size:
            table.add_row(row[0], row[1], f"{row[2]:,}", f"{row[3]:.2f}")
        else:
            table.add_row(row[0], row[1], f"{row[2]:,}")
    
    console.print(table)
    cursor.close()

def describe_table(conn, table_name, database=None, schema=None):
    """Show detailed information about a specific table."""
    if database:
        conn.cursor().execute(f"USE DATABASE {database}")
    if schema:
        conn.cursor().execute(f"USE SCHEMA {schema}")
    
    # Get column information
    query = f"""
    SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        COMMENT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        current_db = conn.cursor().execute("SELECT CURRENT_DATABASE()").fetchone()[0]
        current_schema = conn.cursor().execute("SELECT CURRENT_SCHEMA()").fetchone()[0]
        
        table = Table(title=f"Columns in {current_db}.{current_schema}.{table_name}")
        table.add_column("Column Name", style="cyan")
        table.add_column("Data Type", style="green")
        table.add_column("Nullable", style="yellow")
        table.add_column("Comment", style="white")
        
        for row in cursor:
            table.add_row(row[0], row[1], row[2], row[3] or "")
        
        console.print(table)
        cursor.close()
        
        # Get table statistics
        query = f"""
        SELECT 
            ROW_COUNT,
            BYTES/1024/1024 AS SIZE_MB,
            CREATED,
            LAST_ALTERED
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name}'
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        stats = cursor.fetchone()
        
        if stats:
            console.print("\n[bold]Table Statistics:[/bold]")
            console.print(f"Row Count: {stats[0]:,}")
            console.print(f"Size: {stats[1]:.2f} MB")
            console.print(f"Created: {stats[2]}")
            console.print(f"Last Modified: {stats[3]}")
        
        cursor.close()
        
    except Exception as e:
        console.print(f"[bold red]Error[/bold red]: {e}")

def find_columns(conn, pattern, database=None):
    """Find columns matching a pattern across tables."""
    if database:
        conn.cursor().execute(f"USE DATABASE {database}")
    
    query = f"""
    SELECT 
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE COLUMN_NAME LIKE '%{pattern}%'
    ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    current_db = conn.cursor().execute("SELECT CURRENT_DATABASE()").fetchone()[0]
    
    table = Table(title=f"Columns matching '{pattern}' in {current_db}")
    table.add_column("Schema", style="blue")
    table.add_column("Table", style="cyan")
    table.add_column("Column", style="green")
    table.add_column("Data Type", style="yellow")
    
    count = 0
    for row in cursor:
        table.add_row(row[0], row[1], row[2], row[3])
        count += 1
    
    console.print(table)
    console.print(f"\nFound {count} matching columns")
    cursor.close()

def analyze_relationships(conn, table_name, database=None, schema=None):
    """Analyze potential relationships between tables based on column names."""
    if database:
        conn.cursor().execute(f"USE DATABASE {database}")
    if schema:
        conn.cursor().execute(f"USE SCHEMA {schema}")
    
    # Get columns for the target table
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    columns = [row[0] for row in cursor]
    cursor.close()
    
    potential_relationships = []
    
    with Progress() as progress:
        task = progress.add_task("[cyan]Analyzing relationships...", total=len(columns))
        
        for column in columns:
            # Skip some common non-join columns
            if column.lower() in ('created_at', 'updated_at', 'id', 'created_by', 'updated_by'):
                progress.update(task, advance=1)
                continue
                
            # Look for potential foreign key relationships
            query = f"""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE COLUMN_NAME = '{column}'
                AND (TABLE_NAME != '{table_name}' OR TABLE_SCHEMA != '{schema}')
            """
            
            cursor = conn.cursor()
            cursor.execute(query)
            
            for row in cursor:
                potential_relationships.append({
                    'source_table': table_name,
                    'source_schema': schema,
                    'source_column': column,
                    'target_schema': row[0],
                    'target_table': row[1],
                    'target_column': row[2]
                })
            
            cursor.close()
            progress.update(task, advance=1)
    
    if potential_relationships:
        table = Table(title=f"Potential Relationships for {schema}.{table_name}")
        table.add_column("Source Column", style="cyan")
        table.add_column("Target Table", style="green")
        table.add_column("Target Column", style="yellow")
        
        for rel in potential_relationships:
            table.add_row(
                rel['source_column'],
                f"{rel['target_schema']}.{rel['target_table']}",
                rel['target_column']
            )
        
        console.print(table)
    else:
        console.print("[yellow]No potential relationships found.[/yellow]")

def save_to_allowlist(database, schema, table):
    """Add a table to the allowlist."""
    allowlist_file = "table_allowlist.json"
    
    if os.path.exists(allowlist_file):
        with open(allowlist_file, 'r') as f:
            try:
                allowlist = json.load(f)
            except json.JSONDecodeError:
                allowlist = []
    else:
        allowlist = []
    
    # Check if table already exists
    for entry in allowlist:
        if (isinstance(entry, dict) and 
            entry.get('database') == database and 
            entry.get('schema') == schema and 
            entry.get('table') == table):
            console.print("[yellow]Table already in allowlist.[/yellow]")
            return
    
    # Add new entry
    new_entry = {
        "database": database,
        "schema": schema,
        "table": table,
        "description": "",
        "notes": "",
        "common_joins": [],
        "key_columns": []
    }
    
    allowlist.append(new_entry)
    
    with open(allowlist_file, 'w') as f:
        json.dump(allowlist, f, indent=2)
    
    console.print(f"[green]Added {database}.{schema}.{table} to allowlist.[/green]")

def main():
    parser = argparse.ArgumentParser(description="Snowflake Schema Discovery Tool")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # List databases command
    list_db_parser = subparsers.add_parser("list-databases", help="List available databases")
    
    # List schemas command
    list_schema_parser = subparsers.add_parser("list-schemas", help="List schemas in a database")
    list_schema_parser.add_argument("--database", "-d", help="Database name")
    
    # List tables command
    list_tables_parser = subparsers.add_parser("list-tables", help="List tables in a schema")
    list_tables_parser.add_argument("--database", "-d", help="Database name")
    list_tables_parser.add_argument("--schema", "-s", help="Schema name")
    list_tables_parser.add_argument("--show-size", action="store_true", help="Show table sizes")
    
    # Describe table command
    describe_parser = subparsers.add_parser("describe", help="Describe a table")
    describe_parser.add_argument("table", help="Table name")
    describe_parser.add_argument("--database", "-d", help="Database name")
    describe_parser.add_argument("--schema", "-s", help="Schema name")
    
    # Find columns command
    find_parser = subparsers.add_parser("find", help="Find columns matching a pattern")
    find_parser.add_argument("pattern", help="Column name pattern")
    find_parser.add_argument("--database", "-d", help="Database name")
    
    # Analyze relationships command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze table relationships")
    analyze_parser.add_argument("table", help="Table name")
    analyze_parser.add_argument("--database", "-d", help="Database name")
    analyze_parser.add_argument("--schema", "-s", help="Schema name")
    
    # Add to allowlist command
    allowlist_parser = subparsers.add_parser("add-to-allowlist", help="Add table to allowlist")
    allowlist_parser.add_argument("--database", "-d", required=True, help="Database name")
    allowlist_parser.add_argument("--schema", "-s", required=True, help="Schema name")
    allowlist_parser.add_argument("--table", "-t", required=True, help="Table name")
    
    args = parser.parse_args()
    
    # Connect to Snowflake
    try:
        conn = connect_to_snowflake()
        
        if args.command == "list-databases":
            list_databases(conn)
        elif args.command == "list-schemas":
            list_schemas(conn, args.database)
        elif args.command == "list-tables":
            list_tables(conn, args.database, args.schema, args.show_size)
        elif args.command == "describe":
            describe_table(conn, args.table, args.database, args.schema)
        elif args.command == "find":
            find_columns(conn, args.pattern, args.database)
        elif args.command == "analyze":
            analyze_relationships(conn, args.table, args.database, args.schema)
        elif args.command == "add-to-allowlist":
            save_to_allowlist(args.database, args.schema, args.table)
        else:
            parser.print_help()
        
        conn.close()
        
    except Exception as e:
        console.print(f"[bold red]Error[/bold red]: {e}")

if __name__ == "__main__":
    main() 