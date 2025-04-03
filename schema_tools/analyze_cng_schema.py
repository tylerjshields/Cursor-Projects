#!/usr/bin/env python
"""
Script to analyze the tables in the EDW.CNG schema and generate insights.
This helps understand patterns in table naming, identify table categories, 
and generate statistics about table structure.
"""

import os
import json
import re
from collections import Counter, defaultdict
import snowflake.connector
from snowflake_credentials import get_snowflake_credentials
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from datetime import datetime

load_dotenv()
console = Console()

def connect_to_snowflake():
    """Connect to Snowflake using credentials from snowflake_credentials module"""
    params = get_snowflake_credentials()
    conn = snowflake.connector.connect(**params)
    return conn

def get_cng_tables_with_metadata(conn, database="EDW", schema="CNG", limit=100):
    """Query Snowflake to get tables and their metadata from the specified schema"""
    tables = []
    
    cursor = conn.cursor()
    try:
        # Get tables with row count and last modified
        query = f"""
        SELECT 
            table_name,
            table_type,
            row_count,
            bytes,
            last_altered
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = '{schema}'
        ORDER BY bytes DESC
        LIMIT {limit}
        """
        cursor.execute(query)
        
        # Add each table to our list
        for row in cursor.fetchall():
            table_name, table_type, row_count, bytes, last_altered = row
            tables.append({
                "table_name": table_name,
                "table_type": table_type,
                "row_count": row_count or 0,
                "size_mb": round((bytes or 0) / (1024 * 1024), 2),
                "last_altered": last_altered
            })
                
    except Exception as e:
        console.print(f"[red]Error querying Snowflake: {e}[/red]")
    finally:
        cursor.close()
        
    return tables

def get_table_columns(conn, database, schema, table_name):
    """Get column information for a specific table"""
    columns = []
    
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        cursor.execute(query)
        
        for row in cursor.fetchall():
            column_name, data_type, is_nullable = row
            columns.append({
                "name": column_name,
                "type": data_type,
                "nullable": is_nullable
            })
    except Exception as e:
        console.print(f"[red]Error getting columns for {table_name}: {e}[/red]")
    finally:
        cursor.close()
        
    return columns

def analyze_table_prefixes(tables):
    """Analyze common prefixes in table names"""
    prefixes = Counter()
    
    # Common prefixes to look for
    prefix_patterns = [
        ("dimension_", "dimension tables"),
        ("dim_", "dimension tables"),
        ("fact_", "fact tables"),
        ("agg_", "aggregated tables"),
        ("vw_", "views"),
        ("stg_", "staging tables"),
        ("tmp_", "temporary tables"),
        ("lkp_", "lookup tables"),
        ("non_rx_", "non-restaurant"),
        ("temp_", "temporary tables"),
        ("snapshot_", "snapshot tables")
    ]
    
    # Count the occurrences of each prefix
    for table in tables:
        table_name = table["table_name"].lower()
        for prefix, desc in prefix_patterns:
            if table_name.startswith(prefix):
                prefixes[prefix] += 1
                break
    
    return prefixes

def analyze_table_categories(tables):
    """Analyze tables by business category based on name patterns"""
    categories = defaultdict(list)
    
    # Define category patterns to look for
    category_patterns = [
        (r"convenience|conv\b", "Convenience"),
        (r"grocery", "Grocery"),
        (r"alcohol|liquor|beer|wine", "Alcohol"),
        (r"retail", "Retail"),
        (r"pharmacy|rx\b", "Pharmacy/Rx"),
        (r"dashmart", "DashMart"),
        (r"pet|petco", "Pet"),
        (r"flower", "Flowers"),
        (r"store_tag", "Store Tagging"),
        (r"order_item", "Order Items"),
        (r"delivery", "Deliveries")
    ]
    
    # Categorize each table
    for table in tables:
        table_name = table["table_name"].lower()
        categorized = False
        
        for pattern, category in category_patterns:
            if re.search(pattern, table_name):
                categories[category].append(table["table_name"])
                categorized = True
                break
        
        if not categorized:
            categories["Other"].append(table["table_name"])
    
    return categories

def display_largest_tables(tables, limit=20):
    """Display the largest tables by size"""
    console.print("\n[bold cyan]Largest Tables by Size[/bold cyan]")
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("Table Name")
    table.add_column("Type")
    table.add_column("Row Count", justify="right")
    table.add_column("Size (MB)", justify="right")
    table.add_column("Last Modified")
    
    for t in sorted(tables, key=lambda x: x["size_mb"], reverse=True)[:limit]:
        table.add_row(
            t["table_name"],
            t["table_type"],
            f"{t['row_count']:,}",
            f"{t['size_mb']:,.2f}",
            str(t["last_altered"])
        )
    
    console.print(table)

def display_prefix_analysis(prefixes):
    """Display analysis of table name prefixes"""
    console.print("\n[bold cyan]Table Name Prefix Analysis[/bold cyan]")
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("Prefix")
    table.add_column("Count", justify="right")
    table.add_column("Description")
    
    # Mapping of prefixes to descriptions
    prefix_desc = {
        "dimension_": "Dimension tables containing descriptive attributes",
        "dim_": "Dimension tables (abbreviated form)",
        "fact_": "Fact tables containing metrics and measurements",
        "agg_": "Pre-aggregated tables for reporting",
        "vw_": "Views",
        "stg_": "Staging tables for ETL processes",
        "tmp_": "Temporary tables",
        "lkp_": "Lookup tables",
        "non_rx_": "Non-restaurant related tables",
        "temp_": "Temporary tables",
        "snapshot_": "Point-in-time snapshot tables"
    }
    
    for prefix, count in prefixes.most_common():
        table.add_row(
            prefix,
            str(count),
            prefix_desc.get(prefix, "")
        )
    
    console.print(table)

def display_category_analysis(categories):
    """Display analysis of table categories"""
    console.print("\n[bold cyan]Table Category Analysis[/bold cyan]")
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("Category")
    table.add_column("Count", justify="right")
    table.add_column("Example Tables")
    
    for category, tables in sorted(categories.items(), key=lambda x: len(x[1]), reverse=True):
        example_tables = ", ".join(tables[:3])
        if len(tables) > 3:
            example_tables += f", ... ({len(tables)-3} more)"
            
        table.add_row(
            category,
            str(len(tables)),
            example_tables
        )
    
    console.print(table)

def save_analysis_to_file(tables, prefixes, categories, filename="cng_schema_analysis.json"):
    """Save the analysis results to a JSON file"""
    # Convert datetime objects to strings and handle other non-serializable types
    def prepare_for_json(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)
    
    analysis = {
        "metadata": {
            "analysis_date": datetime.now().strftime("%Y-%m-%d"),
            "total_tables": len(tables)
        },
        "largest_tables": [
            {k: prepare_for_json(v) if not isinstance(v, (int, float, str, bool, type(None))) else v 
             for k, v in table.items()}
            for table in sorted(tables, key=lambda x: x["size_mb"], reverse=True)[:20]
        ],
        "prefixes": {prefix: count for prefix, count in prefixes.most_common()},
        "categories": {category: tables for category, tables in categories.items()}
    }
    
    with open(filename, 'w') as f:
        json.dump(analysis, f, indent=2)
    
    console.print(f"\n[green]Analysis saved to {filename}[/green]")

def main():
    """Main function to analyze the CNG schema"""
    console.print("[bold]Analyzing EDW.CNG Schema...[/bold]")
    
    conn = connect_to_snowflake()
    
    console.print("Fetching table metadata...")
    tables = get_cng_tables_with_metadata(conn)
    
    if not tables:
        console.print("[red]No tables found or error occurred.[/red]")
        return
    
    console.print(f"[green]Found {len(tables)} tables in EDW.CNG schema[/green]")
    
    # Analyze table prefixes
    prefixes = analyze_table_prefixes(tables)
    
    # Analyze table categories
    categories = analyze_table_categories(tables)
    
    # Display analysis
    display_largest_tables(tables)
    display_prefix_analysis(prefixes)
    display_category_analysis(categories)
    
    # Save analysis to file
    save_analysis_to_file(tables, prefixes, categories)
    
    conn.close()
    console.print("[bold green]Analysis complete![/bold green]")

if __name__ == "__main__":
    main() 