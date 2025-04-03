# Snowflake SQL Tools

A collection of scripts for connecting to Snowflake, executing SQL queries, and documenting schema metadata.

## Setup

### Prerequisites
- Python 3.8+
- Git (optional, for version control)

### Installation

1. Clone or download this repository to your local machine.

2. Create a virtual environment:
   ```bash
   python -m venv snowflake_env
   ```

3. Activate the virtual environment:
   - On macOS/Linux:
     ```bash
     source snowflake_env/bin/activate
     ```
   - On Windows:
     ```bash
     snowflake_env\Scripts\activate
     ```

4. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

5. Set up your credentials:
   - Copy the `.env.example` file to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Edit the `.env` file with your actual Snowflake credentials

### Credentials Management

This project uses environment variables from a `.env` file to securely manage credentials. The credentials are loaded through the `snowflake_credentials.py` module, which ensures you don't need to hardcode sensitive information in your scripts.

⚠️ **IMPORTANT**: Never commit your `.env` file to git or share it publicly!

## Usage

### Testing Connection

To verify your Snowflake connection:

```bash
python test_connection.py
```

### Running SQL Queries

To run an ad-hoc query:

```bash
python run_query.py "SELECT current_user(), current_warehouse()"
```

To execute a SQL file with one or more statements:

```bash
python run_sql_file.py path/to/your_query.sql
```

### Working with Schema Documentation

#### Discovering Tables

To discover and explore available tables:

```bash
python discover_tables.py
```

This interactive tool allows you to:
- List databases and schemas
- Search for tables by name
- Add tables to your allowlist

#### Generating Documentation

To create documentation for your allowlisted tables:

```bash
python generate_schema_docs.py
```

This generates a Markdown file with detailed information about the tables in your allowlist.

#### Managing Table Allowlists

Tables to document are managed through the `table_allowlist.json` file. This ensures you only document trusted tables and can include business context beyond technical metadata.

To create a default allowlist:

```bash
python generate_schema_docs.py create-allowlist
```

Example allowlist entry:
```json
{
  "table": "dimension_deliveries",
  "schema": "finance",
  "database": "edw",
  "description": "Basic go-to table for delivery-level detail",
  "notes": "Primary key is delivery_id. Filter on is_filtered_core=TRUE to screen out test data.",
  "common_joins": ["edw.cng.dimension_new_verticals_store_tags"],
  "key_columns": ["delivery_id", "is_filtered_core", "variable_profit"]
}
```

## Table Tier System

Tables in the allowlist are categorized into tiers to indicate their reliability and recommended usage:

1. **Tier 1: Primary Tables**
   - Very commonly used, highly trustworthy tables
   - Cover all of New Verticals (and possibly inclusive of Rx)
   - Always preferred first for query recommendations

2. **Tier 2: Specialized Tables**
   - Trustworthy but more niche tables
   - Applicable only to specific verticals or less common use cases
   - Use when Tier 1 tables don't provide the necessary data

3. **Tier 3: Limited Use Tables**
   - Tables created for adhoc analysis or specific use cases
   - Should not be recommended for query generation unless there is very high confidence
   - Include clear comments when using these tables

4. **Tier 4: Adhoc/Scratch Tables**
   - Should not be referenced unless directly asked for
   - Generally should not be included in the allowlist
   - Subject to cleanup and may be removed periodically

## Column Schema Documentation

Tables in the allowlist can now include column names and datatypes:

```json
{
  "table": "dimension_deliveries",
  "schema": "finance",
  "database": "edw",
  "tier": 1,
  "description": "Main table for delivery-level data",
  "columns": {
    "delivery_id": "VARCHAR",
    "is_filtered_core": "BOOLEAN",
    "order_id": "VARCHAR",
    "store_id": "VARCHAR"
  }
}
```

### Adding Column Information

There are three ways to add column information to the allowlist:

1. When adding a new table with manual column specifications:
   ```
   python add_to_allowlist.py --schema finance --table dimension_deliveries --tier 1 --description "Main table for delivery data" --columns "delivery_id:VARCHAR" "store_id:VARCHAR"
   ```

2. When adding a new table with automatic column fetching:
   ```
   python add_to_allowlist.py --schema finance --table dimension_deliveries --tier 1 --description "Main table for delivery data" --fetch-columns
   ```

3. Updating existing tables with column information:
   ```
   python update_table_columns.py --all
   python update_table_columns.py --schema cng
   python update_table_columns.py --table dimension_deliveries --schema finance
   ```

## File Structure

- `snowflake_credentials.py` - Module for secure credential management
- `snowflake_connector.py` - Core connection functions
- `test_connection.py` - Script to verify Snowflake connectivity
- `run_query.py` - Execute ad-hoc queries
- `run_sql_file.py` - Run queries from SQL files
- `discover_tables.py` - Interactive table discovery tool
- `generate_schema_docs.py` - Generate schema documentation
- `generate_mcp_reference.py` - Create AI assistant configuration
- `.env` - Stores your Snowflake credentials (not committed to git)
- `.env.example` - Template for credential setup
- `requirements.txt` - Python package dependencies

## Contributing

If you'd like to contribute to this project, please:

1. Create a branch for your changes
2. Make your changes
3. Submit a pull request

Please ensure any changes maintain security best practices, especially around credential management. 