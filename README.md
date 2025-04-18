# NV Analytics Toolkit

Tools and scripts for New Verticals analytics at DoorDash.

## Project Structure

```
.
├── config/              # Configuration files
├── docs/                # Documentation
├── schema_tools/        # Tools for managing schema information
├── sql_queries/         # SQL query files organized by pod/team/workstream
├── utils/               # Utility scripts
└── tests/               # Test scripts
```

## Getting Started

1. Clone this repository
2. Copy `.env.example` to `.env` and update with your credentials
3. Install the required dependencies: `pip install -r config/requirements.txt`
4. Activate the virtual environment: `source snowflake_env/bin/activate`

## Usage

### Running Queries

```bash
python utils/run_query.py "SELECT current_user(), current_role(), current_database()"
python utils/run_sql_file.py sql_queries/examples/example_queries.sql
```

### Working with Schema

The allowlist system helps manage and document tables:

```bash
# Add a table to the allowlist
python schema_tools/add_to_allowlist.py --schema FINANCE --table DIMENSION_DELIVERIES --tier 1 --description "Main delivery table" --fetch-columns
```

## Table Tier System

1. **Tier 1: Primary Tables** - Very commonly used, highly trustworthy tables
2. **Tier 2: Specialized Tables** - Trustworthy but more niche tables
3. **Tier 3: Limited Use Tables** - Tables created for specific use cases
4. **Tier 4: Adhoc/Scratch Tables** - Should not be referenced unless directly requested 

# Cursor Projects

## Snowflake Connection Update

**Important:** We are transitioning from our custom Snowflake connection implementation to the official VS Code Snowflake extension. 

### What's Changing

- The custom Python-based Snowflake connector (`utils/snowflake_connector.py`) is being deprecated
- The official Snowflake VS Code extension is now the recommended way to connect

### What You Need to Do

1. Install the Snowflake VS Code extension in Cursor
2. Verify your connection settings in `~/.snowflake/connections.toml`
3. Begin using the extension for running SQL queries
4. Update any scripts that import from the old connector

For detailed migration instructions, see [Snowflake Migration Guide](docs/snowflake_migration.md). 