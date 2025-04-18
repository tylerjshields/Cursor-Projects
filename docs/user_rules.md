# DoorDash NV Snowflake Query Rules

REVIEW THESE RULES BEFORE EVERY PROMPT. TO ENSURE COMPLIANCE, QUICKLY ACKNOWLEDGE TO ME THAT YOU HAVE REVIEWED THE RULES BEFORE EVERY ANSWER, BUT ONLY IF YOU ARE 100% SURE YOU UNDERSTAND THEM AND WILL CONSIDER ALL OF THEM IN YOUR ANSWER.

## MANDATORY RULES (NEVER VIOLATE THESE)

1. ⚠️ CRITICAL: NEVER MAKE CHANGES THAT I DID NOT SPECIFICALLY ASK FOR IN MY PROMPT. REVIEW THE FILE YOU ARE EDITING TO INDEX ANY CHANGES THAT I MADE MANUALLY since your last edit, and don't revert them.
2. ⚠️ CRITICAL: NEVER create multiple exploratory/temporary scripts unless explicitly requested. Focus ONLY on the exact task I give you.
3. ⚠️ CRITICAL: ALWAYS verify and understand data structures BEFORE writing complex queries.
4. You MUST NEVER assume table structures exist without verification. Your workflow MUST be: 1) Confirm table structure 2) Verify data availability 3) THEN write the main query. Any table or column not explicitly confirmed should be treated as non-existant.
5. Ask me before running SQL Scripts to update my tables, especially if they may take a long time. You can run your own validation scripts.
6. Always give me an estimated confidence level for your responses.
7. When I refer to a "new task" I generally mean we are switching context to a different workstream. If you can guess which folder structure I should be working out of, confirm with me that you want to use the associated directory (or follow my explicit instructions).

## Temporary Files:
1. NEVER write inside my folders unless specifically instructed. All queries/scripts that are autogenerated, such as for validation, packaging queries to run together, should be placed in a tmp folder for the relevant directory (e.g. tmp files for notifications work should live in notifications/tmp)
2. Before creating any script outside of my tmp folders, beyond what I've specifically requested, STOP and ask for permission first.
3. Avoid creating lots of temporary files that will need to be cleaned up. Prefer to overwrite/update your temporary/validation scripts rather than creating a new one.

## SQL Best Practices

For SQL, always use SnowFlake SQL syntax. Prefer:
- Compact SQL with consistent indentations, including single-line joins where only one or two conditions exist, and putting the first condition on the same line as the where clause.
- Comments related to code functionality, not tracking what you did, e.g. "optimized version" or "removed extra columns"
- "Group By All" over listing specific columns or column numbers. 
- Use IFF() instead of binary case when statements.
- Don't add unnecessary parentheses such as current_date() or wrapping create table statements
- Commas at the end of lines

## Table Usage Rules For Query Generation

1. **Only recommend tables from the trusted allowlist for queries**
   - The `table_allowlist.json` contains trusted, fully-documented tables
   - These tables have been vetted and contain accurate metadata
   - Always prefer these tables for query recommendations
   - Prioritize tables based on their tier (see Table Tier Guidelines below)

2. **Verify table existence using the schema repository**
   - Before referencing any table not in the allowlist, check `schema_repository.json`
   - This repository contains a lightweight list of tables confirmed to exist
   - Never reference tables not found in either the allowlist or schema repository

3. **Prevent hallucination of non-existent tables**
   - Do not infer the existence of tables based on naming patterns
   - Do not suggest joins to tables that aren't explicitly in our repositories
   - Only include data directly pulled from the database

4. **Terminology clarifications**
   - "Rx" almost always refers to "restaurants", not prescriptions. For example the table fact_non_rx_order_item_details refers to non-restaurant orders, not non-prescription.
   - "New Verticals" or "NV" refers to non-restaurant orders (grocery, convenience aka 3PC, DashMart aka 1PC, alcohol, retail)

## Table Tier Guidelines

Tables in the allowlist are categorized into tiers to indicate their reliability and recommended usage:

1. **Tier 1: Primary Tables**
   - Very commonly used, highly trustworthy tables
   - Cover all of New Verticals (and possibly inclusive of Rx)
   - Always prefer these tables first for query recommendations

2. **Tier 2: Specialized Tables**
   - Trustworthy but more niche tables
   - Applicable only to specific verticals or less common use cases
   - Use when Tier 1 tables don't provide the necessary data

3. **Tier 3: Limited Use Tables**
   - Tables created for adhoc analysis or specific use cases
   - Should not be recommended for query generation unless there is very high confidence that it's directly related to the specific task
   - Include clear comments when using these tables

4. **Tier 4: Adhoc/Scratch Tables**
   - Should not be referenced unless directly asked for
   - Generally should not be included in the allowlist
   - Subject to cleanup and may be removed periodically

## Workflow for Table References

1. First, check if the table exists in `table_allowlist.json`
   - Begin with Tier 1 tables that are relevant to the query
   - Only move to lower tiers if necessary for the specific use case
2. If not found, verify existence in `schema_repository.json`
3. If the table isn't in either source, do not reference it

## Adding Tables to Allowlist

1. **Verify table existence**
   - Use the schema repository to verify table existence before adding to allowlist
   - If a table doesn't exist in the schema repository, first confirm it exists in Snowflake

2. **Use the add_to_allowlist.py script**
   - This ensures all required metadata is provided
   - Example: `python add_to_allowlist.py --schema finance --table dimension_deliveries --tier 1 --description "Main table for delivery data" --notes "Always filter on is_filtered_core=TRUE" --key-columns delivery_id order_id store_id`

3. **Provide comprehensive documentation**
   - Always include a clear description
   - Assign an appropriate tier based on the table's reliability and usage
   - Add notes about filtering requirements
   - List key columns that are commonly used
   - Include common join tables when applicable
   - Document the schema with column names and datatypes when possible for important tables
   - Example command with manual columns: `python add_to_allowlist.py --schema finance --table dimension_deliveries --tier 1 --description "Main table for delivery data" --notes "filter on is_filtered_core=TRUE unless you want to include cancellations" --key-columns delivery_id order_id store_id --columns "delivery_id:VARCHAR" "store_id:VARCHAR" "created_at:TIMESTAMP_NTZ"`
   - Alternatively, automatically fetch column information with: `python add_to_allowlist.py --schema finance --table dimension_deliveries --tier 1 --description "Main table for delivery data" --notes "Always filter on is_filtered_core=TRUE" --key-columns delivery_id order_id store_id --fetch-columns`

4. **Regularly validate the allowlist**
   - Use `verify_allowlist.py` to check if all tables in the allowlist exist in the schema repository
   - Run `update_schema_repo.py` periodically to refresh the schema repository with actual tables from Snowflake
   - Use `update_table_columns.py` to enhance existing tables in the allowlist with column information from Snowflake
   - Examples:
     - Update all tables: `python update_table_columns.py --all`
     - Update a specific table: `python update_table_columns.py --table dimension_deliveries --schema finance`
     - Update all tables in a schema: `python update_table_columns.py --schema cng`

## Context
- Our team works in the New Verticals org, which is DoorDash orders for anything that is not a restaurant, such as Grocery, Convenience (3rd party or 1st Party, aka DashMart), alcohol, and Retail categories. 