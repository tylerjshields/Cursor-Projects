-- Generate individual INSERT statements for each engagement program query
-- These statements will populate the nvg_channels_ep_daily table with actual results
-- Execute these INSERT statements to get real consumer IDs

-- Set date parameters - modify these before executing the generated queries
SET START_DATE = CURRENT_DATE();
SET END_DATE = CURRENT_DATE();

-- Get all the programs and their queries
WITH program_queries AS (
    SELECT 
        e.name AS program_name,
        e.query AS original_query
    FROM growth_service_prod.public.engagement_program e
    JOIN (
        SELECT DISTINCT ep_name
        FROM proddb.public.nv_channels_notif_index
        WHERE ep_name IS NOT NULL
    ) n ON e.name = n.ep_name
    WHERE e.query IS NOT NULL
)

-- Generate an INSERT statement for each program
SELECT 
    'INSERT INTO proddb.public.nvg_channels_ep_daily (ds, ep_name, consumer_id, total_consumers_in_program)
WITH program_results AS (
    ' || REPLACE(
           REPLACE(original_query, ':START_DATE', '$START_DATE'), 
           ':END_DATE', '$END_DATE'
        ) || '
)
SELECT 
    CURRENT_DATE() AS ds,
    ''' || program_name || ''' AS ep_name,
    consumer_id,
    (SELECT COUNT(*) FROM program_results) AS total_consumers_in_program
FROM program_results;' AS insert_statement,
    program_name
FROM program_queries
ORDER BY program_name;

-- How to use this:
-- 1. Execute this script to generate INSERT statements for each program
-- 2. Copy each INSERT statement
-- 3. Execute each INSERT statement individually to populate the table with real consumer IDs
-- 4. If needed, adjust the date parameters at the top before generating the statements 