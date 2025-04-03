-- Script to fetch the query for ep_dd_consumer_nv_dormant_cx_push_cx_expansion and show what would be executed
-- Set date parameters
SET start_date = 'CURRENT_DATE()';
SET end_date = 'CURRENT_DATE()';

-- Get the original query for the specific EP
WITH program_query AS (
    SELECT 
        e.name AS program_name,
        e.query AS original_query
    FROM growth_service_prod.public.engagement_program e
    WHERE e.name = 'ep_dd_consumer_nv_dormant_cx_push_cx_expansion'
    AND e.query IS NOT NULL
)

-- Generate the actual insert statement that would be executed
SELECT 
    'INSERT INTO proddb.public.nvg_channels_ep_daily (ds, ep_name, consumer_id) ' || 
    'SELECT ' || 
    'TO_DATE(''' || CURRENT_DATE() || ''') AS ds, ' || 
    '''' || program_name || '''' || ' AS ep_name, ' || 
    'consumer_id ' || 
    'FROM (' || 
    REPLACE(
        REPLACE(
            REPLACE(original_query, ';', ''),
            ':START_DATE', '''' || $start_date || ''''
        ),
        ':END_DATE', '''' || $end_date || ''''
    ) || 
    ')' AS actual_insert_statement,
    original_query AS raw_ep_query
FROM program_query;

-- Additionally, show just the inner query to understand how the dates are processed
WITH program_query AS (
    SELECT 
        e.name AS program_name,
        e.query AS original_query
    FROM growth_service_prod.public.engagement_program e
    WHERE e.name = 'ep_dd_consumer_nv_dormant_cx_push_cx_expansion'
    AND e.query IS NOT NULL
)

SELECT 
    REPLACE(
        REPLACE(
            REPLACE(original_query, ';', ''),
            ':START_DATE', '''' || $start_date || ''''
        ),
        ':END_DATE', '''' || $end_date || ''''
    ) AS processed_inner_query
FROM program_query;

-- Check for different variations of to_date functions 
WITH program_query AS (
    SELECT 
        e.name AS program_name,
        e.query AS original_query
    FROM growth_service_prod.public.engagement_program e
    WHERE e.name = 'ep_dd_consumer_nv_dormant_cx_push_cx_expansion'
    AND e.query IS NOT NULL
)

SELECT 
    REGEXP_COUNT(original_query, 'to_date\\s*\\(\\s*:START_DATE\\s*\\)', 'i') AS to_date_start_count,
    REGEXP_COUNT(original_query, 'to_date\\s*\\(\\s*:END_DATE\\s*\\)', 'i') AS to_date_end_count,
    REGEXP_COUNT(original_query, 'date_part.*to_date\\s*\\(\\s*:END_DATE\\s*\\)', 'i') AS date_part_count,
    -- Look for other possible date function patterns
    REGEXP_COUNT(original_query, 'date\\s*\\(\\s*:END_DATE\\s*\\)', 'i') AS date_func_count,
    REGEXP_COUNT(original_query, 'dateadd\\s*\\([^)]*:END_DATE', 'i') AS dateadd_count,
    REGEXP_COUNT(original_query, 'datediff\\s*\\([^)]*:END_DATE', 'i') AS datediff_count
FROM program_query; 