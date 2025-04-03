-- Find the actual table that stores engagement program run results details
SELECT 
    table_catalog,
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema = 'GROWTH_SERVICE_PROD'
  AND table_name LIKE '%ENGAGEMENT%PROGRAM%RUN%'
ORDER BY table_name;

-- Examine the engagement_program_run_results table structure
-- to understand how results are stored
SELECT 
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'GROWTH_SERVICE_PROD'
  AND table_name = 'ENGAGEMENT_PROGRAM_RUN_RESULTS'
ORDER BY ordinal_position;

-- Sample data from engagement_program_run_results
SELECT TOP 10 *
FROM growth_service_prod.public.engagement_program_run_results
ORDER BY run_at DESC;

-- Modified query to get the most recent results using only the confirmed tables
-- This query doesn't rely on the _details table that we're not sure exists
WITH recent_runs AS (
    SELECT 
        program_name,
        run_id,
        run_at,
        consumer_ids, -- Check if consumer IDs are stored in this table
        ROW_NUMBER() OVER (PARTITION BY program_name ORDER BY run_at DESC) AS run_rank
    FROM growth_service_prod.public.engagement_program_run_results
    -- Only include programs that are in our notification index
    WHERE program_name IN (
        SELECT DISTINCT ep_name 
        FROM proddb.public.nv_channels_notif_index
        WHERE ep_name IS NOT NULL
    )
),

-- Get the most recent run_id for each program
latest_runs AS (
    SELECT 
        program_name,
        run_id,
        run_at,
        consumer_ids
    FROM recent_runs
    WHERE run_rank = 1
)

-- Get information about the latest runs
SELECT 
    DATE(run_at) AS ds,
    program_name AS ep_name,
    run_id,
    -- If consumer_ids exists in the results table, we can work with that
    -- Otherwise, we might need to look for a different approach
    consumer_ids,
    -- If there's a count column
    consumer_count
FROM latest_runs
ORDER BY program_name; 