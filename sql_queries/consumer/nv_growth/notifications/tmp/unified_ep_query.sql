-- Unified query that generates and executes the union of all EP queries
-- Set your date parameters here
SET START_DATE = CURRENT_DATE();
SET END_DATE = CURRENT_DATE();

-- Generate and immediately execute the combined query
WITH recent_runs AS (
    SELECT 
        program_name,
        MAX(run_at) AS latest_run_date
    FROM growth_service_prod.public.engagement_program_run_results
    GROUP BY program_name
),

ep_list AS (
    SELECT DISTINCT ep_name
    FROM proddb.public.nv_channels_notif_index
    WHERE ep_name IS NOT NULL
),

program_queries AS (
    SELECT 
        e.name AS program_name,
        e.query AS original_query,
        r.latest_run_date
    FROM growth_service_prod.public.engagement_program e
    JOIN ep_list n ON e.name = n.ep_name
    LEFT JOIN recent_runs r ON e.name = r.program_name
    WHERE e.query IS NOT NULL
),

all_program_results AS (
    -- Execute each query and union the results
    -- This uses dynamic SQL execution through a lateral join approach
    SELECT
        COALESCE(DATE(r.latest_run_date), CURRENT_DATE()) AS ds,
        p.program_name AS ep_name,
        t.consumer_id,
        t.*
    FROM program_queries p
    -- Using LATERAL to dynamically run each program's query
    -- This dynamically executes all program queries with proper date parameters
    ,LATERAL (
        -- Parse the original query and execute it with date parameters
        SELECT * 
        FROM TABLE(RESULT_SCAN(EXECUTE IMMEDIATE 
            'SELECT consumer_id, * FROM (' || 
            REPLACE(
                REPLACE(p.original_query, ':START_DATE', $START_DATE::VARCHAR), 
                ':END_DATE', $END_DATE::VARCHAR
            ) || 
            ') sub_query'
        ))
    ) t
)

-- Final output with consumer counts per program
SELECT 
    ds,
    ep_name,
    consumer_id,
    COUNT(*) OVER (PARTITION BY ep_name) AS total_consumers_in_program
FROM all_program_results
ORDER BY ep_name, consumer_id
LIMIT 1000; 