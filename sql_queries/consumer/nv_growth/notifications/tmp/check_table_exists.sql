-- Check if engagement_program_run_results_detail table exists
SELECT 
    table_catalog,
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema = 'GROWTH_SERVICE_PROD'
  AND table_name LIKE '%ENGAGEMENT_PROGRAM_RUN_RESULTS%'
ORDER BY table_name;

-- Alternative check using show tables command
SHOW TABLES LIKE '%ENGAGEMENT_PROGRAM_RUN_RESULTS%' IN growth_service_prod.public; 