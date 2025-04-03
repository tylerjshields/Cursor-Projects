-- Check the structure of the engagement_program_run_results table
SELECT 
    column_name, 
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'GROWTH_SERVICE_PROD'
  AND table_name = 'ENGAGEMENT_PROGRAM_RUN_RESULTS'
ORDER BY ordinal_position;

-- Get a sample row to see the data
SELECT TOP 1 *
FROM growth_service_prod.public.engagement_program_run_results
ORDER BY run_at DESC; 