-- Check structure of engagement_program table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'GROWTH_SERVICE_PROD'
  AND table_name = 'ENGAGEMENT_PROGRAM'
ORDER BY ordinal_position;

-- Get sample data from engagement_program table
SELECT *
FROM growth_service_prod.public.engagement_program
LIMIT 10;

-- Check available programs that match with the notifs_index
SELECT 
    e.name AS program_name, 
    COUNT(DISTINCT n.clean_campaign_name) AS matching_campaigns,
    COUNT(*) AS total_notifs_records
FROM growth_service_prod.public.engagement_program e
JOIN proddb.public.nv_channels_notif_index n
    ON e.name = n.ep_name
GROUP BY e.name
ORDER BY matching_campaigns DESC;

-- Check example query structure for a specific program
SELECT 
    e.name AS program_name,
    e.query AS query_text,
    LENGTH(e.query) AS query_length
FROM growth_service_prod.public.engagement_program e
JOIN proddb.public.nv_channels_notif_index n
    ON e.name = n.ep_name
WHERE e.query IS NOT NULL
ORDER BY query_length DESC
LIMIT 1; 