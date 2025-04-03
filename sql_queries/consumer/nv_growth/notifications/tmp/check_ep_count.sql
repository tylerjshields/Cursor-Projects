-- Count distinct ep_names in the notifs index and check for corresponding queries
WITH index_eps AS (
    SELECT DISTINCT ep_name
    FROM proddb.public.nv_channels_notif_index
    WHERE ep_name IS NOT NULL
)

SELECT 
    i.ep_name,
    CASE 
        WHEN e.name IS NOT NULL AND e.query IS NOT NULL THEN 'Has query'
        WHEN e.name IS NOT NULL AND e.query IS NULL THEN 'Program exists but no query'
        ELSE 'No matching program'
    END AS status
FROM index_eps i
LEFT JOIN growth_service_prod.public.engagement_program e
    ON i.ep_name = e.name
ORDER BY status, i.ep_name; 