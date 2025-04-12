-- First, verify the table structure before writing the final query
-- DESC TABLE proddb.public.nv_channels_notif_index;

-- Query to generate curie_urls for each analysis_id in notifs_index
-- This is a generic template to be adjusted based on actual column names
SELECT 
    ni.*,  -- Include all notif_index columns for reference
    ea.experiment_id,
    'https://admin-gateway.doordash.com/decision-systems/experiments/' || ea.experiment_id || '?analysisId=' || ea.id AS curie_url
FROM proddb.public.nv_channels_notif_index ni
JOIN curie_experiments_prod.public.maindb_experiment_analysis ea ON ea.id = ni.analysis_id
WHERE ni.analysis_id IS NOT NULL;

-- Note: This query assumes the analysis_id column will exist in nv_channels_notif_index.
-- It will only work once that column has been added to the table.
-- Adjust the query once actual column names are confirmed. 