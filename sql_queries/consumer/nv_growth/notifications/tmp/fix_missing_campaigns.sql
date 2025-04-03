-- Fix for campaigns missing from dashboard due to NULL EP names

-- 1. First, confirm if the missing campaigns have NULL EP names
SELECT 
    clean_campaign_name,
    campaign_id,
    canvas_id,
    ep_name
FROM proddb.public.nv_channels_notif_index
WHERE clean_campaign_name IN ('CG&A Cart Abandon w/ out Promo', 'CG&A Cart Abandon w/ Promo');

-- 2. If they have NULL EP names, let's first see if we should use the same campaign name as EP name
-- Create a temporary table with the corrected EP names
CREATE OR REPLACE TABLE proddb.public.tmp_campaign_ep_name_fixes AS
SELECT 
    clean_campaign_name,
    campaign_id,
    canvas_id,
    -- For these specific campaigns, use the clean_campaign_name as the EP name
    clean_campaign_name AS suggested_ep_name
FROM proddb.public.nv_channels_notif_index
WHERE clean_campaign_name IN ('CG&A Cart Abandon w/ out Promo', 'CG&A Cart Abandon w/ Promo');

-- 3. Update the notification index with the corrected EP names
UPDATE proddb.public.nv_channels_notif_index n
SET ep_name = f.suggested_ep_name
FROM proddb.public.tmp_campaign_ep_name_fixes f
WHERE n.clean_campaign_name = f.clean_campaign_name
  AND n.campaign_id IS NOT DISTINCT FROM f.campaign_id
  AND n.canvas_id IS NOT DISTINCT FROM f.canvas_id
  AND n.ep_name IS NULL;

-- 4. Verify the changes were made correctly
SELECT 
    clean_campaign_name,
    campaign_id,
    canvas_id,
    ep_name
FROM proddb.public.nv_channels_notif_index
WHERE clean_campaign_name IN ('CG&A Cart Abandon w/ out Promo', 'CG&A Cart Abandon w/ Promo');

-- 5. To ensure this change is reflected in the dashboard, we need to rebuild the tables
-- The next steps would be to run the full pipeline:
-- 1. nvg_notif_core_metrics_multi_channel
-- 2. nvg_notif_nv_metrics_multi_channel
-- 3. nvg_notif_metrics_base_multi_channel 
-- 4. nvg_notif_metrics_multi_channel
-- 5. nvg_notifs_metrics_dashboard_multi_channel

-- The fix_email_records.sql and update_full_pipeline.sql scripts can be used for this
-- Comment out this section and run the full pipeline scripts after confirming the EP name update 