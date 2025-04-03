-- Fix for campaign_name in dashboard tables
-- Problem: ep_name values are appearing in the campaign_name column
-- Root cause: When joining records by ep_name, campaign_name is being replaced in some cases

-- The fix is to update the dashboard views to ensure campaign_name is preserved
-- We'll use the original metrics table (nvg_notif_metrics_multi_channel) campaign_name

CREATE OR REPLACE VIEW proddb.public.nvg_notifs_metrics_dashboard_multi_channel AS
WITH metrics AS (
    -- This captures all the metrics with proper campaign_name preservation
    SELECT 
        -- Ensure we're using the original campaign_name, not the ep_name
        m.campaign_name,
        m.ep_name,
        m.notification_channel,
        m.sent_week,
        m.time_window,
        -- Rest of metrics columns
        m.notifs_sent,
        m.cx_notifs_sent,
        m.notifs_sent_per_cx,
        -- etc.
        m.open,
        m.receive,
        m.visit,
        m.dd_order,
        m.nv_order,
        -- etc. for all metrics and rates
        m.send_to_open_rate,
        m.send_to_visit_rate,
        m.send_to_nv_order_rate,
        m.primary_engagement
    FROM proddb.public.nvg_notif_metrics_multi_channel m
),
targeting AS (
    -- Targeting information for campaigns
    SELECT 
        -- Use ep_name for joining with targeting info, but don't replace campaign_name
        program_name,
        week_date,
        approx_targeted_cx
    FROM proddb.public.nvg_notif_targeting_info
)

-- Final selection with targeting information and recency flag
SELECT 
    m.campaign_name,  -- Preserve the original campaign_name  
    m.ep_name,        -- Keep ep_name for reference
    m.notification_channel,
    m.sent_week as week_date,
    m.time_window,
    m.notifs_sent,
    m.cx_notifs_sent,
    m.notifs_sent_per_cx,
    m.open,
    m.receive,
    m.visit,
    m.dd_order,
    m.nv_order,
    -- etc. for all the metrics
    m.send_to_open_rate,
    m.send_to_visit_rate,
    m.send_to_nv_order_rate,
    -- Join with targeting info, but don't modify the campaign_name column
    t.approx_targeted_cx,
    m.notifs_sent / NULLIF(t.approx_targeted_cx, 0) as targeted_to_sent_rate,
    m.cx_notifs_sent / NULLIF(t.approx_targeted_cx, 0) as cx_targeted_to_sent_rate,
    IFF(m.sent_week = (SELECT MAX(sent_week) FROM metrics), 1, 0) AS is_most_recent_week
FROM metrics m
LEFT JOIN targeting t
    ON m.ep_name = t.program_name AND m.sent_week = t.week_date
ORDER BY m.sent_week DESC, m.campaign_name, m.notification_channel, m.time_window;

-- Create a check query to verify the fix
WITH check_result AS (
    SELECT 
        d.campaign_name as dashboard_campaign_name,
        d.ep_name as dashboard_ep_name,
        m.campaign_name as original_campaign_name,
        m.ep_name as original_ep_name,
        CASE
            WHEN d.campaign_name = m.campaign_name THEN 'MATCH'
            ELSE 'MISMATCH'
        END as name_status
    FROM proddb.public.nvg_notifs_metrics_dashboard_multi_channel d
    JOIN proddb.public.nvg_notif_metrics_multi_channel m
        ON d.ep_name = m.ep_name
        AND d.sent_week = m.sent_week
        AND d.time_window = m.time_window
        AND d.notification_channel = m.notification_channel
    WHERE d.campaign_name LIKE '%alcohol%' OR d.campaign_name LIKE '%grocery%'
    LIMIT 20
)

SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN name_status = 'MATCH' THEN 1 ELSE 0 END) as matching_names,
    SUM(CASE WHEN name_status = 'MISMATCH' THEN 1 ELSE 0 END) as mismatched_names,
    ROUND(SUM(CASE WHEN name_status = 'MATCH' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as match_percentage
FROM check_result; 