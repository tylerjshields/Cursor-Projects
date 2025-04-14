-- Notification Platform Funnel Analysis
-- Analyze drop rates in the notification funnel, focusing on prioritization and send stages


-- Create a daily funnel summary showing drop rates by team
CREATE OR REPLACE TABLE proddb.tylershields.nvg_notif_platform_funnel_summary AS
WITH combined_data AS (
    SELECT 
        event_date,
        team,
        campaign_name,
        clean_campaign_name,
        stage_name,
        status,
        SUM(event_count) as count
    FROM (
        SELECT 
            -- event_date,
            date_trunc('day', PRIORITIZATION_TIME)::date as event_date,
            coalesce(idx.team, 'Other') as team,
            coalesce(idx.campaign_name, p.PROGRAM_NAME) as campaign_name,
            coalesce(idx.clean_campaign_name, p.PROGRAM_NAME) as clean_campaign_name,
            
            'PRIORITIZATION' as stage_name,
            PRIORITIZATION_STATUS as status,
            count(*) as event_count
        FROM edw.growth.fact_notification_platform p
        LEFT JOIN proddb.public.nv_channels_notif_index idx
            ON p.PROGRAM_ID = coalesce(idx.campaign_id, idx.canvas_id)
        WHERE PRIORITIZATION_TIME >= dateadd('day', -14, current_date())
        AND p.user_entity_type = 'CONSUMER_DOORDASH'
        GROUP BY all
        
        UNION ALL
        
        SELECT 
            -- event_date,
            date_trunc('day', SEND_TIME)::date as event_date,
            coalesce(idx.team, 'Other') as team,
            coalesce(idx.campaign_name, p.PROGRAM_NAME) as campaign_name,
            coalesce(idx.clean_campaign_name, p.PROGRAM_NAME) as clean_campaign_name,
            'SEND' as stage_name,
            SEND_STATUS as status,
            count(*) as event_count
        FROM edw.growth.fact_notification_platform p
        LEFT JOIN proddb.public.nv_channels_notif_index idx
            ON p.PROGRAM_ID = coalesce(idx.campaign_id, idx.canvas_id)
        WHERE SEND_TIME >= dateadd('day', -14, current_date())
        AND p.user_entity_type = 'CONSUMER_DOORDASH'
        GROUP BY all
    )
    GROUP BY all
),

daily_team_metrics AS (
    SELECT
        
        team,
        event_date,
        date_trunc('week', event_date)::date as sent_week,
        campaign_name,
        clean_campaign_name,
        SUM(CASE WHEN stage_name = 'PRIORITIZATION' THEN count ELSE 0 END) as prioritization_total,
        SUM(CASE WHEN stage_name = 'PRIORITIZATION' AND status = 'SELECTED' THEN count ELSE 0 END) as prioritization_selected,
        SUM(CASE WHEN stage_name = 'SEND' AND status = 'SUCCESS' THEN count ELSE 0 END) as send_success,
        SUM(CASE WHEN stage_name = 'SEND' AND status IN ('CAPPED', 'DROPPED') THEN count ELSE 0 END) as send_dropped
    FROM combined_data
    GROUP BY all
)

SELECT
    event_date,
    sent_week,
    team,
    campaign_name,
    clean_campaign_name,
    prioritization_total,
    prioritization_selected,
    send_success,
    send_dropped,
    CASE 
        WHEN prioritization_selected > 0 THEN send_success / prioritization_selected 
        ELSE NULL 
    END as selected_to_success_rate,
    CASE 
        WHEN prioritization_selected > 0 THEN send_dropped / prioritization_selected 
        ELSE NULL 
    END as selected_to_dropped_rate
FROM daily_team_metrics
ORDER BY 1 DESC, 2, 3;

-- Grant select permissions on the summary table
GRANT SELECT ON proddb.tylershields.nvg_notif_platform_funnel_summary TO ROLE read_only_users; 




-- -- First, let's get prioritization stage data with eligibility check types
-- with prioritization_data as (
--     SELECT 
--         date_trunc('day', PRIORITIZATION_TIME)::date as event_date,
--         PROGRAM_ID,
--         PROGRAM_NAME,
--         'PRIORITIZATION' as stage_name,
--         parse_json(META_DATA)['eligibility_check']['eligibility_check_type']::string as eligibility_check_type,
--         PRIORITIZATION_STATUS as status,
--         PRIORITIZATION_REASON as reason,
--         count(*) as event_count
--     FROM edw.growth.fact_notification_platform
--     WHERE PRIORITIZATION_TIME >= dateadd('day', -30, current_date())
--     AND user_entity_type = 'CONSUMER_DOORDASH'
--     GROUP BY 1, 2, 3, 4, 5, 6, 7
-- ),

-- -- Get send stage data
-- send_data as (
--     SELECT 
--         date_trunc('day', SEND_TIME)::date as event_date,
--         PROGRAM_ID,
--         PROGRAM_NAME,
--         'SEND' as stage_name,
--         NULL as eligibility_check_type,
--         SEND_STATUS as status,
--         SEND_REASON as reason,
--         count(*) as event_count
--     FROM edw.growth.fact_notification_platform
--     WHERE SEND_TIME >= dateadd('day', -30, current_date())
--     AND user_entity_type = 'CONSUMER_DOORDASH'
--     GROUP BY 1, 2, 3, 4, 5, 6, 7
-- ),

-- -- Combine the two stages
-- funnel_data as (
--     SELECT * FROM prioritization_data
--     UNION ALL
--     SELECT * FROM send_data
-- )

-- -- Main analysis query with notification index join
-- SELECT 
--     f.event_date,
--     f.stage_name,
--     coalesce(idx.team, 'Other') as team,
--     coalesce(idx.name, f.PROGRAM_NAME) as campaign_name,
--     f.eligibility_check_type,
--     f.status,
--     f.reason,
--     sum(f.event_count) as event_count
-- FROM funnel_data f
-- LEFT JOIN proddb.public.nv_channels_notif_index idx
--     ON f.PROGRAM_ID = coalesce(idx.campaign_id, idx.canvas_id)
-- GROUP BY 1, 2, 3, 4, 5, 6, 7
-- ORDER BY 1 DESC, 2, 3, 4, 5, 6, 7;