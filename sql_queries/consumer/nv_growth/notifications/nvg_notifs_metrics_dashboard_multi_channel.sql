-- Multi-channel Notification Metrics Dashboard
-- With fix to preserve original campaign_name values

CREATE OR REPLACE TABLE proddb.public.nvg_notifs_metrics_dashboard_multi_channel AS

WITH metrics AS (
    -- Base metrics with time windows
    SELECT 
        campaign_name,
        clean_campaign_name,
        team,
        ep_name,
        notification_channel,
        sent_week,
        '1h' as time_window,
        notifs_sent,
        cx_notifs_sent,
        notifs_sent_per_cx,
        approx_targeted_cx,
        targeted_to_sent_rate,
        cx_targeted_to_sent_rate,

        -- 1h Metrics
        engagement_1h as open,
        receive_1h as receive,
        visit_1h as visit,
        order_1h as dd_order,
        nv_order_1h as nv_order,
        nv_trial_1h as nv_trial,
        nv_retrial_1h as nv_retrial,
        nv_trial_or_retrial_1h as nv_trial_or_retrial,
        bounce_1h as bounce,
        unsubscribe_1h as unsubscribe,
        uninstall_1h as uninstall,
        engagement_to_nv_order_1h as open_to_nv_order,
        
        -- 1h Rate Metrics
        receive_rate_1h as receive_rate,
        send_to_engagement_rate_1h as send_to_open_rate,
        send_to_visit_rate_1h as send_to_visit_rate,
        engagement_to_visit_rate_1h as open_to_visit_rate,
        engagement_to_order_rate_1h as open_to_order_rate,
        engagement_to_nv_order_rate_1h as open_to_nv_order_rate,
        visit_to_order_rate_1h as visit_to_order_rate,
        visit_to_nv_order_rate_1h as visit_to_nv_order_rate,
        visit_to_nv_trial_rate_1h as visit_to_nv_trial_rate,
        visit_to_nv_retrial_rate_1h as visit_to_nv_retrial_rate,
        visit_to_nv_trial_or_retrial_rate_1h as visit_to_nv_trial_or_retrial_rate,
        send_to_nv_order_rate_1h as send_to_nv_order_rate,
        send_to_nv_trial_or_retrial_rate_1h as send_to_nv_trial_or_retrial_rate,
        send_to_unsubscribe_rate_1h as send_to_unsubscribe_rate,
        pct_nv_orders_1h as pct_nv_orders,
        pct_nv_orders_from_engagement_1h as pct_nv_orders_from_open,
        -- Pull individual trial and retrial rates directly from source
        send_to_nv_trial_rate_1h as send_to_nv_trial_rate,
        send_to_nv_retrial_rate_1h as send_to_nv_retrial_rate
        
    from proddb.public.nvg_notif_metrics_multi_channel
    
    union all

    select 
        campaign_name,
        clean_campaign_name,
        team,
        ep_name,
        notification_channel,
        sent_week,
        '4h' as time_window,
        notifs_sent,
        cx_notifs_sent,
        notifs_sent_per_cx,
        approx_targeted_cx,
        targeted_to_sent_rate,
        cx_targeted_to_sent_rate,
        
        -- 4h Metrics
        engagement_4h as open,
        receive_4h as receive,
        visit_4h as visit,
        order_4h as dd_order,
        nv_order_4h as nv_order,
        nv_trial_4h as nv_trial,
        nv_retrial_4h as nv_retrial,
        nv_trial_or_retrial_4h as nv_trial_or_retrial,
        bounce_4h as bounce,
        unsubscribe_4h as unsubscribe,
        uninstall_4h as uninstall,
        engagement_to_nv_order_4h as open_to_nv_order,
        
        -- 4h Rate Metrics
        receive_rate_4h as receive_rate,
        send_to_engagement_rate_4h as send_to_open_rate,
        send_to_visit_rate_4h as send_to_visit_rate,
        engagement_to_visit_rate_4h as open_to_visit_rate,
        engagement_to_order_rate_4h as open_to_order_rate,
        engagement_to_nv_order_rate_4h as open_to_nv_order_rate,
        visit_to_order_rate_4h as visit_to_order_rate,
        visit_to_nv_order_rate_4h as visit_to_nv_order_rate,
        visit_to_nv_trial_rate_4h as visit_to_nv_trial_rate,
        visit_to_nv_retrial_rate_4h as visit_to_nv_retrial_rate,
        visit_to_nv_trial_or_retrial_rate_4h as visit_to_nv_trial_or_retrial_rate,
        send_to_nv_order_rate_4h as send_to_nv_order_rate,
        send_to_nv_trial_or_retrial_rate_4h as send_to_nv_trial_or_retrial_rate,
        send_to_unsubscribe_rate_4h as send_to_unsubscribe_rate,
        pct_nv_orders_4h as pct_nv_orders,
        pct_nv_orders_from_engagement_4h as pct_nv_orders_from_open,
        -- Pull individual trial and retrial rates directly from source
        send_to_nv_trial_rate_4h as send_to_nv_trial_rate,
        send_to_nv_retrial_rate_4h as send_to_nv_retrial_rate
        
    from proddb.public.nvg_notif_metrics_multi_channel

    union all

    select 
        campaign_name,
        clean_campaign_name,
        team,
        ep_name,
        notification_channel,
        sent_week,
        '24h' as time_window,
        notifs_sent,
        cx_notifs_sent,
        notifs_sent_per_cx,
        approx_targeted_cx,
        targeted_to_sent_rate,
        cx_targeted_to_sent_rate,
        
        -- 24h Metrics
        engagement_24h as open,
        receive_24h as receive,
        visit_24h as visit,
        order_24h as dd_order,
        nv_order_24h as nv_order,
        nv_trial_24h as nv_trial,
        nv_retrial_24h as nv_retrial,
        nv_trial_or_retrial_24h as nv_trial_or_retrial,
        bounce_24h as bounce,
        unsubscribe_24h as unsubscribe,
        uninstall_24h as uninstall,
        engagement_to_nv_order_24h as open_to_nv_order,
        
        -- 24h Rate Metrics
        receive_rate_24h as receive_rate,
        send_to_engagement_rate_24h as send_to_open_rate,
        send_to_visit_rate_24h as send_to_visit_rate,
        engagement_to_visit_rate_24h as open_to_visit_rate,
        engagement_to_order_rate_24h as open_to_order_rate,
        engagement_to_nv_order_rate_24h as open_to_nv_order_rate,
        visit_to_order_rate_24h as visit_to_order_rate,
        visit_to_nv_order_rate_24h as visit_to_nv_order_rate,
        visit_to_nv_trial_rate_24h as visit_to_nv_trial_rate,
        visit_to_nv_retrial_rate_24h as visit_to_nv_retrial_rate,
        visit_to_nv_trial_or_retrial_rate_24h as visit_to_nv_trial_or_retrial_rate,
        send_to_nv_order_rate_24h as send_to_nv_order_rate,
        send_to_nv_trial_or_retrial_rate_24h as send_to_nv_trial_or_retrial_rate,
        send_to_unsubscribe_rate_24h as send_to_unsubscribe_rate,
        pct_nv_orders_24h as pct_nv_orders,
        pct_nv_orders_from_engagement_24h as pct_nv_orders_from_open,
        -- Pull individual trial and retrial rates directly from source
        send_to_nv_trial_rate_24h as send_to_nv_trial_rate,
        send_to_nv_retrial_rate_24h as send_to_nv_retrial_rate
        
    from proddb.public.nvg_notif_metrics_multi_channel
)

-- Final output with is_most_recent_week flag
select 
    m.campaign_name,
    m.clean_campaign_name,
    m.team,
    m.ep_name,
    m.notification_channel,
    m.sent_week,
    m.time_window,
    m.notifs_sent,
    m.cx_notifs_sent,
    m.notifs_sent_per_cx,
    
    m.open,
    m.receive,
    m.visit,
    m.dd_order,
    m.nv_order,
    m.nv_trial,
    m.nv_retrial,
    m.nv_trial_or_retrial,
    m.bounce,
    m.unsubscribe,
    m.uninstall,
    m.open_to_nv_order,
    
    m.receive_rate,
    m.send_to_open_rate,
    m.send_to_visit_rate,
    m.open_to_visit_rate,
    m.open_to_order_rate,
    m.open_to_nv_order_rate,
    m.visit_to_order_rate,
    m.visit_to_nv_order_rate,
    m.visit_to_nv_trial_rate,
    m.visit_to_nv_retrial_rate,
    m.visit_to_nv_trial_or_retrial_rate,
    m.send_to_nv_order_rate,
    m.send_to_nv_trial_or_retrial_rate,
    m.send_to_unsubscribe_rate,
    m.pct_nv_orders,
    m.pct_nv_orders_from_open,
    
    -- New individual trial and retrial rate metrics
    m.send_to_nv_trial_rate,
    m.send_to_nv_retrial_rate,
    
    m.approx_targeted_cx, 
    m.targeted_to_sent_rate,
    m.cx_targeted_to_sent_rate,
    iff(m.sent_week = date_trunc('week', current_date-7), 1, 0) is_most_recent_week
from metrics m
order by m.sent_week DESC, m.campaign_name, m.team, m.notification_channel, m.time_window; 