-- Data cube for notification metrics with all dimension combinations and count metrics
-- This pre-aggregates data for fast dashboard queries

create or replace table proddb.tylershields.nvg_notif_cube as
select 
    -- Time dimensions (or NULL if aggregated)
    sent_at_date,
    date_trunc('week', sent_at_date)::date as sent_week,
    date_trunc('month', sent_at_date)::date as sent_month,
    
    -- Campaign dimensions
    notification_channel,
    notification_source,
    notification_message_type,
    notification_message_type_overall,
    team,
    ep_name,
    clean_campaign_name,
    
    -- Consumer dimensions
    days_since_order_bucket,
    daypart,
    nv_cx_growth_state,
    dd_nv_cx_growth_state,
    dd_cx_growth_state,
    
    -- Count metrics (no rates)
    count(distinct deduped_message_id_consumer) as notifications_sent,
    count(distinct consumer_id) as consumers_reached,
    
    -- 1h metrics
    sum(is_valid_send) as valid_sends,
    sum(receive_within_1h) as receive_count_1h,
    sum(engagement_within_1h) as engagement_count_1h,
    sum(visit_within_1h) as visit_count_1h,
    sum(order_within_1h) as order_count_1h,
    sum(nv_order_within_1h) as nv_order_count_1h,
    sum(engagement_to_nv_order_within_1h) as engagement_to_nv_order_count_1h,
    sum(nv_trial_within_1h) as nv_trial_count_1h,
    sum(nv_retrial_within_1h) as nv_retrial_count_1h,
    sum(nv_trial_or_retrial_within_1h) as nv_trial_or_retrial_count_1h,
    sum(total_nv_gov_within_1h) as total_nv_gov_1h,
    sum(bounce_within_1h) as bounce_count_1h,
    sum(unsubscribe_within_1h) as unsubscribe_count_1h,
    sum(uninstall_within_1h) as uninstall_count_1h,
    
    -- 4h metrics
    sum(receive_within_4h) as receive_count_4h,
    sum(engagement_within_4h) as engagement_count_4h,
    sum(visit_within_4h) as visit_count_4h,
    sum(order_within_4h) as order_count_4h,
    sum(nv_order_within_4h) as nv_order_count_4h,
    sum(engagement_to_nv_order_within_4h) as engagement_to_nv_order_count_4h,
    sum(nv_trial_within_4h) as nv_trial_count_4h,
    sum(nv_retrial_within_4h) as nv_retrial_count_4h,
    sum(nv_trial_or_retrial_within_4h) as nv_trial_or_retrial_count_4h,
    sum(total_nv_gov_within_4h) as total_nv_gov_4h,
    sum(bounce_within_4h) as bounce_count_4h,
    sum(unsubscribe_within_4h) as unsubscribe_count_4h,
    sum(uninstall_within_4h) as uninstall_count_4h,
    
    -- 24h metrics
    sum(receive_within_24h) as receive_count_24h,
    sum(engagement_within_24h) as engagement_count_24h,
    sum(visit_within_24h) as visit_count_24h,
    sum(order_within_24h) as order_count_24h,
    sum(nv_order_within_24h) as nv_order_count_24h,
    sum(engagement_to_nv_order_within_24h) as engagement_to_nv_order_count_24h,
    sum(nv_trial_within_24h) as nv_trial_count_24h,
    sum(nv_retrial_within_24h) as nv_retrial_count_24h,
    sum(nv_trial_or_retrial_within_24h) as nv_trial_or_retrial_count_24h,
    sum(total_nv_gov_within_24h) as total_nv_gov_24h,
    sum(bounce_within_24h) as bounce_count_24h,
    sum(unsubscribe_within_24h) as unsubscribe_count_24h,
    sum(uninstall_within_24h) as uninstall_count_24h
from proddb.tylershields.nvg_notif_base_data
group by cube (
    -- Time dimensions
    sent_at_date,
    sent_week,
    sent_month,
    
    -- Campaign dimensions
    notification_channel,
    notification_source,
    notification_message_type,
    notification_message_type_overall,
    team,
    ep_name,
    clean_campaign_name,
    
    -- Consumer dimensions
    days_since_order_bucket,
    daypart,
    nv_cx_growth_state,
    dd_nv_cx_growth_state,
    dd_cx_growth_state
);

-- Add indexes for improved query performance
create or replace table proddb.tylershields.nvg_notif_cube_performance as
select
    nvl(sent_at_date, '1900-01-01')::date as sent_at_date_key,
    nvl(sent_week, '1900-01-01')::date as sent_week_key,
    nvl(sent_month, '1900-01-01')::date as sent_month_key,
    nvl(notification_channel, 'ALL') as notification_channel_key,
    nvl(team, 'ALL') as team_key,
    nvl(ep_name, 'ALL') as ep_name_key,
    nvl(clean_campaign_name, 'ALL') as clean_campaign_name_key,
    nvl(days_since_order_bucket, 'ALL') as days_since_order_bucket_key,
    nvl(daypart, 'ALL') as daypart_key,
    nvl(nv_cx_growth_state, 'ALL') as nv_cx_growth_state_key,
    nvl(dd_nv_cx_growth_state, 'ALL') as dd_nv_cx_growth_state_key,
    nvl(dd_cx_growth_state, 'ALL') as dd_cx_growth_state_key,
    *
from proddb.tylershields.nvg_notif_cube; 