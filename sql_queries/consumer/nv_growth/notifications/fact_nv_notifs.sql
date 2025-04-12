-- Notification Comparison v2: Cleaner dataset at deduped_message_id_consumer grain
-- Creates a clean notification-level dataset for flexible analysis 

-- Set the analysis window (default to last 30 days if not specified)
set window_start = '2025-01-01'; --dateadd('d', -30, current_date);
set window_end = current_date;

-- Create temporary NV orders table for order attribution
create or replace temporary table ts_nv_orders as 
select 
    d.delivery_id,
    d.created_at, 
    d.creator_id as consumer_id,
    ddr.prev_active_date_nv, 
    ddr.order_number_nv,
    iff(order_number_nv=1, 1, 0) as is_trial,
    iff(datediff('d', ddr.prev_active_date_nv, ddr.active_date) >= 180, 1, 0) as is_retrial,
    coalesce(d.gov, 0)*0.01 as gov,
    d.business_id,
    d.business_name,
    nv.vertical_name
from stefaniemontgomery.dimension_deliveries_ranked ddr 
join edw.finance.dimension_deliveries d on ddr.delivery_id = d.delivery_id
join edw.cng.dimension_new_vertical_store_tags nv on d.store_id = nv.store_id
where ddr.is_nv = 'NV'
and d.is_filtered_core
and d.active_date_utc between $window_start and $window_end;

-- Get daily growth states for all consumers across the entire window
create or replace temporary table ts_daily_growth_states as
select 
    d.ds as status_date,
    d.consumer_id,
    coalesce(n.nv_cx_growth_state, 'unknown') as nv_cx_growth_state,
    coalesce(n.dd_nv_cx_growth_state, 'unknown') as dd_nv_cx_growth_state,
    d.CX_LIFESTAGE_STATUS as dd_cx_growth_state
from edw.growth.dimension_consumer_growth_accounting_state_detailed_all d 
left join metrics_repo.public.nv_cx_growth_state n
    on d.consumer_id = n.consumer_id 
    and d.ds = n.event_ts::DATE
where d.experience = 'doordash'
    and d.ds between $window_start and $window_end
    and vertical = 'core_dd_utc';

-- Create clean notification-level dataset at deduped_message_id_consumer grain
create or replace table proddb.tylershields.nvg_notif_base_data as
select 
    -- Dimensions
    concat(e.deduped_message_id, '_', e.consumer_id) as deduped_message_id_consumer,
    e.consumer_id,
    e.notification_channel,
    e.notification_source,
    e.notification_message_type,
    e.notification_message_type_overall,
    e.postal_service_source,
    coalesce(e.campaign_id, e.canvas_id) as campaign_canvas_id,
    coalesce(e.campaign_name, e.canvas_name) as campaign_name,
    coalesce(idx.team, 'All Others') as team,
    coalesce(idx.ep_name, 'Other') as ep_name,
    coalesce(idx.clean_campaign_name, 'Other') as clean_campaign_name,
    min(e.sent_at) as sent_at,
    min(e.sent_at_date) as sent_at_date,
    date_trunc('week', min(e.sent_at_date))::date as sent_week,
    date_trunc('month', min(e.sent_at_date))::date as sent_month,
    case
        when nv.last_nv_order_ds is null then -1 
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) > 365 then 366 
        else datediff('day', nv.last_nv_order_ds, e.sent_at_date) 
    end as days_since_order,
    case
        when nv.last_nv_order_ds is null then 'Never ordered'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) > 365 then 'Over 1 year'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) between 0 and 7 then '0-7 days'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) between 8 and 14 then '8-14 days'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) between 15 and 30 then '15-30 days'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) between 31 and 60 then '31-60 days'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) between 61 and 90 then '61-90 days'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) between 91 and 180 then '91-180 days'
        when datediff('day', nv.last_nv_order_ds, e.sent_at_date) between 181 and 365 then '181-365 days'
        else 'Unknown'
    end as days_since_order_bucket,
    CASE 
        WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(e.sent_at)), 'HH24:MI:SS') BETWEEN '06:00:00' AND '09:59:59' THEN 'a. Morning 6am to 10am'
        WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(e.sent_at)), 'HH24:MI:SS') BETWEEN '10:00:00' AND '13:59:59' THEN 'b. Late Morning 10am to 2pm'
        WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(e.sent_at)), 'HH24:MI:SS') BETWEEN '14:00:00' AND '17:59:59' THEN 'c. Afternoon 2pm to 6pm'
        WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(e.sent_at)), 'HH24:MI:SS') BETWEEN '18:00:00' AND '21:59:59' THEN 'd. Evening 6pm to 10pm'
        ELSE 'e. Night 10pm to 6am'
    END AS daypart,
    g.nv_cx_growth_state,
    g.dd_nv_cx_growth_state, 
    g.dd_cx_growth_state,
    
    -- Metrics - 1h window
    max(e.is_valid_send)::int as is_valid_send,
    max(e.receive_within_1h)::int as receive_within_1h,
    max(e.receive_within_4h)::int as receive_within_4h,
    max(e.receive_within_24h)::int as receive_within_24h,
    
    max(CASE WHEN e.notification_channel = 'PUSH' THEN e.open_within_1h WHEN e.notification_channel = 'EMAIL' THEN e.link_click_within_1h ELSE 0 END) as engagement_within_1h,
    max(CASE WHEN e.notification_channel = 'PUSH' THEN e.open_within_4h WHEN e.notification_channel = 'EMAIL' THEN e.link_click_within_4h ELSE 0 END) as engagement_within_4h,
    max(CASE WHEN e.notification_channel = 'PUSH' THEN e.open_within_24h WHEN e.notification_channel = 'EMAIL' THEN e.link_click_within_24h ELSE 0 END) as engagement_within_24h,
    
    max(e.visit_within_1h)::int as visit_within_1h,
    max(e.visit_within_4h)::int as visit_within_4h,
    max(e.visit_within_24h)::int as visit_within_24h,
    
    max(e.order_within_1h)::int as order_within_1h,
    max(e.order_within_4h)::int as order_within_4h,
    max(e.order_within_24h)::int as order_within_24h,
    
    max(case when o.created_at between e.sent_at and dateadd('h', 1, e.sent_at) then 1 else 0 end) as nv_order_within_1h,
    max(case when o.created_at between e.sent_at and dateadd('h', 4, e.sent_at) then 1 else 0 end) as nv_order_within_4h,
    max(case when o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then 1 else 0 end) as nv_order_within_24h,
    
    max(case when ((e.notification_channel = 'PUSH' and e.open_within_1h = 1) or (e.notification_channel = 'EMAIL' and e.link_click_within_1h = 1))
             and o.created_at between e.sent_at and dateadd('h', 1, e.sent_at) then 1 else 0 end) as engagement_to_nv_order_within_1h,
    max(case when ((e.notification_channel = 'PUSH' and e.open_within_4h = 1) or (e.notification_channel = 'EMAIL' and e.link_click_within_4h = 1))
             and o.created_at between e.sent_at and dateadd('h', 4, e.sent_at) then 1 else 0 end) as engagement_to_nv_order_within_4h,
    max(case when ((e.notification_channel = 'PUSH' and e.open_within_24h = 1) or (e.notification_channel = 'EMAIL' and e.link_click_within_24h = 1))
             and o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then 1 else 0 end) as engagement_to_nv_order_within_24h,
    
    max(case when o.is_trial = 1 and o.created_at between e.sent_at and dateadd('h', 1, e.sent_at) then 1 else 0 end) as nv_trial_within_1h,
    max(case when o.is_trial = 1 and o.created_at between e.sent_at and dateadd('h', 4, e.sent_at) then 1 else 0 end) as nv_trial_within_4h,
    max(case when o.is_trial = 1 and o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then 1 else 0 end) as nv_trial_within_24h,
    
    max(case when o.is_retrial = 1 and o.created_at between e.sent_at and dateadd('h', 1, e.sent_at) then 1 else 0 end) as nv_retrial_within_1h,
    max(case when o.is_retrial = 1 and o.created_at between e.sent_at and dateadd('h', 4, e.sent_at) then 1 else 0 end) as nv_retrial_within_4h,
    max(case when o.is_retrial = 1 and o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then 1 else 0 end) as nv_retrial_within_24h,
    
    max(case when (o.is_retrial = 1 or o.is_trial = 1) and o.created_at between e.sent_at and dateadd('h', 1, e.sent_at) then 1 else 0 end) as nv_trial_or_retrial_within_1h,
    max(case when (o.is_retrial = 1 or o.is_trial = 1) and o.created_at between e.sent_at and dateadd('h', 4, e.sent_at) then 1 else 0 end) as nv_trial_or_retrial_within_4h,
    max(case when (o.is_retrial = 1 or o.is_trial = 1) and o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then 1 else 0 end) as nv_trial_or_retrial_within_24h,
    
    sum(case when o.created_at between e.sent_at and dateadd('h', 1, e.sent_at) then o.gov else 0 end) as total_nv_gov_within_1h,
    sum(case when o.created_at between e.sent_at and dateadd('h', 4, e.sent_at) then o.gov else 0 end) as total_nv_gov_within_4h,
    sum(case when o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then o.gov else 0 end) as total_nv_gov_within_24h,
    
    max(e.bounce_within_1h)::int as bounce_within_1h,
    max(e.bounce_within_4h)::int as bounce_within_4h,
    max(e.bounce_within_24h)::int as bounce_within_24h,
    
    max(e.unsubscribe_within_1h)::int as unsubscribe_within_1h,
    max(e.unsubscribe_within_4h)::int as unsubscribe_within_4h,
    max(e.unsubscribe_within_24h)::int as unsubscribe_within_24h,
    
    max(e.uninstall_within_1h)::int as uninstall_within_1h,
    max(e.uninstall_within_4h)::int as uninstall_within_4h,
    max(e.uninstall_within_24h)::int as uninstall_within_24h
from edw.consumer.fact_consumer_notification_engagement e
left join proddb.public.nv_channels_notif_index idx
    on coalesce(e.campaign_id, e.canvas_id) = coalesce(idx.campaign_id, idx.canvas_id)
left join ts_daily_growth_states g
    on e.consumer_id = g.consumer_id and e.sent_at_date = g.status_date
left join proddb.public.dimension_nv_users nv
    on e.consumer_id = nv.consumer_id and nv.ds = e.sent_at_date
    and (nv.last_nv_order_ds is null or nv.last_nv_order_ds < e.sent_at_date)
left join ts_nv_orders o
    on e.consumer_id = o.consumer_id and o.created_at between e.sent_at and dateadd('h', 24, e.sent_at)
where e.sent_at_date between $window_start and $window_end
and e.notification_channel in ('PUSH', 'EMAIL')
and (e.notification_message_type is null or e.notification_message_type != 'Transactional')
and e.is_valid_send = 1
group by all;
