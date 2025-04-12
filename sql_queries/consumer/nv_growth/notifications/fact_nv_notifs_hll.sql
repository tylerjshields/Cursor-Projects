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

-- Create HLL objects from the base data for flexible aggregation
create or replace table proddb.tylershields.nvg_notif_hll_objects_v2 as
select 
    -- Dimensions
    sent_at_date, sent_week, sent_month, team, ep_name, clean_campaign_name,
    notification_channel, notification_source, notification_message_type_overall,
    daypart, days_since_order, days_since_order_bucket,
    nv_cx_growth_state, dd_nv_cx_growth_state, dd_cx_growth_state,
    
    -- Metrics - we track all three time windows
    HLL(consumer_id) as consumer_hll,
    count(deduped_message_id_consumer) as notification_count,
    
    -- 1h metrics
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
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;

-- Example aggregation: Weekly notifs per consumer by team and channel (1h metrics)
select 
    sent_week, team, notification_channel,
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(engagement_count_1h) as notifications_engaged_1h,
    sum(nv_order_count_1h) as nv_orders_1h,
    sum(nv_trial_count_1h) as nv_trials_1h,
    sum(nv_retrial_count_1h) as nv_retrials_1h,
    sum(notification_count) / nullif(HLL_ESTIMATE(consumer_hll), 0) as notifs_per_consumer,
    sum(engagement_count_1h) / nullif(sum(notification_count), 0) as engagement_rate_1h,
    sum(visit_count_1h) / nullif(sum(engagement_count_1h), 0) as engagement_to_visit_rate_1h,
    sum(nv_order_count_1h) / nullif(sum(notification_count), 0) as send_to_nv_order_rate_1h,
    sum(nv_trial_count_1h) / nullif(sum(notification_count), 0) as send_to_nv_trial_rate_1h,
    sum(nv_retrial_count_1h) / nullif(sum(notification_count), 0) as send_to_nv_retrial_rate_1h,
    sum(nv_trial_or_retrial_count_1h) / nullif(sum(notification_count), 0) as send_to_nv_trial_or_retrial_rate_1h,
    sum(unsubscribe_count_1h) / nullif(sum(notification_count), 0) as unsubscribe_rate_1h
from proddb.tylershields.nvg_notif_hll_objects_v2
group by 1, 2, 3
order by 1 desc, 2, 3;

-- Example: Analyze metrics by days since last order (using 4h metrics)
select 
    days_since_order_bucket, team, notification_channel,
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(engagement_count_4h) as notifications_engaged_4h,
    sum(nv_order_count_4h) as nv_orders_4h,
    sum(nv_trial_count_4h) as nv_trials_4h,
    sum(nv_retrial_count_4h) as nv_retrials_4h,
    sum(engagement_count_4h) / nullif(sum(notification_count), 0) as engagement_rate_4h,
    sum(nv_order_count_4h) / nullif(sum(notification_count), 0) as send_to_nv_order_rate_4h,
    sum(nv_trial_count_4h) / nullif(sum(notification_count), 0) as send_to_nv_trial_rate_4h,
    sum(nv_retrial_count_4h) / nullif(sum(notification_count), 0) as send_to_nv_retrial_rate_4h,
    sum(nv_trial_or_retrial_count_4h) / nullif(sum(notification_count), 0) as send_to_nv_trial_or_retrial_rate_4h
from proddb.tylershields.nvg_notif_hll_objects_v2
group by 1, 2, 3
order by case when days_since_order_bucket = 'Never ordered' then 0
              when days_since_order_bucket = '0-7 days' then 1
              when days_since_order_bucket = '8-14 days' then 2
              when days_since_order_bucket = '15-30 days' then 3
              when days_since_order_bucket = '31-60 days' then 4
              when days_since_order_bucket = '61-90 days' then 5
              when days_since_order_bucket = '91-180 days' then 6
              when days_since_order_bucket = '181-365 days' then 7
              when days_since_order_bucket = 'Over 1 year' then 8
              else 9 end, team, notification_channel;

-- Create dashboard output with same dimensions as v1 for compatibility (using 24h metrics)
create or replace table proddb.public.nvg_notif_compare_index_vs_all_dashboard_v2 as
select 
    sent_at_date, team, notification_channel, days_since_order_bucket,
    dd_cx_growth_state, nv_cx_growth_state, dd_nv_cx_growth_state, daypart,
    HLL_ESTIMATE(consumer_hll) as consumers_notified,
    sum(notification_count) as notifs_sent,
    sum(engagement_count_24h) as notifs_engaged,
    sum(engagement_count_24h) / nullif(sum(notification_count), 0) as engagement_rate,
    sum(nv_order_count_24h) as nv_orders,
    sum(nv_trial_count_24h) as nv_trials,
    sum(nv_retrial_count_24h) as nv_retrials,
    sum(nv_trial_or_retrial_count_24h) as nv_trials_or_retrials,
    sum(nv_order_count_24h) / nullif(sum(notification_count), 0) as send_to_nv_order_rate,
    sum(nv_trial_count_24h) / nullif(sum(notification_count), 0) as send_to_nv_trial_rate,
    sum(nv_retrial_count_24h) / nullif(sum(notification_count), 0) as send_to_nv_retrial_rate,
    sum(nv_trial_or_retrial_count_24h) / nullif(sum(notification_count), 0) as send_to_nv_trial_or_retrial_rate,
    sum(notification_count) / nullif(HLL_ESTIMATE(consumer_hll), 0) as notifs_per_consumer,
    case when max(case when team != 'All Others' then 1 else 0 end) over (partition by sent_at_date) = 1 
         then sum(notification_count) / nullif(sum(sum(case when team != 'All Others' then notification_count else 0 end)) 
              over (partition by sent_at_date, notification_channel), 0) 
         else null end as pct_of_nv_notifs,
    case when max(case when team = 'All Others' then 1 else 0 end) over (partition by sent_at_date) = 1 
         then sum(notification_count) / nullif(sum(sum(case when team = 'All Others' then notification_count else 0 end)) 
              over (partition by sent_at_date, notification_channel), 0) 
         else null end as pct_of_others_notifs
from proddb.tylershields.nvg_notif_hll_objects_v2
group by all
order by 1 desc, 2, 3, 4, 5, 6, 7; 