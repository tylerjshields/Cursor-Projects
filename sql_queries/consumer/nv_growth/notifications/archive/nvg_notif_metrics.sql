create or replace table proddb.public.nvg_notif_nv_orders as 
-- with nv_orders as (
select 
d.delivery_id,
d.created_at, 
d.creator_id, 
ddr.prev_active_date_nv, 
ddr.order_number_nv,
iff(order_number_nv=1, 1, 0) as is_trial,
iff(datediff('d', ddr.prev_active_date_nv, ddr.active_date) >= 180, 1, 0) as is_retrial,
coalesce(d.gov, 0)*0.01 as gov,
d.business_id,
d.business_name,
nv.vertical_name
-- cxg.nv_cx_growth_state
from stefaniemontgomery.dimension_deliveries_ranked ddr 
join edw.finance.dimension_deliveries d on ddr.delivery_id = d.delivery_id
join edw.cng.dimension_new_vertical_store_tags nv on d.store_id = nv.store_id
-- left join metrics_repo.public.nv_cx_growth_state cxg on d.creator_id = cxg.consumer_id and d.active_date_utc = cxg.event_ts
where ddr.is_nv = 'NV'
and d.is_filtered_core
and d.active_date_utc between '2025-01-01' and current_date-1
;

create or replace table proddb.public.nvg_notif_core_metrics as 
-- base_metrics as (
select 
e.deduped_message_id,
e.consumer_id,
e.campaign_name,
ep_name,
clean_campaign_name,
sent_at_date,
CASE 
    WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(sent_at)), 'HH24:MI:SS') BETWEEN '06:00:00' AND '09:59:59' 
        THEN 'a. Morning 6am to 10am'
    WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(sent_at)), 'HH24:MI:SS') BETWEEN '10:00:00' AND '13:59:59' 
        THEN 'b. Late Morning 10am to 2pm'
    WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(sent_at)), 'HH24:MI:SS') BETWEEN '14:00:00' AND '17:59:59' 
        THEN 'c. Afternoon 2pm to 6pm'
    WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', MIN(sent_at)), 'HH24:MI:SS') BETWEEN '18:00:00' AND '21:59:59' 
        THEN 'd. Evening 6pm to 10pm'
    ELSE 'e. Night 10pm to 6am'
END AS daypart,
min(sent_at) as sent_at,
min(opened_at) as opened_at,
min(visited_at) as visited_at,

MAX(receive_within_1h) AS receive_within_1h,
MAX(open_within_1h) AS open_within_1h,
MAX(visit_within_1h) AS visit_within_1h,
MAX(order_within_1h) AS order_within_1h,
MAX(bounce_within_1h) AS bounce_within_1h,
MAX(unsubscribe_within_1h) AS unsubscribe_within_1h,
MAX(uninstall_within_1h) AS uninstall_within_1h,

MAX(receive_within_4h) AS receive_within_4h,
MAX(open_within_4h) AS open_within_4h,
MAX(visit_within_4h) AS visit_within_4h,
MAX(order_within_4h) AS order_within_4h,
MAX(bounce_within_4h) AS bounce_within_4h,
MAX(unsubscribe_within_4h) AS unsubscribe_within_4h,
MAX(uninstall_within_4h) AS uninstall_within_4h,

MAX(receive_within_24h) AS receive_within_24h,
MAX(open_within_24h) AS open_within_24h,
MAX(visit_within_24h) AS visit_within_24h,
MAX(order_within_24h) AS order_within_24h,
MAX(bounce_within_24h) AS bounce_within_24h,
MAX(unsubscribe_within_24h) AS unsubscribe_within_24h,
MAX(uninstall_within_24h) AS uninstall_within_24h
from edw.consumer.fact_consumer_notification_engagement e 
join proddb.public.nv_channels_notif_index n on coalesce(e.campaign_id, e.canvas_id) = coalesce(n.campaign_id, n.canvas_id) 
where sent_at_date between '2025-01-01'and current_date-2
and notification_channel = 'PUSH'
group by all
;

create or replace table proddb.public.nvg_notif_nv_metrics as 
-- nv_metrics as (
select 
    e.deduped_message_id,
    max(case when o.created_at between e.sent_at and dateadd('h', 1, e.sent_at) then 1 else 0 end) as nv_order_within_1h,
    max(case when o.created_at between e.sent_at and dateadd('h', 4, e.sent_at) then 1 else 0 end) as nv_order_within_4h,
    max(case when o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then 1 else 0 end) as nv_order_within_24h,

    max(case when o.created_at between e.opened_at and dateadd('h', 1, e.opened_at) then 1 else 0 end) as open_to_nv_order_within_1h,
    max(case when o.created_at between e.opened_at and dateadd('h', 4, e.opened_at) then 1 else 0 end) as open_to_nv_order_within_4h,
    max(case when o.created_at between e.opened_at and dateadd('h', 24, e.opened_at) then 1 else 0 end) as open_to_nv_order_within_24h,

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
    sum(case when o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) then o.gov else 0 end) as total_nv_gov_within_24h

from edw.consumer.fact_consumer_notification_engagement e 
join proddb.public.nv_channels_notif_index n on coalesce(e.campaign_id, e.canvas_id) = coalesce(n.campaign_id, n.canvas_id) 
left join proddb.public.nvg_notif_nv_orders o on e.consumer_id = o.creator_id and o.created_at between e.sent_at and dateadd('h', 24, e.sent_at) 
where e.sent_at_date  between '2025-01-01' and current_date-2 -- need to have 24 hours look forward window for conversion events
and notification_channel = 'PUSH'
group by all
;

create or replace table proddb.public.nvg_notif_metrics_base as 

SELECT
b.deduped_message_id,
b.consumer_id,
b.campaign_name,
b.clean_campaign_name,
b.ep_name,
b.sent_at_date,
b.sent_at,
b.opened_at,
b.visited_at,
b.receive_within_1h,
b.open_within_1h,
b.visit_within_1h,
b.order_within_1h,
b.bounce_within_1h,
b.unsubscribe_within_1h,
b.uninstall_within_1h,
b.receive_within_4h,
b.open_within_4h,
b.visit_within_4h,
b.order_within_4h,
b.bounce_within_4h,
b.unsubscribe_within_4h,
b.uninstall_within_4h,
b.receive_within_24h,
b.open_within_24h,
b.visit_within_24h,
b.order_within_24h,
b.bounce_within_24h,
b.unsubscribe_within_24h,
b.uninstall_within_24h,
coalesce(n.nv_order_within_1h, 0) as nv_order_within_1h,
coalesce(n.nv_order_within_4h, 0) as nv_order_within_4h,
coalesce(n.nv_order_within_24h, 0) as nv_order_within_24h,
coalesce(open_to_nv_order_within_1h, 0) open_to_nv_order_within_1h,
coalesce(open_to_nv_order_within_4h, 0) open_to_nv_order_within_4h,
coalesce(open_to_nv_order_within_24h, 0) open_to_nv_order_within_24h,
coalesce(n.nv_trial_within_1h, 0) as nv_trial_within_1h,
coalesce(n.nv_trial_within_4h, 0) as nv_trial_within_4h,
coalesce(n.nv_trial_within_24h, 0) as nv_trial_within_24h,
coalesce(n.nv_retrial_within_1h, 0) as nv_retrial_within_1h,
coalesce(n.nv_retrial_within_4h, 0) as nv_retrial_within_4h,
coalesce(n.nv_retrial_within_24h, 0) as nv_retrial_within_24h,
coalesce(n.nv_trial_or_retrial_within_1h, 0) as nv_trial_or_retrial_within_1h,
coalesce(n.nv_trial_or_retrial_within_4h, 0) as nv_trial_or_retrial_within_4h,
coalesce(n.nv_trial_or_retrial_within_24h, 0) as nv_trial_or_retrial_within_24h,
coalesce(n.total_nv_gov_within_1h, 0) as total_nv_gov_within_1h,
coalesce(n.total_nv_gov_within_4h, 0) as total_nv_gov_within_4h,
coalesce(n.total_nv_gov_within_24h, 0) as total_nv_gov_within_24h
FROM proddb.public.nvg_notif_core_metrics b
JOIN proddb.public.nvg_notif_nv_metrics n ON b.deduped_message_id = n.deduped_message_id
-- )
-- select * from metrics
;
create or replace table proddb.public.nvg_notif_metrics as 

with metrics as (
    select 
    campaign_name,
    clean_campaign_name,
    ep_name,
    date_trunc('week', sent_at_date) sent_week,
    count(distinct deduped_message_id) notifs_sent,
    count(distinct consumer_id) cx_notifs_sent,
    notifs_sent / nullif(cx_notifs_sent, 0) notifs_sent_per_cx,
    
    -- 1h Metrics
    count(distinct iff(open_within_1h = 1, deduped_message_id, null)) as open_1h,
    count(distinct iff(receive_within_1h = 1, deduped_message_id, null)) as receive_1h,
    count(distinct iff(visit_within_1h = 1, deduped_message_id, null)) as visit_1h,
    count(distinct iff(order_within_1h = 1, deduped_message_id, null)) as order_1h,
    count(distinct iff(nv_order_within_1h = 1, deduped_message_id, null)) as nv_order_1h,
    count(distinct iff(nv_trial_within_1h = 1, deduped_message_id, null)) as nv_trial_1h,
    count(distinct iff(nv_retrial_within_1h = 1, deduped_message_id, null)) as nv_retrial_1h,
    count(distinct iff(nv_trial_or_retrial_within_1h = 1, deduped_message_id, null)) as nv_trial_or_retrial_1h,
    count(distinct iff(bounce_within_1h = 1, deduped_message_id, null)) as bounce_1h,
    count(distinct iff(unsubscribe_within_1h = 1, deduped_message_id, null)) as unsubscribe_1h,
    count(distinct iff(uninstall_within_1h = 1, deduped_message_id, null)) as uninstall_1h,
    count(distinct iff(open_to_nv_order_within_1h = 1, deduped_message_id, null)) as open_to_nv_order_1h,
    
    -- 1h Customer Count Metrics
    count(distinct iff(open_within_1h = 1, consumer_id, null)) as cx_open_1h,
    count(distinct iff(receive_within_1h = 1, consumer_id, null)) as cx_receive_1h,
    count(distinct iff(visit_within_1h = 1, consumer_id, null)) as cx_visit_1h,
    count(distinct iff(order_within_1h = 1, consumer_id, null)) as cx_order_1h,
    count(distinct iff(nv_order_within_1h = 1, consumer_id, null)) as cx_nv_order_1h,
    count(distinct iff(nv_trial_within_1h = 1, consumer_id, null)) as cx_nv_trial_1h,
    count(distinct iff(nv_retrial_within_1h = 1, consumer_id, null)) as cx_nv_retrial_1h,
    count(distinct iff(nv_trial_or_retrial_within_1h = 1, consumer_id, null)) as cx_nv_trial_or_retrial_1h,
    count(distinct iff(bounce_within_1h = 1, consumer_id, null)) as cx_bounce_1h,
    count(distinct iff(unsubscribe_within_1h = 1, consumer_id, null)) as cx_unsubscribe_1h,
    count(distinct iff(uninstall_within_1h = 1, consumer_id, null)) as cx_uninstall_1h,

    
    
    -- 1h Rate Metrics
    receive_1h / nullif(notifs_sent, 0) as receive_rate_1h,
    open_1h / nullif(notifs_sent, 0) as send_to_open_rate_1h,
    visit_1h / nullif(notifs_sent, 0) as send_to_visit_rate_1h,
    visit_1h / nullif(open_1h, 0) as open_to_visit_rate_1h,
    order_1h / nullif(open_1h, 0) as open_to_order_rate_1h,
    order_1h / nullif(visit_1h, 0) as visit_to_order_rate_1h,
    nv_order_1h / nullif(notifs_sent, 0) as send_to_nv_order_rate_1h,
    nv_order_1h / nullif(visit_1h, 0) as visit_to_nv_order_rate_1h,
    nv_trial_1h / nullif(visit_1h, 0) as visit_to_nv_trial_rate_1h,
    nv_retrial_1h / nullif(visit_1h, 0) as visit_to_nv_retrial_rate_1h,
    nv_trial_or_retrial_1h / nullif(visit_1h, 0) as visit_to_nv_trial_or_retrial_rate_1h,
    nv_trial_or_retrial_1h / nullif(notifs_sent, 0) as send_to_nv_trial_or_retrial_rate_1h,
    unsubscribe_1h / nullif(notifs_sent, 0) as send_to_unsubscribe_rate_1h,
    nv_order_1h / nullif(order_1h, 0) as pct_nv_orders_1h,
    open_to_nv_order_1h / nullif(nv_order_1h, 0) as pct_nv_orders_from_open_1h,
    open_to_nv_order_1h / nullif(open_1h, 0) as open_to_nv_order_rate_1h,
    
    -- 1h Customer Rate Metrics
    cx_receive_1h / nullif(cx_notifs_sent, 0) as cx_receive_rate_1h,
    cx_open_1h / nullif(cx_notifs_sent, 0) as cx_send_to_open_rate_1h,
    cx_visit_1h / nullif(cx_notifs_sent, 0) as cx_send_to_visit_rate_1h,
    cx_visit_1h / nullif(cx_open_1h, 0) as cx_open_to_visit_rate_1h,
    cx_order_1h / nullif(cx_open_1h, 0) as cx_open_to_order_rate_1h,
    cx_order_1h / nullif(cx_visit_1h, 0) as cx_visit_to_order_rate_1h,
    cx_nv_order_1h / nullif(cx_visit_1h, 0) as cx_visit_to_nv_order_rate_1h,
    cx_nv_trial_1h / nullif(cx_visit_1h, 0) as cx_visit_to_nv_trial_rate_1h,
    cx_nv_retrial_1h / nullif(cx_visit_1h, 0) as cx_visit_to_nv_retrial_rate_1h,
    cx_nv_trial_or_retrial_1h / nullif(cx_visit_1h, 0) as cx_visit_to_nv_trial_or_retrial_rate_1h,
    cx_nv_trial_or_retrial_1h / nullif(cx_notifs_sent, 0) as cx_send_to_nv_trial_or_retrial_rate_1h,
    cx_unsubscribe_1h / nullif(cx_notifs_sent, 0) as cx_send_to_unsubscribe_rate_1h,
    
    -- 4h Metrics
    count(distinct iff(open_within_4h = 1, deduped_message_id, null)) as open_4h,
    count(distinct iff(receive_within_4h = 1, deduped_message_id, null)) as receive_4h,
    count(distinct iff(visit_within_4h = 1, deduped_message_id, null)) as visit_4h,
    count(distinct iff(order_within_4h = 1, deduped_message_id, null)) as order_4h,
    count(distinct iff(nv_order_within_4h = 1, deduped_message_id, null)) as nv_order_4h,
    count(distinct iff(nv_trial_within_4h = 1, deduped_message_id, null)) as nv_trial_4h,
    count(distinct iff(nv_retrial_within_4h = 1, deduped_message_id, null)) as nv_retrial_4h,
    count(distinct iff(nv_trial_or_retrial_within_4h = 1, deduped_message_id, null)) as nv_trial_or_retrial_4h,
    count(distinct iff(bounce_within_4h = 1, deduped_message_id, null)) as bounce_4h,
    count(distinct iff(unsubscribe_within_4h = 1, deduped_message_id, null)) as unsubscribe_4h,
    count(distinct iff(uninstall_within_4h = 1, deduped_message_id, null)) as uninstall_4h,
    count(distinct iff(open_to_nv_order_within_4h = 1, deduped_message_id, null)) as open_to_nv_order_4h,
    
    -- 4h Customer Count Metrics
    count(distinct iff(open_within_4h = 1, consumer_id, null)) as cx_open_4h,
    count(distinct iff(receive_within_4h = 1, consumer_id, null)) as cx_receive_4h,
    count(distinct iff(visit_within_4h = 1, consumer_id, null)) as cx_visit_4h,
    count(distinct iff(order_within_4h = 1, consumer_id, null)) as cx_order_4h,
    count(distinct iff(nv_order_within_4h = 1, consumer_id, null)) as cx_nv_order_4h,
    count(distinct iff(nv_trial_within_4h = 1, consumer_id, null)) as cx_nv_trial_4h,
    count(distinct iff(nv_retrial_within_4h = 1, consumer_id, null)) as cx_nv_retrial_4h,
    count(distinct iff(nv_trial_or_retrial_within_4h = 1, consumer_id, null)) as cx_nv_trial_or_retrial_4h,
    count(distinct iff(bounce_within_4h = 1, consumer_id, null)) as cx_bounce_4h,
    count(distinct iff(unsubscribe_within_4h = 1, consumer_id, null)) as cx_unsubscribe_4h,
    count(distinct iff(uninstall_within_4h = 1, consumer_id, null)) as cx_uninstall_4h,
    
    -- 4h Rate Metrics
    receive_4h / nullif(notifs_sent, 0) as receive_rate_4h,
    open_4h / nullif(notifs_sent, 0) as send_to_open_rate_4h,
    visit_4h / nullif(notifs_sent, 0) as send_to_visit_rate_4h,
    visit_4h / nullif(open_4h, 0) as open_to_visit_rate_4h,
    order_4h / nullif(open_4h, 0) as open_to_order_rate_4h,
    order_4h / nullif(visit_4h, 0) as visit_to_order_rate_4h,
    nv_order_4h / nullif(notifs_sent, 0) as send_to_nv_order_rate_4h,
    nv_order_4h / nullif(visit_4h, 0) as visit_to_nv_order_rate_4h,
    nv_trial_4h / nullif(visit_4h, 0) as visit_to_nv_trial_rate_4h,
    nv_retrial_4h / nullif(visit_4h, 0) as visit_to_nv_retrial_rate_4h,
    nv_trial_or_retrial_4h / nullif(visit_4h, 0) as visit_to_nv_trial_or_retrial_rate_4h,
    nv_trial_or_retrial_4h / nullif(notifs_sent, 0) as send_to_nv_trial_or_retrial_rate_4h,
    unsubscribe_4h / nullif(notifs_sent, 0) as send_to_unsubscribe_rate_4h,
    nv_order_4h / nullif(order_4h, 0) as pct_nv_orders_4h,
    open_to_nv_order_4h / nullif(nv_order_4h, 0) as pct_nv_orders_from_open_4h,  
    open_to_nv_order_4h / nullif(open_4h, 0) as open_to_nv_order_rate_4h,
    
    -- 4h Customer Rate Metrics
    cx_receive_4h / nullif(cx_notifs_sent, 0) as cx_receive_rate_4h,
    cx_open_4h / nullif(cx_notifs_sent, 0) as cx_send_to_open_rate_4h,
    cx_visit_4h / nullif(cx_notifs_sent, 0) as cx_send_to_visit_rate_4h,
    cx_visit_4h / nullif(cx_open_4h, 0) as cx_open_to_visit_rate_4h,
    cx_order_4h / nullif(cx_open_4h, 0) as cx_open_to_order_rate_4h,
    cx_order_4h / nullif(cx_visit_4h, 0) as cx_visit_to_order_rate_4h,
    cx_nv_order_4h / nullif(cx_visit_4h, 0) as cx_visit_to_nv_order_rate_4h,
    cx_nv_trial_4h / nullif(cx_visit_4h, 0) as cx_visit_to_nv_trial_rate_4h,
    cx_nv_retrial_4h / nullif(cx_visit_4h, 0) as cx_visit_to_nv_retrial_rate_4h,
    cx_nv_trial_or_retrial_4h / nullif(cx_visit_4h, 0) as cx_visit_to_nv_trial_or_retrial_rate_4h,
    cx_nv_trial_or_retrial_4h / nullif(cx_notifs_sent, 0) as cx_send_to_nv_trial_or_retrial_rate_4h,
    cx_unsubscribe_4h / nullif(cx_notifs_sent, 0) as cx_send_to_unsubscribe_rate_4h,
    
    -- 24h Metrics
    count(distinct iff(open_within_24h = 1, deduped_message_id, null)) as open_24h,
    count(distinct iff(receive_within_24h = 1, deduped_message_id, null)) as receive_24h,
    count(distinct iff(visit_within_24h = 1, deduped_message_id, null)) as visit_24h,
    count(distinct iff(order_within_24h = 1, deduped_message_id, null)) as order_24h,
    count(distinct iff(nv_order_within_24h = 1, deduped_message_id, null)) as nv_order_24h,
    count(distinct iff(nv_trial_within_24h = 1, deduped_message_id, null)) as nv_trial_24h,
    count(distinct iff(nv_retrial_within_24h = 1, deduped_message_id, null)) as nv_retrial_24h,
    count(distinct iff(nv_trial_or_retrial_within_24h = 1, deduped_message_id, null)) as nv_trial_or_retrial_24h,
    count(distinct iff(bounce_within_24h = 1, deduped_message_id, null)) as bounce_24h,
    count(distinct iff(unsubscribe_within_24h = 1, deduped_message_id, null)) as unsubscribe_24h,
    count(distinct iff(uninstall_within_24h = 1, deduped_message_id, null)) as uninstall_24h,
    count(distinct iff(open_to_nv_order_within_24h = 1, deduped_message_id, null)) as open_to_nv_order_24h,
    
    -- 24h Customer Count Metrics
    count(distinct iff(open_within_24h = 1, consumer_id, null)) as cx_open_24h,
    count(distinct iff(receive_within_24h = 1, consumer_id, null)) as cx_receive_24h,
    count(distinct iff(visit_within_24h = 1, consumer_id, null)) as cx_visit_24h,
    count(distinct iff(order_within_24h = 1, consumer_id, null)) as cx_order_24h,
    count(distinct iff(nv_order_within_24h = 1, consumer_id, null)) as cx_nv_order_24h,
    count(distinct iff(nv_trial_within_24h = 1, consumer_id, null)) as cx_nv_trial_24h,
    count(distinct iff(nv_retrial_within_24h = 1, consumer_id, null)) as cx_nv_retrial_24h, 
    count(distinct iff(nv_trial_or_retrial_within_24h = 1, consumer_id, null)) as cx_nv_trial_or_retrial_24h,
    count(distinct iff(bounce_within_24h = 1, consumer_id, null)) as cx_bounce_24h,
    count(distinct iff(unsubscribe_within_24h = 1, consumer_id, null)) as cx_unsubscribe_24h,
    count(distinct iff(uninstall_within_24h = 1, consumer_id, null)) as cx_uninstall_24h,
    
    -- 24h Rate Metrics
    receive_24h / nullif(notifs_sent, 0) as receive_rate_24h,
    open_24h / nullif(notifs_sent, 0) as send_to_open_rate_24h,
    visit_24h / nullif(notifs_sent, 0) as send_to_visit_rate_24h,
    visit_24h / nullif(open_24h, 0) as open_to_visit_rate_24h,
    order_24h / nullif(open_24h, 0) as open_to_order_rate_24h,
    order_24h / nullif(visit_24h, 0) as visit_to_order_rate_24h,
    nv_order_24h / nullif(notifs_sent, 0) as send_to_nv_order_rate_24h,
    nv_order_24h / nullif(visit_24h, 0) as visit_to_nv_order_rate_24h,
    nv_trial_24h / nullif(visit_24h, 0) as visit_to_nv_trial_rate_24h,
    nv_retrial_24h / nullif(visit_24h, 0) as visit_to_nv_retrial_rate_24h,
    nv_trial_or_retrial_24h / nullif(visit_24h, 0) as visit_to_nv_trial_or_retrial_rate_24h,
    nv_trial_or_retrial_24h / nullif(notifs_sent, 0) as send_to_nv_trial_or_retrial_rate_24h,
    unsubscribe_24h / nullif(notifs_sent, 0) as send_to_unsubscribe_rate_24h,
    nv_order_24h /  nullif(order_24h, 0) as pct_nv_orders_24h,
    open_to_nv_order_24h / nullif(nv_order_24h, 0) as pct_nv_orders_from_open_24h,
    open_to_nv_order_24h / nullif(open_24h, 0) as open_to_nv_order_rate_24h,
    
    -- 24h Customer Rate Metrics
    cx_receive_24h / nullif(cx_notifs_sent, 0) as cx_receive_rate_24h,
    cx_open_24h / nullif(cx_notifs_sent, 0) as cx_send_to_open_rate_24h,
    cx_visit_24h / nullif(cx_notifs_sent, 0) as cx_send_to_visit_rate_24h,
    cx_visit_24h / nullif(cx_open_24h, 0) as cx_open_to_visit_rate_24h,
    cx_order_24h / nullif(cx_open_24h, 0) as cx_open_to_order_rate_24h,
    cx_order_24h / nullif(cx_visit_24h, 0) as cx_visit_to_order_rate_24h,
    cx_nv_order_24h / nullif(cx_visit_24h, 0) as cx_visit_to_nv_order_rate_24h,
    cx_nv_trial_24h / nullif(cx_visit_24h, 0) as cx_visit_to_nv_trial_rate_24h,
    cx_nv_retrial_24h / nullif(cx_visit_24h, 0) as cx_visit_to_nv_retrial_rate_24h,
    cx_nv_trial_or_retrial_24h / nullif(cx_visit_24h, 0) as cx_visit_to_nv_trial_or_retrial_rate_24h,
    cx_nv_trial_or_retrial_24h / nullif(cx_notifs_sent, 0) as cx_send_to_nv_trial_or_retrial_rate_24h,
    cx_unsubscribe_24h / nullif(cx_notifs_sent, 0) as cx_send_to_unsubscribe_rate_24h
    from  proddb.public.nvg_notif_metrics_base b
    where campaign_name is not null
    group by all
),
targeting as (
 select *
 from  growth_service_prod.public.engagement_program_run_results r
 where r.program_name in (select distinct ep_name from proddb.public.nv_channels_notif_index)
 qualify  row_number() over (partition by program_name, date_trunc('week', run_at::DATE) order by run_at asc) =1
)
 
select m.*, 
run_count approx_targeted_cx, 
notifs_sent / approx_targeted_cx targeted_to_sent_rate,
cx_notifs_sent / approx_targeted_cx cx_targeted_to_sent_rate
from metrics m
left join  targeting r
    on m.ep_name = r.program_name 
    and date_trunc('week', r.run_at::DATE) = m.sent_week
group by all
;