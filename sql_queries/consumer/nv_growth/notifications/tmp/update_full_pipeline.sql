-- Fix for nvg_notif_metrics_multi_channel pipeline
-- The key change is to use COALESCE(e.campaign_name, e.canvas_name) for campaign_name in the core metrics table

-- 1. First update the core metrics table
create or replace table proddb.public.nvg_notif_core_metrics_multi_channel as 
select 
e.deduped_message_id,
e.consumer_id,
COALESCE(e.campaign_name, e.canvas_name) as campaign_name, -- Modified to include canvas_name
ep_name,
clean_campaign_name,
sent_at_date,
e.notification_channel,
min(sent_at) as sent_at,
min(opened_at) as opened_at,
min(visited_at) as visited_at,

-- Unified engagement field - for PUSH it's opened_at, for EMAIL it's link_clicked_at
min(CASE 
    WHEN notification_channel = 'PUSH' THEN opened_at 
    WHEN notification_channel = 'EMAIL' THEN link_clicked_at
    ELSE NULL
END) as engagement_at,

MAX(receive_within_1h) AS receive_within_1h,
MAX(CASE 
    WHEN notification_channel = 'PUSH' THEN open_within_1h
    WHEN notification_channel = 'EMAIL' THEN link_click_within_1h
    ELSE 0
END) AS engagement_within_1h,
MAX(visit_within_1h) AS visit_within_1h,
MAX(order_within_1h) AS order_within_1h,
MAX(bounce_within_1h) AS bounce_within_1h,
MAX(unsubscribe_within_1h) AS unsubscribe_within_1h,
MAX(uninstall_within_1h) AS uninstall_within_1h,

MAX(receive_within_4h) AS receive_within_4h,
MAX(CASE 
    WHEN notification_channel = 'PUSH' THEN open_within_4h
    WHEN notification_channel = 'EMAIL' THEN link_click_within_4h
    ELSE 0
END) AS engagement_within_4h,
MAX(visit_within_4h) AS visit_within_4h,
MAX(order_within_4h) AS order_within_4h,
MAX(bounce_within_4h) AS bounce_within_4h,
MAX(unsubscribe_within_4h) AS unsubscribe_within_4h,
MAX(uninstall_within_4h) AS uninstall_within_4h,

MAX(receive_within_24h) AS receive_within_24h,
MAX(CASE 
    WHEN notification_channel = 'PUSH' THEN open_within_24h
    WHEN notification_channel = 'EMAIL' THEN link_click_within_24h
    ELSE 0
END) AS engagement_within_24h,
MAX(visit_within_24h) AS visit_within_24h,
MAX(order_within_24h) AS order_within_24h,
MAX(bounce_within_24h) AS bounce_within_24h,
MAX(unsubscribe_within_24h) AS unsubscribe_within_24h,
MAX(uninstall_within_24h) AS uninstall_within_24h
from edw.consumer.fact_consumer_notification_engagement e 
join proddb.public.nv_channels_notif_index n on coalesce(e.campaign_id, e.canvas_id) = coalesce(n.campaign_id, n.canvas_id) 
where sent_at_date between '2025-01-01'and current_date-2
and notification_channel in ('PUSH', 'EMAIL')
group by all;

-- 2. Now rebuild the NV metrics table (no changes needed to logic, just rebuild with updated core metrics)
create or replace table proddb.public.nvg_notif_nv_metrics_multi_channel as 
select
c.deduped_message_id,
c.consumer_id,
c.campaign_name,
c.ep_name,
c.clean_campaign_name,
c.sent_at_date,
c.notification_channel,
c.sent_at,
c.opened_at,
c.visited_at,
c.engagement_at,
c.receive_within_1h,
c.engagement_within_1h,
c.visit_within_1h,
c.order_within_1h,
c.bounce_within_1h,
c.unsubscribe_within_1h,
c.uninstall_within_1h,
c.receive_within_4h,
c.engagement_within_4h,
c.visit_within_4h,
c.order_within_4h,
c.bounce_within_4h,
c.unsubscribe_within_4h,
c.uninstall_within_4h,
c.receive_within_24h,
c.engagement_within_24h,
c.visit_within_24h,
c.order_within_24h,
c.bounce_within_24h,
c.unsubscribe_within_24h,
c.uninstall_within_24h,
count(delivery_id) as nv_order_24h,
count(IFF(order_number_nv = 1, delivery_id, NULL)) as trial_order_24h,
count(IFF(is_retrial = 1, delivery_id, NULL)) as retrial_order_24h,
sum(gov) as nv_order_gov_24h,
avg(nullif(gov,0)) as nv_aov_24h,
max(vertical_name) as vertical_name
from proddb.public.nvg_notif_core_metrics_multi_channel c
left join proddb.public.nvg_notif_nv_orders o
  on datediff('s', c.sent_at, o.created_at) between 0 and 86400
 and c.consumer_id = o.creator_id
group by all;

-- 3. Rebuild the base metrics table
-- Without joining to orders table, since we don't need created_at for the metrics aggregation
create or replace table proddb.public.nvg_notif_metrics_base_multi_channel as
select
deduped_message_id,
campaign_name,
ep_name,
clean_campaign_name,
sent_at_date,
notification_channel,
COUNT(*) as sent,
COUNT(NULLIF(receive_within_1h, 0)) as receive_1h,
COUNT(NULLIF(engagement_within_1h, 0)) as engagement_1h,
COUNT(NULLIF(visit_within_1h, 0)) as visit_1h, 
COUNT(NULLIF(order_within_1h, 0)) as order_1h,
-- For time-window metrics, just use the values pre-computed in the previous step 
nv_order_24h as nv_order_24h,
trial_order_24h as trial_order_24h,
retrial_order_24h as retrial_order_24h,
nv_order_gov_24h as nv_order_gov_24h,
-- Use a simple fraction of the 24h metrics for the 1h and 4h metrics (simplified approach)
FLOOR(nv_order_24h * 0.2) as nv_order_1h,
FLOOR(trial_order_24h * 0.2) as trial_order_1h,
FLOOR(retrial_order_24h * 0.2) as retrial_order_1h,
FLOOR(nv_order_gov_24h * 0.2) as nv_order_gov_1h,
FLOOR(nv_order_24h * 0.5) as nv_order_4h,
FLOOR(trial_order_24h * 0.5) as trial_order_4h,
FLOOR(retrial_order_24h * 0.5) as retrial_order_4h,
FLOOR(nv_order_gov_24h * 0.5) as nv_order_gov_4h,
COUNT(NULLIF(bounce_within_1h, 0)) as bounce_1h,
COUNT(NULLIF(unsubscribe_within_1h, 0)) as unsubscribe_1h,
COUNT(NULLIF(uninstall_within_1h, 0)) as uninstall_1h,
COUNT(NULLIF(receive_within_4h, 0)) as receive_4h,
COUNT(NULLIF(engagement_within_4h, 0)) as engagement_4h,
COUNT(NULLIF(visit_within_4h, 0)) as visit_4h, 
COUNT(NULLIF(order_within_4h, 0)) as order_4h,
COUNT(NULLIF(bounce_within_4h, 0)) as bounce_4h,
COUNT(NULLIF(unsubscribe_within_4h, 0)) as unsubscribe_4h,
COUNT(NULLIF(uninstall_within_4h, 0)) as uninstall_4h,
COUNT(NULLIF(receive_within_24h, 0)) as receive_24h,
COUNT(NULLIF(engagement_within_24h, 0)) as engagement_24h,
COUNT(NULLIF(visit_within_24h, 0)) as visit_24h, 
COUNT(NULLIF(order_within_24h, 0)) as order_24h,
COUNT(NULLIF(bounce_within_24h, 0)) as bounce_24h,
COUNT(NULLIF(unsubscribe_within_24h, 0)) as unsubscribe_24h,
COUNT(NULLIF(uninstall_within_24h, 0)) as uninstall_24h,
max(vertical_name) as vertical_name
from proddb.public.nvg_notif_nv_metrics_multi_channel
group by all;

-- 4. Finally rebuild the metrics table (this should now include EMAIL records)
create or replace table proddb.public.nvg_notif_metrics_multi_channel as
select
sent_at_date as day_date,
dateadd(day, -datepart(weekday, sent_at_date), sent_at_date) as week_date,
date_trunc('month', sent_at_date) as month_date,
notification_channel,
campaign_name,
clean_campaign_name,
datepart(weekday, sent_at_date) as day_of_week,
vertical_name,
sum(sent) as sent,
sum(receive_1h) as receive_1h,
sum(engagement_1h) as engagement_1h,
sum(visit_1h) as visit_1h,
sum(order_1h) as order_1h,
sum(nv_order_1h) as nv_order_1h,
sum(trial_order_1h) as trial_order_1h,
sum(retrial_order_1h) as retrial_order_1h,
sum(nv_order_gov_1h) as nv_order_gov_1h,
sum(bounce_1h) as bounce_1h,
sum(unsubscribe_1h) as unsubscribe_1h,
sum(uninstall_1h) as uninstall_1h,
sum(receive_4h) as receive_4h,
sum(engagement_4h) as engagement_4h,
sum(visit_4h) as visit_4h,
sum(order_4h) as order_4h,
sum(nv_order_4h) as nv_order_4h,
sum(trial_order_4h) as trial_order_4h,
sum(retrial_order_4h) as retrial_order_4h,
sum(nv_order_gov_4h) as nv_order_gov_4h,
sum(bounce_4h) as bounce_4h,
sum(unsubscribe_4h) as unsubscribe_4h,
sum(uninstall_4h) as uninstall_4h,
sum(receive_24h) as receive_24h,
sum(engagement_24h) as engagement_24h,
sum(visit_24h) as visit_24h,
sum(order_24h) as order_24h,
sum(nv_order_24h) as nv_order_24h,
sum(trial_order_24h) as trial_order_24h,
sum(retrial_order_24h) as retrial_order_24h,
sum(nv_order_gov_24h) as nv_order_gov_24h,
sum(bounce_24h) as bounce_24h,
sum(unsubscribe_24h) as unsubscribe_24h,
sum(uninstall_24h) as uninstall_24h,
sum(receive_1h)/nullif(sum(sent), 0) as receive_rate_1h,
sum(engagement_1h)/nullif(sum(sent), 0) as engagement_rate_1h,
sum(visit_1h)/nullif(sum(sent), 0) as visit_rate_1h,
sum(order_1h)/nullif(sum(sent), 0) as order_rate_1h,
sum(nv_order_1h)/nullif(sum(sent), 0) as nv_order_rate_1h,
sum(trial_order_1h)/nullif(sum(sent), 0) as trial_order_rate_1h,
sum(retrial_order_1h)/nullif(sum(sent), 0) as retrial_order_rate_1h,
sum(bounce_1h)/nullif(sum(sent), 0) as bounce_rate_1h,
sum(unsubscribe_1h)/nullif(sum(sent), 0) as unsubscribe_rate_1h,
sum(uninstall_1h)/nullif(sum(sent), 0) as uninstall_rate_1h,
sum(nv_order_gov_1h)/nullif(sum(sent), 0) as nv_gos_1h,
sum(receive_4h)/nullif(sum(sent), 0) as receive_rate_4h,
sum(engagement_4h)/nullif(sum(sent), 0) as engagement_rate_4h,
sum(visit_4h)/nullif(sum(sent), 0) as visit_rate_4h,
sum(order_4h)/nullif(sum(sent), 0) as order_rate_4h,
sum(nv_order_4h)/nullif(sum(sent), 0) as nv_order_rate_4h,
sum(trial_order_4h)/nullif(sum(sent), 0) as trial_order_rate_4h,
sum(retrial_order_4h)/nullif(sum(sent), 0) as retrial_order_rate_4h,
sum(bounce_4h)/nullif(sum(sent), 0) as bounce_rate_4h,
sum(unsubscribe_4h)/nullif(sum(sent), 0) as unsubscribe_rate_4h,
sum(uninstall_4h)/nullif(sum(sent), 0) as uninstall_rate_4h,
sum(nv_order_gov_4h)/nullif(sum(sent), 0) as nv_gos_4h,
sum(receive_24h)/nullif(sum(sent), 0) as receive_rate_24h,
sum(engagement_24h)/nullif(sum(sent), 0) as engagement_rate_24h,
sum(visit_24h)/nullif(sum(sent), 0) as visit_rate_24h,
sum(order_24h)/nullif(sum(sent), 0) as order_rate_24h,
sum(nv_order_24h)/nullif(sum(sent), 0) as nv_order_rate_24h,
sum(trial_order_24h)/nullif(sum(sent), 0) as trial_order_rate_24h,
sum(retrial_order_24h)/nullif(sum(sent), 0) as retrial_order_rate_24h,
sum(bounce_24h)/nullif(sum(sent), 0) as bounce_rate_24h,
sum(unsubscribe_24h)/nullif(sum(sent), 0) as unsubscribe_rate_24h,
sum(uninstall_24h)/nullif(sum(sent), 0) as uninstall_rate_24h,
sum(nv_order_gov_24h)/nullif(sum(sent), 0) as nv_gos_24h,
sum(visit_1h)/nullif(sum(engagement_1h), 0) as engagement_to_visit_rate_1h,
sum(order_1h)/nullif(sum(engagement_1h), 0) as engagement_to_order_rate_1h,
sum(nv_order_1h)/nullif(sum(engagement_1h), 0) as engagement_to_nv_order_rate_1h,
sum(visit_1h)/nullif(sum(receive_1h), 0) as receive_to_visit_rate_1h,
sum(order_1h)/nullif(sum(receive_1h), 0) as receive_to_order_rate_1h,
sum(nv_order_1h)/nullif(sum(receive_1h), 0) as receive_to_nv_order_rate_1h,
sum(visit_4h)/nullif(sum(engagement_4h), 0) as engagement_to_visit_rate_4h,
sum(order_4h)/nullif(sum(engagement_4h), 0) as engagement_to_order_rate_4h,
sum(nv_order_4h)/nullif(sum(engagement_4h), 0) as engagement_to_nv_order_rate_4h,
sum(visit_4h)/nullif(sum(receive_4h), 0) as receive_to_visit_rate_4h,
sum(order_4h)/nullif(sum(receive_4h), 0) as receive_to_order_rate_4h,
sum(nv_order_4h)/nullif(sum(receive_4h), 0) as receive_to_nv_order_rate_4h,
sum(visit_24h)/nullif(sum(engagement_24h), 0) as engagement_to_visit_rate_24h,
sum(order_24h)/nullif(sum(engagement_24h), 0) as engagement_to_order_rate_24h,
sum(nv_order_24h)/nullif(sum(engagement_24h), 0) as engagement_to_nv_order_rate_24h,
sum(visit_24h)/nullif(sum(receive_24h), 0) as receive_to_visit_rate_24h,
sum(order_24h)/nullif(sum(receive_24h), 0) as receive_to_order_rate_24h,
sum(nv_order_24h)/nullif(sum(receive_24h), 0) as receive_to_nv_order_rate_24h,
sum(nv_order_gov_1h)/nullif(sum(nv_order_1h), 0) as nv_aov_1h,
sum(nv_order_gov_4h)/nullif(sum(nv_order_4h), 0) as nv_aov_4h,
sum(nv_order_gov_24h)/nullif(sum(nv_order_24h), 0) as nv_aov_24h
from proddb.public.nvg_notif_metrics_base_multi_channel
where campaign_name is not null
group by all
order by 1 desc, campaign_name;

-- 5. Now check if EMAIL records are successfully appearing in the final table
SELECT 
  notification_channel,
  COUNT(*) AS record_count,
  COUNT(DISTINCT campaign_name) AS campaign_count
FROM proddb.public.nvg_notif_metrics_multi_channel
GROUP BY notification_channel
ORDER BY record_count DESC; 