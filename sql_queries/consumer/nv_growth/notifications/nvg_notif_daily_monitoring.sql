
create or replace table proddb.public.nvg_notif_daily_monitoring as

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
    from  proddb.public.nvg_notif_metrics_base b
    where campaign_name is not null
    group by all
),
targeting as (
    select program_name, run_at::DATE run_date, max(run_count) run_count
    from  growth_service_prod.public.engagement_program_run_results r
    where r.program_name in (select distinct ep_name from proddb.public.nv_channels_notif_index)
    group by all
 )
 
select m.*, 
run_count approx_targeted_cx, 
notifs_sent / approx_targeted_cx targeted_to_sent_rate,
cx_notifs_sent / approx_targeted_cx cx_targeted_to_sent_rate
from metrics m
left join  targeting r
    on m.ep_name = r.program_name 
    and m.sent_at_date = r.run_date
group by all
;