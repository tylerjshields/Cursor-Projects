create or replace table proddb.public.nvg_notif_metrics_dashboard as 

with metrics as (
    select 
    campaign_name,
    clean_campaign_name,
    ep_name,
    sent_week,
    '1h' as time_window,
    notifs_sent,
    cx_notifs_sent,
    notifs_sent_per_cx,

    open_1h as open,
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
    open_to_nv_order_1h as open_to_nv_order,

    -- 1h Rate Metrics
    receive_rate_1h as receive_rate,
    send_to_open_rate_1h as send_to_open_rate,
    send_to_visit_rate_1h as send_to_visit_rate,
    send_to_nv_order_rate_1h as send_to_nv_order_rate,
    send_to_nv_trial_or_retrial_rate_1h as send_to_nv_trial_or_retrial_rate,
    send_to_unsubscribe_rate_1h as send_to_unsubscribe_rate,
    open_to_visit_rate_1h as open_to_visit_rate,
    open_to_order_rate_1h as open_to_order_rate,
    open_to_nv_order_rate_1h as open_to_nv_order_rate,
    visit_to_order_rate_1h as visit_to_order_rate,
    visit_to_nv_order_rate_1h as visit_to_nv_order_rate,
    visit_to_nv_trial_rate_1h as visit_to_nv_trial_rate,
    visit_to_nv_retrial_rate_1h as visit_to_nv_retrial_rate,
    visit_to_nv_trial_or_retrial_rate_1h as visit_to_nv_trial_or_retrial_rate,
    pct_nv_orders_1h as pct_nv_orders,
    pct_nv_orders_from_open_1h as pct_nv_orders_from_open
    
    
    from proddb.public.nvg_notif_metrics
    union all

    select 
    campaign_name,
    clean_campaign_name,
    ep_name,
    sent_week,
    '4h' as time_window,
    notifs_sent,
    cx_notifs_sent,
    notifs_sent_per_cx,

    open_4h as open,
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
    open_to_nv_order_4h as open_to_nv_order,

    -- 4h Rate Metrics
    receive_rate_4h as receive_rate,
    send_to_open_rate_4h as send_to_open_rate,
    send_to_visit_rate_4h as send_to_visit_rate,
    send_to_nv_order_rate_4h as send_to_nv_order_rate,
    send_to_nv_trial_or_retrial_rate_4h as send_to_nv_trial_or_retrial_rate,
    send_to_unsubscribe_rate_4h as send_to_unsubscribe_rate,
    open_to_visit_rate_4h as open_to_visit_rate,
    open_to_order_rate_4h as open_to_order_rate,
    open_to_nv_order_rate_4h as open_to_nv_order_rate,
    visit_to_order_rate_4h as visit_to_order_rate,
    visit_to_nv_order_rate_4h as visit_to_nv_order_rate,
    visit_to_nv_trial_rate_4h as visit_to_nv_trial_rate,
    visit_to_nv_retrial_rate_4h as visit_to_nv_retrial_rate,
    visit_to_nv_trial_or_retrial_rate_4h as visit_to_nv_trial_or_retrial_rate,
    pct_nv_orders_4h as pct_nv_orders,
    pct_nv_orders_from_open_4h as pct_nv_orders_from_open

    from proddb.public.nvg_notif_metrics
    union all

    select 
    campaign_name,
    clean_campaign_name,
    ep_name,
    sent_week,
    '24h' as time_window,
    notifs_sent,
    cx_notifs_sent,
    notifs_sent_per_cx,
    
    open_24h as open,
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
    open_to_nv_order_24h as open_to_nv_order,

    -- 24h Rate Metrics
    receive_rate_24h as receive_rate,
    send_to_open_rate_24h as send_to_open_rate,
    send_to_visit_rate_24h as send_to_visit_rate,
    send_to_nv_order_rate_24h as send_to_nv_order_rate,
    send_to_nv_trial_or_retrial_rate_24h as send_to_nv_trial_or_retrial_rate,
    send_to_unsubscribe_rate_24h as send_to_unsubscribe_rate,
    open_to_visit_rate_24h as open_to_visit_rate,
    open_to_order_rate_24h as open_to_order_rate,
    open_to_nv_order_rate_24h as open_to_nv_order_rate,
    visit_to_order_rate_24h as visit_to_order_rate,
    visit_to_nv_order_rate_24h as visit_to_nv_order_rate,
    visit_to_nv_trial_rate_24h as visit_to_nv_trial_rate,
    visit_to_nv_retrial_rate_24h as visit_to_nv_retrial_rate,
    visit_to_nv_trial_or_retrial_rate_24h as visit_to_nv_trial_or_retrial_rate,
    pct_nv_orders_4h as pct_nv_orders,
    pct_nv_orders_from_open_4h as pct_nv_orders_from_open
    
    from proddb.public.nvg_notif_metrics
),
targeting as (
 select *
 from  growth_service_prod.public.engagement_program_run_results r
 where r.program_name in (select distinct ep_name from proddb.public.nv_channels_notif_index)
 qualify  row_number() over (partition by program_name, date_trunc('week', run_at::DATE) order by run_at asc) =1
)
select 
m.campaign_name,
m.ep_name,
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
m.send_to_nv_order_rate,
m.send_to_nv_trial_or_retrial_rate,
m.send_to_unsubscribe_rate,
m.open_to_visit_rate,
m.open_to_order_rate,
m.open_to_nv_order_rate,
m.visit_to_order_rate,
m.visit_to_nv_order_rate,
m.visit_to_nv_trial_rate,
m.visit_to_nv_retrial_rate,
m.visit_to_nv_trial_or_retrial_rate,
m.pct_nv_orders,
m.pct_nv_orders_from_open,

run_count as approx_targeted_cx, 
notifs_sent / approx_targeted_cx as targeted_to_sent_rate,
cx_notifs_sent / approx_targeted_cx as cx_targeted_to_sent_rate,
iff(sent_week=date_trunc('week', current_date-7), 1, 0) is_most_recent_week
from metrics m
left join targeting r
    on m.ep_name = r.program_name 
    and date_trunc('week', r.run_at::date) = m.sent_week
order by m.sent_week DESC, m.campaign_name, m.time_window;