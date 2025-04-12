create or replace table proddb.public.nvg_notif_compare_index_vs_all_summary as
select 
    sent_at_date,
    team,
    notification_channel,
    dd_cx_growth_state,
    nv_cx_growth_state,
    dd_nv_cx_growth_state,
    sum(consumers_notified) as consumers_notified,
    sum(notifs_sent) as my_notifs_sent,
    sum(notifs_received_24h) as notifs_received_24h,
    sum(notifs_received_24h) / nullif(sum(notifs_sent), 0) as received_rate_24h,
    sum(notifs_sent) / nullif(sum(consumers_notified), 0) as notifs_per_consumer,
    -- Share of total metrics
    sum(notifs_sent) / nullif(sum(sum(notifs_sent)) over (partition by sent_at_date, notification_channel), 0) as daily_channel_share
from proddb.public.nvg_notif_compare_index_vs_all_dashboard
group by 
    sent_at_date,
    team,
    notification_channel,
    dd_cx_growth_state,
    nv_cx_growth_state,
    dd_nv_cx_growth_state
order by sent_at_date desc, team, notification_channel, dd_cx_growth_state, nv_cx_growth_state, dd_nv_cx_growth_state; 