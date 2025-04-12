--This will eventaully get replaced with HLL

-- Send volume comparison between specified teams (CRM, NVG) vs all others by notification channel
select 
    sent_at_date,
    notification_channel,
    team,
    count(1) as total_notifications,
    count(distinct deduped_message_id_consumer) as notifications_sent,
    count(distinct consumer_id) as consumers_reached
from proddb.tylershields.nvg_notif_base_data
group by all;


