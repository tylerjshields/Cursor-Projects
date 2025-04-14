-- Set date range for the last 30 days
set window_start = dateadd('day', -30, current_date());
set window_end = dateadd('day', -1, current_date());

-- L28 version that aggregates across the entire date range
create or replace table proddb.tylershields.nvg_notif_send_funnel_l28 as
select
clean_campaign_name, 
'fpn' as source, 
sum(num_ingested) as notifs_generated,
sum(num_prioritized) as num_prioritized,
null as num_prioritized_sent,
sum(num_sent) as num_sent,
sum(num_postal_sent) as num_postal_sent,
sum(num_postal_sent_success) as notifs_sent,
sum(num_dropped) as notifs_dropped
from edw.growth.notification_platform_metrics n
join proddb.public.nv_channels_notif_index idx on n.program_id = coalesce(idx.campaign_id, idx.canvas_id)
where reporting_date between $window_start and $window_end
and reporting_grain = 'DAILY'
and user_entity_type = 'CONSUMER_DOORDASH'
and n.channel = 'CHANNEL_TYPE_PUSH'
group by 1, 2

union all

select 
clean_campaign_name, 
'braze' as source,
sum(attempted_sends) as notifs_generated,
null,
null,
null,
null,
sum(successful_sends) as notifs_sent,
sum(total_capped) as notifs_dropped
from proddb.tylershields.nvg_notif_braze_capping_daily
where day_date between $window_start and $window_end
group by 1, 2;

-- Daily version that preserves day-by-day data
create or replace table proddb.tylershields.nvg_notif_send_funnel_daily as
select
reporting_date as day_date,
clean_campaign_name, 
'fpn' as source, 
sum(num_ingested) as notifs_generated,
sum(num_prioritized) as num_prioritized,
null as num_prioritized_sent,
sum(num_sent) as num_sent,
sum(num_postal_sent) as num_postal_sent,
sum(num_postal_sent_success) as notifs_sent,
sum(num_dropped) as notifs_dropped
from edw.growth.notification_platform_metrics n
join proddb.public.nv_channels_notif_index idx on n.program_id = coalesce(idx.campaign_id, idx.canvas_id)
where reporting_date between $window_start and $window_end
and reporting_grain = 'DAILY'
and user_entity_type = 'CONSUMER_DOORDASH'
and n.channel = 'CHANNEL_TYPE_PUSH'
group by 1, 2, 3

union all

select 
day_date,
clean_campaign_name, 
'braze' as source,
sum(attempted_sends) as notifs_generated,
null,
null,
null,
null,
sum(successful_sends) as notifs_sent,
sum(total_capped) as notifs_dropped
from proddb.tylershields.nvg_notif_braze_capping_daily
where day_date between $window_start and $window_end
group by 1, 2, 3
order by day_date desc, clean_campaign_name, source;

-- Grant permissions
grant select on proddb.tylershields.nvg_notif_send_funnel_daily to role read_only_users;

grant select on proddb.tylershields.nvg_notif_send_funnel_l28 to role read_only_users;

-- limit 10