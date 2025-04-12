-- Query to compare notifications/emails: those in our index vs all others
-- Based on the fact_consumer_notification_engagement table

-- Set the analysis window (default to last 30 days if not specified)
set window_start = dateadd('d', -30, current_date);
set window_end = current_date;

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


-- Get all notification engagements from the base table
create or replace temporary table ts_all_notifs as
    select 
        e.consumer_id,
        concat(e.deduped_message_id, '_', e.consumer_id) as deduped_message_id_consumer,
        min(e.sent_at_date) as sent_at_date,
        e.notification_channel,
        coalesce(e.campaign_id, e.canvas_id) as campaign_canvas_id,
        coalesce(e.campaign_name, e.canvas_name) as campaign_name,
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
        -- Check if this notification is in our NV index
        case 
            when idx.team is not null then idx.team
            else 'All Others'
        end as team,
        -- Create receive flag for 24h window
        case
            when max(e.receive_within_24h) = 1 then 1
            when e.notification_channel = 'EMAIL' and max(e.open_within_24h) = 1 then 1
            when e.notification_channel = 'PUSH' and max(e.opened_at) is not null then 1
            else 0
        end as received_24h
    from edw.consumer.fact_consumer_notification_engagement e
    left join proddb.public.nv_channels_notif_index idx
        on coalesce(e.campaign_id, e.canvas_id) = coalesce(idx.campaign_id, idx.canvas_id)
    where e.sent_at_date between $window_start and $window_end
    and e.notification_channel in ('PUSH', 'EMAIL')  -- Filter out SMS, only include PUSH and EMAIL
    and (e.notification_message_type is null or e.notification_message_type != 'Transactional')
    and is_valid_send = 1
    group by all
    ;  -- Filter out Transactional notifications

-- Create the dashboard with daily aggregates by team, channel and growth state
create or replace table proddb.public.nvg_notif_compare_index_vs_all_dashboard as
select 
    n.sent_at_date,
    n.team,
    n.notification_channel,
    g.dd_cx_growth_state,
    g.nv_cx_growth_state,
    g.dd_nv_cx_growth_state,
    n.daypart,
    count(distinct n.consumer_id) as consumers_notified,
    count(distinct n.deduped_message_id_consumer) as notifs_sent,
    sum(n.received_24h) as notifs_received_24h,
    notifs_received_24h / nullif(notifs_sent, 0) as received_rate_24h,
    -- Calculate daily stats per consumer
    notifs_sent / nullif(consumers_notified, 0) as notifs_per_consumer,
    -- Relative metrics for NV teams vs others
    case 
        when max(case when n.team != 'All Others' then 1 else 0 end) over (partition by n.sent_at_date) = 1 
        then notifs_sent / nullif(sum(case when n.team != 'All Others' then notifs_sent else 0 end) 
            over (partition by n.sent_at_date, n.notification_channel), 0) 
        else null
    end as pct_of_nv_notifs,
    case 
        when max(case when n.team = 'All Others' then 1 else 0 end) over (partition by n.sent_at_date) = 1 
        then notifs_sent / nullif(sum(case when n.team = 'All Others' then notifs_sent else 0 end) 
            over (partition by n.sent_at_date, n.notification_channel), 0) 
        else null
    end as pct_of_others_notifs
from ts_all_notifs n
left join ts_daily_growth_states g
    on n.consumer_id = g.consumer_id
    and n.sent_at_date = g.status_date
group by all
order by n.sent_at_date desc, n.team, n.notification_channel, g.dd_cx_growth_state, g.nv_cx_growth_state, g.dd_nv_cx_growth_state;


create or replace table proddb.public.nvg_notif_compare_index_vs_all_summary as
select 
    sent_at_date,
    team,
    notification_channel,
    dd_cx_growth_state,
    nv_cx_growth_state,
    dd_nv_cx_growth_state,
    daypart,
    sum(consumers_notified) as consumers_notified,
    sum(notifs_sent) as notifs_sent,
    sum(notifs_received_24h) as notifs_received_24h,
    sum(notifs_received_24h) / nullif(sum(notifs_sent), 0) as received_rate_24h,
    sum(notifs_sent) / nullif(sum(consumers_notified), 0) as notifs_per_consumer,
    -- Share of total metrics
    sum(notifs_sent) / nullif(sum(sum(notifs_sent)) over (partition by sent_at_date, notification_channel), 0) as daily_channel_share
from proddb.public.nvg_notif_compare_index_vs_all_dashboard
group by all
order by sent_at_date desc, team, notification_channel, dd_cx_growth_state, nv_cx_growth_state, dd_nv_cx_growth_state; 