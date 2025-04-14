-- Canvas ID Analysis - Week of 3/31
-- Basic campaign-level stats tied to notifs_index

-- Set date parameters
set analysis_start_date = current_date-30; --'2025-01-01';
set analysis_end_date = current_date();

-- Step 1: Get all Braze canvas/campaign IDs from notifs_index
create or replace temporary table notifs_index_programs as
select 
    n.campaign_id as campaign_id,
    n.canvas_id as canvas_id,
    coalesce(n.campaign_id, n.canvas_id) as program_id,
    n.team,
    n.ep_name,
    n.clean_campaign_name
from proddb.public.nv_channels_notif_index n
where n.campaign_id is not null or n.canvas_id is not null;

-- Step 2: Gather frequency capped notifications for each program
create or replace temporary table program_capped_notifs as 
-- Canvas notifications
select 
    n.program_id,
    n.team,
    n.ep_name,
    n.clean_campaign_name,
    a.external_user_id as consumer_id,
    a.id,
    a.timezone,
    to_timestamp_ntz(a.time) as capped_at,
    convert_timezone('UTC', a.timezone, to_timestamp_ntz(a.time)) as capped_at_local,
    coalesce(c.canvas_name, 'Unknown Canvas') as program_name,
    'canvas' as program_type
from braze_shared.datalake_sharing.USERS_CANVAS_FREQUENCYCAP_SHARED a
join notifs_index_programs n on a.canvas_id = n.canvas_id
left join (
    select 
        canvas_id, 
        canvas_name,
        row_number() over (partition by canvas_id order by event_time desc) as rn
    from ext_braze.inbound.braze_canvas_window
) c on a.canvas_id = c.canvas_id and c.rn = 1
where to_timestamp_ntz(a.time)::date between $analysis_start_date and $analysis_end_date
    and a.channel in ('ios_push', 'android_push')
qualify row_number() over (
    partition by a.external_user_id, a.canvas_id, a.canvas_step_api_id, 
    to_timestamp_ntz(a.time)::date 
    order by to_timestamp_ntz(a.time)
) = 1

union all

-- Campaign notifications
select 
    n.program_id,
    n.team,
    n.ep_name,
    n.clean_campaign_name,
    a.external_user_id as consumer_id,
    a.dispatch_id as id,
    a.timezone,
    to_timestamp_ntz(a.time) as capped_at,
    convert_timezone('UTC', a.timezone, to_timestamp_ntz(a.time)) as capped_at_local,
    coalesce(c.campaign_name, 'Unknown Campaign') as program_name,
    'campaign' as program_type
from braze_shared.datalake_sharing.USERS_CAMPAIGNS_FREQUENCYCAP_SHARED a
join notifs_index_programs n on a.campaign_id = n.campaign_id
left join (
    select 
        campaign_id, 
        campaign_name,
        row_number() over (partition by campaign_id order by event_time desc) as rn
    from ext_braze.inbound.braze_campaign_window
) c on a.campaign_id = c.campaign_id and c.rn = 1
where to_timestamp_ntz(a.time)::date between $analysis_start_date and $analysis_end_date
    and a.channel in ('ios_push', 'android_push')
    and a.campaign_id <> '67ae538bec4aec0064a399fb' -- Remove silent push campaign
    -- Dedupe when consumer has both devices for the same capping event
qualify row_number() over (
    partition by a.external_user_id, a.dispatch_id
    order by to_timestamp_ntz(a.time)
) = 1;

-- Step 3: Attribution - determine which platform caused the capping
create or replace temporary table program_capped_attribution as
with fpn_sent as (
    select 
        consumer_id,
        dd_event_id as notification_uuid,
        campaign_name,
        min(sent_at) as sent_at
    from edw.consumer.fact_consumer_notification_engagement
    where is_valid_send = 1
      and notification_message_type_overall = 'MARKETING'
      and notification_channel = 'PUSH'
      and notification_source ilike '%FPN%'
      and postal_service_source in ('growth-service', 'notification-atc')
      and sent_at::date between dateadd('day', -1, $analysis_start_date) and $analysis_end_date
    group by 1, 2, 3
),

braze_sent as (
    select 
        consumer_id,
        concat(consumer_id, sent_at_date, split_part(message_id, '::', -1)) as notification_uuid,
        coalesce(campaign_name, canvas_name) as campaign_name,
        min(sent_at) as sent_at
    from edw.consumer.fact_consumer_notification_engagement
    where is_valid_send = 1
      and notification_message_type_overall = 'MARKETING'
      and notification_channel = 'PUSH'
      and notification_source = 'Braze'
      and sent_at::date between dateadd('day', -1, $analysis_start_date) and $analysis_end_date
    group by 1, 2, 3
)

-- Join to attribution sources and pick the last sent notification as the blocker
select 
    c.*,
    c.capped_at_local::date as capped_day,
    -- FPN attribution
    f.notification_uuid as fpn_notification_uuid,
    f.campaign_name as fpn_campaign_name,
    f.sent_at as fpn_sent_at,
    -- Braze attribution
    b.notification_uuid as braze_notification_uuid,
    b.campaign_name as braze_campaign_name,
    b.sent_at as braze_sent_at
from program_capped_notifs c
-- FPN attribution - same local date
left join fpn_sent f
    on c.consumer_id = f.consumer_id
    and c.capped_at_local::date = convert_timezone('UTC', c.timezone, f.sent_at)::date
-- Braze attribution - same local date
left join braze_sent b
    on c.consumer_id = b.consumer_id
    and c.capped_at_local::date = convert_timezone('UTC', c.timezone, b.sent_at)::date
-- For each capped notification, get the latest blocking notification
qualify row_number() over (
    partition by c.id 
    order by coalesce(f.sent_at, '1900-01-01'), coalesce(b.sent_at, '1900-01-01') desc
) = 1;

-- Debug: Check if we have multiple capping events per program_id
/* 
select 
    program_id, 
    count(*) as total_events,
    count(distinct id) as distinct_ids
from program_capped_attribution
group by 1
order by 2 desc
limit 10;
*/

-- Step 4: Calculate capping metrics for each program by day
-- create or replace temporary table program_metrics as

create or replace table proddb.tylershields.nvg_notif_braze_capping_daily as 
with program_successful_sends as (
    -- Get total notifications successfully sent per program per day
    -- (these are only the ones that were actually delivered, not capped)
    select
        coalesce(canvas_id, campaign_id) as program_id,
        sent_at::date as day_date,
        count(distinct concat(consumer_id, sent_at_date, split_part(message_id, '::', -1))) as successful_sends
    from edw.consumer.fact_consumer_notification_engagement
    where is_valid_send = 1
      and notification_message_type_overall = 'MARKETING'
      and notification_channel = 'PUSH'
      and notification_source = 'Braze'
      and sent_at_date between $analysis_start_date and $analysis_end_date
    group by 1, 2
),

attribution_summary as (
    -- Attribution counts per program per day
    select
        program_id,
        team,
        ep_name,
        clean_campaign_name,
        program_name,
        program_type,
        capped_day,
        count(distinct id) as total_capped,
        count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is null 
              then id end) as fpn_attributed,
        count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is not null 
              then id end) as braze_attributed,
        count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is not null 
              then id end) as multi_attributed,
        count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is null 
              then id end) as unattributed
    from program_capped_attribution
    group by 1, 2, 3, 4, 5, 6, 7
);

-- Final output table
-- create or replace table proddb.tylershields.nvg_notif_braze_capping_daily as 
-- select * from program_metrics; 