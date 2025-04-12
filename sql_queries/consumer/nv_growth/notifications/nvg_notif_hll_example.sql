-- Example of using HyperLogLog (HLL) for distinct count approximation across dimensions
-- This allows for flexible aggregation without pre-calculating all dimension combinations

-- Set the analysis window (default to last 30 days if not specified)
set window_start = dateadd('d', -30, current_date);
set window_end = current_date;

-- Step 1: Create base data with HLL objects and notification counts at the most granular level
create or replace temporary table notif_hll_base as
select 
    sent_at_date,
    team,
    notification_channel,
    coalesce(nv_cx_growth_state, 'unknown') as nv_cx_growth_state,
    -- Create HLL object for consumer_id at most granular level
    HLL(consumer_id) as consumer_hll,
    -- Count notifications (for notifications per consumer ratios)
    count(*) as notification_count
from (
    select 
        e.consumer_id,
        e.sent_at_date,
        e.notification_channel,
        -- Check if this notification is in our NV index
        case 
            when idx.team is not null then idx.team
            else 'All Others'
        end as team,
        n.nv_cx_growth_state
    from edw.consumer.fact_consumer_notification_engagement e
    left join proddb.public.nv_channels_notif_index idx
        on coalesce(e.campaign_id, e.canvas_id) = coalesce(idx.campaign_id, idx.canvas_id)
    left join metrics_repo.public.nv_cx_growth_state n
        on e.consumer_id = n.consumer_id 
        and e.sent_at_date = n.event_ts::DATE
    where e.sent_at_date between $window_start and $window_end
    and e.notification_channel in ('PUSH', 'EMAIL')
    and (e.notification_message_type is null or e.notification_message_type != 'Transactional')
    and e.is_valid_send = 1
)
group by 1, 2, 3, 4;

-- Step 2: Create reusable table with HLL objects that can be used for flexible querying
create or replace table proddb.tylershields.nvg_notif_hll_objects as
select * from notif_hll_base;

-- Step 3: Example queries using HLL objects for different dimension combinations

-- Example 1: Daily notifications by team and channel (2 dimensions)
select 
    sent_at_date,
    team,
    notification_channel,
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(notification_count) / HLL_ESTIMATE(consumer_hll) as notifs_per_consumer
from proddb.tylershields.nvg_notif_hll_objects
group by 1, 2, 3
order by 1 desc, 2, 3;

-- Example 2: Daily notifications by team (1 dimension)
select 
    sent_at_date,
    team,
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(notification_count) / HLL_ESTIMATE(consumer_hll) as notifs_per_consumer
from proddb.tylershields.nvg_notif_hll_objects
group by 1, 2
order by 1 desc, 2;

-- Example 3: Daily notifications by growth state (1 dimension)
select 
    sent_at_date,
    nv_cx_growth_state,
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(notification_count) / HLL_ESTIMATE(consumer_hll) as notifs_per_consumer
from proddb.tylershields.nvg_notif_hll_objects
group by 1, 2
order by 1 desc, 2;

-- Example 4: Cross-dimension aggregation - total consumers per team across the entire period
select 
    team,
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(notification_count) / HLL_ESTIMATE(consumer_hll) as notifs_per_consumer
from proddb.tylershields.nvg_notif_hll_objects
group by 1
order by 2 desc;

-- Example 5: Comparing team overlaps using HLL_COMBINE and HLL_INTERSECT
-- This shows how many consumers received notifications from both NV teams and others
with team_hlls as (
    select 
        case when team = 'All Others' then 'Others' else 'NV Teams' end as team_group,
        HLL_COMBINE(consumer_hll) as team_consumer_hll,
        sum(notification_count) as notifications_sent
    from proddb.tylershields.nvg_notif_hll_objects
    group by 1
)
select 
    'Consumers who received notifications from NV Teams' as metric,
    HLL_ESTIMATE(team_consumer_hll) as distinct_consumers,
    notifications_sent,
    notifications_sent / HLL_ESTIMATE(team_consumer_hll) as notifs_per_consumer
from team_hlls
where team_group = 'NV Teams'

union all

select 
    'Consumers who received notifications from Others' as metric,
    HLL_ESTIMATE(team_consumer_hll) as distinct_consumers,
    notifications_sent,
    notifications_sent / HLL_ESTIMATE(team_consumer_hll) as notifs_per_consumer
from team_hlls
where team_group = 'Others'

union all

select
    'Consumers who received notifications from both NV Teams and Others' as metric,
    HLL_ESTIMATE(HLL_INTERSECT(
        (select team_consumer_hll from team_hlls where team_group = 'NV Teams'),
        (select team_consumer_hll from team_hlls where team_group = 'Others')
    )) as distinct_consumers,
    (select sum(notifications_sent) from team_hlls) as notifications_sent,
    (select sum(notifications_sent) from team_hlls) / 
    HLL_ESTIMATE(HLL_INTERSECT(
        (select team_consumer_hll from team_hlls where team_group = 'NV Teams'),
        (select team_consumer_hll from team_hlls where team_group = 'Others')
    )) as notifs_per_consumer;

-- Example 6: Create an on-the-fly cube for all dimension combinations
-- This is the power of HLL - we can create any aggregation without pre-computing everything
select 
    -- Use GROUPING() to identify which dimensions are aggregated
    case when grouping(sent_at_date) = 0 then to_varchar(sent_at_date) else 'All Dates' end as date_dim,
    case when grouping(team) = 0 then team else 'All Teams' end as team_dim,
    case when grouping(notification_channel) = 0 then notification_channel else 'All Channels' end as channel_dim,
    case when grouping(nv_cx_growth_state) = 0 then nv_cx_growth_state else 'All States' end as state_dim,
    
    -- Dimension flags for filtering
    grouping(sent_at_date) as is_date_aggregated,
    grouping(team) as is_team_aggregated,
    grouping(notification_channel) as is_channel_aggregated,
    grouping(nv_cx_growth_state) as is_state_aggregated,
    
    -- Metrics with HLL for distinct consumers
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(notification_count) / nullif(HLL_ESTIMATE(consumer_hll), 0) as notifs_per_consumer
from proddb.tylershields.nvg_notif_hll_objects
-- Create a cube with all possible aggregation combinations
group by cube(sent_at_date, team, notification_channel, nv_cx_growth_state)
-- Filter out the "all dimensions aggregated" row (optional)
having not (is_date_aggregated = 1 and is_team_aggregated = 1 and is_channel_aggregated = 1 and is_state_aggregated = 1)
order by sent_at_date desc nulls last, team, notification_channel, nv_cx_growth_state;

-- Example 7: Weekly trend of notifications per consumer by team
select 
    date_trunc('week', sent_at_date)::date as week_start,
    team,
    HLL_ESTIMATE(consumer_hll) as distinct_consumers,
    sum(notification_count) as notifications_sent,
    sum(notification_count) / nullif(HLL_ESTIMATE(consumer_hll), 0) as notifs_per_consumer
from proddb.tylershields.nvg_notif_hll_objects
group by 1, 2
order by 1 desc, 2; 