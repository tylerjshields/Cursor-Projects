-- Analyze notification metrics by days since last NV order
-- Reference tables from nvg_notif_metrics_multi_channel.sql

-- Set default analysis window (last 30 days)
set window_start = dateadd('d', -30, current_date);
set window_end = current_date;

-- Create table with all consumer NV orders by date
-- This will allow us to calculate days_since_order relative to each notification's sent date
create temporary table consumer_nv_orders as
select 
    d.creator_id as consumer_id,
    d.active_date_utc as order_date
from edw.finance.dimension_deliveries d
join edw.cng.dimension_new_vertical_store_tags nv 
    on d.store_id = nv.store_id and is_filtered_mp_vertical
where d.is_filtered_core = true
and d.active_date_utc <= current_date;

-- Create main detailed metrics table
create or replace table proddb.public.nvg_notif_metrics_by_days_since_order as
with notifications_with_days as (
    -- Original data with specific NV teams (NVG and CRM)
    select
        n.consumer_id,
        n.sent_at_date,
        n.team,
        n.notification_channel,
        -- Calculate days_since_order by finding the most recent order before the notification was sent
        case
            when max_order_date is null then -1 -- Never ordered
            when datediff('day', max_order_date, n.sent_at_date) > 365 then 366 -- Over a year
            else datediff('day', max_order_date, n.sent_at_date) -- Days since order as of notification date
        end as days_since_order,
        
        -- Flag for counting events
        1 as notif_sent,
        case when n.engagement_at is not null then 1 else 0 end as opened,
        -- 1h event flags
        case when n.visit_within_1h = 1 then 1 else 0 end as visited_1h,
        case when n.order_within_1h = 1 then 1 else 0 end as ordered_1h,
        case when n.nv_order_within_1h = 1 then 1 else 0 end as nv_ordered_1h,
        case when n.nv_trial_within_1h = 1 then 1 else 0 end as nv_trial_1h,
        case when n.nv_retrial_within_1h = 1 then 1 else 0 end as nv_retrial_1h,
        case when n.nv_trial_or_retrial_within_1h = 1 then 1 else 0 end as nv_trial_or_retrial_1h,
        -- 24h event flags
        case when n.visit_within_24h = 1 then 1 else 0 end as visited_24h,
        case when n.order_within_24h = 1 then 1 else 0 end as ordered_24h,
        case when n.nv_order_within_24h = 1 then 1 else 0 end as nv_ordered_24h,
        case when n.nv_trial_within_24h = 1 then 1 else 0 end as nv_trial_24h,
        case when n.nv_retrial_within_24h = 1 then 1 else 0 end as nv_retrial_24h,
        case when n.nv_trial_or_retrial_within_24h = 1 then 1 else 0 end as nv_trial_or_retrial_24h
    from proddb.public.nvg_notif_metrics_base_multi_channel n
    left join (
        -- Subquery to get the most recent order date BEFORE the notification was sent
        select 
            c.consumer_id,
            n.sent_at_date,
            max(c.order_date) as max_order_date
        from proddb.public.nvg_notif_metrics_base_multi_channel n
        left join consumer_nv_orders c
            on n.consumer_id = c.consumer_id
            and c.order_date < n.sent_at_date  -- Only consider orders BEFORE notification
        where n.sent_at_date between $window_start and $window_end
        group by c.consumer_id, n.sent_at_date
    ) o on n.consumer_id = o.consumer_id and n.sent_at_date = o.sent_at_date
    where n.sent_at_date between $window_start and $window_end
    and n.team in ('NVG', 'CRM')  -- Include only NVG and CRM teams
    
    union all
    
    -- Added "All NV" aggregation across NVG and CRM teams
    select
        n.consumer_id,
        n.sent_at_date,
        'All NV' as team,  -- New team grouping for all NV notifications
        n.notification_channel,
        -- Calculate days_since_order by finding the most recent order before the notification was sent
        case
            when max_order_date is null then -1 -- Never ordered
            when datediff('day', max_order_date, n.sent_at_date) > 365 then 366 -- Over a year
            else datediff('day', max_order_date, n.sent_at_date) -- Days since order as of notification date
        end as days_since_order,
        
        -- Flag for counting events
        1 as notif_sent,
        case when n.engagement_at is not null then 1 else 0 end as opened,
        -- 1h event flags
        case when n.visit_within_1h = 1 then 1 else 0 end as visited_1h,
        case when n.order_within_1h = 1 then 1 else 0 end as ordered_1h,
        case when n.nv_order_within_1h = 1 then 1 else 0 end as nv_ordered_1h,
        case when n.nv_trial_within_1h = 1 then 1 else 0 end as nv_trial_1h,
        case when n.nv_retrial_within_1h = 1 then 1 else 0 end as nv_retrial_1h,
        case when n.nv_trial_or_retrial_within_1h = 1 then 1 else 0 end as nv_trial_or_retrial_1h,
        -- 24h event flags
        case when n.visit_within_24h = 1 then 1 else 0 end as visited_24h,
        case when n.order_within_24h = 1 then 1 else 0 end as ordered_24h,
        case when n.nv_order_within_24h = 1 then 1 else 0 end as nv_ordered_24h,
        case when n.nv_trial_within_24h = 1 then 1 else 0 end as nv_trial_24h,
        case when n.nv_retrial_within_24h = 1 then 1 else 0 end as nv_retrial_24h,
        case when n.nv_trial_or_retrial_within_24h = 1 then 1 else 0 end as nv_trial_or_retrial_24h
    from proddb.public.nvg_notif_metrics_base_multi_channel n
    left join (
        -- Subquery to get the most recent order date BEFORE the notification was sent
        select 
            c.consumer_id,
            n.sent_at_date,
            max(c.order_date) as max_order_date
        from proddb.public.nvg_notif_metrics_base_multi_channel n
        left join consumer_nv_orders c
            on n.consumer_id = c.consumer_id
            and c.order_date < n.sent_at_date  -- Only consider orders BEFORE notification
        where n.sent_at_date between $window_start and $window_end
        group by c.consumer_id, n.sent_at_date
    ) o on n.consumer_id = o.consumer_id and n.sent_at_date = o.sent_at_date
    where n.sent_at_date between $window_start and $window_end
    and n.team in ('NVG', 'CRM')  -- Include only NVG and CRM teams for the "All NV" grouping
    
    union all
    
    -- All other teams
    select
        n.consumer_id,
        n.sent_at_date,
        'All Others' as team,  -- Group all non-NV teams as "All Others"
        n.notification_channel,
        -- Calculate days_since_order by finding the most recent order before the notification was sent
        case
            when max_order_date is null then -1 -- Never ordered
            when datediff('day', max_order_date, n.sent_at_date) > 365 then 366 -- Over a year
            else datediff('day', max_order_date, n.sent_at_date) -- Days since order as of notification date
        end as days_since_order,
        
        -- Flag for counting events
        1 as notif_sent,
        case when n.engagement_at is not null then 1 else 0 end as opened,
        -- 1h event flags
        case when n.visit_within_1h = 1 then 1 else 0 end as visited_1h,
        case when n.order_within_1h = 1 then 1 else 0 end as ordered_1h,
        case when n.nv_order_within_1h = 1 then 1 else 0 end as nv_ordered_1h,
        case when n.nv_trial_within_1h = 1 then 1 else 0 end as nv_trial_1h,
        case when n.nv_retrial_within_1h = 1 then 1 else 0 end as nv_retrial_1h,
        case when n.nv_trial_or_retrial_within_1h = 1 then 1 else 0 end as nv_trial_or_retrial_1h,
        -- 24h event flags
        case when n.visit_within_24h = 1 then 1 else 0 end as visited_24h,
        case when n.order_within_24h = 1 then 1 else 0 end as ordered_24h,
        case when n.nv_order_within_24h = 1 then 1 else 0 end as nv_ordered_24h,
        case when n.nv_trial_within_24h = 1 then 1 else 0 end as nv_trial_24h,
        case when n.nv_retrial_within_24h = 1 then 1 else 0 end as nv_retrial_24h,
        case when n.nv_trial_or_retrial_within_24h = 1 then 1 else 0 end as nv_trial_or_retrial_24h
    from proddb.public.nvg_notif_metrics_base_multi_channel n
    left join (
        -- Subquery to get the most recent order date BEFORE the notification was sent
        select 
            c.consumer_id,
            n.sent_at_date,
            max(c.order_date) as max_order_date
        from proddb.public.nvg_notif_metrics_base_multi_channel n
        left join consumer_nv_orders c
            on n.consumer_id = c.consumer_id
            and c.order_date < n.sent_at_date  -- Only consider orders BEFORE notification
        where n.sent_at_date between $window_start and $window_end
        group by c.consumer_id, n.sent_at_date
    ) o on n.consumer_id = o.consumer_id and n.sent_at_date = o.sent_at_date
    where n.sent_at_date between $window_start and $window_end
    and n.team not in ('NVG', 'CRM')  -- Include all teams except NVG and CRM
),

-- Pre-aggregate metrics by the desired dimensions
aggregated_metrics as (
    select
        team,
        notification_channel,
        days_since_order,
        
        -- Counts of events
        count(*) as notifications_sent,
        sum(opened) as notifications_opened,
        -- 1h event counts
        sum(visited_1h) as visits_1h,
        sum(ordered_1h) as orders_1h,
        sum(nv_ordered_1h) as nv_orders_1h,
        sum(nv_trial_1h) as nv_trials_1h,
        sum(nv_retrial_1h) as nv_retrials_1h,
        sum(nv_trial_or_retrial_1h) as nv_trial_or_retrials_1h,
        -- 24h event counts
        sum(visited_24h) as visits_24h,
        sum(ordered_24h) as orders_24h,
        sum(nv_ordered_24h) as nv_orders_24h,
        sum(nv_trial_24h) as nv_trials_24h,
        sum(nv_retrial_24h) as nv_retrials_24h,
        sum(nv_trial_or_retrial_24h) as nv_trial_or_retrials_24h,
        count(distinct consumer_id) as unique_consumers
    from notifications_with_days
    group by team, notification_channel, days_since_order
)

-- Calculate metrics properly with correct aggregation
select
    team,
    notification_channel,
    days_since_order,
    
    -- Event counts
    notifications_sent,
    notifications_opened,
    -- 1h counts
    visits_1h,
    orders_1h,
    nv_orders_1h,
    nv_trials_1h,
    nv_retrials_1h,
    nv_trial_or_retrials_1h,
    -- 24h counts
    visits_24h,
    orders_24h,
    nv_orders_24h,
    nv_trials_24h,
    nv_retrials_24h,
    nv_trial_or_retrials_24h,
    unique_consumers,
    
    -- Rate metrics with proper decimal division - 1h timeframe
    cast(notifications_opened as decimal(18,4)) / nullif(notifications_sent, 0) as open_rate,
    cast(visits_1h as decimal(18,4)) / nullif(notifications_sent, 0) as visit_rate_1h,
    cast(orders_1h as decimal(18,4)) / nullif(notifications_sent, 0) as order_rate_1h,
    cast(nv_orders_1h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_order_rate_1h,
    cast(nv_orders_1h as decimal(18,4)) / nullif(orders_1h, 0) as pct_orders_nv_1h,
    cast(nv_trials_1h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_trial_rate_1h,
    cast(nv_retrials_1h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_retrial_rate_1h,
    cast(nv_trial_or_retrials_1h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_trial_or_retrial_rate_1h,
    
    -- Rate metrics with proper decimal division - 24h timeframe
    cast(visits_24h as decimal(18,4)) / nullif(notifications_sent, 0) as visit_rate_24h,
    cast(orders_24h as decimal(18,4)) / nullif(notifications_sent, 0) as order_rate_24h,
    cast(nv_orders_24h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_order_rate_24h,
    cast(nv_orders_24h as decimal(18,4)) / nullif(orders_24h, 0) as pct_orders_nv_24h,
    cast(nv_trials_24h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_trial_rate_24h,
    cast(nv_retrials_24h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_retrial_rate_24h,
    cast(nv_trial_or_retrials_24h as decimal(18,4)) / nullif(notifications_sent, 0) as nv_trial_or_retrial_rate_24h,
    
    -- Open-to-conversion metrics - 1h
    cast(visits_1h as decimal(18,4)) / nullif(notifications_opened, 0) as open_to_visit_rate_1h,
    cast(orders_1h as decimal(18,4)) / nullif(notifications_opened, 0) as open_to_order_rate_1h,
    cast(nv_orders_1h as decimal(18,4)) / nullif(notifications_opened, 0) as open_to_nv_order_rate_1h,
    
    -- Open-to-conversion metrics - 24h
    cast(visits_24h as decimal(18,4)) / nullif(notifications_opened, 0) as open_to_visit_rate_24h,
    cast(orders_24h as decimal(18,4)) / nullif(notifications_opened, 0) as open_to_order_rate_24h,
    cast(nv_orders_24h as decimal(18,4)) / nullif(notifications_opened, 0) as open_to_nv_order_rate_24h,
    
    -- Visit-to-conversion metrics - 1h
    cast(orders_1h as decimal(18,4)) / nullif(visits_1h, 0) as visit_to_order_rate_1h,
    cast(nv_orders_1h as decimal(18,4)) / nullif(visits_1h, 0) as visit_to_nv_order_rate_1h,
    cast(nv_trials_1h as decimal(18,4)) / nullif(visits_1h, 0) as visit_to_nv_trial_rate_1h,
    cast(nv_retrials_1h as decimal(18,4)) / nullif(visits_1h, 0) as visit_to_nv_retrial_rate_1h,
    cast(nv_trial_or_retrials_1h as decimal(18,4)) / nullif(visits_1h, 0) as visit_to_nv_trial_or_retrial_rate_1h,
    
    -- Visit-to-conversion metrics - 24h
    cast(orders_24h as decimal(18,4)) / nullif(visits_24h, 0) as visit_to_order_rate_24h,
    cast(nv_orders_24h as decimal(18,4)) / nullif(visits_24h, 0) as visit_to_nv_order_rate_24h,
    cast(nv_trials_24h as decimal(18,4)) / nullif(visits_24h, 0) as visit_to_nv_trial_rate_24h,
    cast(nv_retrials_24h as decimal(18,4)) / nullif(visits_24h, 0) as visit_to_nv_retrial_rate_24h,
    cast(nv_trial_or_retrials_24h as decimal(18,4)) / nullif(visits_24h, 0) as visit_to_nv_trial_or_retrial_rate_24h,
    
    -- Notifications per consumer
    cast(notifications_sent as decimal(18,4)) / nullif(unique_consumers, 0) as notifs_per_consumer
from aggregated_metrics
order by 
    case 
        when team = 'All NV' then '1' -- Put "All NV" first
        when team = 'All Others' then '3' -- Put "All Others" last
        else '2_' || team -- Sort other teams alphabetically
    end,
    notification_channel, 
    days_since_order;

-- Create bucketed version table as a separate statement
create or replace table proddb.public.nvg_notif_metrics_by_days_since_order_bucketed as
with bucketed_data as (
    select
        team,
        notification_channel,
        case
            when days_since_order = -1 then 'Never ordered'
            when days_since_order = 366 then 'Over 1 year'
            when days_since_order between 0 and 7 then '0-7 days'
            when days_since_order between 8 and 14 then '8-14 days'
            when days_since_order between 15 and 30 then '15-30 days'
            when days_since_order between 31 and 60 then '31-60 days'
            when days_since_order between 61 and 90 then '61-90 days'
            when days_since_order between 91 and 180 then '91-180 days'
            when days_since_order between 181 and 365 then '181-365 days'
            else 'Unknown'
        end as days_since_order_bucket,
        
        notifications_sent,
        notifications_opened,
        -- 1h metrics
        visits_1h,
        orders_1h,
        nv_orders_1h,
        nv_trials_1h,
        nv_retrials_1h,
        nv_trial_or_retrials_1h,
        -- 24h metrics
        visits_24h,
        orders_24h,
        nv_orders_24h,
        nv_trials_24h,
        nv_retrials_24h,
        nv_trial_or_retrials_24h,
        unique_consumers
    from proddb.public.nvg_notif_metrics_by_days_since_order
),

-- Pre-aggregate metrics by bucket
aggregated_buckets as (
    select
        team,
        notification_channel,
        days_since_order_bucket,
        
        sum(notifications_sent) as notifications_sent,
        sum(notifications_opened) as notifications_opened,
        -- 1h metrics
        sum(visits_1h) as visits_1h,
        sum(orders_1h) as orders_1h,
        sum(nv_orders_1h) as nv_orders_1h,
        sum(nv_trials_1h) as nv_trials_1h,
        sum(nv_retrials_1h) as nv_retrials_1h,
        sum(nv_trial_or_retrials_1h) as nv_trial_or_retrials_1h,
        -- 24h metrics
        sum(visits_24h) as visits_24h,
        sum(orders_24h) as orders_24h,
        sum(nv_orders_24h) as nv_orders_24h,
        sum(nv_trials_24h) as nv_trials_24h,
        sum(nv_retrials_24h) as nv_retrials_24h,
        sum(nv_trial_or_retrials_24h) as nv_trial_or_retrials_24h,
        sum(unique_consumers) as unique_consumers
    from bucketed_data
    group by team, notification_channel, days_since_order_bucket
)

-- Final bucketed metrics
select
    team,
    notification_channel,
    days_since_order_bucket,
    
    -- Raw counts
    notifications_sent,
    notifications_opened,
    visits_1h,
    orders_1h,
    nv_orders_1h,
    nv_trials_1h,
    nv_retrials_1h,
    nv_trial_or_retrials_1h,
    visits_24h,
    orders_24h,
    nv_orders_24h,
    nv_trials_24h,
    nv_retrials_24h,
    nv_trial_or_retrials_24h,
    unique_consumers,
    
    -- 1h rate metrics
    case when notifications_sent > 0 then (notifications_opened::decimal * 100.0 / notifications_sent) else 0 end as open_rate,
    case when notifications_sent > 0 then (visits_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_visit_rate_1h,
    case when notifications_sent > 0 then (orders_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_order_rate_1h,
    case when notifications_sent > 0 then (nv_orders_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_order_rate_1h,
    case when notifications_sent > 0 then (nv_trials_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_rate_1h,
    case when notifications_sent > 0 then (nv_retrials_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_retrial_rate_1h,
    case when notifications_sent > 0 then (nv_trial_or_retrials_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_or_retrial_rate_1h,
    
    -- 1h open-to-conversion metrics
    case when notifications_opened > 0 then (visits_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_visit_rate_1h,
    case when notifications_opened > 0 then (orders_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_order_rate_1h,
    case when notifications_opened > 0 then (nv_orders_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_order_rate_1h,
    case when notifications_opened > 0 then (nv_trials_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_rate_1h,
    case when notifications_opened > 0 then (nv_retrials_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_retrial_rate_1h,
    case when notifications_opened > 0 then (nv_trial_or_retrials_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_or_retrial_rate_1h,
    
    -- 24h rate metrics
    case when notifications_sent > 0 then (visits_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_visit_rate_24h,
    case when notifications_sent > 0 then (orders_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_order_rate_24h,
    case when notifications_sent > 0 then (nv_orders_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_order_rate_24h,
    case when notifications_sent > 0 then (nv_trials_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_rate_24h,
    case when notifications_sent > 0 then (nv_retrials_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_retrial_rate_24h,
    case when notifications_sent > 0 then (nv_trial_or_retrials_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_or_retrial_rate_24h,
    
    -- 24h open-to-conversion metrics
    case when notifications_opened > 0 then (visits_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_visit_rate_24h,
    case when notifications_opened > 0 then (orders_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_order_rate_24h,
    case when notifications_opened > 0 then (nv_orders_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_order_rate_24h,
    case when notifications_opened > 0 then (nv_trials_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_rate_24h,
    case when notifications_opened > 0 then (nv_retrials_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_retrial_rate_24h,
    case when notifications_opened > 0 then (nv_trial_or_retrials_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_or_retrial_rate_24h
    
from aggregated_buckets
order by 
    case 
        when days_since_order_bucket = 'Never ordered' then 0
        when days_since_order_bucket = '0-7 days' then 1
        when days_since_order_bucket = '8-14 days' then 2
        when days_since_order_bucket = '15-30 days' then 3
        when days_since_order_bucket = '31-60 days' then 4
        when days_since_order_bucket = '61-90 days' then 5
        when days_since_order_bucket = '91-180 days' then 6
        when days_since_order_bucket = '181-365 days' then 7
        when days_since_order_bucket = 'Over 1 year' then 8
        else 9
    end; 