-- Analyze notification metrics by days since last NV order - V2
-- Leveraging existing nvg_notif_base_data table from fact_nv_notifs.sql

-- Set default analysis window (last 30 days)
set window_start = dateadd('d', -30, current_date);
set window_end = dateadd('d', -3, current_date);

-- Create detailed metrics table using existing base data
create or replace table proddb.public.nvg_notif_metrics_by_days_since_order_v2 as
with notifications_with_days as (
    -- Select data for specific NV teams (NVG and CRM)
    select
        consumer_id,
        sent_at_date,
        team,
        notification_channel,
        days_since_order,
        -- Flag for counting events
        1 as notif_sent,
        case when engagement_within_24h = 1 then 1 else 0 end as opened,
        -- 1h event flags
        visit_within_1h,
        order_within_1h,
        nv_order_within_1h,
        nv_trial_within_1h,
        nv_retrial_within_1h,
        nv_trial_or_retrial_within_1h,
        -- 24h event flags
        visit_within_24h,
        order_within_24h,
        nv_order_within_24h,
        nv_trial_within_24h,
        nv_retrial_within_24h,
        nv_trial_or_retrial_within_24h
    from proddb.tylershields.nvg_notif_base_data
    where sent_at_date between $window_start and $window_end
    and team in ('NVG', 'CRM')
    
    union all
    
    -- Added "All NV" aggregation across NVG and CRM teams
    select
        consumer_id,
        sent_at_date,
        'All NV' as team,
        notification_channel,
        days_since_order,
        -- Flag for counting events
        1 as notif_sent,
        case when engagement_within_24h = 1 then 1 else 0 end as opened,
        -- 1h event flags
        visit_within_1h,
        order_within_1h,
        nv_order_within_1h,
        nv_trial_within_1h,
        nv_retrial_within_1h,
        nv_trial_or_retrial_within_1h,
        -- 24h event flags
        visit_within_24h,
        order_within_24h,
        nv_order_within_24h,
        nv_trial_within_24h,
        nv_retrial_within_24h,
        nv_trial_or_retrial_within_24h
    from proddb.tylershields.nvg_notif_base_data
    where sent_at_date between $window_start and $window_end
    and team in ('NVG', 'CRM')
    
    union all
    
    -- All other teams
    select
        consumer_id,
        sent_at_date,
        'All Others' as team,
        notification_channel,
        days_since_order,
        -- Flag for counting events
        1 as notif_sent,
        case when engagement_within_24h = 1 then 1 else 0 end as opened,
        -- 1h event flags
        visit_within_1h,
        order_within_1h,
        nv_order_within_1h,
        nv_trial_within_1h,
        nv_retrial_within_1h,
        nv_trial_or_retrial_within_1h,
        -- 24h event flags
        visit_within_24h,
        order_within_24h,
        nv_order_within_24h,
        nv_trial_within_24h,
        nv_retrial_within_24h,
        nv_trial_or_retrial_within_24h
    from proddb.tylershields.nvg_notif_base_data
    where sent_at_date between $window_start and $window_end
    and team not in ('NVG', 'CRM')
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
        sum(visit_within_1h) as visits_1h,
        sum(order_within_1h) as orders_1h,
        sum(nv_order_within_1h) as nv_orders_1h,
        sum(nv_trial_within_1h) as nv_trials_1h,
        sum(nv_retrial_within_1h) as nv_retrials_1h,
        sum(nv_trial_or_retrial_within_1h) as nv_trial_or_retrials_1h,
        -- 24h event counts
        sum(visit_within_24h) as visits_24h,
        sum(order_within_24h) as orders_24h,
        sum(nv_order_within_24h) as nv_orders_24h,
        sum(nv_trial_within_24h) as nv_trials_24h,
        sum(nv_retrial_within_24h) as nv_retrials_24h,
        sum(nv_trial_or_retrial_within_24h) as nv_trial_or_retrials_24h,
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

-- -- Create bucketed version table as a separate statement
-- create or replace table proddb.public.nvg_notif_metrics_by_days_since_order_bucketed_v2 as
-- with bucketed_data as (
--     select
--         team,
--         notification_channel,
--         case
--             when days_since_order = -1 then 'Never ordered'
--             when days_since_order = 366 then 'Over 1 year'
--             when days_since_order between 0 and 7 then '0-7 days'
--             when days_since_order between 8 and 14 then '8-14 days'
--             when days_since_order between 15 and 30 then '15-30 days'
--             when days_since_order between 31 and 60 then '31-60 days'
--             when days_since_order between 61 and 90 then '61-90 days'
--             when days_since_order between 91 and 180 then '91-180 days'
--             when days_since_order between 181 and 365 then '181-365 days'
--             else 'Unknown'
--         end as days_since_order_bucket,
        
--         notifications_sent,
--         notifications_opened,
--         -- 1h metrics
--         visits_1h,
--         orders_1h,
--         nv_orders_1h,
--         nv_trials_1h,
--         nv_retrials_1h,
--         nv_trial_or_retrials_1h,
--         -- 24h metrics
--         visits_24h,
--         orders_24h,
--         nv_orders_24h,
--         nv_trials_24h,
--         nv_retrials_24h,
--         nv_trial_or_retrials_24h,
--         unique_consumers
--     from proddb.public.nvg_notif_metrics_by_days_since_order_v2
-- ),

-- -- Pre-aggregate metrics by bucket
-- aggregated_buckets as (
--     select
--         team,
--         notification_channel,
--         days_since_order_bucket,
        
--         sum(notifications_sent) as notifications_sent,
--         sum(notifications_opened) as notifications_opened,
--         -- 1h metrics
--         sum(visits_1h) as visits_1h,
--         sum(orders_1h) as orders_1h,
--         sum(nv_orders_1h) as nv_orders_1h,
--         sum(nv_trials_1h) as nv_trials_1h,
--         sum(nv_retrials_1h) as nv_retrials_1h,
--         sum(nv_trial_or_retrials_1h) as nv_trial_or_retrials_1h,
--         -- 24h metrics
--         sum(visits_24h) as visits_24h,
--         sum(orders_24h) as orders_24h,
--         sum(nv_orders_24h) as nv_orders_24h,
--         sum(nv_trials_24h) as nv_trials_24h,
--         sum(nv_retrials_24h) as nv_retrials_24h,
--         sum(nv_trial_or_retrials_24h) as nv_trial_or_retrials_24h,
--         sum(unique_consumers) as unique_consumers
--     from bucketed_data
--     group by team, notification_channel, days_since_order_bucket
-- )

-- -- Final bucketed metrics
-- select
--     team,
--     notification_channel,
--     days_since_order_bucket,
    
--     -- Raw counts
--     notifications_sent,
--     notifications_opened,
--     visits_1h,
--     orders_1h,
--     nv_orders_1h,
--     nv_trials_1h,
--     nv_retrials_1h,
--     nv_trial_or_retrials_1h,
--     visits_24h,
--     orders_24h,
--     nv_orders_24h,
--     nv_trials_24h,
--     nv_retrials_24h,
--     nv_trial_or_retrials_24h,
--     unique_consumers,
    
--     -- 1h rate metrics
--     case when notifications_sent > 0 then (notifications_opened::decimal * 100.0 / notifications_sent) else 0 end as open_rate,
--     case when notifications_sent > 0 then (visits_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_visit_rate_1h,
--     case when notifications_sent > 0 then (orders_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_order_rate_1h,
--     case when notifications_sent > 0 then (nv_orders_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_order_rate_1h,
--     case when notifications_sent > 0 then (nv_trials_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_rate_1h,
--     case when notifications_sent > 0 then (nv_retrials_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_retrial_rate_1h,
--     case when notifications_sent > 0 then (nv_trial_or_retrials_1h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_or_retrial_rate_1h,
    
--     -- 1h open-to-conversion metrics
--     case when notifications_opened > 0 then (visits_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_visit_rate_1h,
--     case when notifications_opened > 0 then (orders_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_order_rate_1h,
--     case when notifications_opened > 0 then (nv_orders_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_order_rate_1h,
--     case when notifications_opened > 0 then (nv_trials_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_rate_1h,
--     case when notifications_opened > 0 then (nv_retrials_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_retrial_rate_1h,
--     case when notifications_opened > 0 then (nv_trial_or_retrials_1h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_or_retrial_rate_1h,
    
--     -- 24h rate metrics
--     case when notifications_sent > 0 then (visits_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_visit_rate_24h,
--     case when notifications_sent > 0 then (orders_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_order_rate_24h,
--     case when notifications_sent > 0 then (nv_orders_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_order_rate_24h,
--     case when notifications_sent > 0 then (nv_trials_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_rate_24h,
--     case when notifications_sent > 0 then (nv_retrials_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_retrial_rate_24h,
--     case when notifications_sent > 0 then (nv_trial_or_retrials_24h::decimal * 100.0 / notifications_sent) else 0 end as sent_to_nv_trial_or_retrial_rate_24h,
    
--     -- 24h open-to-conversion metrics
--     case when notifications_opened > 0 then (visits_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_visit_rate_24h,
--     case when notifications_opened > 0 then (orders_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_order_rate_24h,
--     case when notifications_opened > 0 then (nv_orders_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_order_rate_24h,
--     case when notifications_opened > 0 then (nv_trials_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_rate_24h,
--     case when notifications_opened > 0 then (nv_retrials_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_retrial_rate_24h,
--     case when notifications_opened > 0 then (nv_trial_or_retrials_24h::decimal * 100.0 / notifications_opened) else 0 end as open_to_nv_trial_or_retrial_rate_24h
    
-- from aggregated_buckets
-- order by 
--     case 
--         when days_since_order_bucket = 'Never ordered' then 0
--         when days_since_order_bucket = '0-7 days' then 1
--         when days_since_order_bucket = '8-14 days' then 2
--         when days_since_order_bucket = '15-30 days' then 3
--         when days_since_order_bucket = '31-60 days' then 4
--         when days_since_order_bucket = '61-90 days' then 5
--         when days_since_order_bucket = '91-180 days' then 6
--         when days_since_order_bucket = '181-365 days' then 7
--         when days_since_order_bucket = 'Over 1 year' then 8
--         else 9
--     end; 