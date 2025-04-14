-- Dimension table for NV notification users with growth states and reachability
set window_start = current_date - 30;
set window_end = current_date;

-- Create clean dimension table at consumer grain with growth states and reachability
create or replace table proddb.tylershields.dimension_nv_notif_users as
with daily_growth_states as (
    -- Get daily growth state for each consumer
    select 
        d.ds,
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
        and vertical = 'core_dd_utc'
),

push_reachability_by_message_type as (
    -- Get push reachability by message type for each date
    select 
        r.date as ds,
        r.consumer_id,
        r.message_type,
        max(r.reached_28d) as is_reachable
    from edw.consumer.dimension_consumer_notification_reach r
    where r.date between $window_start and $window_end
    and r.channel = 'PUSH'
    and r.message_type != 'TRANSACTIONAL'  -- Exclude transactional notifications
    group by r.date, r.consumer_id, r.message_type
),

push_reachability_overall as (
    -- Get overall push reachability (any non-transactional message type) for each date
    select
        date as ds,
        consumer_id,
        'Overall' as message_type,
        max(reached_28d) as is_reachable
    from edw.consumer.dimension_consumer_notification_reach
    where date between $window_start and $window_end
    and channel = 'PUSH'
    and message_type != 'TRANSACTIONAL'  -- Exclude transactional notifications
    group by ds, consumer_id
),

push_reachability_combined as (
    -- Combine specific message types and overall
    select * from push_reachability_by_message_type
    union all
    select * from push_reachability_overall
),

email_reachability as (
    -- Get email reachability information by date
    select 
        date as ds,
        consumer_id,
        max(reached_28d) as is_reachable_email
    from edw.consumer.dimension_consumer_notification_reach
    where date between $window_start and $window_end
    and channel = 'EMAIL'
    and message_type != 'TRANSACTIONAL'  -- Exclude transactional notifications
    group by ds, consumer_id
),

-- Create a pivot to convert message types to columns for each day
push_reachability_pivot as (
    select
        ds,
        consumer_id,
        max(case when message_type = 'Overall' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_overall,
        max(case when message_type = 'RECOMMENDATIONS' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_recommendations,
        max(case when message_type = 'REMINDERS' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_reminders,
        max(case when message_type = 'DOORDASH_OFFERS' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_doordash_offers,
        max(case when message_type = 'STORE_OFFERS' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_store_offers,
        max(case when message_type = 'SPECIAL_OFFERS' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_special_offers,
        max(case when message_type not in ('Overall', 'RECOMMENDATIONS', 'REMINDERS', 'DOORDASH_OFFERS', 'STORE_OFFERS', 'SPECIAL_OFFERS', 'TRANSACTIONAL') 
                 and is_reachable = 1 then 1 else 0 end) as is_reachable_push_other
    from push_reachability_combined
    group by ds, consumer_id
),

-- Get last NV order dates and days since last order
last_order_data as (
    select
        ds,
        consumer_id,
        last_nv_order_ds,
        -- Calculate days since order for each consumer and date
        case
            when last_nv_order_ds is null then -1 
            when datediff('day', last_nv_order_ds, ds) > 365 then 366 
            else datediff('day', last_nv_order_ds, ds) 
        end as days_since_order,
        -- Create bucketed version
        case
            when last_nv_order_ds is null then 'Never ordered'
            when datediff('day', last_nv_order_ds, ds) > 365 then 'Over 1 year'
            when datediff('day', last_nv_order_ds, ds) between 0 and 7 then '0-7 days'
            when datediff('day', last_nv_order_ds, ds) between 8 and 14 then '8-14 days'
            when datediff('day', last_nv_order_ds, ds) between 15 and 30 then '15-30 days'
            when datediff('day', last_nv_order_ds, ds) between 31 and 60 then '31-60 days'
            when datediff('day', last_nv_order_ds, ds) between 61 and 90 then '61-90 days'
            when datediff('day', last_nv_order_ds, ds) between 91 and 180 then '91-180 days'
            when datediff('day', last_nv_order_ds, ds) between 181 and 365 then '181-365 days'
            else 'Unknown'
        end as days_since_order_bucket
    from proddb.public.dimension_nv_users
    where ds between $window_start and $window_end
)

-- Final dimension table - maintains history by DS
select
    g.ds,  -- Use the date as the primary DS (as-of date)
    g.consumer_id,
    g.nv_cx_growth_state,
    g.dd_nv_cx_growth_state,
    g.dd_cx_growth_state,
    
    -- Days since last order metrics
    o.last_nv_order_ds,
    o.days_since_order,
    o.days_since_order_bucket,
    
    -- Numeric reachability values (original format)
    coalesce(p.is_reachable_push_overall, 0) as is_reachable_push,
    coalesce(e.is_reachable_email, 0) as is_reachable_email,
    
    -- Boolean reachability flags for overall push and email
    case when coalesce(p.is_reachable_push_overall, 0) = 1 then TRUE else FALSE end as is_push_reachable,
    case when coalesce(e.is_reachable_email, 0) = 1 then TRUE else FALSE end as is_email_reachable,
    
    -- Combined reachability
    case when coalesce(p.is_reachable_push_overall, 0) = 1 or coalesce(e.is_reachable_email, 0) = 1 
         then TRUE else FALSE end as is_any_channel_reachable,
    
    -- Push reachability by message type - Boolean flags
    case when coalesce(p.is_reachable_push_recommendations, 0) = 1 then TRUE else FALSE end as is_push_reachable_recommendations,
    case when coalesce(p.is_reachable_push_reminders, 0) = 1 then TRUE else FALSE end as is_push_reachable_reminders,
    case when coalesce(p.is_reachable_push_doordash_offers, 0) = 1 then TRUE else FALSE end as is_push_reachable_doordash_offers,
    case when coalesce(p.is_reachable_push_store_offers, 0) = 1 then TRUE else FALSE end as is_push_reachable_store_offers,
    case when coalesce(p.is_reachable_push_special_offers, 0) = 1 then TRUE else FALSE end as is_push_reachable_special_offers,
    case when coalesce(p.is_reachable_push_other, 0) = 1 then TRUE else FALSE end as is_push_reachable_other,
    
    -- Push reachability by message type - Numeric values
    coalesce(p.is_reachable_push_recommendations, 0) as is_reachable_push_recommendations,
    coalesce(p.is_reachable_push_reminders, 0) as is_reachable_push_reminders,
    coalesce(p.is_reachable_push_doordash_offers, 0) as is_reachable_push_doordash_offers,
    coalesce(p.is_reachable_push_store_offers, 0) as is_reachable_push_store_offers,
    coalesce(p.is_reachable_push_special_offers, 0) as is_reachable_push_special_offers,
    coalesce(p.is_reachable_push_other, 0) as is_reachable_push_other
from daily_growth_states g
left join push_reachability_pivot p
    on g.consumer_id = p.consumer_id and g.ds = p.ds
left join email_reachability e
    on g.consumer_id = e.consumer_id and g.ds = e.ds
left join last_order_data o
    on g.consumer_id = o.consumer_id and g.ds = o.ds;

-- Latest values lookup for easy access to current state
create or replace table proddb.tylershields.dimension_nv_notif_users_lookup as
with latest_states as (
    select 
        consumer_id,
        max(ds) as latest_ds
    from proddb.tylershields.dimension_nv_notif_users
    group by consumer_id
)
select 
    d.consumer_id,
    d.ds,
    d.nv_cx_growth_state,
    d.dd_nv_cx_growth_state,
    d.dd_cx_growth_state,
    d.last_nv_order_ds,
    d.days_since_order,
    d.days_since_order_bucket,
    d.is_push_reachable,
    d.is_email_reachable,
    d.is_any_channel_reachable,
    d.is_push_reachable_recommendations,
    d.is_push_reachable_reminders,
    d.is_push_reachable_doordash_offers,
    d.is_push_reachable_store_offers,
    d.is_push_reachable_special_offers,
    d.is_push_reachable_other
from proddb.tylershields.dimension_nv_notif_users d
join latest_states l
    on d.consumer_id = l.consumer_id
    and d.ds = l.latest_ds;

