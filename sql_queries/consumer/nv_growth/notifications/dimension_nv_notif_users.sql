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

-- Get distinct message types from the actual data
message_types as (
    select distinct message_type
    from edw.consumer.dimension_consumer_notification_reach
    where date between $window_start and $window_end
    and message_type is not null
    and message_type != 'Transactional'  -- Exclude transactional notifications
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
    and r.message_type != 'Transactional'  -- Exclude transactional notifications
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
    and message_type != 'Transactional'  -- Exclude transactional notifications
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
    and message_type != 'Transactional'  -- Exclude transactional notifications
    group by ds, consumer_id
),

-- Create a pivot to convert message types to columns for each day
push_reachability_pivot as (
    select
        ds,
        consumer_id,
        max(case when message_type = 'Overall' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_overall,
        max(case when message_type = 'Marketing' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_marketing,
        max(case when message_type = 'Promotional' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_promotional,
        max(case when message_type = 'Lifecycle' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_lifecycle,
        max(case when message_type = 'Re-engagement' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_reengagement,
        max(case when message_type = 'Triggered' and is_reachable = 1 then 1 else 0 end) as is_reachable_push_triggered
    from push_reachability_combined
    group by ds, consumer_id
)

-- Final dimension table - maintains history by DS
select
    g.ds,  -- Use the date as the primary DS (as-of date)
    g.consumer_id,
    g.nv_cx_growth_state,
    g.dd_nv_cx_growth_state,
    g.dd_cx_growth_state,
    
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
    case when coalesce(p.is_reachable_push_marketing, 0) = 1 then TRUE else FALSE end as is_push_reachable_marketing,
    case when coalesce(p.is_reachable_push_promotional, 0) = 1 then TRUE else FALSE end as is_push_reachable_promotional,
    case when coalesce(p.is_reachable_push_lifecycle, 0) = 1 then TRUE else FALSE end as is_push_reachable_lifecycle,
    case when coalesce(p.is_reachable_push_reengagement, 0) = 1 then TRUE else FALSE end as is_push_reachable_reengagement,
    case when coalesce(p.is_reachable_push_triggered, 0) = 1 then TRUE else FALSE end as is_push_reachable_triggered,
    
    -- Push reachability by message type - Numeric values
    coalesce(p.is_reachable_push_marketing, 0) as is_reachable_push_marketing,
    coalesce(p.is_reachable_push_promotional, 0) as is_reachable_push_promotional,
    coalesce(p.is_reachable_push_lifecycle, 0) as is_reachable_push_lifecycle,
    coalesce(p.is_reachable_push_reengagement, 0) as is_reachable_push_reengagement,
    coalesce(p.is_reachable_push_triggered, 0) as is_reachable_push_triggered
from daily_growth_states g
left join push_reachability_pivot p
    on g.consumer_id = p.consumer_id and g.ds = p.ds
left join email_reachability e
    on g.consumer_id = e.consumer_id and g.ds = e.ds;

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
    d.is_push_reachable,
    d.is_email_reachable,
    d.is_any_channel_reachable,
    d.is_push_reachable_marketing,
    d.is_push_reachable_promotional,
    d.is_push_reachable_lifecycle,
    d.is_push_reachable_reengagement,
    d.is_push_reachable_triggered
from proddb.tylershields.dimension_nv_notif_users d
join latest_states l
    on d.consumer_id = l.consumer_id
    and d.ds = l.latest_ds;