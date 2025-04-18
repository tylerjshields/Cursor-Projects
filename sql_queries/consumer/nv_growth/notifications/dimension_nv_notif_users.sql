-- Dimension table for NV notification users with growth states and reachability
set window_start = current_date - 30;
set window_end = current_date;

-- Consolidated query for all push reachability metrics (combined and pivoted)
create or replace table proddb.tylershields.stg_push_reachability as
with push_data as (
    -- Base push reachability data with message types
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
    
    union all
    
    -- Overall push reachability (across all non-transactional message types)
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
)
-- Pivot the message types directly
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
from push_data
group by ds, consumer_id;

-- Email reachability - kept separate due to different channel
create or replace table proddb.tylershields.stg_email_reachability as
select 
    date as ds,
    consumer_id,
    max(reached_28d) as is_reachable_email
from edw.consumer.dimension_consumer_notification_reach
where date between $window_start and $window_end
and channel = 'EMAIL'
and message_type != 'TRANSACTIONAL'  -- Exclude transactional notifications
group by ds, consumer_id;

-- Create raw growth state data table (simplified)
create or replace table proddb.tylershields.stg_nv_notif_growth_states as
SELECT 
    CAST(ds AS DATE) AS ds,
    consumer_id,
    MAX(
      CASE
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'Active' THEN '1. active'
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'New Cx' THEN '2. new'
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'Dormant Resurrected' THEN '3. resurrected (from dormant)'
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'Churned Resurrected' THEN '4. resurrected (from churned)'
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'Dormant' THEN '5. dormant'
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'Churned' THEN '6. churned'
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'Very Churned' THEN '7. very churned'
        WHEN vertical = 'new_verticals_utc' AND cx_lifestage_detailed_status = 'Non-Purchaser' THEN '8. never trialed'
        END
    ) AS nv_cx_growth_state,
    Max(case when vertical = 'new_verticals_utc' then last_order_date end) as last_nv_order_ds,
    MAX(
      CASE
        WHEN vertical = 'core_dd_utc' THEN cx_lifestage_detailed_status
        END 
    ) AS dd_cx_growth_state,
    Max(case when vertical = 'core_dd_utc' then last_order_date end) as last_dd_order_ds
FROM 
    edw.growth.dimension_consumer_growth_accounting_state_detailed_all
WHERE 
    ds between $window_start and $window_end
    -- and dd_cx_growth_state <> 'Very Churned' and dd_cx_growth_state is not null
GROUP BY 1,2;

-- Final dimension table - maintains history by DS with all calculations
create or replace table proddb.public.dimension_nv_notif_users as
select
    g.ds,  -- Use the date as the primary DS (as-of date)
    g.consumer_id,
    g.nv_cx_growth_state,
    g.dd_cx_growth_state,
    
    -- Order data - for both NV and DD
    g.last_nv_order_ds,
    -- Calculate days since NV order for each consumer and date
    case
        when g.last_nv_order_ds is null then -1 
        when datediff('day', g.last_nv_order_ds, g.ds) > 365 then 366 
        else datediff('day', g.last_nv_order_ds, g.ds) 
    end as days_since_last_nv_order,
    -- Create bucketed version for NV orders
    case
        when g.last_nv_order_ds is null then 'Never ordered'
        when datediff('day', g.last_nv_order_ds, g.ds) > 365 then 'Over 1 year'
        when datediff('day', g.last_nv_order_ds, g.ds) between 0 and 7 then '0-7 days'
        when datediff('day', g.last_nv_order_ds, g.ds) between 8 and 14 then '8-14 days'
        when datediff('day', g.last_nv_order_ds, g.ds) between 15 and 30 then '15-30 days'
        when datediff('day', g.last_nv_order_ds, g.ds) between 31 and 60 then '31-60 days'
        when datediff('day', g.last_nv_order_ds, g.ds) between 61 and 90 then '61-90 days'
        when datediff('day', g.last_nv_order_ds, g.ds) between 91 and 180 then '91-180 days'
        when datediff('day', g.last_nv_order_ds, g.ds) between 181 and 365 then '181-365 days'
        else 'Unknown'
    end as days_since_last_nv_order_bucket,
    g.last_dd_order_ds,
    -- Calculate days since DD order for each consumer and date
    case
        when g.last_dd_order_ds is null then -1 
        when datediff('day', g.last_dd_order_ds, g.ds) > 365 then 366 
        else datediff('day', g.last_dd_order_ds, g.ds) 
    end as days_since_last_dd_order,
    -- Create bucketed version for DD orders
    case
        when g.last_dd_order_ds is null then 'Never ordered'
        when datediff('day', g.last_dd_order_ds, g.ds) > 365 then 'Over 1 year'
        when datediff('day', g.last_dd_order_ds, g.ds) between 0 and 7 then '0-7 days'
        when datediff('day', g.last_dd_order_ds, g.ds) between 8 and 14 then '8-14 days'
        when datediff('day', g.last_dd_order_ds, g.ds) between 15 and 30 then '15-30 days'
        when datediff('day', g.last_dd_order_ds, g.ds) between 31 and 60 then '31-60 days'
        when datediff('day', g.last_dd_order_ds, g.ds) between 61 and 90 then '61-90 days'
        when datediff('day', g.last_dd_order_ds, g.ds) between 91 and 180 then '91-180 days'
        when datediff('day', g.last_dd_order_ds, g.ds) between 181 and 365 then '181-365 days'
        else 'Unknown'
    end as days_since_last_dd_order_bucket,
    
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
from proddb.tylershields.stg_nv_notif_growth_states g
left join proddb.tylershields.stg_push_reachability p
    on g.consumer_id = p.consumer_id and g.ds = p.ds
left join proddb.tylershields.stg_email_reachability e
    on g.consumer_id = e.consumer_id and g.ds = e.ds;
