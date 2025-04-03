-- Query to analyze global notification audience reach 
set window_start = '2025-02-17';

create or replace temporary table ts_growth_states as (
    select 
        d.consumer_id,
        coalesce(n.nv_cx_growth_state, 'unknown') as nv_cx_growth_state,
        coalesce(n.dd_nv_cx_growth_state, 'unknown') as dd_nv_cx_growth_state,
        d.CX_LIFESTAGE_STATUS as dd_cx_growth_state
    from edw.growth.dimension_consumer_growth_accounting_state_detailed_all d 
    left join metrics_repo.public.nv_cx_growth_state n
        on d.consumer_id = n.consumer_id 
        and d.ds = n.event_ts::DATE
    where d.experience = 'doordash'
        and d.ds = $window_start
);

create or replace temporary table ts_notification_reach as (
    select consumer_id, date, channel, max(reached_28d) as reached_28d 
    from edw.consumer.dimension_consumer_notification_reach 
    where date = dateadd('d', 28, $window_start)
    and channel = 'push'
    group by all
);

create or replace temporary table ts_notification_coverage as (
    select 
        consumer_id,
        count(distinct deduped_message_id) as num_notifs_sent,
        array_agg(distinct campaign_name) as campaigns_received
    from proddb.public.nvg_notif_metrics_base
    where sent_at_date between $window_start and dateadd('d', 28, $window_start)
    group by all
);

-- Detailed view of individual customers
select 
    g.*,
    coalesce(n.num_notifs_sent, 0) as num_notifs_sent,
    coalesce(n.campaigns_received, array_construct()) as campaigns_received
from ts_growth_states g
left join ts_notification_coverage n
    on g.consumer_id = n.consumer_id
order by g.consumer_id;

-- Aggregated view by growth state
select 
    g.dd_cx_growth_state,
    g.nv_cx_growth_state,
    count(distinct g.consumer_id) as total_customers,
    count(distinct case when r.reached_28d = 1 then g.consumer_id end) as reachable_customers,
    count(distinct case when n.num_notifs_sent > 0 then g.consumer_id end) as customers_with_notifs,
    reachable_customers / nullif(total_customers, 0) as pct_reachable,
    customers_with_notifs / nullif(reachable_customers, 0) as pct_notified_of_reachable,
    customers_with_notifs / nullif(total_customers, 0) as pct_notified_of_total
from ts_growth_states g
left join ts_notification_coverage n
    on g.consumer_id = n.consumer_id
left join ts_notification_reach r
    on g.consumer_id = r.consumer_id
    and r.date = dateadd('d', 28, $window_start)
where g.dd_cx_growth_state is not null 
and g.dd_cx_growth_state != 'other' 
and g.dd_cx_growth_state != 'Non-Purchaser'
group by all
order by 1, 2;
