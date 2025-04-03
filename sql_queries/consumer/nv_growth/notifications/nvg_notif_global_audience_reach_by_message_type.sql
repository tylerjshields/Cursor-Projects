-- Query to analyze global notification audience reach 
set window_start = '2025-02-17';

create or replace temporary table ts_growth_states as
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
        and d.ds = $window_start;

create or replace temporary table ts_notification_reach_by_message_type as 
    select consumer_id, date, channel, message_type, max(reached_28d) as reached_28d 
    from edw.consumer.dimension_consumer_notification_reach 
    where date = dateadd('d', 28, $window_start)
    and channel = 'PUSH'
    group by all;

create or replace temporary table ts_notification_reach_all as
    select consumer_id, date, channel, 'Overall' as message_type, max(reached_28d) as reached_28d 
    from ts_notification_reach_by_message_type 
    where date = dateadd('d', 28, $window_start)
    and channel = 'PUSH'
    group by all

    union all

    select consumer_id, date, channel, message_type, max(reached_28d) as reached_28d 
    from ts_notification_reach_by_message_type 
    where date = dateadd('d', 28, $window_start)
    and channel = 'PUSH'
    group by all;

create or replace temporary table ts_notification_coverage_all as
    select 
        consumer_id,
        notification_message_type as message_type,
        campaign_name,
        campaign_name_clean,
        count(*) as num_notifs_sent,
        array_agg(distinct campaign_name) as campaigns_received
    from proddb.public.nvg_notif_metrics_base
    where sent_at_date between $window_start and dateadd('d', 28, $window_start)
    group by all

    union all

    select 
        consumer_id,
        'Overall' as message_type,
        'All Campaigns' as campaign_name,
        'All Campaigns' as campaign_name_clean,
        count(*) as num_notifs_sent,
        array_agg(distinct campaign_name) as campaigns_received
    from proddb.public.nvg_notif_metrics_base
    where sent_at_date between $window_start and dateadd('d', 28, $window_start)
    group by consumer_id;

-- Aggregated view by growth state
create or replace table proddb.public.nvg_notif_reach_dashboard as
with growth_state_totals as
    select 
        dd_cx_growth_state,
        nv_cx_growth_state,
        count(distinct consumer_id) as total_customers_in_state
    from ts_growth_states
    where dd_cx_growth_state is not null 
    and dd_cx_growth_state != 'other' 
    and dd_cx_growth_state != 'Non-Purchaser'
    group by all,
reachable_by_message_type as
    select 
        g.dd_cx_growth_state,
        g.nv_cx_growth_state,
        r.message_type,
        count(distinct iff(r.reached_28d = 1, g.consumer_id, null)) as reachable_customers
    from ts_growth_states g
    left join ts_notification_reach_all r
        on g.consumer_id = r.consumer_id
        and r.date = dateadd('d', 28, $window_start)
    where g.dd_cx_growth_state is not null 
    and g.dd_cx_growth_state != 'other' 
    and g.dd_cx_growth_state != 'Non-Purchaser'
    group by all
select 
    g.dd_cx_growth_state,
    g.nv_cx_growth_state,
    n.message_type,
    n.campaign_name,
    n.campaign_name_clean,
    t.total_customers_in_state as total_customers,
    r.reachable_customers,
    count(distinct iff(n.num_notifs_sent > 0, g.consumer_id, null)) as customers_with_notifs,
    r.reachable_customers / nullif(t.total_customers_in_state, 0) as pct_reachable,
    customers_with_notifs / nullif(r.reachable_customers, 0) as pct_notified_of_reachable,
    customers_with_notifs / nullif(t.total_customers_in_state, 0) as pct_notified_of_total
from ts_growth_states g
left join ts_notification_coverage_all n
    on g.consumer_id = n.consumer_id
join growth_state_totals t
    on g.dd_cx_growth_state = t.dd_cx_growth_state
    and g.nv_cx_growth_state = t.nv_cx_growth_state
join reachable_by_message_type r
    on g.dd_cx_growth_state = r.dd_cx_growth_state
    and g.nv_cx_growth_state = r.nv_cx_growth_state
    and coalesce(n.message_type, 'Overall') = r.message_type
where g.dd_cx_growth_state is not null 
and g.dd_cx_growth_state != 'other' 
and g.dd_cx_growth_state != 'Non-Purchaser'
group by all
order by 1, 2, 3, 4, 5;
