-- Dashboard query for notification audience reach analysis
-- Requires ts_growth_states and ts_notification_coverage temporary tables to be populated

set window_start = '2025-02-17';

with filtered_notifications as (
    select 
        consumer_id,
        count(distinct deduped_message_id) as num_notifs_sent,
        array_agg(distinct campaign_name) as campaigns_received
    from ts_notification_coverage
    where 1=1
        -- Campaign filter placeholder
        -- and campaign_name in (select value from table(flatten(input => parse_json('["campaign1", "campaign2"]'))))
    group by 1
)
select 
    g.dd_cx_growth_state,
    g.nv_cx_growth_state,
    count(distinct g.consumer_id) as total_customers,
    count(distinct case when n.num_notifs_sent > 0 then g.consumer_id end) as customers_with_notifs,
    count(distinct case when n.num_notifs_sent = 0 then g.consumer_id end) as customers_without_notifs,
    round(customers_with_notifs / nullif(total_customers, 0) * 100, 2) as pct_customers_with_notifs,
    -- Additional metrics for heat map
    round(customers_with_notifs / nullif(sum(customers_with_notifs) over (), 0) * 100, 2) as pct_of_total_notified,
    round(total_customers / nullif(sum(total_customers) over (), 0) * 100, 2) as pct_of_total_customers
from ts_growth_states g
left join filtered_notifications n
    on g.consumer_id = n.consumer_id
group by 1, 2
order by 1, 2; 