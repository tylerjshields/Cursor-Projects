--This will eventaully get replaced with HLL

-- Send volume comparison between specified teams (CRM, NVG) vs all others by notification channel
create or replace table proddb.tylershields.dash_nv_notif_volume_by_team_l28 as 
select 
    -- sent_at_date,
    notification_channel,
    team,
    count(1) as total_notifications,
    count(distinct deduped_message_id_consumer) as notifications_sent,
    count(distinct consumer_id) as consumers_reached,
    notifications_sent / nullif(consumers_reached, 0) as notifs_per_cx
from proddb.tylershields.nvg_notif_base_data
where sent_at_date between current_date-28 and current_date-1
group by all;

grant select on proddb.tylershields.dash_nv_notif_volume_by_team_l28 to role read_only_users;

create or replace table proddb.tylershields.dash_nv_notif_volume_by_team_agg_l28 as 
select 
    notification_channel,
    iff(team='All Others', team, 'NV') as team_agg,
    count(1) as total_notifications,
    count(distinct deduped_message_id_consumer) as notifications_sent,
    count(distinct consumer_id) as distinct_cx,
    notifications_sent / nullif(distinct_cx, 0) as notifs_per_cx
from proddb.tylershields.nvg_notif_base_data
where sent_at_date between current_date-28 and current_date-1
group by all;
grant select on proddb.tylershields.dash_nv_notif_volume_by_team_agg_l28 to role read_only_users;

-- Percentile view showing the distribution of notifications per consumer
create or replace table proddb.tylershields.dash_nv_notif_percentiles_by_team_agg_l28 as 
with consumer_notif_counts as (
    -- Calculate notifications per consumer
    select 
        consumer_id,
        notification_channel,
        iff(team='All Others', team, 'NV') as team_agg,
        count(distinct deduped_message_id_consumer) as notifs_received
    from proddb.tylershields.nvg_notif_base_data
    where sent_at_date between current_date-28 and current_date-1
    group by consumer_id, notification_channel, team
)
select
    notification_channel,
    team_agg,
    count(distinct consumer_id) as consumers_reached,
    avg(notifs_received) as avg_notifs_per_cx,
    percentile_cont(0.25) within group (order by notifs_received) as p25_notifs,
    percentile_cont(0.50) within group (order by notifs_received) as median_notifs,
    percentile_cont(0.75) within group (order by notifs_received) as p75_notifs,
    percentile_cont(0.90) within group (order by notifs_received) as p90_notifs,
    percentile_cont(0.95) within group (order by notifs_received) as p95_notifs,
    percentile_cont(0.99) within group (order by notifs_received) as p99_notifs,
    percentile_cont(0.999) within group (order by notifs_received) as p999_notifs,
    max(notifs_received) as max_notifs
from consumer_notif_counts
group by all;

grant select on proddb.tylershields.dash_nv_notif_percentiles_by_team_agg_l28 to role read_only_users;

-- Detailed percentile view showing 1-99 percentiles for more granular distribution analysis
create or replace table proddb.tylershields.dash_nv_notif_detailed_percentiles_l28 as 
with consumer_notif_counts as (
    -- Calculate notifications per consumer
    select 
        consumer_id,
        notification_channel,
        iff(team='All Others', team, 'NV') as team_agg,
        count(distinct deduped_message_id_consumer) as notifs_received
    from proddb.tylershields.nvg_notif_base_data
    where sent_at_date between current_date-28 and current_date-1
    group by consumer_id, notification_channel, team
)
select
    notification_channel,
    team_agg,
    count(distinct consumer_id) as consumers_reached,
    avg(notifs_received) as avg_notifs_per_cx,
    -- 1st through 9th percentiles
    percentile_cont(0.01) within group (order by notifs_received) as p01_notifs,
    percentile_cont(0.02) within group (order by notifs_received) as p02_notifs,
    percentile_cont(0.03) within group (order by notifs_received) as p03_notifs,
    percentile_cont(0.04) within group (order by notifs_received) as p04_notifs,
    percentile_cont(0.05) within group (order by notifs_received) as p05_notifs,
    percentile_cont(0.06) within group (order by notifs_received) as p06_notifs,
    percentile_cont(0.07) within group (order by notifs_received) as p07_notifs,
    percentile_cont(0.08) within group (order by notifs_received) as p08_notifs,
    percentile_cont(0.09) within group (order by notifs_received) as p09_notifs,
    -- 10th through 19th percentiles
    percentile_cont(0.10) within group (order by notifs_received) as p10_notifs,
    percentile_cont(0.11) within group (order by notifs_received) as p11_notifs,
    percentile_cont(0.12) within group (order by notifs_received) as p12_notifs,
    percentile_cont(0.13) within group (order by notifs_received) as p13_notifs,
    percentile_cont(0.14) within group (order by notifs_received) as p14_notifs,
    percentile_cont(0.15) within group (order by notifs_received) as p15_notifs,
    percentile_cont(0.16) within group (order by notifs_received) as p16_notifs,
    percentile_cont(0.17) within group (order by notifs_received) as p17_notifs,
    percentile_cont(0.18) within group (order by notifs_received) as p18_notifs,
    percentile_cont(0.19) within group (order by notifs_received) as p19_notifs,
    -- 20th through 29th percentiles
    percentile_cont(0.20) within group (order by notifs_received) as p20_notifs,
    percentile_cont(0.21) within group (order by notifs_received) as p21_notifs,
    percentile_cont(0.22) within group (order by notifs_received) as p22_notifs,
    percentile_cont(0.23) within group (order by notifs_received) as p23_notifs,
    percentile_cont(0.24) within group (order by notifs_received) as p24_notifs,
    percentile_cont(0.25) within group (order by notifs_received) as p25_notifs,
    percentile_cont(0.26) within group (order by notifs_received) as p26_notifs,
    percentile_cont(0.27) within group (order by notifs_received) as p27_notifs,
    percentile_cont(0.28) within group (order by notifs_received) as p28_notifs,
    percentile_cont(0.29) within group (order by notifs_received) as p29_notifs,
    -- 30th through 39th percentiles
    percentile_cont(0.30) within group (order by notifs_received) as p30_notifs,
    percentile_cont(0.31) within group (order by notifs_received) as p31_notifs,
    percentile_cont(0.32) within group (order by notifs_received) as p32_notifs,
    percentile_cont(0.33) within group (order by notifs_received) as p33_notifs,
    percentile_cont(0.34) within group (order by notifs_received) as p34_notifs,
    percentile_cont(0.35) within group (order by notifs_received) as p35_notifs,
    percentile_cont(0.36) within group (order by notifs_received) as p36_notifs,
    percentile_cont(0.37) within group (order by notifs_received) as p37_notifs,
    percentile_cont(0.38) within group (order by notifs_received) as p38_notifs,
    percentile_cont(0.39) within group (order by notifs_received) as p39_notifs,
    -- 40th through 49th percentiles
    percentile_cont(0.40) within group (order by notifs_received) as p40_notifs,
    percentile_cont(0.41) within group (order by notifs_received) as p41_notifs,
    percentile_cont(0.42) within group (order by notifs_received) as p42_notifs,
    percentile_cont(0.43) within group (order by notifs_received) as p43_notifs,
    percentile_cont(0.44) within group (order by notifs_received) as p44_notifs,
    percentile_cont(0.45) within group (order by notifs_received) as p45_notifs,
    percentile_cont(0.46) within group (order by notifs_received) as p46_notifs,
    percentile_cont(0.47) within group (order by notifs_received) as p47_notifs,
    percentile_cont(0.48) within group (order by notifs_received) as p48_notifs,
    percentile_cont(0.49) within group (order by notifs_received) as p49_notifs,
    -- 50th through 59th percentiles
    percentile_cont(0.50) within group (order by notifs_received) as p50_notifs,
    percentile_cont(0.51) within group (order by notifs_received) as p51_notifs,
    percentile_cont(0.52) within group (order by notifs_received) as p52_notifs,
    percentile_cont(0.53) within group (order by notifs_received) as p53_notifs,
    percentile_cont(0.54) within group (order by notifs_received) as p54_notifs,
    percentile_cont(0.55) within group (order by notifs_received) as p55_notifs,
    percentile_cont(0.56) within group (order by notifs_received) as p56_notifs,
    percentile_cont(0.57) within group (order by notifs_received) as p57_notifs,
    percentile_cont(0.58) within group (order by notifs_received) as p58_notifs,
    percentile_cont(0.59) within group (order by notifs_received) as p59_notifs,
    -- 60th through 69th percentiles
    percentile_cont(0.60) within group (order by notifs_received) as p60_notifs,
    percentile_cont(0.61) within group (order by notifs_received) as p61_notifs,
    percentile_cont(0.62) within group (order by notifs_received) as p62_notifs,
    percentile_cont(0.63) within group (order by notifs_received) as p63_notifs,
    percentile_cont(0.64) within group (order by notifs_received) as p64_notifs,
    percentile_cont(0.65) within group (order by notifs_received) as p65_notifs,
    percentile_cont(0.66) within group (order by notifs_received) as p66_notifs,
    percentile_cont(0.67) within group (order by notifs_received) as p67_notifs,
    percentile_cont(0.68) within group (order by notifs_received) as p68_notifs,
    percentile_cont(0.69) within group (order by notifs_received) as p69_notifs,
    -- 70th through 79th percentiles
    percentile_cont(0.70) within group (order by notifs_received) as p70_notifs,
    percentile_cont(0.71) within group (order by notifs_received) as p71_notifs,
    percentile_cont(0.72) within group (order by notifs_received) as p72_notifs,
    percentile_cont(0.73) within group (order by notifs_received) as p73_notifs,
    percentile_cont(0.74) within group (order by notifs_received) as p74_notifs,
    percentile_cont(0.75) within group (order by notifs_received) as p75_notifs,
    percentile_cont(0.76) within group (order by notifs_received) as p76_notifs,
    percentile_cont(0.77) within group (order by notifs_received) as p77_notifs,
    percentile_cont(0.78) within group (order by notifs_received) as p78_notifs,
    percentile_cont(0.79) within group (order by notifs_received) as p79_notifs,
    -- 80th through 89th percentiles
    percentile_cont(0.80) within group (order by notifs_received) as p80_notifs,
    percentile_cont(0.81) within group (order by notifs_received) as p81_notifs,
    percentile_cont(0.82) within group (order by notifs_received) as p82_notifs,
    percentile_cont(0.83) within group (order by notifs_received) as p83_notifs,
    percentile_cont(0.84) within group (order by notifs_received) as p84_notifs,
    percentile_cont(0.85) within group (order by notifs_received) as p85_notifs,
    percentile_cont(0.86) within group (order by notifs_received) as p86_notifs,
    percentile_cont(0.87) within group (order by notifs_received) as p87_notifs,
    percentile_cont(0.88) within group (order by notifs_received) as p88_notifs,
    percentile_cont(0.89) within group (order by notifs_received) as p89_notifs,
    -- 90th through 99th percentiles
    percentile_cont(0.90) within group (order by notifs_received) as p90_notifs,
    percentile_cont(0.91) within group (order by notifs_received) as p91_notifs,
    percentile_cont(0.92) within group (order by notifs_received) as p92_notifs,
    percentile_cont(0.93) within group (order by notifs_received) as p93_notifs,
    percentile_cont(0.94) within group (order by notifs_received) as p94_notifs,
    percentile_cont(0.95) within group (order by notifs_received) as p95_notifs,
    percentile_cont(0.96) within group (order by notifs_received) as p96_notifs,
    percentile_cont(0.97) within group (order by notifs_received) as p97_notifs,
    percentile_cont(0.98) within group (order by notifs_received) as p98_notifs,
    percentile_cont(0.99) within group (order by notifs_received) as p99_notifs,
    -- Additional high percentiles and max for outlier analysis
    percentile_cont(0.995) within group (order by notifs_received) as p995_notifs,
    percentile_cont(0.999) within group (order by notifs_received) as p999_notifs,
    max(notifs_received) as max_notifs
from consumer_notif_counts
group by all;

grant select on proddb.tylershields.dash_nv_notif_detailed_percentiles_l28 to role read_only_users;

