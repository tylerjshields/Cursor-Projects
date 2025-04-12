-- ios and android are logged separately and have different canvas_step_message_variation_api_id
SELECT *

FROM braze_shared.datalake_sharing.USERS_CANVAS_FREQUENCYCAP_SHARED

WHERE to_timestamp_ntz(time)::date BETWEEN '2025-02-06' AND '2025-03-06'
AND channel IN ('ios_push', 'android_push')

QUALIFY count(channel) over (partition by external_user_id, canvas_id, to_timestamp_ntz(time)::date) > 1

ORDER BY external_user_id, canvas_id, to_timestamp_ntz(time)::date

LIMIT 100;

-- this table has dispatch_id (already deduped for ios/android)
SELECT *
 
FROM braze_shared.datalake_sharing.USERS_CAMPAIGNS_FREQUENCYCAP_SHARED
    
WHERE to_timestamp_ntz(time)::date BETWEEN '2025-02-06' AND '2025-03-06'
AND channel IN ('ios_push', 'android_push')

QUALIFY count(channel) over (partition by external_user_id, campaign_id, to_timestamp_ntz(time)::date) > 1

ORDER BY external_user_id, campaign_id, to_timestamp_ntz(time)::date
;

create or replace table jenniferyang.braze_capped_notif_0225_0306 AS 

WITH canvas_map AS (
    SELECT canvas_id, canvas_name

    FROM ext_braze.inbound.braze_canvas_window

    QUALIFY row_number() over (partition by canvas_id order by event_time desc) = 1
),

campaign_map AS (
    SELECT campaign_id, campaign_name

    FROM ext_braze.inbound.braze_campaign_window

    QUALIFY row_number() over (partition by campaign_id order by event_time desc) = 1
)

SELECT a.external_user_id AS consumer_id,
a.id,
a.timezone,
to_timestamp_ntz(a.time) AS capped_at,
convert_timezone('UTC', a.timezone, capped_at) AS capped_at_local,
b.canvas_name AS campaign_name

FROM braze_shared.datalake_sharing.USERS_CANVAS_FREQUENCYCAP_SHARED a

JOIN canvas_map b
ON a.canvas_id = b.canvas_id

WHERE to_timestamp_ntz(a.time)::date BETWEEN '2025-02-25' AND '2025-03-06'
AND a.channel IN ('ios_push', 'android_push')

-- need to dedupe when Cx has both ios and android device manually
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.external_user_id, a.canvas_id, a.canvas_step_api_id, to_timestamp_ntz(a.time)::date ORDER BY to_timestamp_ntz(a.time)) = 1

UNION ALL

SELECT a.external_user_id AS consumer_id,
a.dispatch_id AS id,
a.timezone,
to_timestamp_ntz(a.time) AS capped_at,
convert_timezone('UTC', a.timezone, capped_at) AS capped_at_local,
b.campaign_name

FROM braze_shared.datalake_sharing.USERS_CAMPAIGNS_FREQUENCYCAP_SHARED a

JOIN campaign_map b
ON a.campaign_id = b.campaign_id

WHERE to_timestamp_ntz(a.time)::date BETWEEN '2025-02-25' AND '2025-03-06'
AND a.channel IN ('ios_push', 'android_push')
AND a.campaign_id <> '67ae538bec4aec0064a399fb' -- Remove the silent push campaign

QUALIFY ROW_NUMBER() OVER (PARTITION BY a.external_user_id, a.dispatch_id ORDER BY to_timestamp_ntz(a.time)) = 1
;

create or replace table jenniferyang.braze_capped_notif_0225_0306_attribution_tmp AS 

WITH fpn_sent AS (
    -- SELECT profile_id AS consumer_id,
    -- dd_event_id AS notification_uuid,
    -- min(status_timestamp) AS sent_at

    -- FROM iguazu.server_events_production.postal_push_send
        
    -- WHERE status_code = '200'
    -- AND profile_type = 'CONSUMER'
    -- AND message_category <> 'BACKGROUND'
    -- AND source IN ('growth-service', 'notification-atc')
    -- AND status_timestamp::date BETWEEN  '2025-02-23' AND '2025-03-06'

    -- GROUP BY 1,2

    SELECT consumer_id,
    dd_event_id AS notification_uuid,
    campaign_name,
    min(sent_at) AS sent_at

    FROM edw.consumer.fact_consumer_notification_engagement
    
    WHERE 1=1
    AND is_valid_send = 1
    AND notification_message_type_overall = 'MARKETING'
    AND notification_channel = 'PUSH'
    AND notification_source ilike '%FPN%'
    AND postal_service_source IN ('growth-service', 'notification-atc')
    AND sent_at::date BETWEEN  '2025-02-23' AND '2025-03-06'

    GROUP BY 1,2,3
),

braze_sent AS (
    SELECT consumer_id,
    concat(consumer_id, sent_at_date, SPLIT_PART(message_id, '::', -1)) AS notification_uuid,
    coalesce(campaign_name, canvas_name) AS campaign_name,
    min(sent_at) AS sent_at

    FROM edw.consumer.fact_consumer_notification_engagement
    
    WHERE 1=1
    AND is_valid_send = 1
    AND notification_message_type_overall = 'MARKETING'
    AND notification_channel = 'PUSH'
    AND notification_source = 'Braze'
    AND sent_at::date BETWEEN  '2025-02-23' AND '2025-03-06'

    GROUP BY 1,2,3
)

SELECT a.*,
b.notification_uuid AS fpn_notification_uuid,
b.campaign_name AS fpn_campaign_name,
b.sent_at AS fpn_sent_at,
convert_timezone('UTC', a.timezone, b.sent_at) AS fpn_sent_at_local,
c.notification_uuid AS braze_notification_uuid,
c.campaign_name AS braze_campaign_name,
c.sent_at AS braze_sent_at,
convert_timezone('UTC', a.timezone, c.sent_at) AS braze_sent_at_local

FROM jenniferyang.braze_capped_notif_0225_0306 a

LEFT JOIN fpn_sent b
ON a.consumer_id = b.consumer_id
AND a.capped_at_local::date = convert_timezone('UTC', a.timezone, b.sent_at)::date

LEFT JOIN braze_sent c
ON a.consumer_id = c.consumer_id
AND a.capped_at_local::date = convert_timezone('UTC', a.timezone, c.sent_at)::date
;

-- About 20% of the capped notifications can be attributed to multiple notifications
WITH t AS (
    SELECT id,
    count(distinct fpn_notification_uuid) AS num_fpn,
    count(distinct braze_notification_uuid) AS num_braze
    
    FROM jenniferyang.braze_capped_notif_0225_0306_attribution_tmp
    
    GROUP BY id
)

SELECT count(distinct id) AS num_capped_notif,
count(distinct case when num_fpn + num_braze > 1 then id end) AS num_multi_attributed_notif,
num_multi_attributed_notif/num_capped_notif AS multi_attributed_pct

FROM t
;

-- within fpn/braze, pick the notif that was sent last as the blocking notifications
-- for fpn, there could be multiple given post check out 'marketing' notifications do not subject to the daily frequency cap
-- for braze, there could be multiple given cart abandonment do not subject to the daily frequency cap

CREATE OR REPLACE TABLE jenniferyang.braze_capped_notif_0225_0306_attribution AS

WITH fpn_sent AS (
    -- SELECT profile_id AS consumer_id,
    -- dd_event_id AS notification_uuid,
    -- min(status_timestamp) AS sent_at

    -- FROM iguazu.server_events_production.postal_push_send
        
    -- WHERE status_code = '200'
    -- AND profile_type = 'CONSUMER'
    -- AND message_category <> 'BACKGROUND'
    -- AND source IN ('growth-service', 'notification-atc')
    -- AND status_timestamp::date BETWEEN  '2025-02-23' AND '2025-03-06'

    -- GROUP BY 1,2

    SELECT consumer_id,
    dd_event_id AS notification_uuid,
    campaign_name,
    min(sent_at) AS sent_at

    FROM edw.consumer.fact_consumer_notification_engagement
    
    WHERE 1=1
    AND is_valid_send = 1
    AND notification_message_type_overall = 'MARKETING'
    AND notification_channel = 'PUSH'
    AND notification_source ilike '%FPN%'
    AND postal_service_source IN ('growth-service', 'notification-atc')
    AND sent_at::date BETWEEN  '2025-02-23' AND '2025-03-06'

    GROUP BY 1,2,3
),

braze_sent AS (
    SELECT consumer_id,
    concat(consumer_id, sent_at_date, SPLIT_PART(message_id, '::', -1)) AS notification_uuid,
    coalesce(campaign_name, canvas_name) AS campaign_name,
    min(sent_at) AS sent_at

    FROM edw.consumer.fact_consumer_notification_engagement
    
    WHERE 1=1
    AND is_valid_send = 1
    AND notification_message_type_overall = 'MARKETING'
    AND notification_channel = 'PUSH'
    AND notification_source = 'Braze'
    AND sent_at::date BETWEEN  '2025-02-23' AND '2025-03-06'

    GROUP BY 1,2,3
),

t AS (
    SELECT a.*,
    b.notification_uuid AS fpn_notification_uuid,
    b.campaign_name AS fpn_campaign_name,
    b.sent_at AS fpn_sent_at,
    convert_timezone('UTC', a.timezone, b.sent_at) AS fpn_sent_at_local
    
    FROM jenniferyang.braze_capped_notif_0225_0306 a
    
    LEFT JOIN fpn_sent b
    ON a.consumer_id = b.consumer_id
    AND a.capped_at_local::date = convert_timezone('UTC', a.timezone, b.sent_at)::date
    
    QUALIFY row_number() over (partition by a.id order by fpn_sent_at_local desc) = 1
)

SELECT t.*,
c.notification_uuid AS braze_notification_uuid,
c.campaign_name AS braze_campaign_name,
c.sent_at AS braze_sent_at,
convert_timezone('UTC', t.timezone, c.sent_at) AS braze_sent_at_local

FROM t

LEFT JOIN braze_sent c
ON t.consumer_id = c.consumer_id
AND t.capped_at_local::date = convert_timezone('UTC', t.timezone, c.sent_at)::date

QUALIFY row_number() over (partition by t.id order by braze_sent_at_local desc) = 1
;

-- Attribution
-- 52% to FPN
-- 29% to Braze
-- 19% to both
-- small % to neither

grant select on jenniferyang.braze_capped_notif_0225_0306_attribution to public;
SELECT cx_segment,
count(distinct id) as num_capped_notif,
count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is null then id end) AS num_fpn_attributed_notif,
count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is not null then id end) AS num_braze_attributed_notif,
count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is not null then id end) AS num_multi_attributed_notif,
count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is null then id end) AS num_unattributed_notif,
num_fpn_attributed_notif/num_capped_notif AS fpn_pct,
num_braze_attributed_notif/num_capped_notif AS braze_pct,
num_multi_attributed_notif/num_capped_notif AS multi_attributed_pct,
num_unattributed_notif/num_capped_notif AS unattributed_pct

FROM jenniferyang.braze_capped_notif_0225_0306_attribution

GROUP BY 1

UNION ALL

SELECT 'Overall' AS cx_segment,
count(distinct id) as num_capped_notif,
count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is null then id end) AS num_fpn_attributed_notif,
count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is not null then id end) AS num_braze_attributed_notif,
count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is not null then id end) AS num_multi_attributed_notif,
count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is null then id end) AS num_unattributed_notif,
num_fpn_attributed_notif/num_capped_notif AS fpn_pct,
num_braze_attributed_notif/num_capped_notif AS braze_pct,
num_multi_attributed_notif/num_capped_notif AS multi_attributed_pct,
num_unattributed_notif/num_capped_notif AS unattributed_pct

FROM jenniferyang.braze_capped_notif_0225_0306_attribution

GROUP BY 1
;

-- % of Braze notif capped
-- % of Braze notif capped due to different source

WITH braze_sent_agg AS (
    -- SELECT sent_at_date AS dte,
    -- count(distinct concat(consumer_id, sent_at_date, SPLIT_PART(message_id, '::', -1))) AS num_sent_notif
    
    -- FROM edw.consumer.fact_consumer_notification_engagement
        
    -- WHERE 1=1
    -- AND is_valid_send = 1
    -- AND notification_message_type_overall = 'MARKETING'
    -- AND notification_channel = 'PUSH'
    -- AND notification_source = 'Braze'
    -- AND sent_at_date BETWEEN '2025-02-25' AND '2025-03-06'

    -- GROUP BY 1

    SELECT
    dte,
    CASE
        WHEN lifestage = 'New Cx' THEN 'New'
        WHEN lifestage = 'Resurrected' THEN 'Active'
        WHEN lifestage = 'Very Churned' THEN 'Churned'
        ELSE lifestage
    END AS cx_segment,
    sum(num_notifications) AS num_sent_notif
    
    FROM edw.consumer.notification_engagement_deep_dive_weekly_metrics
    
    WHERE source = 'Braze'
    AND reporting_grain = 'DAILY'
    AND channel = 'PUSH'
    AND message_type_overall = 'MARKETING'
    AND time_period = '24H'
    AND lifestage NOT IN ('OVERALL', 'UNKNOWN')
    AND dte BETWEEN '2025-02-25' AND '2025-03-06'
    
    GROUP BY 1,2
),

braze_capped_agg AS (
    SELECT capped_at::date AS dte,
    cx_segment,
    count(distinct id) AS num_capped_notif,
    count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is null then id end) AS num_fpn_attributed_notif,
    count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is not null then id end) AS num_braze_attributed_notif,
    count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is not null then id end) AS num_multi_attributed_notif,
    count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is null then id end) AS num_unattributed_notif,
    FROM jenniferyang.braze_capped_notif_0225_0306_attribution

    GROUP BY 1,2
),

braze_sent_agg_overall AS (
    SELECT
    dte,
    'Overall' AS cx_segment,
    sum(num_notifications) AS num_sent_notif
    
    from edw.consumer.notification_engagement_deep_dive_weekly_metrics
    WHERE source = 'Braze'
    AND reporting_grain = 'DAILY'
    AND channel = 'PUSH'
    AND message_type_overall = 'MARKETING'
    AND time_period = '24H'
    AND lifestage NOT IN ('OVERALL', 'UNKNOWN')
    AND dte BETWEEN '2025-02-25' AND '2025-03-06'
    
    GROUP BY 1,2
),

braze_capped_agg_overall AS (
    SELECT capped_at::date AS dte,
    'Overall' AS cx_segment,
    count(distinct id) AS num_capped_notif,
    count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is null then id end) AS num_fpn_attributed_notif,
    count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is not null then id end) AS num_braze_attributed_notif,
    count(distinct case when fpn_notification_uuid is not null and braze_notification_uuid is not null then id end) AS num_multi_attributed_notif,
    count(distinct case when fpn_notification_uuid is null and braze_notification_uuid is null then id end) AS num_unattributed_notif,
    FROM jenniferyang.braze_capped_notif_0225_0306_attribution

    GROUP BY 1,2
)

SELECT a.dte,
a.cx_segment,
avg(a.num_sent_notif) AS num_sent_notif,
avg(b.num_capped_notif) AS num_capped_notif,
avg(round(b.num_capped_notif / (a.num_sent_notif + b.num_capped_notif),2)) AS capped_pct,
avg(round(b.num_fpn_attributed_notif / (a.num_sent_notif + b.num_capped_notif),2)) AS fpn_capped_pct,
avg(round(b.num_braze_attributed_notif / (a.num_sent_notif + b.num_capped_notif),2)) AS braze_capped_pct,
avg(round(b.num_multi_attributed_notif / (a.num_sent_notif + b.num_capped_notif),2)) AS multi_attributed_capped_pct,
avg(round(b.num_unattributed_notif / (a.num_sent_notif + b.num_capped_notif),2)) AS unattributed_capped_pct

FROM braze_sent_agg a

LEFT JOIN braze_capped_agg b
ON a.dte = b.dte
AND a.cx_segment = b.cx_segment

GROUP BY 1,2

UNION ALL

SELECT c.dte,
c.cx_segment,
avg(c.num_sent_notif) AS num_sent_notif,
avg(d.num_capped_notif) AS num_capped_notif,
avg(round(d.num_capped_notif / (c.num_sent_notif + d.num_capped_notif),2)) AS capped_pct,
avg(round(d.num_fpn_attributed_notif / (c.num_sent_notif + d.num_capped_notif),2)) AS fpn_capped_pct,
avg(round(d.num_braze_attributed_notif / (c.num_sent_notif + d.num_capped_notif),2)) AS braze_capped_pct,
avg(round(d.num_multi_attributed_notif / (c.num_sent_notif + d.num_capped_notif),2)) AS multi_attributed_capped_pct,
avg(round(d.num_unattributed_notif / (c.num_sent_notif + d.num_capped_notif),2)) AS unattributed_capped_pct

FROM braze_sent_agg_overall c

LEFT JOIN braze_capped_agg_overall d
ON c.dte = d.dte
AND c.cx_segment = d.cx_segment

GROUP BY 1,2
;

-- top Braze campaigns that are blocked
WITH t AS (
    SELECT cx_segment, campaign_name, count(distinct id) AS num_notif
    
    FROM jenniferyang.braze_capped_notif_0225_0306_attribution 
    
    WHERE fpn_notification_uuid is not null
    AND braze_notification_uuid is null
    
    GROUP BY 1,2
)

SELECT t.*

FROM t

QUALIFY row_number() over (partition by cx_segment order by num_notif desc) <= 5

ORDER BY 1, 3 DESC
;

-- top blocking FPN campaigns
WITH t AS (
    SELECT cx_segment, fpn_campaign_name, count(distinct id) AS num_notif
    
    FROM jenniferyang.braze_capped_notif_0225_0306_attribution 
    
    WHERE fpn_notification_uuid is not null
    
    GROUP BY 1,2
)

SELECT t.*

FROM t

QUALIFY row_number() over (partition by cx_segment order by num_notif desc) <= 5

ORDER BY 1, 3 DESC
;

-- top blocking Braze campaigns
SELECT braze_campaign_name, count(distinct id) AS num_notif

FROM jenniferyang.braze_capped_notif_0225_0306_attribution 

WHERE fpn_notification_uuid is not null

GROUP BY 1

ORDER BY 2 DESC

LIMIT 100;

-- engagement; are we blocking the better campaign

select *
FROM edw.consumer.notification_engagement_deep_dive_weekly_metrics
    
    WHERE reporting_grain = 'DAILY'
    AND channel = 'PUSH'
    AND message_type_overall = 'MARKETING'
    AND time_period = '24H'
    AND lifestage NOT IN ('OVERALL', 'UNKNOWN')
    AND dte BETWEEN '2025-02-25' AND '2025-03-06'
    limit 100;

select *
from jenniferyang.braze_capped_notif_0225_0306_attribution
limit 100;

    
create or replace table jenniferyang.braze_capped_notif_0225_0306_attribution_with_engagement as

WITH braze_campaign_performance AS (
    SELECT
    dte,
    campaign_name,
    CASE
        WHEN lifestage = 'New Cx' THEN 'New'
        WHEN lifestage = 'Resurrected' THEN 'Active'
        WHEN lifestage = 'Very Churned' THEN 'Churned'
        ELSE lifestage
    END AS cx_segment,
    sum(num_notifications) AS num_notif,
    sum(num_open)/nullif(num_notif,0) AS open_rate,
    sum(num_visit)/nullif(num_notif,0) AS visit_rate,
    sum(num_order)/nullif(num_notif,0) AS order_rate,
    sum(num_unsubscribe)/nullif(num_notif,0) AS unsub_rate
    
    
    FROM edw.consumer.notification_engagement_deep_dive_weekly_metrics
    
    WHERE reporting_grain = 'DAILY'
    AND channel = 'PUSH'
    AND message_type_overall = 'MARKETING'
    AND time_period = '24H'
    AND lifestage NOT IN ('OVERALL', 'UNKNOWN')
    AND dte BETWEEN '2025-02-25' AND '2025-03-06'
    AND source = 'Braze'
    
    GROUP BY 1,2,3
),

fpn_campaign_performance AS (
    SELECT
    dte,
    campaign_name,
    CASE
        WHEN lifestage = 'New Cx' THEN 'New'
        WHEN lifestage = 'Resurrected' THEN 'Active'
        WHEN lifestage = 'Very Churned' THEN 'Churned'
        ELSE lifestage
    END AS cx_segment,
    sum(num_notifications) AS num_notif,
    sum(num_open)/nullif(num_notif,0) AS open_rate,
    sum(num_visit)/nullif(num_notif,0) AS visit_rate,
    sum(num_order)/nullif(num_notif,0) AS order_rate,
    sum(num_unsubscribe)/nullif(num_notif,0) AS unsub_rate
    
    
    FROM edw.consumer.notification_engagement_deep_dive_weekly_metrics
    
    WHERE reporting_grain = 'DAILY'
    AND channel = 'PUSH'
    AND message_type_overall = 'MARKETING'
    AND time_period = '24H'
    AND lifestage NOT IN ('OVERALL', 'UNKNOWN')
    AND dte BETWEEN '2025-02-04' AND '2025-02-13' --open event is broken from 2/14 to 3/7 so have to use another date
    AND source = 'FPN Postal Service'
    GROUP BY 1,2,3
)

SELECT a.*,
b.open_rate AS capped_campaign_open_rate,
b.visit_rate AS capped_campaign_visit_rate,
b.order_rate AS capped_campaign_order_rate,
b.unsub_rate AS capped_campaign_unsub_rate,
b.num_notif AS num_notif_from_capped_campaign,
c.open_rate AS fpn_open_rate,
c.visit_rate AS fpn_visit_rate,
c.order_rate AS fpn_order_rate,
c.unsub_rate AS fpn_unsub_rate,
c.num_notif AS num_notif_from_fpn_campaign,
d.open_rate AS braze_open_rate,
d.visit_rate AS braze_visit_rate,
d.order_rate AS braze_order_rate,
d.unsub_rate AS braze_unsub_rate,
d.num_notif AS num_notif_from_braze_campaign

FROM jenniferyang.braze_capped_notif_0225_0306_attribution a

LEFT JOIN braze_campaign_performance b
ON a.campaign_name = b.campaign_name
AND a.capped_at::date = b.dte
AND a.cx_segment = b.cx_segment

LEFT JOIN fpn_campaign_performance c
ON a.fpn_campaign_name = c.campaign_name
AND a.fpn_sent_at::date = dateadd('day', 21, c.dte)
AND a.cx_segment = c.cx_segment

LEFT JOIN braze_campaign_performance d
ON a.braze_campaign_name = d.campaign_name
AND a.braze_sent_at::date = d.dte
AND a.cx_segment = d.cx_segment
;

SELECT CASE
    WHEN fpn_notification_uuid is not null and braze_notification_uuid is null THEN 'is_fpn_attributed'
    WHEN fpn_notification_uuid is null and braze_notification_uuid is not null THEN 'is_braze_attributed'
    WHEN fpn_notification_uuid is not null and braze_notification_uuid is not null THEN 'is_multi_attributed'
    WHEN fpn_notification_uuid is null and braze_notification_uuid is null THEN 'is_unattributed'
    END AS attribution_type,
    cx_segment,
    avg(capped_campaign_open_rate) AS capped_campaign_open_rate,
    avg(capped_campaign_visit_rate) AS capped_campaign_visit_rate,
    avg(capped_campaign_order_rate) AS capped_campaign_order_rate,
    avg(capped_campaign_unsub_rate) AS capped_campaign_unsub_rate,
    avg(fpn_open_rate) AS fpn_open_rate,
    avg(fpn_visit_rate) AS fpn_visit_rate,
    avg(fpn_order_rate) AS fpn_order_rate,
    avg(fpn_unsub_rate) AS fpn_unsub_rate,
    avg(braze_open_rate) AS braze_open_rate,
    avg(braze_visit_rate) AS braze_visit_rate,
    avg(braze_order_rate) AS braze_order_rate,
    avg(braze_unsub_rate) AS braze_unsub_rate

FROM jenniferyang.braze_capped_notif_0225_0306_attribution_with_engagement

GROUP BY 1,2

ORDER BY 1,2;


select *
from jenniferyang.braze_capped_notif_0225_0306_attribution_with_engagement
where fpn_notification_uuid is not null and braze_notification_uuid is null
limit 100;

-- top Braze campaigns that are blocked
WITH t AS (
    SELECT cx_segment,
    campaign_name,
    count(distinct id) AS num_notif,
    avg(capped_campaign_open_rate) AS capped_campaign_open_rate,
    avg(capped_campaign_visit_rate) AS capped_campaign_visit_rate,
    avg(capped_campaign_order_rate) AS capped_campaign_order_rate,
    avg(capped_campaign_unsub_rate) AS capped_campaign_unsub_rate,
    avg(fpn_open_rate) AS fpn_open_rate,
    avg(fpn_visit_rate) AS fpn_visit_rate,
    avg(fpn_order_rate) AS fpn_order_rate,
    avg(fpn_unsub_rate) AS fpn_unsub_rate,
    
    FROM jenniferyang.braze_capped_notif_0225_0306_attribution_with_engagement
    
    WHERE fpn_notification_uuid is not null
    AND braze_notification_uuid is null
    
    GROUP BY 1,2
)

SELECT t.*

FROM t

QUALIFY row_number() over (partition by cx_segment order by num_notif desc) <= 5

ORDER BY 1, 3 DESC
;

-- future improvement: rerun using data post 3/11 after FPN look back is removed from all Braze suppression list