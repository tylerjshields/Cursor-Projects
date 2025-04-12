-- Analysis of dormant/churned notifications with store name extraction
-- Created based on ep_dd_consumer_dormant_churned_push campaigns

/*
CHANGE INDEX:
- Added Spanish extraction patterns for business names
- Focused on 4h metrics only for consistency
- Switched from business_id to business_name for matching throughout
- Used created_at instead of active_date_utc for more precise timing
- Added active push campaigns to capture more notifications 
- Optimized joins using subqueries to reduce data volume
- Created temp table for notifications processing to improve performance
- Created business_mapping table to ensure 1-to-1 relationship between business name and vertical
*/

-- Set date parameters for consistency
set notif_start_date = current_date-29;
set notif_end_date = current_date-2;
set activity_end_date = current_date-1;

-- Step 1: Create business vertical mapping table to ensure one business maps to one vertical
create or replace table proddb.tylershields.nvg_notif_bia_business_mapping as
select 
    business_name,
    vertical_name,
    count(*) as occurrence_count,
    row_number() over (
        partition by business_name 
        order by count(*) desc
    ) as rn
from edw.cng.dimension_new_vertical_store_tags
where business_name is not null
    and vertical_name is not null
    and is_filtered_core = true
group by business_name, vertical_name
qualify row_number() over ( 
    partition by business_name, vertical_name
    order by count(*) desc
) = 1;

-- Step 2: Create notifications temp table
create or replace temporary table nvg_notif_bia_notifications as
select 
    concat(e.deduped_message_id, '_', e.consumer_id) as deduped_message_id_consumer,
    e.deduped_message_id,
    e.consumer_id,
    COALESCE(e.campaign_name, e.canvas_name) as campaign_name,
    coalesce(e.campaign_id, e.canvas_id) as campaign_canvas_id,
    max(ep_name) as ep_name,
    max(clean_campaign_name) as clean_campaign_name,
    max(n.team) as team,
    min(sent_at_date) as sent_at_date,
    max(e.notification_channel) as notification_channel,
    max(e.title) as title,    
    -- Extract store name using CASE statement to handle both patterns
    max(case
        -- English patterns
        when e.title like 'Your %items are on sale' 
            then trim(regexp_replace(e.title, '^Your | items are on sale$', ''))
        when e.title like 'Need more from %?' 
            then trim(regexp_replace(e.title, '^Need more from |\\?$', ''))
        -- Spanish patterns
        when e.title like 'Tus artículos de % están en oferta'
            then trim(regexp_replace(e.title, '^Tus artículos de | están en oferta$', ''))
        when e.title like '¿Necesitas más de %?'
            then trim(regexp_replace(e.title, '^¿Necesitas más de |\\?$', ''))
        else null
    end) as store_name,
    -- Binary metrics - use correct column names from multi-channel metrics
    max(CASE 
        WHEN e.notification_channel = 'PUSH' THEN (opened_at is not null)
        WHEN e.notification_channel = 'EMAIL' THEN (link_clicked_at is not null)
        ELSE 0
    END) as is_engaged,
    -- Timestamp metrics - use MIN for earliest timestamp
    min(e.sent_at) as sent_at,
    min(CASE 
        WHEN e.notification_channel = 'PUSH' THEN e.opened_at 
        WHEN e.notification_channel = 'EMAIL' THEN e.link_clicked_at
        ELSE NULL
    END) as engagement_at,
    -- Only keeping 4h metrics for consistency
    max(e.receive_within_4h) AS receive_within_4h,
    max(CASE 
        WHEN e.notification_channel = 'PUSH' THEN e.open_within_4h
        WHEN e.notification_channel = 'EMAIL' THEN e.link_click_within_4h
        ELSE 0
    END) AS engagement_within_4h,
    max(e.visit_within_4h) AS visit_within_4h,
    max(e.order_within_4h) AS order_within_4h
from edw.consumer.fact_consumer_notification_engagement e 
join proddb.public.nv_channels_notif_index n 
    on coalesce(e.campaign_id, e.canvas_id) = coalesce(n.campaign_id, n.canvas_id) 
where sent_at_date between $notif_start_date and $notif_end_date
    and notification_channel in ('PUSH', 'EMAIL')
    and e.campaign_name in (
        'ep_dd_consumer_dormant_churned_push_sale', 
        'ep_dd_consumer_dormant_churned_push_reminder',
        'ep_dd_consumer_nv_active_push_sale',
        'ep_dd_consumer_nv_active_push_reminder'
    )
group by all;

-- Fix the business_info CTE to guarantee one business name maps to one vertical
-- Let's add a debug table to verify our approach
-- Simple query to identify business names that aren't being matched to any business in our system
-- create or replace table proddb.public.nvg_notif_unmatched_businesses as
-- with extracted_business_names as (
--     -- Extract all distinct business names from the temp table
--     select distinct
--         store_name as extracted_business_name,
--         count(*) as notification_count
--     from nvg_notif_bia_notifications
--     where store_name is not null
--     group by 1
-- )

-- -- Find business names without a match
-- select 
--     e.extracted_business_name,
--     e.notification_count
-- from extracted_business_names e
-- left join (
--     select distinct business_name 
--     from edw.cng.dimension_new_vertical_store_tags
-- ) b on regexp_replace(lower(trim(e.extracted_business_name)), '[^a-z0-9]', '') = 
--       regexp_replace(lower(trim(b.business_name)), '[^a-z0-9]', '')
-- where b.business_name is null
-- order by e.notification_count desc;

-- Debug table to verify our business mapping logic is working
create or replace table proddb.tylershields.nvg_notif_bia_debug_business_mapping as
select 
    bm.business_name,
    bm.vertical_name,
    bm.occurrence_count,
    bm.rn,
    count(*) as vertical_count
from proddb.tylershields.nvg_notif_bia_business_mapping bm
join (
    select
        business_name,
        count(distinct vertical_name) as distinct_verticals
    from proddb.tylershields.nvg_notif_bia_business_mapping
    group by business_name
    having count(distinct vertical_name) > 1
) as multi_vert on bm.business_name = multi_vert.business_name
group by bm.business_name, bm.vertical_name, bm.occurrence_count, bm.rn
order by bm.business_name, bm.rn;

-- Step 3: Create the main analysis table
create or replace table proddb.tylershields.nvg_notif_bia_store_analysis as
with 
-- Extra-simplified business mapping with guaranteed 1-to-1 relationship
business_info as (
    -- This is the ONLY place vertical_name should come from
    select 
        upper(business_name) as business_name,  -- Force uppercase for consistency
        vertical_name
    from proddb.tylershields.nvg_notif_bia_business_mapping
    where rn = 1
),

-- Map notification extracted business names to business_info (only place with verticals)
matched_businesses as (
    select
        n.deduped_message_id_consumer,
        n.store_name as extracted_business_name,
        b.business_name,
        b.vertical_name  -- This is the ONLY source of vertical_name in the entire query
    from nvg_notif_bia_notifications n
    join business_info b
        on regexp_replace(upper(trim(n.store_name)), '[^A-Z0-9]', '') = 
           regexp_replace(upper(trim(b.business_name)), '[^A-Z0-9]', '')
),

-- Business page views without vertical references
business_visits as (
    select
        n.deduped_message_id_consumer,
        s.iguazu_timestamp as visit_timestamp,
        upper(si.business_name) as store_business_name,
        m.business_name as matched_business_name
    from nvg_notif_bia_notifications n
    join iguazu.server_events_production.m_store_page_load s 
        on n.consumer_id = try_to_number(s.consumer_id)
        and s.iguazu_timestamp between n.sent_at and dateadd('hour', 4, n.sent_at)
    join edw.cng.dimension_new_vertical_store_tags si 
        on try_to_number(s.store_id) = si.store_id
    left join matched_businesses m 
        on n.deduped_message_id_consumer = m.deduped_message_id_consumer
    where si.is_filtered_mp_vertical = true
),

-- Orders without vertical references
orders as (
    select
        n.deduped_message_id_consumer,
        d.delivery_id,
        d.created_at as order_timestamp,
        upper(si.business_name) as store_business_name,
        m.business_name as matched_business_name
    from nvg_notif_bia_notifications n
    join edw.finance.dimension_deliveries d 
        on n.consumer_id = d.creator_id
        and d.created_at between n.sent_at and dateadd('hour', 4, n.sent_at)
    join edw.cng.dimension_new_vertical_store_tags si 
        on d.store_id = si.store_id
    left join matched_businesses m 
        on n.deduped_message_id_consumer = m.deduped_message_id_consumer
    where d.is_filtered_core = true
        and si.is_filtered_mp_vertical = true
)

-- Final query with guaranteed single vertical per business
select 
    n.*,
    -- Business mapping information - This should be the ONLY source of vertical_name
    m.business_name as matched_business_name,
    m.vertical_name,
    
    -- 4-hour visit and order metrics (simplified)
    count(distinct v.visit_timestamp) > 0 as had_business_visit_4h,
    -- Calculate matches directly in the select
    max(case when upper(v.store_business_name) = upper(v.matched_business_name) then 1 else 0 end) = 1 as visited_recommended_business_4h,
    count(distinct ord.delivery_id) > 0 as had_nv_order_4h,
    -- Calculate matches directly in the select
    max(case when upper(ord.store_business_name) = upper(ord.matched_business_name) then 1 else 0 end) = 1 as ordered_from_recommended_business_4h
    
from nvg_notif_bia_notifications n
left join matched_businesses m on n.deduped_message_id_consumer = m.deduped_message_id_consumer
left join business_visits v on n.deduped_message_id_consumer = v.deduped_message_id_consumer
left join orders ord on n.deduped_message_id_consumer = ord.deduped_message_id_consumer
group by n.deduped_message_id_consumer, n.deduped_message_id, n.consumer_id, n.campaign_name, n.campaign_canvas_id, 
         n.ep_name, n.clean_campaign_name, n.team, n.sent_at_date, n.notification_channel, n.title, n.store_name,
         n.is_engaged, n.sent_at, n.engagement_at, n.receive_within_4h, n.engagement_within_4h, n.visit_within_4h, 
         n.order_within_4h, m.business_name, m.vertical_name;
  