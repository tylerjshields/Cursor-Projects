--table I will use
select * from edw.consumer.fact_store_impressions limit 10


-- Colin's list
-- Impression count of each LCM carousel
-- %session when LCM carousel is shown (sessions shown /all sessions)
-- Avg viewed position of each carousel
-- CTR of each carousel
-- Avg #impressions per user
-- Latency
-- Error rate
-- get m_card_view for carousel


-- Individual source queries to identify our new carousels we want to track
-- RV carousel
select * from IGUAZU.CONSUMER.M_CARD_VIEW 
    where Container_id = 'homepage_recently_viewed_items' 
    and page = 'explore_page' 
    and TIMESTAMP >= '2025-03-11'
    limit 10;

-- BIA carousel
select * from IGUAZU.CONSUMER.M_CARD_VIEW 
    where Container_id = 'reorder' 
    and page = 'explore_page' 
    and TIMESTAMP >= '2025-03-11' 
    limit 10;

-- deals carousel
select is_store_view_discoverable, * from IGUAZU.CONSUMER.M_CARD_VIEW 
    where Carousel_name = 'Continue shopping' 
    and page = 'explore_page' 
    and container_id != 'homepage_recently_viewed_items' 
    and TIMESTAMP >= '2025-03-11'
LIMIT 10;
-- select container_id, TIMESTAMP ts, * 
-- from IGUAZU.CONSUMER.M_CARD_VIEW 
-- where lower(Carousel_name) = 'continue shopping' 
-- and page = 'explore_page' 
-- and container_id != 'homepage_recently_viewed_items' and TIMESTAMP >= '2025-03-11'
-- limit 10;

-- Basic query to examine carousel data with separate fields
create or replace table proddb.tylershields.fact_store_impressions_lcm as
SELECT 
    DATE_TRUNC('day', EVENT_DATE) AS date,
    timestamp, 
    user_id as consumer_id,  
    session_id, 
    PARSE_JSON(EVENT_OTHER_PROPERTIES):container_id::STRING AS container_id,
    container_name,
    container,
    -- Categorize based on the three example M_CARD_VIEW queries
    CASE 
        WHEN LOWER(container_id) = 'homepage_recently_viewed_items' 
             THEN 'RV carousel'
        WHEN LOWER(container_id) = 'reorder' 
             THEN 'BIA carousel'
        WHEN LOWER(CAROUSEL_NAME) = 'continue shopping' 
             AND LOWER(container_id) != 'homepage_recently_viewed_items'
             THEN 'deals carousel'
        ELSE 'Other'
    END AS carousel_category,
 
    store_page_visitor, 
    EVENT_OTHER_PROPERTIES,
    vertical_position,
    card_position

FROM edw.consumer.fact_store_impressions fsi
WHERE EVENT_DATE >= current_date-7
AND page = 'explore_page'
AND (
    -- Only include records that match the categories in the CASE statement
    (LOWER(container_id) = 'homepage_recently_viewed_items')
    OR
    (LOWER(container_id) = 'reorder')
    OR
    (LOWER(CAROUSEL_NAME) = 'continue shopping' 
     AND LOWER(container_id) != 'homepage_recently_viewed_items')
)
GROUP BY ALL;
