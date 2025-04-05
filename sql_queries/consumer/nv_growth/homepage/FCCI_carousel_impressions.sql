-- Daily data for carousels using fact_consumer_carousel_impressions
-- Aligned with the criteria from LCM_monitoring.sql

-- Using UNION to match the exact same criteria as in LCM_monitoring.sql
-- "Recently Viewed Items" carousel
SELECT 
    event_date,
    'Recently Viewed Items' AS carousel_type,
    session_id,
    vertical_position,
    is_clicked_flg AS clicked,
    discovery_surface,
    experience,
    platform,
    merchant_country,
    container_id,
    container_name
FROM 
    edw.consumer.fact_consumer_carousel_impressions
WHERE
    event_date >= DATEADD('day', -7, CURRENT_DATE())
    AND discovery_surface = 'Home Page'
    AND container_id = 'homepage_recently_viewed_items'
    AND (carousel_status = true OR carousel_status IS NULL)

UNION ALL

-- "Buy it again" carousel (BIA)
SELECT 
    event_date,
    'BIA Carousel' AS carousel_type,
    session_id,
    vertical_position,
    is_clicked_flg AS clicked,
    discovery_surface,
    experience,
    platform,
    merchant_country,
    container_id,
    container_name
FROM 
    edw.consumer.fact_consumer_carousel_impressions
WHERE
    event_date >= DATEADD('day', -7, CURRENT_DATE())
    AND discovery_surface = 'Home Page'
    AND container_id = 'reorder'
    AND (carousel_status = true OR carousel_status IS NULL)

UNION ALL

-- "Continue shopping" carousel
SELECT 
    event_date,
    'Continue Shopping' AS carousel_type,
    session_id,
    vertical_position,
    is_clicked_flg AS clicked,
    discovery_surface,
    experience,
    platform,
    merchant_country,
    container_id,
    container_name
FROM 
    edw.consumer.fact_consumer_carousel_impressions
WHERE
    event_date >= DATEADD('day', -7, CURRENT_DATE())
    AND discovery_surface = 'Home Page'
    AND container_name = 'Continue shopping'
    AND container_id != 'homepage_recently_viewed_items'
    AND (carousel_status = true OR carousel_status IS NULL)

ORDER BY 
    event_date DESC, 
    carousel_type,
    session_id; 