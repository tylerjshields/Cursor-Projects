-- Daily summary counts for 3 carousels (RVI, BIA, Continue Shopping) for the last week
-- Based on queries from LCM_monitoring.sql
-- Using confirmed column names from the M_CARD_VIEW table
-- Aggregated by day and carousel type

WITH carousel_data AS (
    SELECT 
        'Recently Viewed Items' AS carousel_type,
        DATE_TRUNC('day', TIMESTAMP) AS date
    FROM IGUAZU.CONSUMER.M_CARD_VIEW 
    WHERE CONTAINER_ID = 'homepage_recently_viewed_items' 
        AND PAGE = 'explore_page' 
        AND TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())

    UNION ALL

    SELECT 
        'BIA Carousel' AS carousel_type,
        DATE_TRUNC('day', TIMESTAMP) AS date
    FROM IGUAZU.CONSUMER.M_CARD_VIEW 
    WHERE CONTAINER_ID = 'reorder'
        AND PAGE = 'explore_page' 
        AND TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())

    UNION ALL

    SELECT 
        'Continue Shopping' AS carousel_type,
        DATE_TRUNC('day', TIMESTAMP) AS date
    FROM IGUAZU.CONSUMER.M_CARD_VIEW 
    WHERE CAROUSEL_NAME = 'Continue shopping'
        AND PAGE = 'explore_page' 
        AND CONTAINER_ID != 'homepage_recently_viewed_items'
        AND TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())
)

SELECT 
    date,
    carousel_type,
    COUNT(*) AS impression_count,
    COUNT(DISTINCT USER_ID) AS unique_users,
    COUNT(DISTINCT DD_SESSION_ID) AS unique_sessions,
    impression_count / NULLIF(unique_users, 0) AS avg_impressions_per_user,
    impression_count / NULLIF(unique_sessions, 0) AS avg_impressions_per_session
FROM carousel_data
GROUP BY date, carousel_type
ORDER BY date DESC, impression_count DESC; 