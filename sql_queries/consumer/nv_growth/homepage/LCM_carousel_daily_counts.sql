-- Daily data for 3 carousels (RVI, BIA, Continue Shopping) for the last week
-- Based on queries from LCM_monitoring.sql
-- Using confirmed column names from the M_CARD_VIEW table

SELECT 
    'Recently Viewed Items' AS carousel_type,
    TIMESTAMP,
    DATE_TRUNC('day', TIMESTAMP) AS date,
    USER_ID,
    CONSUMER_ID,
    DD_SESSION_ID,
    CONTAINER_ID,
    CAROUSEL_NAME,
    CONTAINER,
    CONTAINER_NAME,
    STORE_ID,
    STORE_NAME,
    PAGE
FROM IGUAZU.CONSUMER.M_CARD_VIEW 
WHERE CONTAINER_ID = 'homepage_recently_viewed_items' 
    AND PAGE = 'explore_page' 
    AND TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())

UNION ALL

SELECT 
    'BIA Carousel' AS carousel_type,
    TIMESTAMP,
    DATE_TRUNC('day', TIMESTAMP) AS date,
    USER_ID,
    CONSUMER_ID,
    DD_SESSION_ID,
    CONTAINER_ID,
    CAROUSEL_NAME,
    CONTAINER,
    CONTAINER_NAME,
    STORE_ID,
    STORE_NAME,
    PAGE
FROM IGUAZU.CONSUMER.M_CARD_VIEW 
WHERE CONTAINER_ID = 'reorder'
    AND PAGE = 'explore_page' 
    AND TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())

UNION ALL

SELECT 
    'Continue Shopping' AS carousel_type,
    TIMESTAMP,
    DATE_TRUNC('day', TIMESTAMP) AS date,
    USER_ID,
    CONSUMER_ID,
    DD_SESSION_ID,
    CONTAINER_ID,
    CAROUSEL_NAME,
    CONTAINER,
    CONTAINER_NAME,
    STORE_ID,
    STORE_NAME,
    PAGE
FROM IGUAZU.CONSUMER.M_CARD_VIEW 
WHERE CAROUSEL_NAME = 'Continue shopping'
    AND PAGE = 'explore_page' 
    AND CONTAINER_ID != 'homepage_recently_viewed_items'
    AND TIMESTAMP >= DATEADD('day', -7, CURRENT_DATE())

ORDER BY date DESC, TIMESTAMP DESC;

-- NOTE: Please verify all column names (user_id, session_id, store_page_visitor, etc.)
-- after running the describe_m_card_view.sql script, and adjust as needed 