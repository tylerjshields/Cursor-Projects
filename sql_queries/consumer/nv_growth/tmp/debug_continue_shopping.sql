-- Diagnostic query to investigate "Continue Shopping" records

-- First, let's look at the actual M_CARD_VIEW data to understand what we're looking for
SELECT 
    DATE_TRUNC('day', TIMESTAMP) AS date,
    Carousel_name,
    Container_id,
    page,
    COUNT(*) AS record_count
FROM IGUAZU.CONSUMER.M_CARD_VIEW 
WHERE LOWER(Carousel_name) = 'continue shopping' 
  AND page = 'explore_page' 
  AND TIMESTAMP >= '2025-03-11'
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 5 DESC;

-- Now let's examine fact_store_impressions and look for potential "Continue Shopping" records
-- Using a more relaxed filter to see what's available
SELECT 
    DATE_TRUNC('day', EVENT_DATE) AS date,
    CAROUSEL_NAME,
    CONTAINER_NAME,
    CONTAINER,
    PARSE_JSON(EVENT_OTHER_PROPERTIES):container_id::STRING AS container_id,
    PARSE_JSON(EVENT_OTHER_PROPERTIES):carousel_name::STRING AS json_carousel_name,
    COUNT(*) AS record_count
FROM edw.consumer.fact_store_impressions fsi
WHERE EVENT_DATE >= DATEADD('day', -3, CURRENT_DATE())
  AND page = 'explore_page'
  AND (
      -- Look for any potential "Continue Shopping" indicators
      LOWER(CAROUSEL_NAME) LIKE '%continue%shop%'
      OR LOWER(CONTAINER_NAME) LIKE '%continue%shop%'
      OR LOWER(CONTAINER) LIKE '%continue%shop%'
      OR LOWER(PARSE_JSON(EVENT_OTHER_PROPERTIES):container_id::STRING) LIKE '%continue%shop%'
      OR LOWER(PARSE_JSON(EVENT_OTHER_PROPERTIES):carousel_name::STRING) LIKE '%continue%shop%'
  )
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC, 7 DESC;

-- Check for potential variations in spacing or capitalization
SELECT DISTINCT
    CAROUSEL_NAME,
    CONTAINER_NAME,
    CONTAINER,
    PARSE_JSON(EVENT_OTHER_PROPERTIES):container_id::STRING AS container_id,
    PARSE_JSON(EVENT_OTHER_PROPERTIES):carousel_name::STRING AS json_carousel_name
FROM edw.consumer.fact_store_impressions fsi
WHERE EVENT_DATE >= DATEADD('day', -3, CURRENT_DATE())
  AND page = 'explore_page'
  AND (
      -- Look for any potential "Continue Shopping" indicators with a very broad match
      LOWER(CAROUSEL_NAME) LIKE '%cont%'
      OR LOWER(CONTAINER_NAME) LIKE '%cont%'
      OR LOWER(CONTAINER) LIKE '%cont%'
      OR LOWER(PARSE_JSON(EVENT_OTHER_PROPERTIES):container_id::STRING) LIKE '%cont%'
      OR LOWER(PARSE_JSON(EVENT_OTHER_PROPERTIES):carousel_name::STRING) LIKE '%cont%'
  )
ORDER BY 1, 2, 3, 4, 5; 