-- Simple test query for fact_store_impressions
-- Run this using the Snowflake extension

-- Check table exists
SHOW TABLES LIKE 'fact_store_impressions' IN edw.consumer;

-- Check first 3 columns and 5 rows
SELECT * 
FROM edw.consumer.fact_store_impressions
LIMIT 5;

-- Describe the M_CARD_VIEW table to understand its schema
DESCRIBE TABLE IGUAZU.CONSUMER.M_CARD_VIEW;

-- Sample query to check column names
SELECT TOP 5 * FROM IGUAZU.CONSUMER.M_CARD_VIEW
WHERE TIMESTAMP >= DATEADD('day', -30, CURRENT_DATE());

-- Get column names from the information schema for M_CARD_VIEW table
SELECT 
    column_name, 
    data_type
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
ORDER BY 
    ordinal_position;

-- Test our carousel counts query with a limited count
-- Using only confirmed available columns

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
LIMIT 10;

-- Query for BIA Carousel
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
LIMIT 10;

-- Query for Continue Shopping Carousel
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
LIMIT 10; 