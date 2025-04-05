-- Describe the M_CARD_VIEW table to understand its schema
DESCRIBE TABLE IGUAZU.CONSUMER.M_CARD_VIEW;

-- Get column names and data types from information schema
SELECT 
    column_name, 
    data_type,
    character_maximum_length,
    numeric_precision,
    is_nullable
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
ORDER BY 
    ordinal_position;

-- Select a sample row to see the actual data
SELECT * FROM IGUAZU.CONSUMER.M_CARD_VIEW LIMIT 1;

-- Check if specific columns exist that we're interested in (based on LCM_monitoring.sql)
SELECT 
    COUNT(*) AS has_consumer_id, 
    'consumer_id' AS column_name
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
    AND column_name = 'CONSUMER_ID'
UNION ALL
SELECT 
    COUNT(*) AS has_session_id, 
    'session_id' AS column_name
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
    AND column_name = 'SESSION_ID'
UNION ALL
SELECT 
    COUNT(*) AS has_store_page_visitor, 
    'store_page_visitor' AS column_name
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
    AND column_name = 'STORE_PAGE_VISITOR'
UNION ALL
SELECT 
    COUNT(*) AS has_vertical_position, 
    'vertical_position' AS column_name
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
    AND column_name = 'VERTICAL_POSITION'
UNION ALL
SELECT 
    COUNT(*) AS has_card_position, 
    'card_position' AS column_name
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
    AND column_name = 'CARD_POSITION'
UNION ALL
SELECT 
    COUNT(*) AS has_click, 
    'is_click' AS column_name
FROM 
    IGUAZU.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'CONSUMER' 
    AND table_name = 'M_CARD_VIEW'
    AND column_name = 'IS_CLICK';
