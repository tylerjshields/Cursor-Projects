-- Check columns in fact_store_impressions (fsi)
DESCRIBE TABLE edw.consumer.fact_store_impressions;

-- Look for columns that might contain 'container' in their name or contain JSON
SELECT column_name, data_type
FROM edw.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'CONSUMER'
  AND table_name = 'FACT_STORE_IMPRESSIONS'
  AND (LOWER(column_name) LIKE '%container%' 
       OR LOWER(column_name) LIKE '%carousel%'
       OR LOWER(column_name) LIKE '%json%' 
       OR LOWER(column_name) LIKE '%extra%'
       OR LOWER(column_name) LIKE '%data%'
       OR LOWER(column_name) LIKE '%meta%'
       OR LOWER(column_name) LIKE '%context%'
       OR LOWER(data_type) LIKE '%json%'
       OR LOWER(data_type) LIKE '%variant%'
       OR LOWER(data_type) LIKE '%object%');

-- Sample 5 rows with all columns to examine the data
SELECT *
FROM edw.consumer.fact_store_impressions
LIMIT 5;

-- If we find a JSON column, let's examine its structure
-- Assuming there's a column called 'extra_data', 'metadata', or similar that might contain container info
-- This is a template that we'll adjust based on the actual column names
SELECT TOP 10 
    *,
    -- Check various potential JSON fields
    TRY_PARSE_JSON(COALESCE(
        extra_data, 
        metadata,
        context_data,
        additional_data,
        event_data,
        null
    )) as parsed_json
FROM edw.consumer.fact_store_impressions;

-- If we find container data in a JSON field, let's extract it specifically
-- This is a template we'll adjust after finding the actual field
/*
SELECT TOP 100
    parsed_json:container_id::STRING as container_id,
    parsed_json:container_name::STRING as container_name
FROM (
    SELECT 
        TRY_PARSE_JSON(COALESCE(
            extra_data, 
            metadata,
            context_data,
            additional_data,
            event_data,
            null
        )) as parsed_json
    FROM edw.consumer.fact_store_impressions
)
WHERE parsed_json:container_id IS NOT NULL
   OR parsed_json:container_name IS NOT NULL;
*/ 