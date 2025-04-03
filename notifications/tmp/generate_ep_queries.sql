-- Script to generate INSERT statements for engagement program queries
-- This will extract program queries from the engagement_program table and join with distinct EP names
-- from nv_channels_notif_index, then generate INSERT statements for each program

-- Set date parameters (adjust as needed)
SET start_date = '2023-01-01';
SET end_date = CURRENT_DATE();

-- Generate INSERT statements for each EP
WITH ep_programs AS (
    SELECT DISTINCT 
        nci.ep_name,
        ep.program_query
    FROM nv_channels_notif_index nci
    JOIN engagement_program ep ON nci.ep_name = ep.program_name
    WHERE ep.program_query IS NOT NULL
)
SELECT 
    'INSERT INTO nvg_channels_ep_daily (ep_name, consumer_id)' || 
    ' SELECT ' || 
    '''' || ep_name || '''' || ', ' ||
    'consumer_id' || 
    ' FROM (' || 
    REPLACE(
        REPLACE(
            program_query, 
            '{START_DATE}', $start_date
        ),
        '{END_DATE}', $end_date
    ) || 
    ');' AS insert_query
FROM ep_programs;

/*
INSTRUCTIONS:
1. Run this script to generate the INSERT statements
2. Copy and execute the generated INSERT statements in Snowflake
3. These INSERT statements will populate the nvg_channels_ep_daily table with consumer IDs from each engagement program
*/ 