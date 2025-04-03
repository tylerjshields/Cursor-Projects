-- Verify table existence and check for marginal_variable_profit column
-- First, check if the table exists
SHOW TABLES LIKE 'fact_delivery_allocation_custom' IN edw.finance;

-- Next, check the table structure to see if marginal_variable_profit column exists
DESC TABLE edw.finance.fact_delivery_allocation_custom;

-- Check if the marginal_variable_profit column is populated with recent data
-- Using ACTIVE_DATE_UTC as the date field
SELECT 
    MIN(ACTIVE_DATE_UTC) AS earliest_date,
    MAX(ACTIVE_DATE_UTC) AS latest_date,
    COUNT(*) AS total_records,
    COUNT(marginal_variable_profit) AS records_with_mvp,
    AVG(marginal_variable_profit) AS avg_mvp,
    MIN(marginal_variable_profit) AS min_mvp,
    MAX(marginal_variable_profit) AS max_mvp
FROM edw.finance.fact_delivery_allocation_custom
WHERE ACTIVE_DATE_UTC >= '2024-01-01';

-- Check if there's any backfill gap in the marginal_variable_profit column
SELECT 
    ACTIVE_DATE_UTC,
    COUNT(*) AS total_records,
    COUNT(marginal_variable_profit) AS records_with_mvp,
    COUNT(marginal_variable_profit) / COUNT(*) * 100 AS pct_with_mvp
FROM edw.finance.fact_delivery_allocation_custom
WHERE ACTIVE_DATE_UTC >= '2024-01-01'
GROUP BY ACTIVE_DATE_UTC
ORDER BY ACTIVE_DATE_UTC
LIMIT 30; 