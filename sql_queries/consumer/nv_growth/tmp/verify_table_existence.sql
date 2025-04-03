-- Verify table exists
SHOW TABLES LIKE 'fact_delivery_allocation_custom' IN edw.finance;

-- Describe table structure
DESCRIBE TABLE edw.finance.fact_delivery_allocation_custom;

-- Check if marginal_variable_profit column is populated and backfilled to at least 2024-01-01
SELECT 
    COUNT(*) AS total_rows,
    COUNT(marginal_variable_profit) AS non_null_rows,
    MIN(active_date_utc) AS min_date,
    MAX(active_date_utc) AS max_date,
    COUNT(CASE WHEN active_date_utc >= '2024-01-01' THEN 1 END) AS total_rows_since_2024,
    COUNT(CASE WHEN active_date_utc >= '2024-01-01' AND marginal_variable_profit IS NOT NULL THEN 1 END) AS non_null_rows_since_2024,
    COUNT(CASE WHEN active_date_utc >= '2024-01-01' AND marginal_variable_profit IS NOT NULL THEN 1 END) / 
    NULLIF(COUNT(CASE WHEN active_date_utc >= '2024-01-01' THEN 1 END), 0) AS population_ratio_since_2024
FROM edw.finance.fact_delivery_allocation_custom
WHERE active_date_utc >= '2024-01-01'
LIMIT 1000; 