-- Check more recent dates for marginal_variable_profit backfill
SELECT 
    ACTIVE_DATE_UTC,
    COUNT(*) AS total_records,
    COUNT(marginal_variable_profit) AS records_with_mvp,
    COUNT(marginal_variable_profit) / COUNT(*) * 100 AS pct_with_mvp
FROM edw.finance.fact_delivery_allocation_custom
WHERE ACTIVE_DATE_UTC >= '2025-01-01'  -- Looking at 2025 data
GROUP BY ACTIVE_DATE_UTC
ORDER BY ACTIVE_DATE_UTC
LIMIT 50; 