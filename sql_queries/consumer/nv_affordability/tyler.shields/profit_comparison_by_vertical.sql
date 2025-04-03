-- Compare average variable profit metrics across tables with vertical breakdown
-- Tables: dimension_deliveries, fact_delivery_allocation, fact_delivery_allocation_custom
-- Write results to proddb.public.tmp_ts_nv_mvp

-- First, create or replace the table
CREATE OR REPLACE TABLE proddb.public.tmp_ts_nv_mvp AS

WITH delivery_data AS (
    SELECT 
        dd.delivery_id,
        dd.created_at_local_date AS date,
        dd.variable_profit AS dd_variable_profit,
        fda.variable_profit AS fda_variable_profit,
        fdac.marginal_variable_profit AS fdac_marginal_variable_profit,
        COALESCE(nvst.vertical_name, 'Rx') AS vertical_name
    FROM edw.public.dimension_deliveries dd
    LEFT JOIN edw.finance.fact_delivery_allocation fda
        ON dd.delivery_id = fda.delivery_id
    LEFT JOIN edw.finance.fact_delivery_allocation_custom fdac
        ON dd.delivery_id = fdac.delivery_id
        AND fdac.active_date_utc = dd.created_at_local_date
    LEFT JOIN edw.cx.dimension_new_vertical_store_tags nvst
        ON dd.store_id = nvst.store_id
    WHERE dd.created_at_local_date >= DATEADD(MONTH, -3, CURRENT_DATE())
      AND dd.is_filtered_core = TRUE
)

-- Results by vertical - put into the table
SELECT 
    vertical_name,
    COUNT(DISTINCT delivery_id) AS delivery_count,
    
    -- Variable profit metrics from dimension_deliveries
    AVG(dd_variable_profit) AS avg_dd_variable_profit,
    MEDIAN(dd_variable_profit) AS median_dd_variable_profit,
    
    -- Variable profit metrics from fact_delivery_allocation
    AVG(fda_variable_profit) AS avg_fda_variable_profit,
    MEDIAN(fda_variable_profit) AS median_fda_variable_profit,
    
    -- Marginal variable profit from fact_delivery_allocation_custom
    AVG(fdac_marginal_variable_profit) AS avg_fdac_marginal_variable_profit,
    MEDIAN(fdac_marginal_variable_profit) AS median_fdac_marginal_variable_profit,
    
    -- Differences between tables
    AVG(dd_variable_profit - fda_variable_profit) AS avg_dd_fda_diff,
    AVG(dd_variable_profit - fdac_marginal_variable_profit) AS avg_dd_fdac_diff,
    AVG(fda_variable_profit - fdac_marginal_variable_profit) AS avg_fda_fdac_diff,
    'By Vertical' AS grouping_type
FROM delivery_data
GROUP BY vertical_name

UNION ALL

-- Overall metrics - add to the same table
SELECT 
    'OVERALL' AS vertical_name,
    COUNT(DISTINCT delivery_id) AS delivery_count,
    
    -- Variable profit metrics from dimension_deliveries
    AVG(dd_variable_profit) AS avg_dd_variable_profit,
    MEDIAN(dd_variable_profit) AS median_dd_variable_profit,
    
    -- Variable profit metrics from fact_delivery_allocation
    AVG(fda_variable_profit) AS avg_fda_variable_profit,
    MEDIAN(fda_variable_profit) AS median_fda_variable_profit,
    
    -- Marginal variable profit from fact_delivery_allocation_custom
    AVG(fdac_marginal_variable_profit) AS avg_fdac_marginal_variable_profit,
    MEDIAN(fdac_marginal_variable_profit) AS median_fdac_marginal_variable_profit,
    
    -- Differences between tables
    AVG(dd_variable_profit - fda_variable_profit) AS avg_dd_fda_diff,
    AVG(dd_variable_profit - fdac_marginal_variable_profit) AS avg_dd_fdac_diff,
    AVG(fda_variable_profit - fdac_marginal_variable_profit) AS avg_fda_fdac_diff,
    'Overall' AS grouping_type
FROM delivery_data
ORDER BY delivery_count DESC;

-- Confirm that data was written
SELECT * FROM proddb.public.tmp_ts_nv_mvp; 