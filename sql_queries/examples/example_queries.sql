-- Example Snowflake SQL Queries
-- This file contains sample queries to demonstrate how to use Snowflake effectively

-- 1. Basic user and session information
SELECT 
    CURRENT_USER() AS username,
    CURRENT_ROLE() AS role,
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_DATABASE() AS database,
    CURRENT_SCHEMA() AS schema;

-- 2. List available databases
SELECT 
    DATABASE_NAME,
    CREATED,
    COMMENT
FROM INFORMATION_SCHEMA.DATABASES
ORDER BY CREATED DESC;

-- 3. List tables in current schema with row counts
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT,
    BYTES/1024/1024 AS SIZE_MB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
ORDER BY ROW_COUNT DESC;

-- 4. Example query with IFF and GROUP BY ALL
-- This demonstrates Snowflake best practices
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    product_category,
    SUM(order_amount) AS total_sales,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(IFF(is_first_purchase, order_amount, 0)) AS new_customer_sales,
    AVG(order_amount) AS avg_order_value
FROM sales_data
WHERE order_date >= DATEADD(year, -1, CURRENT_DATE())
    AND is_test_order = FALSE
GROUP BY ALL
ORDER BY order_month, product_category;

-- 5. Example using a CTE and window functions
WITH customer_orders AS (
    SELECT
        customer_id,
        order_id,
        order_date,
        order_amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_sequence,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date
    FROM sales_data
    WHERE is_test_order = FALSE
)
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(IFF(order_sequence = 1, customer_id, NULL)) AS new_customers,
    COUNT(IFF(order_sequence > 1, customer_id, NULL)) AS returning_customers,
    AVG(IFF(order_sequence > 1, DATEDIFF('day', prev_order_date, order_date), NULL)) AS avg_days_between_orders
FROM customer_orders
GROUP BY ALL
ORDER BY order_month; 