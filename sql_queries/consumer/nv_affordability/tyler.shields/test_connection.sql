-- Simple test query to verify Snowflake connection
SELECT 
    CURRENT_USER() AS current_user,
    CURRENT_ROLE() AS current_role,
    CURRENT_WAREHOUSE() AS current_warehouse,
    CURRENT_DATABASE() AS current_database,
    CURRENT_SCHEMA() AS current_schema,
    CURRENT_TIMESTAMP() AS current_time; 