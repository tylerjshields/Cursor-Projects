-- Create the target table for notification exposures
CREATE TABLE IF NOT EXISTS proddb.tylershields.notif_exposures (
    consumer_id NUMBER,
    run_date DATE
);

-- Create temporary table for dates
CREATE OR REPLACE TEMPORARY TABLE tmp_date_spine AS
SELECT DATEADD('day', -seq4() - 1, CURRENT_DATE()) as run_date
FROM TABLE(GENERATOR(ROWCOUNT => 7))
ORDER BY run_date DESC;

-- Insert historical exposures into the table
INSERT INTO proddb.tylershields.notif_exposures (consumer_id, run_date)
WITH nv_stores AS (
    SELECT DISTINCT
        store_id
    FROM edw.cng.dimension_new_vertical_store_tags
    WHERE country_id = 1
        AND (vertical_name IN ('Alcohol','3P Convenience','1P Convenience','Flowers','Pets','Emerging Retail') OR business_sub_type IN ('Grocery'))
),

nv_dormant_churned_cx AS (
    SELECT DISTINCT 
        dd.creator_id,
        d.run_date
    FROM edw.finance.dimension_deliveries dd
    CROSS JOIN tmp_date_spine d
    LEFT JOIN nv_stores nv ON nv.store_id = dd.store_id
    LEFT JOIN proddb.ml.fact_cx_vertical_targeting_score_v1 ml ON dd.creator_id = ml.consumer_id 
    WHERE dd.is_filtered_core = 1 
        AND dd.country_id = 1
        AND dd.submarket_id != 6
        AND (ml.CAF_CS_CONSUMER_7D_ALCOHOL_TRIAL_SCORE >= 0.35
             OR ml.CAF_CS_CONSUMER_7D_GROCERY_TRIAL_SCORE >= 0.35
             OR ml.CAF_CS_CONSUMER_7D_3PC_TRIAL_SCORE >= 0.35
             OR ml.CAF_CS_CONSUMER_7D_ALCOHOL_ORDER_SCORE >= 0.35
             OR ml.CAF_CS_CONSUMER_7D_GROCERY_ORDER_SCORE >= 0.35
             OR ml.CAF_CS_CONSUMER_7D_3PC_ORDER_SCORE >= 0.35)
    GROUP BY dd.creator_id, d.run_date
    HAVING MAX(dd.active_date) BETWEEN d.run_date - 270 AND d.run_date - 29
),

pre_trial AS (
    SELECT DISTINCT 
        dd.creator_id,
        d.run_date
    FROM edw.finance.dimension_deliveries dd
    CROSS JOIN tmp_date_spine d
    LEFT JOIN nv_stores nv ON nv.store_id = dd.store_id
    LEFT JOIN proddb.ml.fact_cx_vertical_targeting_score_v1 ml ON dd.creator_id = ml.consumer_id 
    WHERE dd.is_filtered_core = 1 
        AND dd.country_id = 1
        AND dd.submarket_id != 6
        AND nv.store_id IS NULL
        AND (ml.CAF_CS_CONSUMER_7D_ALCOHOL_TRIAL_SCORE >= 0.35
            OR ml.CAF_CS_CONSUMER_7D_GROCERY_TRIAL_SCORE >= 0.35
            OR ml.CAF_CS_CONSUMER_7D_3PC_TRIAL_SCORE >= 0.35
            OR ml.CAF_CS_CONSUMER_7D_ALCOHOL_ORDER_SCORE >= 0.35
            OR ml.CAF_CS_CONSUMER_7D_GROCERY_ORDER_SCORE >= 0.35
            OR ml.CAF_CS_CONSUMER_7D_3PC_ORDER_SCORE >= 0.35)
    GROUP BY dd.creator_id, d.run_date
    HAVING MAX(dd.active_date) < d.run_date - 120
),

combined_cx AS (
    SELECT creator_id, run_date
    FROM nv_dormant_churned_cx
    
    UNION DISTINCT
    
    SELECT creator_id, run_date
    FROM pre_trial
),

subscribe_cx AS (
    SELECT DISTINCT 
        s.consumer_id
    FROM edw.consumer.dimension_consumer_push_settings s
    JOIN EDW.GROWTH.CX360_MODEL_DLCOPY c
        ON c.consumer_id = s.consumer_id
        AND is_reachable_push_marketing = TRUE
        AND is_blacklisted = FALSE
        AND is_fraud = FALSE
        AND recommendations_status = 'on'
),

all_exposures AS (
    SELECT 
        c.creator_id as consumer_id,
        c.run_date
    FROM combined_cx c
    JOIN subscribe_cx s
        ON c.creator_id = s.consumer_id
    -- Snowflake's DAYOFWEEK returns 0-6 (Sunday=0)
    -- Modulo 7 of creator_id will also give 0-6
    WHERE MOD(c.creator_id, 7) = DAYOFWEEK(c.run_date)
)

SELECT 
    consumer_id,
    MIN(run_date) as run_date
FROM all_exposures
GROUP BY consumer_id;

-- Verify the results and show the date distribution
SELECT 
    run_date,
    COUNT(DISTINCT consumer_id) as consumer_count
FROM proddb.tylershields.notif_exposures
GROUP BY run_date
ORDER BY run_date;

-- Show the actual dates we generated
SELECT * FROM tmp_date_spine ORDER BY run_date;