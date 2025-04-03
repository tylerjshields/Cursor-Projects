-- Create a query to assign a "perceived as convenience store" score
-- First, examine the structure of the table
DESC TABLE proddb.hudsonmcculloch.sku_count_by_store_adding_sales_data_store_tagging_thresholds_final_test_and_high_count;

-- Select all columns plus a calculated convenience score
SELECT 
    *,
    -- Create a weighted convenience score based on multiple factors
    -- without using the tagged_as_convenience columns
    CASE 
        -- If the store name clearly indicates a convenience store, give it a high base score
        WHEN REGEXP_LIKE(LOWER(STORE_NAME), '\\b(7[ -]?eleven|circle[ -]?k|ampm|am[ -]?pm|kwik[ -]?e[ -]?mart|quick[ -]?e[ -]?mart|kwik[ -]?mart|stop[ -]?n[ -]?go|mini[ -]?mart|express[ -]?mart|pantry|quick[ -]?stop|speedway|casey|wawa|sheetz)\\b') THEN 80
        -- If the store name contains convenience-related keywords
        WHEN REGEXP_LIKE(LOWER(STORE_NAME), '\\b(convenience|conv|corner|quick|kwik|quik|gas|mart)\\b') THEN 60
        -- Default base score
        ELSE 20
    END

    -- Add points based on SKU counts and variety indicators
    + CASE WHEN DISTINCT_SKUS < 100 THEN 10
           WHEN DISTINCT_SKUS BETWEEN 100 AND 500 THEN 15
           WHEN DISTINCT_SKUS BETWEEN 500 AND 2000 THEN 10
           WHEN DISTINCT_SKUS BETWEEN 2000 AND 5000 THEN 0
           WHEN DISTINCT_SKUS > 5000 THEN -10 -- Too many SKUs indicates supermarket
           ELSE 0
      END

    -- Add points for high percentages in convenience categories
    + CASE WHEN TOBACCO_PERC >= 0.05 THEN 15 ELSE 0 END
    + CASE WHEN ALCOHOL_PERC >= 0.15 THEN 15 ELSE 0 END
    + CASE WHEN CANDY_PERC >= 0.08 THEN 10 ELSE 0 END
    + CASE WHEN SNACK_PERC >= 0.10 THEN 10 ELSE 0 END
    + CASE WHEN BEVERAGE_PERC >= 0.15 THEN 10 ELSE 0 END
    
    -- Add points for high customer frequency indicators
    + CASE WHEN DISTINCT_CX_COUNT_180D >= 1000 AND ORDERS_PER_CX <= 3 THEN 10 ELSE 0 END
    
    -- Subtract points for indicators that suggest non-convenience formats
    - CASE WHEN PRODUCE_PERC > 0.10 THEN 15 ELSE 0 END -- High produce suggests grocery
    - CASE WHEN FROZEN_PERC > 0.15 THEN 10 ELSE 0 END -- High frozen suggests grocery
    - CASE WHEN MEAT_PERC > 0.10 THEN 10 ELSE 0 END -- High meat suggests grocery

    AS PERCEIVED_CONVENIENCE_SCORE

FROM proddb.hudsonmcculloch.sku_count_by_store_adding_sales_data_store_tagging_thresholds_final_test_and_high_count

-- Optional: Order by the new score
ORDER BY PERCEIVED_CONVENIENCE_SCORE DESC; 