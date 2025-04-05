-- Business questions based on LCM carousel data
-- Using the proddb.public.fact_store_impressions_lcm table
-- Combined metrics by date and carousel

WITH impressions AS (
    -- Base impression metrics 
    SELECT 
        date,
        carousel_category,
        COUNT(*) AS impression_count,
        COUNT(DISTINCT consumer_id) AS unique_users,
        COUNT(DISTINCT session_id) AS carousel_sessions,
        SUM(CASE WHEN store_page_visitor = TRUE THEN 1 ELSE 0 END) AS click_count
    FROM proddb.public.fact_store_impressions_lcm
    GROUP BY date, carousel_category
),

position_metrics AS (
    -- Position metrics for each carousel (horizontal and vertical)
    SELECT 
        date,
        carousel_category,
        -- Horizontal position metrics (card_position)
        AVG(card_position) AS avg_card_position,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY card_position) AS p20_card_position,
        MEDIAN(card_position) AS median_card_position,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY card_position) AS p80_card_position,
        
        -- Vertical position metrics (vertical_position)
        AVG(vertical_position) AS avg_vertical_position,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY vertical_position) AS p20_vertical_position,
        MEDIAN(vertical_position) AS median_vertical_position,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY vertical_position) AS p80_vertical_position
    FROM proddb.public.fact_store_impressions_lcm
    GROUP BY date, carousel_category
)
-- , all_sessions AS (
--     -- Count of all explore_page sessions per day from source table
--     SELECT 
--         DATE_TRUNC('day', EVENT_DATE) AS date,
--         COUNT(DISTINCT session_id) AS total_sessions
--     FROM edw.consumer.fact_store_impressions
--     WHERE EVENT_DATE >= DATEADD('day', -7, CURRENT_DATE())
--     AND page = 'explore_page'
--     GROUP BY 1
-- )

-- Combine all metrics
SELECT 
    i.date,
    i.carousel_category,
    
    -- Impression metrics
    i.impression_count,
    i.unique_users,
    
    -- Click metrics
    i.click_count,
    i.click_count / NULLIF(i.impression_count, 0) AS ctr,
    
    -- User engagement metrics
    i.impression_count / NULLIF(i.unique_users, 0) AS avg_impressions_per_user,
    
    -- Session metrics
    i.carousel_sessions,
    -- s.total_sessions,
    -- i.carousel_sessions / NULLIF(s.total_sessions, 0) AS session_pct,
    
    -- Position metrics - horizontal
    p.avg_card_position,
    p.p20_card_position,
    p.median_card_position,
    p.p80_card_position,
    
    -- Position metrics - vertical
    p.avg_vertical_position,
    p.p20_vertical_position,
    p.median_vertical_position,
    p.p80_vertical_position
    
FROM impressions i
LEFT JOIN position_metrics p 
    ON i.date = p.date AND i.carousel_category = p.carousel_category
-- LEFT JOIN all_sessions s 
--     ON i.date = s.date

-- NOTE: These session percentage numbers are understated relative to what we would expect if launched to 100%,
-- as they don't account for users who are not in the experiment or are in control groups.
-- The actual percentages in a full launch would likely be higher.

ORDER BY i.date DESC, i.impression_count DESC; 