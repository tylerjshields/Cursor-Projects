-- Comparison of Click-Through Rates (CTR) among different carousel types
-- Using fact_consumer_carousel_impressions table

WITH carousel_impressions AS (
    SELECT
        event_date,
        container_name,
        session_id,
        COUNT(*) AS impression_count,
        SUM(CASE WHEN is_clicked_flg = 1 THEN 1 ELSE 0 END) AS click_count
    FROM
        edw.consumer.fact_consumer_carousel_impressions
    WHERE
        event_date >= DATEADD('day', -7, CURRENT_DATE())
        AND discovery_surface = 'Home Page'
        AND experience = 'doordash'
        AND (carousel_status = true OR carousel_status IS NULL)
        AND platform = 'ios'
        AND merchant_country = 'US'
        AND container_name IN (
            'Recently Viewed Items',
            'Buy it again',
            'Continue shopping',
            'Pet supplies near you',
            'Late night munchies',
            'Food & alcohol, together',
            'Grocery',
            'Quick essentials nearby',
            'Convenience & drugstores',
            'Convenience & grocery',
            'Drinks and snacks',
            'Fresh fruits & vegetables',
            'Alcohol',
            'Late night cravings',
            'Saved stores',
            'National favorites',
            'Your usuals'
        )
    GROUP BY
        event_date,
        container_name,
        session_id
)

-- Daily aggregation by carousel type
SELECT
    event_date,
    container_name,
    COUNT(DISTINCT session_id) AS unique_sessions,
    SUM(impression_count) AS total_impressions,
    SUM(click_count) AS total_clicks,
    SUM(click_count) / NULLIF(SUM(impression_count), 0) * 100.0 AS ctr_percentage,
    
    -- Flag sessions with at least one click
    SUM(CASE WHEN click_count > 0 THEN 1 ELSE 0 END) AS sessions_with_clicks,
    
    -- Calculate percentage of sessions with at least one click
    SUM(CASE WHEN click_count > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT session_id), 0) * 100.0 AS percent_sessions_with_clicks
FROM
    carousel_impressions
GROUP BY
    event_date,
    container_name
ORDER BY
    event_date DESC,
    unique_sessions DESC; 