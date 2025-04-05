-- Analysis of carousel position distribution
-- Focusing on our three carousels of interest

WITH carousel_positions AS (
    SELECT
        event_date,
        container_name,
        vertical_position,
        COUNT(*) AS position_count,
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
            'Continue shopping'
        )
    GROUP BY
        event_date,
        container_name,
        vertical_position
)

SELECT
    container_name,
    
    -- Position statistics
    MIN(vertical_position) AS min_position,
    MAX(vertical_position) AS max_position,
    AVG(vertical_position) AS avg_position,
    MEDIAN(vertical_position) AS median_position,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vertical_position) AS p25_position,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vertical_position) AS p75_position,
    
    -- Click statistics by position
    SUM(position_count) AS total_impressions,
    SUM(click_count) AS total_clicks,
    SUM(click_count) / NULLIF(SUM(position_count), 0) * 100.0 AS overall_ctr
FROM
    carousel_positions
GROUP BY
    container_name
ORDER BY
    avg_position ASC; 