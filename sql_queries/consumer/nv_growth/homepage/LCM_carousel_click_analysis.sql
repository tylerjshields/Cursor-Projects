-- Analysis of whether users click on the first carousel item they see
-- Focusing on our three carousels of interest

WITH first_viewed AS (
    SELECT
        session_id,
        container_name,
        vertical_position,
        is_clicked_flg as clicked,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY vertical_position ASC) AS row_num,
        COUNT(DISTINCT vertical_position) OVER (PARTITION BY session_id) AS content_viewed_count
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
),
session_click_info AS (
    SELECT
        container_name,
        session_id,
        MAX(CASE WHEN clicked = 1 THEN vertical_position END) AS clicked_position,
        MIN(vertical_position) AS first_position
    FROM
        first_viewed
    WHERE
        content_viewed_count >= 2 -- use to understand ctr if cx view at least 2 pieces of content to know if the first one was correct or not
    GROUP BY
        container_name,
        session_id
    HAVING
        MAX(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) = 1 -- ensure at least one click per session
)
SELECT
    container_name,
    COUNT(DISTINCT session_id) AS num_sessions,
    100.0 * COUNT(CASE WHEN clicked_position = first_position THEN 1 END) / COUNT(*) AS percent_clicks_on_top_position,
    AVG(clicked_position) AS avg_clicked_position,
    AVG(first_position) AS avg_first_position
FROM
    session_click_info
GROUP BY 
    container_name
ORDER BY 
    num_sessions DESC; 