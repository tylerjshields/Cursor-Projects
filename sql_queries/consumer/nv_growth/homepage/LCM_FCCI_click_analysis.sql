-- Analysis of whether users click on the first carousel they see
-- Based on the persisted proddb.tylershields.tmp_fcci_lcm table

WITH first_viewed AS (
    SELECT
        session_id,
        carousel_type,
        vertical_position,
        clicked,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY vertical_position ASC) AS row_num,
        COUNT(DISTINCT vertical_position) OVER (PARTITION BY session_id) AS content_viewed_count
    FROM
        proddb.tylershields.tmp_fcci_lcm
),

session_click_info AS (
    SELECT
        carousel_type,
        session_id,
        MAX(CASE WHEN clicked = 1 THEN vertical_position END) AS clicked_position,
        MIN(vertical_position) AS first_position
    FROM
        first_viewed
    WHERE
        content_viewed_count >= 2 -- use to understand ctr if cx view at least 2 pieces of content to know if the first one was correct or not
    GROUP BY
        carousel_type, session_id
    HAVING
        MAX(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) = 1 -- ensure at least one click per session
)

-- Overall click behavior
SELECT
    'All Carousels' AS carousel_type,
    COUNT(DISTINCT session_id) AS num_sessions,
    COUNT(CASE WHEN clicked_position = first_position THEN 1 END) AS clicks_on_first_position,
    100.0 * COUNT(CASE WHEN clicked_position = first_position THEN 1 END) / COUNT(*) AS percent_clicks_on_top_position
FROM
    session_click_info

UNION ALL

-- Click behavior by carousel type
SELECT
    carousel_type,
    COUNT(DISTINCT session_id) AS num_sessions,
    COUNT(CASE WHEN clicked_position = first_position THEN 1 END) AS clicks_on_first_position,
    100.0 * COUNT(CASE WHEN clicked_position = first_position THEN 1 END) / COUNT(*) AS percent_clicks_on_top_position
FROM
    session_click_info
GROUP BY
    carousel_type
ORDER BY
    num_sessions DESC;

-- Deeper analysis by vertical position ranges
WITH position_data AS (
    SELECT
        carousel_type,
        CASE 
            WHEN vertical_position BETWEEN 0 AND 1 THEN '0-1 (Very Top)'
            WHEN vertical_position BETWEEN 1 AND 2 THEN '1-2'
            WHEN vertical_position BETWEEN 2 AND 3 THEN '2-3'
            WHEN vertical_position BETWEEN 3 AND 4 THEN '3-4'
            WHEN vertical_position > 4 THEN '4+ (Lower)'
            ELSE 'Unknown'
        END AS position_range,
        COUNT(*) AS impression_count,
        SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) AS click_count
    FROM
        proddb.tylershields.tmp_fcci_lcm
    GROUP BY
        carousel_type,
        position_range
)

SELECT
    carousel_type,
    position_range,
    impression_count,
    click_count,
    100.0 * click_count / NULLIF(impression_count, 0) AS ctr_percent
FROM
    position_data
ORDER BY
    carousel_type,
    position_range; 