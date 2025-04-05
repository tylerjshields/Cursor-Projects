-- Business questions based on LCM carousel data
-- Using the persisted proddb.tylershields.tmp_fcci_lcm table

-- 1. Impression count of each LCM carousel per day
SELECT 
    event_date AS date,
    carousel_type,
    COUNT(*) AS impression_count
FROM proddb.tylershields.tmp_fcci_lcm
GROUP BY date, carousel_type
ORDER BY date DESC, impression_count DESC;

-- 2. CTR (Click-Through Rate) of each carousel
-- is_clicked_flg = 1 implies a click
SELECT 
    event_date AS date,
    carousel_type,
    COUNT(*) AS impression_count,
    SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) AS click_count,
    SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS ctr
FROM proddb.tylershields.tmp_fcci_lcm
GROUP BY date, carousel_type
ORDER BY date DESC, ctr DESC;

-- 3. Position metrics for each carousel (avg, p20, median, p80)
-- For vertical position
SELECT 
    event_date AS date,
    carousel_type,
    -- Vertical position metrics
    AVG(vertical_position) AS avg_vertical_position,
    PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY vertical_position) AS p20_vertical_position,
    MEDIAN(vertical_position) AS median_vertical_position,
    PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY vertical_position) AS p80_vertical_position
FROM proddb.tylershields.tmp_fcci_lcm
GROUP BY date, carousel_type
ORDER BY date DESC, carousel_type;

-- 4. Average impressions per user
SELECT 
    event_date AS date,
    carousel_type,
    COUNT(*) AS total_impressions,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(*) / NULLIF(COUNT(DISTINCT session_id), 0) AS avg_impressions_per_session
FROM proddb.tylershields.tmp_fcci_lcm
GROUP BY date, carousel_type
ORDER BY date DESC, carousel_type;

-- 5. Percentage of sessions when each LCM carousel is shown (sessions shown / all sessions)
-- NOTE: These numbers are understated relative to what we would expect if launched to 100%,
-- as they don't account for users who are not in the experiment or are in control groups.
-- The actual percentages in a full launch would likely be higher.
WITH all_sessions AS (
    -- Get count of all Home Page sessions per day from source table
    SELECT 
        DATE_TRUNC('day', event_date) AS date,
        COUNT(DISTINCT session_id) AS total_sessions
    FROM edw.consumer.fact_consumer_carousel_impressions
    WHERE event_date >= DATEADD('day', -7, CURRENT_DATE())
    AND discovery_surface = 'Home Page'
    GROUP BY 1
),
lcm_sessions AS (
    -- Get count of sessions per carousel category
    SELECT 
        event_date AS date,
        carousel_type,
        COUNT(DISTINCT session_id) AS carousel_sessions
    FROM proddb.tylershields.tmp_fcci_lcm
    GROUP BY 1, 2
)
SELECT 
    a.date,
    l.carousel_type,
    l.carousel_sessions,
    a.total_sessions,
    l.carousel_sessions / NULLIF(a.total_sessions, 0) AS session_pct
FROM all_sessions a
LEFT JOIN lcm_sessions l ON a.date = l.date
ORDER BY a.date DESC, l.carousel_sessions DESC;

-- 6. Combined metrics query (all in one view)
WITH impressions AS (
    SELECT 
        event_date AS date,
        carousel_type,
        COUNT(*) AS impression_count,
        COUNT(DISTINCT session_id) AS unique_sessions,
        SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) AS click_count
    FROM proddb.tylershields.tmp_fcci_lcm
    GROUP BY date, carousel_type
),

position_metrics AS (
    SELECT 
        event_date AS date,
        carousel_type,
        AVG(vertical_position) AS avg_vertical_position,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY vertical_position) AS p20_vertical_position,
        MEDIAN(vertical_position) AS median_vertical_position,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY vertical_position) AS p80_vertical_position
    FROM proddb.tylershields.tmp_fcci_lcm
    GROUP BY date, carousel_type
),

all_sessions AS (
    SELECT 
        DATE_TRUNC('day', event_date) AS date,
        COUNT(DISTINCT session_id) AS total_sessions
    FROM edw.consumer.fact_consumer_carousel_impressions
    WHERE event_date >= DATEADD('day', -7, CURRENT_DATE())
    AND discovery_surface = 'Home Page'
    GROUP BY 1
)

SELECT 
    i.date,
    i.carousel_type,
    
    -- Impression metrics
    i.impression_count,
    i.unique_sessions,
    
    -- Click metrics
    i.click_count,
    i.click_count / NULLIF(i.impression_count, 0) AS ctr,
    
    -- User engagement metrics
    i.impression_count / NULLIF(i.unique_sessions, 0) AS avg_impressions_per_session,
    
    -- Session metrics
    i.unique_sessions AS carousel_sessions,
    s.total_sessions,
    i.unique_sessions / NULLIF(s.total_sessions, 0) AS session_pct,
    
    -- Position metrics - vertical
    p.avg_vertical_position,
    p.p20_vertical_position,
    p.median_vertical_position,
    p.p80_vertical_position
    
FROM impressions i
LEFT JOIN position_metrics p 
    ON i.date = p.date AND i.carousel_type = p.carousel_type
LEFT JOIN all_sessions s 
    ON i.date = s.date

ORDER BY i.date DESC, i.impression_count DESC; 