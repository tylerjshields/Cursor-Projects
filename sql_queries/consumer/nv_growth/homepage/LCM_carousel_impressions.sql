-- Daily data for carousels using fact_consumer_carousel_impressions
-- Based on the columns and filters from the reference query

SELECT 
    event_date,
    container_name AS carousel_type,
    session_id,
    vertical_position,
    is_clicked_flg AS clicked,
    discovery_surface,
    experience,
    platform,
    merchant_country
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
        'Your usuals',
        -- Add our three carousels of interest
        'Recently Viewed Items',
        'Buy it again',
        'Continue shopping'
    )
ORDER BY 
    event_date DESC, 
    container_name,
    session_id; 