-- Create a table with business names and convenience scores directly
CREATE OR REPLACE TABLE proddb.public.tmp_convenience AS
SELECT 
    business_name,
    -- Directly assign scores based on well-known convenience patterns
    CASE
        -- Major convenience chains (score 95-100)
        WHEN business_name ILIKE '%7-ELEVEN%' OR business_name ILIKE '%7 ELEVEN%' OR business_name ILIKE '%SEVEN ELEVEN%' THEN 100
        WHEN business_name ILIKE '%CIRCLE K%' THEN 98
        WHEN business_name ILIKE '%WAWA%' THEN 97
        WHEN business_name ILIKE '%CASEY%S%' OR business_name ILIKE '%CASEYS%' THEN 96
        WHEN business_name ILIKE '%SPEEDWAY%' THEN 95
        
        -- Well-known gas station convenience stores (90-94)
        WHEN business_name ILIKE '%SHELL%' AND (business_name ILIKE '%FOOD%' OR business_name ILIKE '%MART%' OR business_name ILIKE '%STORE%') THEN 94
        WHEN business_name ILIKE '%CHEVRON%' AND (business_name ILIKE '%FOOD%' OR business_name ILIKE '%MART%' OR business_name ILIKE '%STORE%') THEN 93
        WHEN business_name ILIKE '%EXXON%' AND (business_name ILIKE '%FOOD%' OR business_name ILIKE '%MART%' OR business_name ILIKE '%STORE%') THEN 92
        WHEN business_name ILIKE '%BP%' AND (business_name ILIKE '%FOOD%' OR business_name ILIKE '%MART%' OR business_name ILIKE '%STORE%') THEN 91
        WHEN business_name ILIKE '%MOBIL%' AND (business_name ILIKE '%FOOD%' OR business_name ILIKE '%MART%' OR business_name ILIKE '%STORE%') THEN 90
        
        -- Explicit convenience indicators (80-89)
        WHEN business_name ILIKE '%CONVENIENCE%' THEN 89
        WHEN business_name ILIKE '%QUICK MART%' OR business_name ILIKE '%KWIK MART%' OR business_name ILIKE '%QUIK MART%' THEN 88
        WHEN business_name ILIKE '%MINI MART%' OR business_name ILIKE '%MINIMART%' THEN 87
        WHEN business_name ILIKE '%CORNER STORE%' THEN 86
        WHEN business_name ILIKE '%EXPRESS MART%' OR business_name ILIKE '%EXPRESSMART%' THEN 85
        WHEN business_name ILIKE '%PANTRY%' THEN 84
        WHEN business_name ILIKE '%FOOD MART%' THEN 83
        WHEN business_name ILIKE '%GAS %' AND business_name ILIKE '% MART%' THEN 82
        WHEN business_name ILIKE '%QUICK STOP%' OR business_name ILIKE '%KWIK STOP%' OR business_name ILIKE '%QUIK STOP%' THEN 81
        WHEN business_name ILIKE '%STOP N GO%' OR business_name ILIKE '%STOP & GO%' THEN 80
        
        -- Medium confidence (70-79)
        WHEN business_name ILIKE '%LIQUOR%' AND business_name ILIKE '%MART%' THEN 79
        WHEN business_name ILIKE '%GAS%' AND business_name ILIKE '%STATION%' THEN 78
        WHEN business_name ILIKE '%DELI MART%' OR business_name ILIKE '%DELI & MART%' THEN 77
        WHEN business_name ILIKE '%MARKET%' AND LENGTH(business_name) < 20 THEN 76
        WHEN business_name ILIKE '%TOBACCO%' AND business_name ILIKE '%MART%' THEN 75
        WHEN business_name ILIKE '%24 HOUR%' OR business_name ILIKE '%24HR%' OR business_name ILIKE '%24-HOUR%' THEN 74
        WHEN business_name ILIKE '%GENERAL STORE%' THEN 73
        WHEN business_name ILIKE '%NEIGHBORHOOD MARKET%' AND LENGTH(business_name) < 25 THEN 72
        WHEN business_name ILIKE '%DOLLAR %' THEN 71
        WHEN business_name ILIKE '%MART%' AND LENGTH(business_name) < 15 THEN 70
        
        -- Lower confidence (50-69)
        WHEN business_name ILIKE '%STORE%' AND business_name ILIKE '%GAS%' THEN 65
        WHEN business_name ILIKE '%GROCERY%' AND LENGTH(business_name) < 20 THEN 60
        WHEN business_name ILIKE '%MARKET%' AND business_name ILIKE '%FOOD%' THEN 55
        WHEN business_name ILIKE '%STORE%' AND LENGTH(business_name) < 15 THEN 50
        
        -- Low confidence (1-49)
        WHEN business_name ILIKE '%SHOP%' THEN 40
        WHEN business_name ILIKE '%MARKET%' THEN 30
        WHEN business_name ILIKE '%FOOD%' THEN 20
        WHEN business_name ILIKE '%STORE%' THEN 10
        
        -- Default
        ELSE 0
    END AS convenience_score
FROM proddb.hudsonmcculloch.business_names_convenience
ORDER BY convenience_score DESC, business_name
LIMIT 100;

-- Show the results
SELECT * FROM proddb.public.tmp_convenience; 