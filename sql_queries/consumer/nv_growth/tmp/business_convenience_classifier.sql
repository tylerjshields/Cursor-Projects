-- Classify business names based on convenience store resemblance
-- First check the structure of the source table
DESC TABLE proddb.hudsonmcculloch.business_names_convenience;

-- Create a temporary table to store the classification results
CREATE OR REPLACE TABLE proddb.public.tmp_convenience AS
WITH business_classifier AS (
    SELECT 
        business_name,
        CASE
            -- High confidence convenience stores (score 90-100)
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(7[ -]?eleven|circle[ -]?k|ampm|am[ -]?pm|kwik[ -]?e[ -]?mart|quick[ -]?e[ -]?mart|quickie[ -]?mart|quick[ -]?mart|mini[ -]?mart|minimart|ez[ -]?mart|quik[ -]?mart|fastlane|pit[ -]?stop|24[ -]?hour|24[ -]?hr|express[ -]?mart|expressmart)\\b') THEN 95
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(711|speedway|casey\'?s|wawa|sheetz|racetrac|raceway|sunoco|chevron|exxon|shell|mobil|bp|phillips|valero|marathon|royal[ -]?farms|thorntons|kum[ -]?&[ -]?go|rutters|cenex|mapco|loves)\\b') THEN 90
            
            -- Strong convenience indicators (score 80-89)
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(convenience|conv|c-store|c[ -]?store|corner[ -]?store|corner[ -]?mart|corner[ -]?stop|food[ -]?mart|pantry|gas[ -]?n[ -]?go|gas[ -]?&[ -]?go)\\b') THEN 85
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(mart|market|stop[ -]?n[ -]?go|stop[ -]?&[ -]?go|quick|quik|kwik|express|ez|short[ -]?stop|pit[ -]?stop)\\b') 
                AND NOT REGEXP_LIKE(LOWER(business_name), '\\b(super|grocery|wholesale|department|walmart|target)\\b') THEN 80
            
            -- Medium confidence (score 60-79)
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(gas|fuel|filling[ -]?station|service[ -]?station|petro|petroleum)\\b') THEN 75
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(liquor|beer|wine|tobacco|smoke|vape|cigarette)\\b') 
                AND REGEXP_LIKE(LOWER(business_name), '\\b(store|shop|mart|market|express|stop|outlet)\\b') THEN 70
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(deli|food|grocery|grocer)\\b') 
                AND NOT REGEXP_LIKE(LOWER(business_name), '\\b(restaurant|super|wholesale|walmart|kroger|safeway|publix|albertsons)\\b')
                AND LENGTH(business_name) < 20 THEN 65
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(quick|kwik|fast|speedy|rapid|swift|express|ez|easy)\\b')
                AND REGEXP_LIKE(LOWER(business_name), '\\b(shop|store|buy|go|stop|way)\\b') THEN 60
                
            -- Lower confidence (score 30-59)
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(store|shop)\\b') 
                AND NOT REGEXP_LIKE(LOWER(business_name), '\\b(department|clothing|apparel|furniture|hardware|super|electronic|pet|toy)\\b')
                AND LENGTH(business_name) < 25 THEN 50
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(mini|small|local|neighborhood|town|village|community)\\b')
                AND REGEXP_LIKE(LOWER(business_name), '\\b(store|shop|mart|market|grocer)\\b') THEN 45
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(snack|candy|coffee|donut|bakery)\\b')
                AND NOT REGEXP_LIKE(LOWER(business_name), '\\b(restaurant|cafe|bistro|grill)\\b') THEN 40
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(general|variety|discount)\\b')
                AND REGEXP_LIKE(LOWER(business_name), '\\b(store|shop|mart|market)\\b') THEN 35
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(food|grocery|grocer)\\b') THEN 30
            
            -- Very low confidence (score < 30)
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(market|mart)\\b') THEN 20
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(store|shop)\\b') THEN 15
            
            -- Default case
            ELSE 0
        END AS convenience_score,
        
        -- Add some contextual notes about the classification
        CASE 
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(7[ -]?eleven|circle[ -]?k|ampm|am[ -]?pm)\\b') THEN 'Major convenience chain'
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(convenience|conv|c-store|c[ -]?store|corner[ -]?store)\\b') THEN 'Explicit convenience indicator'
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(gas|fuel|filling[ -]?station|service[ -]?station)\\b') THEN 'Gas station (often has convenience store)'
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(quick|kwik|fast|express|ez|easy)\\b') THEN 'Quick-service naming pattern'
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(mart|market|stop)\\b') THEN 'Mart/Market naming pattern'
            WHEN REGEXP_LIKE(LOWER(business_name), '\\b(liquor|beer|wine|tobacco)\\b') THEN 'Specialty store with convenience aspects'
            ELSE 'Other store type'
        END AS classification_reason
    FROM proddb.hudsonmcculloch.business_names_convenience
)

-- Select the top 100 businesses by convenience score
SELECT 
    business_name,
    convenience_score,
    classification_reason
FROM business_classifier
ORDER BY convenience_score DESC, business_name
LIMIT 100;

-- Show result
SELECT * FROM proddb.public.tmp_convenience
ORDER BY convenience_score DESC, business_name
LIMIT 10; 