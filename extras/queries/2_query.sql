WITH Top5Neighbourhoods AS (
SELECT 
    LISTING_NEIGHBOURHOOD,
    SUM(estimated_revenue_per_active_listing) AS total_estimated_revenue_per_active_listing
FROM datamart.dm_listing_neighbourhood
GROUP BY listing_neighbourhood
ORDER BY total_estimated_revenue_per_active_listing DESC
LIMIT 5
),
BestListingTypes AS (
    SELECT
        l.LISTING_NEIGHBOURHOOD,
        l.property_type,
        l.room_type,
        l.accommodates,
        SUM((30-l.availability_next_30_days)) AS total_stays
    FROM warehouse.fact_listings l
    JOIN Top5Neighbourhoods t5
    ON l.LISTING_NEIGHBOURHOOD = t5.LISTING_NEIGHBOURHOOD
    GROUP BY 
        l.LISTING_NEIGHBOURHOOD, 
        l.property_type, 
        l.room_type, 
        l.accommodates
    ORDER BY 
        l.LISTING_NEIGHBOURHOOD, 
        total_stays DESC
)
SELECT 
    LISTING_NEIGHBOURHOOD,
    property_type,
    room_type,
    accommodates,
    total_stays
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY LISTING_NEIGHBOURHOOD ORDER BY total_stays DESC) AS rn
    FROM BestListingTypes
) AS ranked
WHERE rn = 1;
