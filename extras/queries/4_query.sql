WITH UniqueListingHosts AS (
    SELECT host_id, listing_neighbourhood, SUM(price * (30 - availability_next_30_days)) AS estimated_revenue
    FROM warehouse.fact_listings
    GROUP BY host_id, listing_neighbourhood
    HAVING COUNT(listing_id) = 1
)
SELECT 
    ulh.listing_neighbourhood,
    COUNT(ulh.host_id) AS number_of_unique_hosts,
    SUM(CASE WHEN ulh.estimated_revenue >= table3.median_mortgage_repay_monthly * 12 THEN 1 ELSE 0 END) AS hosts_covering_mortgage,
    SUM(CASE WHEN ulh.estimated_revenue < table3.median_mortgage_repay_monthly * 12 THEN 1 ELSE 0 END) AS hosts_not_covering_mortgage
FROM UniqueListingHosts ulh
		join staging.lga_stg table2
			on ulh.listing_neighbourhood = table2.lga_name 
		join staging.census_stg table3
			on table2.lga_code::text = substring(table3.lga_code, 4)
GROUP BY ulh.listing_neighbourhood
ORDER BY listing_neighbourhood;
