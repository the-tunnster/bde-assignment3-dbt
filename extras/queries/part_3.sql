------------------------------------------------------------------------------------------
--- Census data for the lowest and highest revenue earning neighbourhoods.
------------------------------------------------------------------------------------------
SELECT 
	table3.*
		FROM datamart.dm_listing_neighbourhood table1
		JOIN staging.lga_stg table2
			ON table1.listing_neighbourhood = table2.lga_name 
		JOIN staging.census_stg table3
			ON table2.lga_code::text = substring(table3.lga_code, 4)
		ORDER BY ESTIMATED_REVENUE_PER_ACTIVE_LISTING ASC
			LIMIT 1;

SELECT 
	table3.*
		FROM datamart.dm_listing_neighbourhood table1
		JOIN staging.lga_stg table2
			ON table1.listing_neighbourhood = table2.lga_name 
		JOIN staging.census_stg table3
			ON table2.lga_code::text = substring(table3.lga_code, 4)
		ORDER BY ESTIMATED_REVENUE_PER_ACTIVE_LISTING DESC
			LIMIT 1;


------------------------------------------------------------------------------------------
--- Best type of listing for top 5 neighbourhoods by revenue.
------------------------------------------------------------------------------------------
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
    					GROUP BY l.LISTING_NEIGHBOURHOOD, l.property_type, l.room_type, l.accommodates
    						ORDER BY l.LISTING_NEIGHBOURHOOD, total_stays DESC
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


------------------------------------------------------------------------------------------
--- Do hosts with multiple listings live in the same neighbourhood as the listings?
------------------------------------------------------------------------------------------
WITH table1 AS(
	SELECT host_id
    	FROM warehouse.fact_listings
    		GROUP BY host_id
    			HAVING COUNT(listing_id) > 1
),
table2 AS (
	SELECT
		table1.host_id,
		table2.host_neighbourhood,
		table2.listing_neighbourhood
			FROM table1
				JOIN warehouse.fact_listings table2
					ON table1.host_id = table2.host_id
),
table3 AS (
	SELECT
		table2.*,
		ls.lga_name AS host_neighbourhood_lga
			FROM table2	
				JOIN staging.lga_stg ls	
					ON ls.lga_suburb = table2.host_neighbourhood 
),
table4 AS (
	SELECT
		table3.*,
		ls.lga_name AS listing_neighbourhood_lga
			FROM table3	
				JOIN staging.lga_stg ls	
					ON ls.lga_suburb = table3.listing_neighbourhood 
)
SELECT 
	COUNT(CASE WHEN host_neighbourhood_lga = listing_neighbourhood THEN 1 END) AS same_neighbourhood,
	COUNT(CASE WHEN host_neighbourhood_lga != listing_neighbourhood THEN 1 END) AS different_neighbourhood
		FROM table4 ;


------------------------------------------------------------------------------------------
--- How many hosts with unique listings cover the nerghbouhood's average mortagege?
------------------------------------------------------------------------------------------
WITH UniqueListingHosts AS (
    SELECT
		host_id,
		listing_neighbourhood,
		SUM(price * (30 - availability_next_30_days)) AS estimated_revenue
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
				JOIN staging.lga_stg table2
					ON ulh.listing_neighbourhood = table2.lga_name 
				JOIN staging.census_stg table3
					ON table2.lga_code::text = substring(table3.lga_code, 4)
						GROUP BY ulh.listing_neighbourhood
							ORDER BY listing_neighbourhood;

