with table1 as(
	SELECT host_id
    FROM warehouse.fact_listings
    GROUP BY host_id
    HAVING COUNT(listing_id) > 1
),
table2 as (
	select
		table1.host_id,
		table2.host_neighbourhood,
		table2.listing_neighbourhood
			from table1
				join warehouse.fact_listings table2
				on table1.host_id = table2.host_id
),
table3 as (
select
	table2.*,
	ls.lga_name as host_neighbourhood_lga
		from table2	
			join staging.lga_stg ls	
				on ls.lga_suburb = table2.host_neighbourhood 
),
table4 as (
select
	table3.*,
	ls.lga_name as listing_neighbourhood_lga
		from table3	
			join staging.lga_stg ls	
				on ls.lga_suburb = table3.listing_neighbourhood 
)
select 
	COUNT(case when host_neighbourhood_lga = listing_neighbourhood then 1 end) as same_neighbourhood,
	COUNT(case when host_neighbourhood_lga != listing_neighbourhood then 1 end) as different_neighbourhood
	from table4 ;

