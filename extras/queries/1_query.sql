select 
	table3.*
		from datamart.dm_listing_neighbourhood table1
		join staging.lga_stg table2
			on table1.listing_neighbourhood = table2.lga_name 
		join staging.census_stg table3
			on table2.lga_code::text = substring(table3.lga_code, 4)
		order by ESTIMATED_REVENUE_PER_ACTIVE_LISTING asc
			limit 1;
		
select 
	table3.*
		from datamart.dm_listing_neighbourhood table1
		join staging.lga_stg table2
			on table1.listing_neighbourhood = table2.lga_name 
		join staging.census_stg table3
			on table2.lga_code::text = substring(table3.lga_code, 4)
		order by ESTIMATED_REVENUE_PER_ACTIVE_LISTING desc
			limit 1;
		
 
		
		
	

