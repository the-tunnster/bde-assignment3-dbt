WITH table_1 AS (
	SELECT * FROM {{ ref('listings_stg') }}
),

table_2 AS(
	SELECT * FROM {{ ref('lga_stg') }}
)

SELECT
	LISTING_ID,
	LISTING_NEIGHBOURHOOD AS NEIGHBOURHOOD,
	VALID_FROM,
	VALID_TO,
	table_2.LGA_CODE as lga_code
		FROM table_1
			LEFT JOIN table_2
				ON table_1.LISTING_NEIGHBOURHOOD = table_2.lga_name
