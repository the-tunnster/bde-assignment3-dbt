WITH table_1 AS (
    SELECT * FROM {{ source('raw', 'lga_code_name') }}
),

table_2 AS (
    SELECT * FROM {{ source('raw', 'lga_name_suburb') }}
)

SELECT 
	LOWER(table_1.LGA_NAME) as LGA_NAME,
	LOWER(table_2.LGA_SUBURB) as LGA_SUBURB,
	table_1.LGA_CODE as LGA_CODE
		FROM table_1
			left JOIN table_2
				ON LOWER(table_1.LGA_NAME) = LOWER(table_2.LGA_NAME)
