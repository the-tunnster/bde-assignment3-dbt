WITH table_1 AS (
    SELECT * FROM {{ source('raw', 'lga_code_name') }}
),
table_2 AS (
    SELECT * FROM {{ source('raw', 'lga_name_suburb') }}
)

SELECT table_1.LGA_NAME as lga_name,
	table_1.LGA_SUBURB as lga_suburb,
	table_2.LGA_CODE as lga_code
		FROM raw.lga_name_suburb table_1
			left JOIN raw.lga_code_name table_2
				ON LOWER(table_1.lga_name) = LOWER(table_2.lga_name)
