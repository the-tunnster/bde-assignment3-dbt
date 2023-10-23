WITH source AS (
    SELECT * FROM {{ source('raw', 'lga_code_name') }}
)

SELECT 
	LGA_CODE as lga_code,
	LOWER(source.LGA_NAME) as lga_name
		FROM source
