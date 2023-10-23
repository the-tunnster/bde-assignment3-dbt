WITH source AS (
    SELECT * FROM {{ source('raw', 'lga_name_suburb') }}
)

SELECT 
	LOWER(source.LGA_NAME) as LGA_NAME,
	LOWER(source.LGA_SUBURB) as LGA_SUBURB
		FROM source
