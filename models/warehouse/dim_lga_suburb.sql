WITH source AS (
    SELECT * FROM {{ ref('lga_stg') }}
)

SELECT 
	LGA_NAME as LGA_NAME,
	LGA_SUBURB as LGA_SUBURB
		FROM source
