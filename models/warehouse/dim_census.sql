WITH source AS (
    SELECT * FROM {{ ref('census_stg') }}
)

SELECT * 
	FROM source
