{% snapshot neighborhood_snapshot %}

{{
        config(
          target_schema='raw',
		      unique_key="listing_id",

          strategy='timestamp',
          updated_at='scraped_date'
        )
    }}

WITH table_1 AS (
	SELECT * FROM {{ source('raw', 'listings') }}
),

table_2 AS(
	SELECT * FROM {{ source('raw', 'lga_code_name') }}
)

SELECT
	LISTING_ID,
	SCRAPED_DATE,
	LISTING_NEIGHBOURHOOD AS NEIGHBOURHOOD,
	table_2.LGA_CODE as LGA_CODE
		FROM table_1
			LEFT JOIN table_2
				ON table_1.LISTING_NEIGHBOURHOOD = table_2.LGA_NAME

{% endsnapshot %}
