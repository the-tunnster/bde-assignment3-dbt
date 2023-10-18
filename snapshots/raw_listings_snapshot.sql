{% snapshot listings_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          updated_at='scraped_date',
		  unique_key='LISTING_ID'
        )
    }}

select * from {{ source('raw', 'listings') }}

{% endsnapshot %}
