{% snapshot lga_name_suburb_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='LGA_SUBURB',
          check_cols=['LGA_NAME', 'LGA_SUBURB'],
        )
    }}

select * from {{ source('raw', 'lga_name_suburb') }}

{% endsnapshot %}
