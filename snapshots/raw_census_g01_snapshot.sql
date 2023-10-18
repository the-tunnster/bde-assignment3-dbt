{% snapshot census_g01_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='LGA_CODE',
          check_cols=['LGA_CODE'],
        )
    }}

select * from {{ source('raw', 'census_data_g01') }}

{% endsnapshot %}
