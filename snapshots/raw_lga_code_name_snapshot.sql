{% snapshot lga_code_name_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='LGA_CODE',
          check_cols=['LGA_CODE', 'LGA_NAME'],
        )
    }}

select * from {{ source('raw', 'lga_code_name') }}

{% endsnapshot %}
