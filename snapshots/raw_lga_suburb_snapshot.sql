{% snapshot lga_suburb_snapshot %}

{{
    config(
        target_schema='raw',
        strategy='check',

        unique_key='LGA_NAME',
        check_cols=['LGA_NAME', 'LGA_SUBURB'],
        )
}}

SELECT * FROM {{ source('raw', 'lga_name_suburb') }}

{% endsnapshot %}
