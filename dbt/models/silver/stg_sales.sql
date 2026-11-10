{{
    config(
        materialized='incremental',
        unique_key='sale_id',
        incremental_strategy=upsert_strategy(),
        on_schema_change='append_new_columns'
    )
}}

-- Sales lines, typed and deduplicated on sale_id (upstream sometimes sends a line twice).
with source as (
    select * from {{ source('bronze', 'sales') }}
    {{ incremental_bronze_filter() }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by sale_id order by _ingested_at desc) = 1
)

select
    cast(sale_id as string) as sale_id,
    cast(sale_date as date) as sale_date,
    cast(store_id as string) as store_id,
    cast(product_id as string) as product_id,
    upper(trim(channel)) as sales_channel,
    cast(quantity as integer) as quantity,
    cast(amount as decimal(12, 2)) as amount,
    _ingested_at
from deduplicated
