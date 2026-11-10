{{
    config(
        materialized='incremental',
        unique_key='receipt_id',
        incremental_strategy=upsert_strategy(),
        on_schema_change='append_new_columns'
    )
}}

-- Supplier receipts, typed and deduplicated.
with source as (
    select * from {{ source('bronze', 'receipts') }}
    {{ incremental_bronze_filter() }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by receipt_id order by _ingested_at desc) = 1
)

select
    cast(receipt_id as string) as receipt_id,
    cast(receipt_date as date) as receipt_date,
    cast(store_id as string) as store_id,
    cast(product_id as string) as product_id,
    cast(quantity as integer) as quantity,
    cast(supplier_id as string) as supplier_id,
    _ingested_at
from deduplicated
