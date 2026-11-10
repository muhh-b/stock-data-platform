{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date', 'store_id', 'product_id'],
        incremental_strategy=upsert_strategy(),
        on_schema_change='append_new_columns'
    )
}}

-- End-of-day stock, typed and deduplicated (one row per day x store x product).
-- Invalid rows (negative stock) are excluded here and kept in
-- `stg_stock_quarantine` for investigation.
-- Incremental + upsert on the grain: replaying a day overwrites it, so the
-- model is idempotent.
with source as (
    select * from {{ source('bronze', 'stock') }}
    {{ incremental_bronze_filter() }}
),

typed as (
    select
        cast(snapshot_date as date) as snapshot_date,
        cast(store_id as string) as store_id,
        cast(product_id as string) as product_id,
        cast(stock_qty as integer) as stock_qty,
        _ingested_at
    from source
),

deduplicated as (
    select *
    from typed
    qualify row_number() over (
        partition by snapshot_date, store_id, product_id
        order by _ingested_at desc
    ) = 1
)

select *
from deduplicated
where stock_qty >= 0
