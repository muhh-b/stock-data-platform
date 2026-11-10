{{ config(materialized='view') }}

-- Stock rows rejected by the silver layer, with the reason.
-- Monitored by a warning test: the pipeline keeps running, but the anomaly is visible.
with typed as (
    select
        cast(snapshot_date as date) as snapshot_date,
        cast(store_id as string) as store_id,
        cast(product_id as string) as product_id,
        try_cast(stock_qty as integer) as stock_qty,
        _source_file,
        _ingested_at
    from {{ source('bronze', 'stock') }}
)

select
    *,
    case
        when stock_qty is null then 'STOCK_NOT_NUMERIC'
        when stock_qty < 0 then 'NEGATIVE_STOCK'
    end as rejection_reason
from typed
where stock_qty is null or stock_qty < 0
