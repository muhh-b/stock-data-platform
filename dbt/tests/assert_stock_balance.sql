{{ config(severity='warn') }}

-- Consistency: stock(d) = stock(d-1) + receipts(d) - sales(d).
-- A gap means an inventory adjustment, a lost flow or a wrong snapshot.
-- Only checked when the previous day exists for the same store x product.
with flows as (
    select
        snapshot_date,
        store_id,
        product_id,
        stock_qty,
        received_qty,
        sales_qty,
        lag(stock_qty) over (
            partition by store_id, product_id order by snapshot_date
        ) as previous_stock_qty,
        lag(snapshot_date) over (
            partition by store_id, product_id order by snapshot_date
        ) as previous_date
    from {{ ref('fct_stock_daily') }}
)

select
    *,
    stock_qty - (previous_stock_qty + received_qty - sales_qty) as gap
from flows
where
    previous_date = {{ dbt.dateadd('day', -1, 'snapshot_date') }}
    and stock_qty != previous_stock_qty + received_qty - sales_qty
