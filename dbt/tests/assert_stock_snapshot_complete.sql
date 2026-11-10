{{ config(severity='warn') }}

-- Completeness: every store must send a stock snapshot every day.
-- Returns the (day, store) pairs with no snapshot at all.
with days as (
    select distinct snapshot_date from {{ ref('stg_stock_snapshots') }}
),

stores as (
    select store_id from {{ ref('stg_stores') }}
),

expected as (
    select
        days.snapshot_date,
        stores.store_id
    from days
    cross join stores
),

received as (
    select distinct
        snapshot_date,
        store_id
    from {{ ref('stg_stock_snapshots') }}
)

select expected.*
from expected
left join received
    on
        expected.snapshot_date = received.snapshot_date
        and expected.store_id = received.store_id
where received.store_id is null
