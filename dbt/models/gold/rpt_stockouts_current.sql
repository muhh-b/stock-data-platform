{{ config(grants=consumer_grants()) }}

-- Active references out of stock on the latest day, with how long they have
-- been out of stock and the last supplier receipt. Operational list for the
-- supply teams.
--
-- Consecutive stockout days use a "gaps and islands" pattern: a running count
-- of in-stock days stays constant during a stockout streak, which gives a
-- group id for each streak.
with daily as (
    select
        snapshot_date,
        store_id,
        product_id,
        sport,
        category,
        is_in_stock
    from {{ ref('fct_stock_daily') }}
    where is_active
),

islands as (
    select
        *,
        sum(case when is_in_stock then 1 else 0 end) over (
            partition by store_id, product_id
            order by snapshot_date
            rows between unbounded preceding and current row
        ) as streak_id
    from daily
),

streaks as (
    select
        store_id,
        product_id,
        streak_id,
        min(snapshot_date) as stockout_since,
        count(*) as stockout_days
    from islands
    where not is_in_stock
    group by store_id, product_id, streak_id
),

latest_day as (
    select max(snapshot_date) as snapshot_date from daily
),

last_receipts as (
    select
        store_id,
        product_id,
        max(receipt_date) as last_receipt_date
    from {{ ref('stg_receipts') }}
    group by store_id, product_id
)

select
    islands.snapshot_date,
    islands.store_id,
    stores.city,
    islands.product_id,
    islands.sport,
    islands.category,
    streaks.stockout_since,
    streaks.stockout_days,
    last_receipts.last_receipt_date
from islands
inner join latest_day
    on islands.snapshot_date = latest_day.snapshot_date
inner join streaks
    on
        islands.store_id = streaks.store_id
        and islands.product_id = streaks.product_id
        and islands.streak_id = streaks.streak_id
inner join {{ ref('dim_stores') }} as stores
    on islands.store_id = stores.store_id
left join last_receipts
    on
        islands.store_id = last_receipts.store_id
        and islands.product_id = last_receipts.product_id
where not islands.is_in_stock
