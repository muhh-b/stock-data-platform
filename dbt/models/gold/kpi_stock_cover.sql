{{ config(grants=consumer_grants()) }}

-- Stock coverage and rotation per day x store x product, over a rolling
-- window of `kpi_window_days` days.
--   avg_daily_sales : average units sold per day over the window
--   days_of_cover   : stock / average daily sales (how many days the stock lasts)
--   rotation_rate   : units sold over the window / average stock over the window
--                     ("vitesse de rotation"), higher = stock turns faster
{% set window_rows = var('kpi_window_days') - 1 %}

with windowed as (
    select
        snapshot_date,
        store_id,
        product_id,
        sport,
        is_active,
        stock_qty,
        stock_value,
        avg(sales_qty) over (
            partition by store_id, product_id
            order by snapshot_date
            rows between {{ window_rows }} preceding and current row
        ) as avg_daily_sales,
        sum(sales_qty) over (
            partition by store_id, product_id
            order by snapshot_date
            rows between {{ window_rows }} preceding and current row
        ) as sales_qty_window,
        avg(stock_qty) over (
            partition by store_id, product_id
            order by snapshot_date
            rows between {{ window_rows }} preceding and current row
        ) as avg_stock_window
    from {{ ref('fct_stock_daily') }}
)

select
    snapshot_date,
    store_id,
    product_id,
    sport,
    is_active,
    stock_qty,
    stock_value,
    round(avg_daily_sales, 2) as avg_daily_sales,
    round(stock_qty / nullif(avg_daily_sales, 0), 1) as days_of_cover,
    round(sales_qty_window / nullif(avg_stock_window, 0), 2) as rotation_rate
from windowed
