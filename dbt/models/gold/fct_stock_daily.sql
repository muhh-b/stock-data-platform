{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date', 'store_id', 'product_id'],
        incremental_strategy=upsert_strategy(),
        on_schema_change='append_new_columns',
        grants=consumer_grants()
    )
}}

-- Daily stock fact: one row per day x store x product, with the flows of the
-- day (sales, receipts) and the product attributes valid on that day.
-- Incremental: the last `lookback_days` days are recomputed at each run to
-- absorb late sales or receipts.
{% if is_incremental() %}
    {% set window_start %}
        (
            select coalesce(
                {{ dbt.dateadd('day', -var('lookback_days'), 'max(snapshot_date)') }},
                cast('1900-01-01' as date)
            )
            from {{ this }}
        )
    {% endset %}
{% endif %}

with stock as (
    select
        snapshot_date,
        store_id,
        product_id,
        stock_qty,
        -- end of the business day, used to pick the product version valid that day
        {{ dbt.dateadd('day', 1, 'cast(snapshot_date as timestamp)') }} as end_of_day_ts
    from {{ ref('stg_stock_snapshots') }}
    {% if is_incremental() %}
        where snapshot_date >= {{ window_start }}
    {% endif %}
),

sales as (
    select
        sale_date,
        store_id,
        product_id,
        sum(quantity) as sales_qty,
        sum(amount) as sales_amount
    from {{ ref('stg_sales') }}
    {% if is_incremental() %}
        where sale_date >= {{ window_start }}
    {% endif %}
    group by sale_date, store_id, product_id
),

receipts as (
    select
        receipt_date,
        store_id,
        product_id,
        sum(quantity) as received_qty
    from {{ ref('stg_receipts') }}
    {% if is_incremental() %}
        where receipt_date >= {{ window_start }}
    {% endif %}
    group by receipt_date, store_id, product_id
),

products as (
    select * from {{ ref('dim_products') }}
)

select
    stock.snapshot_date,
    stock.store_id,
    stock.product_id,
    products.sport,
    products.category,
    products.product_status,
    products.unit_price,
    stock.stock_qty,
    coalesce(sales.sales_qty, 0) as sales_qty,
    coalesce(sales.sales_amount, 0) as sales_amount,
    coalesce(receipts.received_qty, 0) as received_qty,
    stock.stock_qty > 0 as is_in_stock,
    coalesce(products.product_status = 'ACTIVE', false) as is_active,
    stock.stock_qty * products.unit_price as stock_value
from stock
left join sales
    on
        stock.snapshot_date = sales.sale_date
        and stock.store_id = sales.store_id
        and stock.product_id = sales.product_id
left join receipts
    on
        stock.snapshot_date = receipts.receipt_date
        and stock.store_id = receipts.store_id
        and stock.product_id = receipts.product_id
left join products
    on
        stock.product_id = products.product_id
        and stock.end_of_day_ts > products.valid_from
        and stock.end_of_day_ts <= products.valid_to
