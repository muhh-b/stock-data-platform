-- Product referential, current state: latest version of each product.
-- History over time is kept by the snapshot `snp_products` (SCD2).
with source as (
    select * from {{ source('bronze', 'products') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by product_id
        order by cast(updated_at as timestamp) desc, _ingested_at desc
    ) = 1
)

select
    cast(product_id as string) as product_id,
    trim(product_name) as product_name,
    trim(sport) as sport,
    trim(category) as category,
    trim(brand) as brand,
    cast(unit_price as decimal(10, 2)) as unit_price,
    upper(trim(status)) as product_status,
    cast(updated_at as timestamp) as updated_at
from deduplicated
