-- Store referential: typed, one row per store (latest version received).
with source as (
    select * from {{ source('bronze', 'stores') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by store_id order by _ingested_at desc) = 1
)

select
    cast(store_id as string) as store_id,
    trim(store_name) as store_name,
    trim(city) as city,
    upper(trim(country)) as country_code,
    upper(trim(store_size)) as store_size,
    cast(opening_date as date) as opening_date,
    _ingested_at
from deduplicated
