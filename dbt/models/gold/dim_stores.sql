{{ config(grants=consumer_grants()) }}

select
    store_id,
    store_name,
    city,
    country_code,
    store_size,
    opening_date
from {{ ref('stg_stores') }}
