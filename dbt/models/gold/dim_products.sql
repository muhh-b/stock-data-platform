{{ config(grants=consumer_grants()) }}

-- Product dimension with full history (SCD2) built from the dbt snapshot.
-- Join on a date with: valid_from <= date < valid_to.
select
    product_id,
    product_name,
    sport,
    category,
    brand,
    unit_price,
    product_status,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31 00:00:00' as timestamp)) as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snp_products') }}
