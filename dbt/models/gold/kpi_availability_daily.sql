{{ config(grants=consumer_grants()) }}

-- Availability rate ("taux de dispo") per day x store x sport.
-- Definition: share of active product references with a positive stock at
-- end of day. Discontinued products are excluded from the denominator.
-- Numerator and denominator are exposed so consumers can re-aggregate
-- (per country, per network) without averaging ratios.
select
    fct.snapshot_date,
    fct.store_id,
    stores.country_code,
    fct.sport,
    count(*) as nb_active_refs,
    sum(case when fct.is_in_stock then 1 else 0 end) as nb_refs_in_stock,
    count(*) - sum(case when fct.is_in_stock then 1 else 0 end) as nb_refs_out_of_stock,
    round(
        cast(sum(case when fct.is_in_stock then 1 else 0 end) as decimal(12, 4))
        / count(*),
        4
    ) as availability_rate
from {{ ref('fct_stock_daily') }} as fct
inner join {{ ref('dim_stores') }} as stores
    on fct.store_id = stores.store_id
where fct.is_active
group by fct.snapshot_date, fct.store_id, stores.country_code, fct.sport
