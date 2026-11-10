{#
    Incremental strategy per adapter:
    - Databricks / Delta Lake: MERGE on the unique key
    - DuckDB (local, CI): delete+insert on the unique key
    Both give the same result: re-processing a period overwrites it, no duplicates.
#}
{% macro upsert_strategy() -%}
    {{ return('merge' if target.type == 'databricks' else 'delete+insert') }}
{%- endmacro %}


{#
    Filter used by incremental models on top of bronze: only rows ingested
    after the last run, minus a safety lookback.
    The coalesce matters: if the target is still empty (first run on an empty
    file), max() is null and "x > null" would filter out every future row.
#}
{% macro incremental_bronze_filter(ingested_col='_ingested_at') -%}
    {% if is_incremental() %}
        where {{ ingested_col }} > (
            select coalesce(
                {{ dbt.dateadd('day', -var('lookback_days'), 'max(' ~ ingested_col ~ ')') }},
                cast('1900-01-01 00:00:00' as timestamp)
            )
            from {{ this }}
        )
    {% endif %}
{%- endmacro %}


{#
    Grants applied to consumer-facing (gold) models.
    Only on Databricks / Unity Catalog: DuckDB has no access control.
    The group comes from the `consumer_group` var; set it to '' to disable
    grants (e.g. before the group exists in the workspace).
#}
{% macro consumer_grants() -%}
    {% if target.type == 'databricks' and var('consumer_group', '') %}
        {{ return({'select': [var('consumer_group')]}) }}
    {% else %}
        {{ return({}) }}
    {% endif %}
{%- endmacro %}
