{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['SYMBOL','DATE'],
  on_schema_change = 'sync_all_columns',
  pre_hook = [ "{% if adapter.get_relation(database=this.database, schema=this.schema, identifier=this.table) is not none %} alter table {{ this }} set comment = 'about to merge new data' {% endif %}" ],
  post_hook = [ "comment on table {{ this }} is 'Upserted from RAW.MARKET_DATA via dbt'" ]
) }}

select
    cast("DATE" as date) as date,
    "SYMBOL" as symbol,
    "OPEN" as open,
    "HIGH" as high,
    "LOW" as low,
    "CLOSE" as close,
    "VOLUME" as volume,
    "CREATED_AT" as created_at
from {{ source('raw', 'market_data') }}

{% if is_incremental() %}
    where "DATE" > (select coalesce(max(date), to_date('1900-01-01')) from {{ this }})
{% endif %}
