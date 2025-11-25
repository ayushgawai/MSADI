{% snapshot snap_daily_indicators %}

{{
    config(
        target_schema='SNAPSHOT',
        unique_key="symbol || '-' || to_char(date, 'YYYY-MM-DD')",
        strategy='check',
        check_cols=['close', 'rsi_14', 'sma_14', 'rsi_signal']
    )
}}

/*
    Snapshot of daily stock indicators.
    Tracks changes in technical indicators for historical analysis.
    Useful for auditing and tracking indicator recalculations.
*/

select
    symbol,
    date,
    close,
    rsi_14,
    sma_7,
    sma_14,
    sma_30,
    rsi_signal,
    price_vs_sma_14,
    volatility_14,
    volume_signal,
    loaded_at
from {{ ref('fct_stock_price') }}
where indicators_valid = true

{% endsnapshot %}

