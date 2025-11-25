{% snapshot snap_stock_summary %}

{{
    config(
        target_schema='SNAPSHOT',
        unique_key='symbol',
        strategy='check',
        check_cols=['latest_close', 'latest_rsi', 'latest_rsi_signal', 'trading_days', 'period_low', 'period_high']
    )
}}

/*
    Snapshot of stock summary metrics.
    Tracks changes in key metrics over time using SCD Type 2.
    Captures history when price, RSI, or signal changes.
*/

select
    symbol,
    first_date,
    last_date,
    trading_days,
    latest_close,
    latest_rsi,
    latest_sma_14,
    latest_rsi_signal,
    period_low,
    period_high,
    avg_close,
    avg_volume,
    avg_daily_return,
    up_day_pct,
    avg_rsi,
    avg_volatility,
    refreshed_at
from {{ ref('agg_stock_summary') }}

{% endsnapshot %}

