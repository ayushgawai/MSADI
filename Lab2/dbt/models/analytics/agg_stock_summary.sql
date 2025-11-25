{{
    config(
        materialized='table',
        schema='ANALYTICS'
    )
}}

/*
    Aggregated stock summary metrics per symbol.
    Used for dashboard KPIs and high-level analytics.
*/

with stock_data as (
    select * from {{ ref('fct_stock_price') }}
    where indicators_valid = true
),

summary as (
    select
        symbol,
        
        -- Date range
        min(date) as first_date,
        max(date) as last_date,
        count(*) as trading_days,
        
        -- Latest values
        max(case when date = (select max(date) from stock_data s2 where s2.symbol = stock_data.symbol) then close end) as latest_close,
        max(case when date = (select max(date) from stock_data s2 where s2.symbol = stock_data.symbol) then rsi_14 end) as latest_rsi,
        max(case when date = (select max(date) from stock_data s2 where s2.symbol = stock_data.symbol) then sma_14 end) as latest_sma_14,
        max(case when date = (select max(date) from stock_data s2 where s2.symbol = stock_data.symbol) then rsi_signal end) as latest_rsi_signal,
        
        -- Price statistics
        round(min(low), 2) as period_low,
        round(max(high), 2) as period_high,
        round(avg(close), 2) as avg_close,
        round(stddev(close), 2) as price_stddev,
        
        -- Volume statistics
        round(avg(volume), 0) as avg_volume,
        max(volume) as max_volume,
        min(volume) as min_volume,
        
        -- Performance metrics
        round(avg(pct_change), 2) as avg_daily_return,
        round(stddev(pct_change), 2) as daily_return_stddev,
        
        -- Directional stats
        sum(case when price_direction = 'UP' then 1 else 0 end) as up_days,
        sum(case when price_direction = 'DOWN' then 1 else 0 end) as down_days,
        sum(case when price_direction = 'FLAT' then 1 else 0 end) as flat_days,
        round(sum(case when price_direction = 'UP' then 1 else 0 end)::float / count(*) * 100, 2) as up_day_pct,
        
        -- RSI statistics
        round(avg(rsi_14), 2) as avg_rsi,
        sum(case when rsi_signal = 'OVERBOUGHT' then 1 else 0 end) as overbought_days,
        sum(case when rsi_signal = 'OVERSOLD' then 1 else 0 end) as oversold_days,
        
        -- Volatility
        round(avg(volatility_14), 2) as avg_volatility,
        
        -- Metadata
        current_timestamp() as refreshed_at
        
    from stock_data
    group by symbol
)

select * from summary

