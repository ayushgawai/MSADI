{{
    config(
        materialized='table',
        schema='ANALYTICS'
    )
}}

/*
    Daily market aggregation across all symbols.
    Provides a market-wide view for trend analysis.
*/

with stock_data as (
    select * from {{ ref('fct_stock_price') }}
),

daily_market as (
    select
        date,
        
        -- Symbol count
        count(distinct symbol) as symbol_count,
        
        -- Aggregate price metrics
        round(avg(close), 2) as avg_close,
        round(avg(pct_change), 2) as avg_pct_change,
        sum(volume) as total_volume,
        round(avg(volume), 0) as avg_volume,
        
        -- Direction distribution
        sum(case when price_direction = 'UP' then 1 else 0 end) as stocks_up,
        sum(case when price_direction = 'DOWN' then 1 else 0 end) as stocks_down,
        sum(case when price_direction = 'FLAT' then 1 else 0 end) as stocks_flat,
        
        -- Market sentiment (based on RSI)
        round(avg(rsi_14), 2) as avg_rsi,
        sum(case when rsi_signal = 'OVERBOUGHT' then 1 else 0 end) as overbought_count,
        sum(case when rsi_signal = 'OVERSOLD' then 1 else 0 end) as oversold_count,
        
        -- Volume signals
        sum(case when volume_signal = 'HIGH_VOLUME' then 1 else 0 end) as high_volume_count,
        sum(case when volume_signal = 'LOW_VOLUME' then 1 else 0 end) as low_volume_count,
        
        -- Volatility
        round(avg(volatility_14), 2) as avg_volatility,
        
        -- Market breadth indicator
        round(
            (sum(case when price_direction = 'UP' then 1 else 0 end)::float - 
             sum(case when price_direction = 'DOWN' then 1 else 0 end)::float) / 
            nullif(count(*), 0) * 100, 
        2) as market_breadth,
        
        -- Metadata
        current_timestamp() as loaded_at
        
    from stock_data
    group by date
)

select * from daily_market
order by date desc

