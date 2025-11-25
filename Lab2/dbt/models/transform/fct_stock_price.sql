{{
    config(
        materialized='table',
        schema='TRANSFORM'
    )
}}

/*
    Fact table for stock prices with all technical indicators.
    This is the primary transform layer table used for analytics.
*/

with indicators as (
    select * from {{ ref('int_stock_indicators') }}
),

final as (
    select
        -- Primary keys
        {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} as stock_price_key,
        symbol,
        date,
        
        -- OHLCV data
        open,
        high,
        low,
        close,
        volume,
        
        -- Basic metrics
        daily_range,
        daily_change,
        price_direction,
        price_change,
        pct_change,
        
        -- Moving Averages
        sma_7,
        sma_14,
        sma_30,
        volume_sma_14,
        
        -- Volatility and Range
        volatility_14,
        high_14,
        low_14,
        
        -- RSI
        rsi_14,
        case 
            when rsi_14 >= 70 then 'OVERBOUGHT'
            when rsi_14 <= 30 then 'OVERSOLD'
            else 'NEUTRAL'
        end as rsi_signal,
        
        -- Trend signals
        price_vs_sma_14,
        volume_signal,
        
        -- Trend strength: price relative to 14-day range
        case 
            when high_14 = low_14 then 50
            else round(((close - low_14) / (high_14 - low_14)) * 100, 2)
        end as price_position_pct,
        
        -- Data quality flag
        indicators_valid,
        
        -- Metadata
        current_timestamp() as loaded_at
        
    from indicators
)

select * from final

