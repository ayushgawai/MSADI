{{
    config(
        materialized='table',
        schema='TRANSFORM'
    )
}}

/*
    Intermediate model calculating technical indicators:
    - Simple Moving Averages (SMA): 7-day, 14-day, 30-day
    - Exponential Moving Average (EMA): 12-day
    - RSI (Relative Strength Index): 14-day
    - Price Momentum
    - Volatility metrics
*/

with staged_prices as (
    select * from {{ ref('stg_stock_price') }}
),

-- Calculate price changes for RSI
price_changes as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        daily_range,
        daily_change,
        price_direction,
        -- Price change from previous day
        close - lag(close) over (partition by symbol order by date) as price_change,
        -- Previous close for percentage calculations
        lag(close) over (partition by symbol order by date) as prev_close
    from staged_prices
),

-- Separate gains and losses for RSI
gains_losses as (
    select
        *,
        case when price_change > 0 then price_change else 0 end as gain,
        case when price_change < 0 then abs(price_change) else 0 end as loss
    from price_changes
),

-- Calculate rolling averages and RSI components
with_indicators as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        daily_range,
        daily_change,
        price_direction,
        price_change,
        prev_close,
        
        -- Percentage change
        case 
            when prev_close > 0 then round((price_change / prev_close) * 100, 2)
            else null 
        end as pct_change,
        
        -- Simple Moving Averages
        round(avg(close) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        ), 2) as sma_7,
        
        round(avg(close) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ), 2) as sma_14,
        
        round(avg(close) over (
            partition by symbol 
            order by date 
            rows between 29 preceding and current row
        ), 2) as sma_30,
        
        -- Volume Moving Average (14-day)
        round(avg(volume) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ), 0) as volume_sma_14,
        
        -- RSI Components (14-day)
        avg(gain) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ) as avg_gain_14,
        
        avg(loss) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ) as avg_loss_14,
        
        -- Volatility: Standard deviation of close over 14 days
        round(stddev(close) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ), 2) as volatility_14,
        
        -- 14-day High/Low
        max(high) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ) as high_14,
        
        min(low) over (
            partition by symbol 
            order by date 
            rows between 13 preceding and current row
        ) as low_14,
        
        -- Row number for filtering incomplete windows
        row_number() over (partition by symbol order by date) as row_num
        
    from gains_losses
),

-- Calculate RSI and additional metrics
final as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        daily_range,
        daily_change,
        price_direction,
        price_change,
        pct_change,
        sma_7,
        sma_14,
        sma_30,
        volume_sma_14,
        volatility_14,
        high_14,
        low_14,
        
        -- RSI Calculation: 100 - (100 / (1 + RS))
        case 
            when avg_loss_14 = 0 then 100
            when avg_loss_14 is null then null
            else round(100 - (100 / (1 + (avg_gain_14 / avg_loss_14))), 2)
        end as rsi_14,
        
        -- Price vs SMA signals
        case 
            when close > sma_14 then 'ABOVE_SMA'
            when close < sma_14 then 'BELOW_SMA'
            else 'AT_SMA'
        end as price_vs_sma_14,
        
        -- Volume signal
        case 
            when volume > volume_sma_14 * 1.5 then 'HIGH_VOLUME'
            when volume < volume_sma_14 * 0.5 then 'LOW_VOLUME'
            else 'NORMAL_VOLUME'
        end as volume_signal,
        
        -- Only include rows with sufficient data for indicators
        case when row_num >= 14 then true else false end as indicators_valid
        
    from with_indicators
)

select * from final

