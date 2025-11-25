{{
    config(
        materialized='view',
        schema='STAGE'
    )
}}

/*
    Staging model for stock price data from RAW layer.
    Performs basic data cleansing and type casting.
*/

with source as (
    select * from {{ source('raw', 'stock_price') }}
),

staged as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        -- Derived fields for validation
        (high - low) as daily_range,
        (close - open) as daily_change,
        case 
            when close > open then 'UP'
            when close < open then 'DOWN'
            else 'FLAT'
        end as price_direction
    from source
    where 
        symbol is not null 
        and date is not null
        and close > 0
)

select * from staged

