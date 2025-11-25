/*
    Singular test to validate close price is within high-low range.
    Close should always be between low and high for the day.
*/

select
    symbol,
    date,
    high,
    low,
    close
from {{ ref('stg_stock_price') }}
where close < low or close > high

