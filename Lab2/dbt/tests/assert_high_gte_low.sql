/*
    Singular test to validate high price is always >= low price.
    This is a fundamental data quality check for OHLCV data.
*/

select
    symbol,
    date,
    high,
    low
from {{ ref('stg_stock_price') }}
where high < low

