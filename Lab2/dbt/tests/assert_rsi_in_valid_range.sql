/*
    Singular test to validate RSI values are within valid range (0-100).
    RSI should never exceed these bounds if calculated correctly.
*/

select
    symbol,
    date,
    rsi_14
from {{ ref('fct_stock_price') }}
where 
    rsi_14 is not null
    and (rsi_14 < 0 or rsi_14 > 100)

