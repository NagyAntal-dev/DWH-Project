-- Staging view: daily OHLC data
SELECT
    ohlc_key,
    coin_key,
    date_key,
    currency_key,
    open_price,
    high_price,
    low_price,
    close_price
FROM {{ source('dw', 'fact_ohlc_daily') }}
