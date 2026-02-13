-- assert_ohlc_high_gte_low
-- Custom test: high_price should always be >= low_price
-- Returns rows that violate this constraint (should return 0 rows to pass)

SELECT
    ohlc_key,
    coin_key,
    date_key,
    high_price,
    low_price
FROM {{ ref('stg_ohlc_daily') }}
WHERE high_price < low_price
