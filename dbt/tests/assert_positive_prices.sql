-- assert_positive_prices
-- Custom test: all market snapshot prices should be non-negative
-- Returns rows that violate this constraint (should return 0 rows to pass)

SELECT
    snapshot_key,
    coin_key,
    current_price
FROM {{ ref('stg_market_snapshots') }}
WHERE current_price < 0
