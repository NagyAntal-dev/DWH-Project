-- assert_snapshot_count_reasonable
-- Custom test: a single coin should not have more than 24 snapshots per day
-- (since data is loaded at most hourly)
-- Returns rows that violate this constraint (should return 0 rows to pass)

SELECT
    coin_key,
    date_key,
    COUNT(*) AS snapshot_count
FROM {{ ref('stg_market_snapshots') }}
GROUP BY coin_key, date_key
HAVING COUNT(*) > 24
