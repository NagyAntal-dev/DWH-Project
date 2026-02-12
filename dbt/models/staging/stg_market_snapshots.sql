-- Staging view: market snapshots with readable joins
SELECT
    fs.snapshot_key,
    fs.coin_key,
    fs.date_key,
    fs.time_key,
    fs.currency_key,
    fs.current_price,
    fs.market_cap,
    fs.total_volume,
    fs.high_24h,
    fs.low_24h,
    fs.price_change_percentage_24h,
    fs.circulating_supply,
    fs.total_supply,
    fs.source_snapshot_time
FROM {{ source('dw', 'fact_market_snapshot') }} fs
