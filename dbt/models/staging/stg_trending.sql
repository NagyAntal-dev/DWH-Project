-- Staging view: trending coin facts
SELECT
    trending_key,
    coin_key,
    date_key,
    time_key,
    trending_rank,
    price_btc,
    source_snapshot_time
FROM {{ source('dw', 'fact_trending') }}
