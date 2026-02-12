-- Staging view: global market facts
SELECT
    global_key,
    date_key,
    time_key,
    active_cryptocurrencies,
    markets,
    total_market_cap_usd,
    total_volume_usd,
    market_cap_percentage_btc,
    market_cap_percentage_eth,
    market_cap_change_percentage_24h,
    source_snapshot_time
FROM {{ source('dw', 'fact_global_market') }}
