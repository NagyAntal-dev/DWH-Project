-- mart_global_market_daily
-- Daily aggregated global crypto market metrics

{{ config(materialized='table') }}

SELECT
    dd.full_date,
    ROUND(AVG(g.total_market_cap_usd), 2)             AS avg_total_market_cap_usd,
    ROUND(AVG(g.total_volume_usd), 2)                  AS avg_total_volume_usd,
    ROUND(AVG(g.market_cap_percentage_btc), 4)         AS avg_btc_dominance,
    ROUND(AVG(g.market_cap_percentage_eth), 4)         AS avg_eth_dominance,
    ROUND(AVG(g.market_cap_change_percentage_24h), 4)  AS avg_market_cap_change_pct_24h,
    MAX(g.active_cryptocurrencies)                      AS active_cryptocurrencies,
    MAX(g.markets)                                      AS active_markets,
    COUNT(*)                                            AS snapshot_count
FROM {{ ref('stg_global') }} g
JOIN {{ source('dw', 'dim_date') }} dd ON g.date_key = dd.date_key
GROUP BY dd.full_date
