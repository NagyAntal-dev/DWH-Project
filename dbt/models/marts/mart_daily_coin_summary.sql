-- mart_daily_coin_summary
-- Aggregated daily coin metrics for Metabase dashboards.
-- Combines market snapshots with OHLC data per coin per day.

{{ config(materialized='table') }}

WITH daily_snapshots AS (
    SELECT
        ms.coin_key,
        ms.date_key,
        ROUND(AVG(ms.current_price), 4)                  AS avg_price,
        MAX(ms.current_price)                              AS max_price,
        MIN(ms.current_price)                              AS min_price,
        ROUND(AVG(ms.market_cap), 2)                      AS avg_market_cap,
        ROUND(AVG(ms.total_volume), 2)                    AS avg_volume,
        ROUND(AVG(ms.price_change_percentage_24h), 4)     AS avg_price_change_pct_24h,
        MAX(ms.circulating_supply)                         AS circulating_supply,
        MAX(ms.total_supply)                               AS total_supply,
        COUNT(*)                                           AS snapshot_count
    FROM {{ ref('stg_market_snapshots') }} ms
    GROUP BY ms.coin_key, ms.date_key
),

daily_ohlc AS (
    SELECT
        o.coin_key,
        o.date_key,
        o.open_price,
        o.high_price,
        o.low_price,
        o.close_price
    FROM {{ ref('stg_ohlc_daily') }} o
)

SELECT
    dd.full_date,
    dc.coin_id,
    dc.name               AS coin_name,
    dc.symbol,
    dc.market_cap_rank,
    ds.avg_price,
    ds.max_price,
    ds.min_price,
    ds.avg_market_cap,
    ds.avg_volume,
    ds.avg_price_change_pct_24h,
    ds.circulating_supply,
    ds.total_supply,
    ds.snapshot_count,
    ohlc.open_price,
    ohlc.high_price,
    ohlc.low_price,
    ohlc.close_price
FROM daily_snapshots ds
JOIN {{ ref('stg_coins') }} dc ON ds.coin_key = dc.coin_key
JOIN {{ source('dw', 'dim_date') }} dd ON ds.date_key = dd.date_key
LEFT JOIN daily_ohlc ohlc ON ds.coin_key = ohlc.coin_key AND ds.date_key = ohlc.date_key
