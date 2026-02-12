-- mart_trending_history
-- Trending coin history for tracking which coins trend and when

{{ config(materialized='table') }}

SELECT
    dd.full_date,
    dt.hour_label,
    dc.coin_id,
    dc.name       AS coin_name,
    dc.symbol,
    t.trending_rank,
    t.price_btc,
    t.source_snapshot_time
FROM {{ ref('stg_trending') }} t
JOIN {{ ref('stg_coins') }} dc ON t.coin_key = dc.coin_key
JOIN {{ source('dw', 'dim_date') }} dd ON t.date_key = dd.date_key
JOIN {{ source('dw', 'dim_time') }} dt ON t.time_key = dt.time_key
