-- Staging view: current coins from dim_coin
SELECT
    coin_key,
    coin_id,
    symbol,
    name,
    image_url,
    market_cap_rank,
    valid_from,
    valid_to,
    is_current
FROM {{ source('dw', 'dim_coin') }}
WHERE is_current = TRUE
