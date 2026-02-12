-- ============================================================
-- OLTP Schema — Raw CoinGecko Data
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- ── Coin metadata ──
CREATE TABLE raw.coins (
    coin_id         VARCHAR(100) PRIMARY KEY,
    symbol          VARCHAR(50)  NOT NULL,
    name            VARCHAR(200) NOT NULL,
    image_url       TEXT,
    market_cap_rank INTEGER,
    genesis_date    DATE,
    description     TEXT,
    homepage_url    TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── Hourly market snapshots ──
CREATE TABLE raw.market_snapshots (
    id                          BIGSERIAL    PRIMARY KEY,
    coin_id                     VARCHAR(100) NOT NULL REFERENCES raw.coins(coin_id),
    vs_currency                 VARCHAR(10)  NOT NULL DEFAULT 'usd',
    snapshot_time               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    current_price               NUMERIC(24,8),
    market_cap                  NUMERIC(24,2),
    fully_diluted_valuation     NUMERIC(24,2),
    total_volume                NUMERIC(24,2),
    high_24h                    NUMERIC(24,8),
    low_24h                     NUMERIC(24,8),
    price_change_24h            NUMERIC(24,8),
    price_change_percentage_24h NUMERIC(12,6),
    market_cap_change_24h       NUMERIC(24,2),
    circulating_supply          NUMERIC(24,4),
    total_supply                NUMERIC(24,4),
    max_supply                  NUMERIC(24,4),
    ath                         NUMERIC(24,8),
    ath_date                    TIMESTAMPTZ,
    atl                         NUMERIC(24,8),
    atl_date                    TIMESTAMPTZ
);

CREATE INDEX idx_market_snapshots_coin_time
    ON raw.market_snapshots(coin_id, snapshot_time);

-- ── Daily OHLC ──
CREATE TABLE raw.ohlc_daily (
    id          BIGSERIAL    PRIMARY KEY,
    coin_id     VARCHAR(100) NOT NULL REFERENCES raw.coins(coin_id),
    vs_currency VARCHAR(10)  NOT NULL DEFAULT 'usd',
    ohlc_date   DATE         NOT NULL,
    open_price  NUMERIC(24,8),
    high_price  NUMERIC(24,8),
    low_price   NUMERIC(24,8),
    close_price NUMERIC(24,8),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (coin_id, vs_currency, ohlc_date)
);

-- ── Trending coins ──
CREATE TABLE raw.trending_coins (
    id              BIGSERIAL    PRIMARY KEY,
    coin_id         VARCHAR(100) NOT NULL,
    name            VARCHAR(200),
    symbol          VARCHAR(50),
    market_cap_rank INTEGER,
    score           INTEGER,
    price_btc       NUMERIC(24,18),
    snapshot_time   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_trending_coins_time
    ON raw.trending_coins(snapshot_time);

-- ── Global market data ──
CREATE TABLE raw.global_market (
    id                              BIGSERIAL   PRIMARY KEY,
    snapshot_time                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active_cryptocurrencies         INTEGER,
    markets                         INTEGER,
    total_market_cap_usd            NUMERIC(24,2),
    total_volume_usd                NUMERIC(24,2),
    market_cap_percentage_btc       NUMERIC(8,4),
    market_cap_percentage_eth       NUMERIC(8,4),
    market_cap_change_percentage_24h NUMERIC(12,6),
    updated_at_unix                 BIGINT
);

CREATE INDEX idx_global_market_time
    ON raw.global_market(snapshot_time);
