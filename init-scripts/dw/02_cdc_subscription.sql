-- ============================================================
-- CDC Setup — DW Subscription Schema
-- Creates the cdc_raw schema with mirror tables and subscription
-- NOTE: The SUBSCRIPTION itself is created by the cdc_setup_dag
--       because both DBs start simultaneously and the OLTP
--       publication must exist first.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS cdc_raw;

-- ── Mirror: raw.coins ──
CREATE TABLE cdc_raw.coins (
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

-- ── Mirror: raw.market_snapshots ──
CREATE TABLE cdc_raw.market_snapshots (
    id                          BIGSERIAL    PRIMARY KEY,
    coin_id                     VARCHAR(100) NOT NULL,
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

-- ── Mirror: raw.ohlc_daily ──
CREATE TABLE cdc_raw.ohlc_daily (
    id          BIGSERIAL    PRIMARY KEY,
    coin_id     VARCHAR(100) NOT NULL,
    vs_currency VARCHAR(10)  NOT NULL DEFAULT 'usd',
    ohlc_date   DATE         NOT NULL,
    open_price  NUMERIC(24,8),
    high_price  NUMERIC(24,8),
    low_price   NUMERIC(24,8),
    close_price NUMERIC(24,8),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (coin_id, vs_currency, ohlc_date)
);

-- ── Mirror: raw.trending_coins ──
CREATE TABLE cdc_raw.trending_coins (
    id              BIGSERIAL    PRIMARY KEY,
    coin_id         VARCHAR(100) NOT NULL,
    name            VARCHAR(200),
    symbol          VARCHAR(50),
    market_cap_rank INTEGER,
    score           INTEGER,
    price_btc       NUMERIC(24,18),
    snapshot_time   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── Mirror: raw.global_market ──
CREATE TABLE cdc_raw.global_market (
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
