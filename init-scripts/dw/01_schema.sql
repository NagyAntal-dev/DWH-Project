-- ============================================================
-- Data Warehouse Schema — Star Schema
-- ============================================================

-- ══════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ══════════════════════════════════════════════════════════════

-- ── dim_coin (SCD Type 2) ──
CREATE TABLE dim_coin (
    coin_key        SERIAL       PRIMARY KEY,
    coin_id         VARCHAR(100) NOT NULL,
    symbol          VARCHAR(50)  NOT NULL,
    name            VARCHAR(200) NOT NULL,
    image_url       TEXT,
    market_cap_rank INTEGER,
    valid_from      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMPTZ           DEFAULT '9999-12-31'::TIMESTAMPTZ,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_coin_id ON dim_coin(coin_id);
CREATE INDEX idx_dim_coin_current ON dim_coin(coin_id, is_current) WHERE is_current = TRUE;

-- ── dim_date ──
CREATE TABLE dim_date (
    date_key        INTEGER  PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE     NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    day_of_month    SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,  -- 0=Mon .. 6=Sun
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN  NOT NULL
);

-- ── dim_time ──
CREATE TABLE dim_time (
    time_key    SMALLINT PRIMARY KEY,  -- 0..23
    hour_label  VARCHAR(10) NOT NULL   -- '00:00','01:00',...
);

-- ── dim_currency ──
CREATE TABLE dim_currency (
    currency_key SERIAL      PRIMARY KEY,
    currency_id  VARCHAR(10) NOT NULL UNIQUE,
    currency_name VARCHAR(50)
);

-- ══════════════════════════════════════════════════════════════
-- FACT TABLES
-- ══════════════════════════════════════════════════════════════

-- ── fact_market_snapshot ──
CREATE TABLE fact_market_snapshot (
    snapshot_key                BIGSERIAL PRIMARY KEY,
    coin_key                    INTEGER   NOT NULL REFERENCES dim_coin(coin_key),
    date_key                    INTEGER   NOT NULL REFERENCES dim_date(date_key),
    time_key                    SMALLINT  NOT NULL REFERENCES dim_time(time_key),
    currency_key                INTEGER   NOT NULL REFERENCES dim_currency(currency_key),
    current_price               NUMERIC(24,8),
    market_cap                  NUMERIC(24,2),
    total_volume                NUMERIC(24,2),
    high_24h                    NUMERIC(24,8),
    low_24h                     NUMERIC(24,8),
    price_change_percentage_24h NUMERIC(12,6),
    circulating_supply          NUMERIC(24,4),
    total_supply                NUMERIC(24,4),
    source_snapshot_time        TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_fact_market_coin_date ON fact_market_snapshot(coin_key, date_key);

-- ── fact_ohlc_daily ──
CREATE TABLE fact_ohlc_daily (
    ohlc_key     BIGSERIAL PRIMARY KEY,
    coin_key     INTEGER   NOT NULL REFERENCES dim_coin(coin_key),
    date_key     INTEGER   NOT NULL REFERENCES dim_date(date_key),
    currency_key INTEGER   NOT NULL REFERENCES dim_currency(currency_key),
    open_price   NUMERIC(24,8),
    high_price   NUMERIC(24,8),
    low_price    NUMERIC(24,8),
    close_price  NUMERIC(24,8),
    UNIQUE (coin_key, date_key, currency_key)
);

-- ── fact_global_market ──
CREATE TABLE fact_global_market (
    global_key                       BIGSERIAL PRIMARY KEY,
    date_key                         INTEGER   NOT NULL REFERENCES dim_date(date_key),
    time_key                         SMALLINT  NOT NULL REFERENCES dim_time(time_key),
    active_cryptocurrencies          INTEGER,
    markets                          INTEGER,
    total_market_cap_usd             NUMERIC(24,2),
    total_volume_usd                 NUMERIC(24,2),
    market_cap_percentage_btc        NUMERIC(8,4),
    market_cap_percentage_eth        NUMERIC(8,4),
    market_cap_change_percentage_24h NUMERIC(12,6),
    source_snapshot_time             TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_fact_global_date ON fact_global_market(date_key);

-- ── fact_trending ──
CREATE TABLE fact_trending (
    trending_key    BIGSERIAL PRIMARY KEY,
    coin_key        INTEGER   NOT NULL REFERENCES dim_coin(coin_key),
    date_key        INTEGER   NOT NULL REFERENCES dim_date(date_key),
    time_key        SMALLINT  NOT NULL REFERENCES dim_time(time_key),
    trending_rank   INTEGER,
    price_btc       NUMERIC(24,18),
    source_snapshot_time TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_fact_trending_date ON fact_trending(date_key);

-- ══════════════════════════════════════════════════════════════
-- SEED DATA
-- ══════════════════════════════════════════════════════════════

-- Populate dim_time (hours 0–23)
INSERT INTO dim_time (time_key, hour_label)
SELECT h, LPAD(h::TEXT, 2, '0') || ':00'
FROM generate_series(0, 23) AS h;

-- Populate dim_currency (usd only to start)
INSERT INTO dim_currency (currency_id, currency_name) VALUES ('usd', 'US Dollar');

-- Populate dim_date (2020-01-01 to 2030-12-31)
INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name,
                      week_of_year, day_of_month, day_of_week, day_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d                                AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT   AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT  AS month,
    TO_CHAR(d, 'Month')             AS month_name,
    EXTRACT(WEEK FROM d)::SMALLINT   AS week_of_year,
    EXTRACT(DAY FROM d)::SMALLINT    AS day_of_month,
    EXTRACT(ISODOW FROM d)::SMALLINT - 1 AS day_of_week,
    TO_CHAR(d, 'Day')               AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- ══════════════════════════════════════════════════════════════
-- MART VIEW (aggregated daily coin summary for dashboards)
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW mart_daily_coin_summary AS
SELECT
    dd.full_date,
    dc.coin_id,
    dc.name AS coin_name,
    dc.symbol,
    dc.market_cap_rank,
    ROUND(AVG(fs.current_price), 4)            AS avg_price,
    MAX(fs.current_price)                       AS max_price,
    MIN(fs.current_price)                       AS min_price,
    ROUND(AVG(fs.market_cap), 2)               AS avg_market_cap,
    ROUND(AVG(fs.total_volume), 2)             AS avg_volume,
    ROUND(AVG(fs.price_change_percentage_24h), 4) AS avg_price_change_pct_24h,
    COUNT(*)                                    AS snapshot_count
FROM fact_market_snapshot fs
JOIN dim_coin dc  ON fs.coin_key = dc.coin_key AND dc.is_current = TRUE
JOIN dim_date dd  ON fs.date_key = dd.date_key
GROUP BY dd.full_date, dc.coin_id, dc.name, dc.symbol, dc.market_cap_rank;
