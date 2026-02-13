-- ============================================================
-- CDC Setup — OLTP Publication
-- Creates a logical replication publication for all raw tables
-- ============================================================

-- Grant replication permissions to the OLTP user
ALTER ROLE oltp_user WITH REPLICATION;

-- Create publication for CDC
CREATE PUBLICATION cdc_raw_publication
    FOR TABLE raw.coins, raw.market_snapshots, raw.ohlc_daily,
              raw.trending_coins, raw.global_market;
