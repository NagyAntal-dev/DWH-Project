"""
Crypto ETL Pipeline — Airflow DAG

Schedule : every 2 hours
Flow     : Extract CoinGecko → Load OLTP → Load DW dims/facts → dbt run

CoinGecko free Demo tier: 30 calls/min, 10 K/month.
The DAG inserts 5-second pauses between API calls to stay within limits.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

# ── DAG defaults ──
default_args = {
    "owner": "crypto-dwh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

CALL_DELAY = 5  # seconds between CoinGecko calls
TOP_N_OHLC = 10  # number of coins to fetch OHLC for


# ══════════════════════════════════════════════════════════════
# Python callables
# ══════════════════════════════════════════════════════════════

def _extract_markets(ti):
    from scripts.extract_coingecko import fetch_markets
    data = fetch_markets(vs_currency="usd", per_page=50, page=1)
    ti.xcom_push(key="markets", value=json.dumps(data, default=str))
    log.info("Extracted %d market entries", len(data))


def _extract_trending(ti):
    from scripts.extract_coingecko import fetch_trending
    time.sleep(CALL_DELAY)
    data = fetch_trending()
    ti.xcom_push(key="trending", value=json.dumps(data, default=str))
    log.info("Extracted %d trending coins", len(data))


def _extract_global(ti):
    from scripts.extract_coingecko import fetch_global
    time.sleep(CALL_DELAY)
    data = fetch_global()
    ti.xcom_push(key="global_data", value=json.dumps(data, default=str))
    log.info("Extracted global market data")


def _extract_ohlc(ti):
    from scripts.extract_coingecko import fetch_ohlc
    markets_json = ti.xcom_pull(task_ids="extract_markets", key="markets")
    markets = json.loads(markets_json) if markets_json else []
    coin_ids = [c["id"] for c in markets[:TOP_N_OHLC]]
    all_ohlc = {}
    for cid in coin_ids:
        time.sleep(CALL_DELAY)
        all_ohlc[cid] = fetch_ohlc(cid, vs_currency="usd", days=14)
    ti.xcom_push(key="ohlc", value=json.dumps(all_ohlc, default=str))
    log.info("Extracted OHLC for %d coins", len(all_ohlc))


def _load_to_oltp(ti):
    """Load extracted data into OLTP raw tables."""
    import os
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        host=os.environ["OLTP_DB_HOST"],
        port=int(os.environ.get("OLTP_DB_PORT", 5432)),
        dbname=os.environ["OLTP_DB_NAME"],
        user=os.environ["OLTP_DB_USER"],
        password=os.environ["OLTP_DB_PASSWORD"],
    )
    conn.autocommit = False
    cur = conn.cursor()
    now = datetime.now(timezone.utc)

    try:
        # ── Markets → raw.coins + raw.market_snapshots ──
        markets_json = ti.xcom_pull(task_ids="extract_markets", key="markets")
        markets = json.loads(markets_json) if markets_json else []

        for c in markets:
            cur.execute("""
                INSERT INTO raw.coins (coin_id, symbol, name, image_url, market_cap_rank, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (coin_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    image_url = EXCLUDED.image_url,
                    market_cap_rank = EXCLUDED.market_cap_rank,
                    updated_at = EXCLUDED.updated_at
            """, (
                c["id"], c.get("symbol", ""), c.get("name", ""),
                c.get("image", ""), c.get("market_cap_rank"), now,
            ))

        if markets:
            snapshot_rows = []
            for c in markets:
                snapshot_rows.append((
                    c["id"], "usd", now,
                    c.get("current_price"), c.get("market_cap"),
                    c.get("fully_diluted_valuation"), c.get("total_volume"),
                    c.get("high_24h"), c.get("low_24h"),
                    c.get("price_change_24h"), c.get("price_change_percentage_24h"),
                    c.get("market_cap_change_24h"),
                    c.get("circulating_supply"), c.get("total_supply"),
                    c.get("max_supply"),
                    c.get("ath"), c.get("ath_date"),
                    c.get("atl"), c.get("atl_date"),
                ))
            execute_values(cur, """
                INSERT INTO raw.market_snapshots
                  (coin_id, vs_currency, snapshot_time,
                   current_price, market_cap, fully_diluted_valuation, total_volume,
                   high_24h, low_24h, price_change_24h, price_change_percentage_24h,
                   market_cap_change_24h, circulating_supply, total_supply, max_supply,
                   ath, ath_date, atl, atl_date)
                VALUES %s
            """, snapshot_rows)
            log.info("Loaded %d market snapshots into OLTP", len(snapshot_rows))

        # ── Trending → raw.trending_coins ──
        trending_json = ti.xcom_pull(task_ids="extract_trending", key="trending")
        trending = json.loads(trending_json) if trending_json else []
        if trending:
            trending_rows = [
                (t["coin_id"], t.get("name"), t.get("symbol"),
                 t.get("market_cap_rank"), t.get("score"), t.get("price_btc"), now)
                for t in trending
            ]
            execute_values(cur, """
                INSERT INTO raw.trending_coins
                  (coin_id, name, symbol, market_cap_rank, score, price_btc, snapshot_time)
                VALUES %s
            """, trending_rows)
            log.info("Loaded %d trending coins into OLTP", len(trending_rows))

        # ── Global → raw.global_market ──
        global_json = ti.xcom_pull(task_ids="extract_global", key="global_data")
        gdata = json.loads(global_json) if global_json else {}
        if gdata:
            mcap = gdata.get("total_market_cap", {})
            vol = gdata.get("total_volume", {})
            pct = gdata.get("market_cap_percentage", {})
            cur.execute("""
                INSERT INTO raw.global_market
                  (snapshot_time, active_cryptocurrencies, markets,
                   total_market_cap_usd, total_volume_usd,
                   market_cap_percentage_btc, market_cap_percentage_eth,
                   market_cap_change_percentage_24h, updated_at_unix)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                now, gdata.get("active_cryptocurrencies"), gdata.get("markets"),
                mcap.get("usd"), vol.get("usd"),
                pct.get("btc"), pct.get("eth"),
                gdata.get("market_cap_change_percentage_24h_usd"),
                gdata.get("updated_at"),
            ))
            log.info("Loaded global market data into OLTP")

        # ── OHLC → raw.ohlc_daily ──
        ohlc_json = ti.xcom_pull(task_ids="extract_ohlc", key="ohlc")
        all_ohlc = json.loads(ohlc_json) if ohlc_json else {}
        ohlc_count = 0
        for coin_id, candles in all_ohlc.items():
            # Aggregate sub-daily candles to daily
            daily: dict = {}
            for c in candles:
                dt = datetime.fromtimestamp(c["timestamp_ms"] / 1000, tz=timezone.utc).date()
                if dt not in daily:
                    daily[dt] = {"open": c["open"], "high": c["high"],
                                 "low": c["low"], "close": c["close"]}
                else:
                    d = daily[dt]
                    d["high"] = max(d["high"], c["high"])
                    d["low"] = min(d["low"], c["low"])
                    d["close"] = c["close"]  # last close of the day

            for ohlc_date, vals in daily.items():
                cur.execute("""
                    INSERT INTO raw.ohlc_daily
                      (coin_id, vs_currency, ohlc_date, open_price, high_price, low_price, close_price)
                    VALUES (%s, 'usd', %s, %s, %s, %s, %s)
                    ON CONFLICT (coin_id, vs_currency, ohlc_date) DO UPDATE SET
                        open_price  = EXCLUDED.open_price,
                        high_price  = EXCLUDED.high_price,
                        low_price   = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price
                """, (coin_id, ohlc_date,
                      vals["open"], vals["high"], vals["low"], vals["close"]))
                ohlc_count += 1
        log.info("Loaded %d OHLC daily rows into OLTP", ohlc_count)

        conn.commit()
        log.info("OLTP load complete ✅")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _load_to_dw(ti):
    """
    Load extracted data from OLTP into DW dimensions and facts.
    Reads from OLTP (raw.*) and writes to DW (public.*).
    """
    import os
    import psycopg2

    oltp_conn = psycopg2.connect(
        host=os.environ["OLTP_DB_HOST"],
        port=int(os.environ.get("OLTP_DB_PORT", 5432)),
        dbname=os.environ["OLTP_DB_NAME"],
        user=os.environ["OLTP_DB_USER"],
        password=os.environ["OLTP_DB_PASSWORD"],
    )
    dw_conn = psycopg2.connect(
        host=os.environ["DW_DB_HOST"],
        port=int(os.environ.get("DW_DB_PORT", 5432)),
        dbname=os.environ["DW_DB_NAME"],
        user=os.environ["DW_DB_USER"],
        password=os.environ["DW_DB_PASSWORD"],
    )
    dw_conn.autocommit = False
    oltp_cur = oltp_conn.cursor()
    dw_cur = dw_conn.cursor()

    try:
        # ── Sync dim_coin (SCD-2 simplified: upsert current) ──
        oltp_cur.execute("SELECT coin_id, symbol, name, image_url, market_cap_rank FROM raw.coins")
        for row in oltp_cur.fetchall():
            coin_id, symbol, name, image_url, mcr = row
            # Check if exists and current
            dw_cur.execute("""
                SELECT coin_key, symbol, name, market_cap_rank
                FROM dim_coin WHERE coin_id = %s AND is_current = TRUE
            """, (coin_id,))
            existing = dw_cur.fetchone()
            if existing is None:
                # New coin — insert
                dw_cur.execute("""
                    INSERT INTO dim_coin (coin_id, symbol, name, image_url, market_cap_rank)
                    VALUES (%s, %s, %s, %s, %s)
                """, (coin_id, symbol, name, image_url, mcr))
            elif (existing[1] != symbol or existing[2] != name or existing[3] != mcr):
                # Changed — close old record, insert new (SCD-2)
                dw_cur.execute("""
                    UPDATE dim_coin SET valid_to = NOW(), is_current = FALSE
                    WHERE coin_key = %s
                """, (existing[0],))
                dw_cur.execute("""
                    INSERT INTO dim_coin (coin_id, symbol, name, image_url, market_cap_rank)
                    VALUES (%s, %s, %s, %s, %s)
                """, (coin_id, symbol, name, image_url, mcr))

        # ── fact_market_snapshot ──
        # Load most recent snapshots not yet in DW
        dw_cur.execute("SELECT COALESCE(MAX(source_snapshot_time), '1970-01-01'::TIMESTAMPTZ) FROM fact_market_snapshot")
        last_loaded = dw_cur.fetchone()[0]

        oltp_cur.execute("""
            SELECT coin_id, vs_currency, snapshot_time,
                   current_price, market_cap, total_volume,
                   high_24h, low_24h, price_change_percentage_24h,
                   circulating_supply, total_supply
            FROM raw.market_snapshots
            WHERE snapshot_time > %s
            ORDER BY snapshot_time
        """, (last_loaded,))

        snap_count = 0
        for row in oltp_cur.fetchall():
            coin_id, vs_cur, snap_time = row[0], row[1], row[2]
            # Look up dimension keys
            dw_cur.execute("SELECT coin_key FROM dim_coin WHERE coin_id = %s AND is_current = TRUE", (coin_id,))
            coin_key_row = dw_cur.fetchone()
            if not coin_key_row:
                continue
            coin_key = coin_key_row[0]

            date_key = int(snap_time.strftime("%Y%m%d"))
            time_key = snap_time.hour

            dw_cur.execute("SELECT currency_key FROM dim_currency WHERE currency_id = %s", (vs_cur,))
            cur_row = dw_cur.fetchone()
            if not cur_row:
                continue
            cur_key = cur_row[0]

            dw_cur.execute("""
                INSERT INTO fact_market_snapshot
                  (coin_key, date_key, time_key, currency_key,
                   current_price, market_cap, total_volume,
                   high_24h, low_24h, price_change_percentage_24h,
                   circulating_supply, total_supply, source_snapshot_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (coin_key, date_key, time_key, cur_key,
                  row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                  snap_time))
            snap_count += 1
        log.info("Loaded %d market snapshots into DW", snap_count)

        # ── fact_ohlc_daily ──
        oltp_cur.execute("""
            SELECT coin_id, vs_currency, ohlc_date, open_price, high_price, low_price, close_price
            FROM raw.ohlc_daily
        """)
        ohlc_count = 0
        for row in oltp_cur.fetchall():
            coin_id, vs_cur, ohlc_date = row[0], row[1], row[2]
            dw_cur.execute("SELECT coin_key FROM dim_coin WHERE coin_id = %s AND is_current = TRUE", (coin_id,))
            ck = dw_cur.fetchone()
            if not ck:
                continue
            dw_cur.execute("SELECT currency_key FROM dim_currency WHERE currency_id = %s", (vs_cur,))
            crk = dw_cur.fetchone()
            if not crk:
                continue
            date_key = int(ohlc_date.strftime("%Y%m%d"))
            dw_cur.execute("""
                INSERT INTO fact_ohlc_daily (coin_key, date_key, currency_key, open_price, high_price, low_price, close_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (coin_key, date_key, currency_key) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price  = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price
            """, (ck[0], date_key, crk[0], row[3], row[4], row[5], row[6]))
            ohlc_count += 1
        log.info("Loaded %d OHLC daily rows into DW", ohlc_count)

        # ── fact_global_market ──
        dw_cur.execute("SELECT COALESCE(MAX(source_snapshot_time), '1970-01-01'::TIMESTAMPTZ) FROM fact_global_market")
        last_global = dw_cur.fetchone()[0]

        oltp_cur.execute("""
            SELECT snapshot_time, active_cryptocurrencies, markets,
                   total_market_cap_usd, total_volume_usd,
                   market_cap_percentage_btc, market_cap_percentage_eth,
                   market_cap_change_percentage_24h
            FROM raw.global_market
            WHERE snapshot_time > %s
        """, (last_global,))
        for row in oltp_cur.fetchall():
            snap_time = row[0]
            date_key = int(snap_time.strftime("%Y%m%d"))
            time_key = snap_time.hour
            dw_cur.execute("""
                INSERT INTO fact_global_market
                  (date_key, time_key, active_cryptocurrencies, markets,
                   total_market_cap_usd, total_volume_usd,
                   market_cap_percentage_btc, market_cap_percentage_eth,
                   market_cap_change_percentage_24h, source_snapshot_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (date_key, time_key, row[1], row[2], row[3], row[4],
                  row[5], row[6], row[7], snap_time))
        log.info("Loaded global market facts into DW")

        # ── fact_trending ──
        dw_cur.execute("SELECT COALESCE(MAX(source_snapshot_time), '1970-01-01'::TIMESTAMPTZ) FROM fact_trending")
        last_trending = dw_cur.fetchone()[0]

        oltp_cur.execute("""
            SELECT coin_id, score, price_btc, snapshot_time
            FROM raw.trending_coins
            WHERE snapshot_time > %s
        """, (last_trending,))
        for row in oltp_cur.fetchall():
            coin_id, score, pbtc, snap_time = row
            dw_cur.execute("SELECT coin_key FROM dim_coin WHERE coin_id = %s AND is_current = TRUE", (coin_id,))
            ck = dw_cur.fetchone()
            if not ck:
                # Trending coin might not be in top 50 — insert a minimal dim record
                dw_cur.execute("""
                    INSERT INTO dim_coin (coin_id, symbol, name)
                    VALUES (%s, %s, %s) RETURNING coin_key
                """, (coin_id, coin_id, coin_id))
                ck = dw_cur.fetchone()
            date_key = int(snap_time.strftime("%Y%m%d"))
            time_key = snap_time.hour
            dw_cur.execute("""
                INSERT INTO fact_trending (coin_key, date_key, time_key, trending_rank, price_btc, source_snapshot_time)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (ck[0], date_key, time_key, score, pbtc, snap_time))
        log.info("Loaded trending facts into DW")

        dw_conn.commit()
        log.info("DW load complete ✅")

    except Exception:
        dw_conn.rollback()
        raise
    finally:
        oltp_cur.close()
        dw_cur.close()
        oltp_conn.close()
        dw_conn.close()


# ══════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════

with DAG(
    dag_id="crypto_etl_pipeline",
    default_args=default_args,
    description="Extract CoinGecko data → OLTP → DW star schema → dbt marts",
    schedule_interval="0 */2 * * *",  # every 2 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "etl", "coingecko"],
) as dag:

    extract_markets = PythonOperator(
        task_id="extract_markets",
        python_callable=_extract_markets,
    )

    extract_trending = PythonOperator(
        task_id="extract_trending",
        python_callable=_extract_trending,
    )

    extract_global = PythonOperator(
        task_id="extract_global",
        python_callable=_extract_global,
    )

    extract_ohlc = PythonOperator(
        task_id="extract_ohlc",
        python_callable=_extract_ohlc,
    )

    load_oltp = PythonOperator(
        task_id="load_to_oltp",
        python_callable=_load_to_oltp,
    )

    load_dw = PythonOperator(
        task_id="load_to_dw",
        python_callable=_load_to_dw,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/dbt && DBT_LOG_PATH=/tmp/dbt-logs DBT_TARGET_PATH=/tmp/dbt-target dbt run --profiles-dir . 2>&1",
    )

    # Task dependencies
    # Extract tasks run in parallel (except OHLC needs market list)
    extract_markets >> extract_ohlc
    [extract_markets, extract_trending, extract_global, extract_ohlc] >> load_oltp
    load_oltp >> load_dw >> run_dbt
