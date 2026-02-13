"""
OHLC Backfill DAG — Historical OHLC data loader

Trigger : manual only (schedule=None)
Purpose : Load up to 365 days of historical OHLC candle data for the
          top N coins into OLTP and then DW.

Params  :
  - days  : number of days to backfill (default 365)
  - top_n : number of top coins by market-cap to fetch (default 10)
  - vs_currencies : comma-separated list of quote currencies (default "usd,eur,huf,btc")
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

CALL_DELAY = 5  # seconds between CoinGecko API calls

default_args = {
    "owner": "crypto-dwh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


# ══════════════════════════════════════════════════════════════
# Python callables
# ══════════════════════════════════════════════════════════════

def _extract_coin_list(ti, **context):
    """Fetch top-N coin IDs from CoinGecko /coins/markets."""
    from scripts.extract_coingecko import fetch_markets

    params = context["params"]
    top_n = int(params.get("top_n", 10))

    coins = fetch_markets(vs_currency="usd", per_page=top_n, page=1)
    coin_ids = [c["id"] for c in coins]
    ti.xcom_push(key="coin_ids", value=json.dumps(coin_ids))
    log.info("Backfill target: %d coins — %s", len(coin_ids), coin_ids)


def _extract_ohlc_history(ti, **context):
    """Extract historical OHLC data for each coin × currency pair."""
    from scripts.extract_coingecko import fetch_ohlc

    params = context["params"]
    days = int(params.get("days", 365))
    vs_currencies = [c.strip() for c in params.get("vs_currencies", "usd,eur,huf,btc").split(",")]

    coin_ids = json.loads(ti.xcom_pull(task_ids="extract_coin_list", key="coin_ids"))

    all_ohlc = {}  # { "bitcoin__usd": [...], ... }
    for coin_id in coin_ids:
        for vs in vs_currencies:
            time.sleep(CALL_DELAY)
            candles = fetch_ohlc(coin_id, vs_currency=vs, days=days)
            key = f"{coin_id}__{vs}"
            all_ohlc[key] = candles
            log.info("Fetched %d candles for %s/%s (%d days)", len(candles), coin_id, vs, days)

    ti.xcom_push(key="ohlc_history", value=json.dumps(all_ohlc, default=str))
    log.info("Total coin/currency pairs extracted: %d", len(all_ohlc))


def _load_ohlc_to_oltp(ti, **context):
    """Load historical OHLC into OLTP raw.ohlc_daily (upsert)."""
    import os
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["OLTP_DB_HOST"],
        port=int(os.environ.get("OLTP_DB_PORT", 5432)),
        dbname=os.environ["OLTP_DB_NAME"],
        user=os.environ["OLTP_DB_USER"],
        password=os.environ["OLTP_DB_PASSWORD"],
    )
    conn.autocommit = False
    cur = conn.cursor()

    ohlc_json = ti.xcom_pull(task_ids="extract_ohlc_history", key="ohlc_history")
    all_ohlc = json.loads(ohlc_json) if ohlc_json else {}

    total = 0
    try:
        for compound_key, candles in all_ohlc.items():
            coin_id, vs_currency = compound_key.split("__", 1)

            # Ensure the coin exists in raw.coins (minimal record)
            cur.execute("""
                INSERT INTO raw.coins (coin_id, symbol, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (coin_id) DO NOTHING
            """, (coin_id, coin_id, coin_id))

            # Aggregate sub-daily candles to daily OHLC
            daily: dict = {}
            for c in candles:
                dt = datetime.fromtimestamp(c["timestamp_ms"] / 1000, tz=timezone.utc).date()
                if dt not in daily:
                    daily[dt] = {
                        "open": c["open"], "high": c["high"],
                        "low": c["low"], "close": c["close"],
                    }
                else:
                    d = daily[dt]
                    d["high"] = max(d["high"], c["high"])
                    d["low"] = min(d["low"], c["low"])
                    d["close"] = c["close"]

            for ohlc_date, vals in daily.items():
                cur.execute("""
                    INSERT INTO raw.ohlc_daily
                      (coin_id, vs_currency, ohlc_date,
                       open_price, high_price, low_price, close_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (coin_id, vs_currency, ohlc_date) DO UPDATE SET
                        open_price  = EXCLUDED.open_price,
                        high_price  = EXCLUDED.high_price,
                        low_price   = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price
                """, (coin_id, vs_currency, ohlc_date,
                      vals["open"], vals["high"], vals["low"], vals["close"]))
                total += 1

        conn.commit()
        log.info("Backfill OLTP: loaded %d OHLC daily rows ✅", total)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _load_ohlc_to_dw(ti, **context):
    """Propagate backfilled OHLC from OLTP into DW fact_ohlc_daily."""
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
        oltp_cur.execute("""
            SELECT coin_id, vs_currency, ohlc_date,
                   open_price, high_price, low_price, close_price
            FROM raw.ohlc_daily
            ORDER BY ohlc_date
        """)

        count = 0
        for row in oltp_cur.fetchall():
            coin_id, vs_cur, ohlc_date = row[0], row[1], row[2]

            # Ensure dim_coin exists
            dw_cur.execute(
                "SELECT coin_key FROM dim_coin WHERE coin_id = %s AND is_current = TRUE",
                (coin_id,),
            )
            ck = dw_cur.fetchone()
            if not ck:
                dw_cur.execute("""
                    INSERT INTO dim_coin (coin_id, symbol, name)
                    VALUES (%s, %s, %s) RETURNING coin_key
                """, (coin_id, coin_id, coin_id))
                ck = dw_cur.fetchone()

            # Currency key
            dw_cur.execute(
                "SELECT currency_key FROM dim_currency WHERE currency_id = %s",
                (vs_cur,),
            )
            crk = dw_cur.fetchone()
            if not crk:
                continue  # skip if currency not in dim

            date_key = int(ohlc_date.strftime("%Y%m%d"))

            dw_cur.execute("""
                INSERT INTO fact_ohlc_daily
                  (coin_key, date_key, currency_key,
                   open_price, high_price, low_price, close_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (coin_key, date_key, currency_key) DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high_price  = EXCLUDED.high_price,
                    low_price   = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price
            """, (ck[0], date_key, crk[0], row[3], row[4], row[5], row[6]))
            count += 1

        dw_conn.commit()
        log.info("Backfill DW: loaded %d OHLC daily rows ✅", count)
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
    dag_id="ohlc_backfill",
    default_args=default_args,
    description="Manual backfill: load historical OHLC data for top coins",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "backfill", "ohlc"],
    params={
        "days": 365,
        "top_n": 10,
        "vs_currencies": "usd,eur,huf,btc",
    },
) as dag:

    extract_coin_list = PythonOperator(
        task_id="extract_coin_list",
        python_callable=_extract_coin_list,
    )

    extract_ohlc_history = PythonOperator(
        task_id="extract_ohlc_history",
        python_callable=_extract_ohlc_history,
    )

    load_oltp = PythonOperator(
        task_id="load_ohlc_to_oltp",
        python_callable=_load_ohlc_to_oltp,
    )

    load_dw = PythonOperator(
        task_id="load_ohlc_to_dw",
        python_callable=_load_ohlc_to_dw,
    )

    extract_coin_list >> extract_ohlc_history >> load_oltp >> load_dw
