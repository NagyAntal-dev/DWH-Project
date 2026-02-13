"""
Crypto Alerting DAG — Price Drop Monitor

Schedule : every 30 minutes
Purpose  : Monitor cryptocurrency prices for significant drops and
           generate alerts. Default: BTC > 5% drop within 1 hour.

Alert mechanisms:
  - Airflow log (always)
  - Email via Airflow SMTP (if configured)

Configurable via Airflow Variables:
  - alert_coin_ids   : comma-separated coin IDs to monitor (default: "bitcoin")
  - alert_threshold  : percentage drop threshold (default: 5.0)
  - alert_lookback_minutes : time window in minutes (default: 60)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "crypto-dwh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Default configuration (overridable via Airflow Variables)
DEFAULT_COIN_IDS = "bitcoin"
DEFAULT_THRESHOLD_PCT = 5.0
DEFAULT_LOOKBACK_MINUTES = 60


def _get_config():
    """Load alert configuration from Airflow Variables with fallbacks."""
    from airflow.models import Variable

    coin_ids_str = Variable.get("alert_coin_ids", default_var=DEFAULT_COIN_IDS)
    coin_ids = [c.strip() for c in coin_ids_str.split(",")]
    threshold = float(Variable.get("alert_threshold", default_var=str(DEFAULT_THRESHOLD_PCT)))
    lookback = int(Variable.get("alert_lookback_minutes", default_var=str(DEFAULT_LOOKBACK_MINUTES)))

    return coin_ids, threshold, lookback


def _check_price_drops(ti, **context):
    """
    Query OLTP for recent price snapshots and check for significant drops.
    Pushes alert details to XCom if any threshold is breached.
    """
    import os
    import psycopg2
    from datetime import timezone

    coin_ids, threshold_pct, lookback_minutes = _get_config()

    conn = psycopg2.connect(
        host=os.environ["OLTP_DB_HOST"],
        port=int(os.environ.get("OLTP_DB_PORT", 5432)),
        dbname=os.environ["OLTP_DB_NAME"],
        user=os.environ["OLTP_DB_USER"],
        password=os.environ["OLTP_DB_PASSWORD"],
    )
    cur = conn.cursor()

    alerts = []
    now = datetime.now(timezone.utc)
    lookback_time = now - timedelta(minutes=lookback_minutes)

    try:
        for coin_id in coin_ids:
            # Get the latest price
            cur.execute("""
                SELECT current_price, snapshot_time
                FROM raw.market_snapshots
                WHERE coin_id = %s AND vs_currency = 'usd'
                ORDER BY snapshot_time DESC
                LIMIT 1
            """, (coin_id,))
            latest = cur.fetchone()
            if not latest:
                log.warning("No recent snapshot for %s", coin_id)
                continue

            latest_price, latest_time = latest

            # Get the price ~lookback_minutes ago
            cur.execute("""
                SELECT current_price, snapshot_time
                FROM raw.market_snapshots
                WHERE coin_id = %s
                  AND vs_currency = 'usd'
                  AND snapshot_time <= %s
                ORDER BY snapshot_time DESC
                LIMIT 1
            """, (coin_id, lookback_time))
            older = cur.fetchone()
            if not older or not older[0]:
                log.warning("No historical snapshot for %s within lookback window", coin_id)
                continue

            older_price, older_time = older

            if older_price and older_price > 0 and latest_price is not None:
                pct_change = ((float(latest_price) - float(older_price)) / float(older_price)) * 100.0

                log.info(
                    "%s: price %.2f → %.2f (%.2f%%) over %d min",
                    coin_id, float(older_price), float(latest_price),
                    pct_change, lookback_minutes,
                )

                if pct_change <= -threshold_pct:
                    alert = {
                        "coin_id": coin_id,
                        "current_price": float(latest_price),
                        "previous_price": float(older_price),
                        "pct_change": round(pct_change, 2),
                        "threshold": threshold_pct,
                        "lookback_minutes": lookback_minutes,
                        "latest_time": str(latest_time),
                        "older_time": str(older_time),
                    }
                    alerts.append(alert)
                    log.critical(
                        "🚨 ALERT: %s dropped %.2f%% (threshold: %.1f%%) "
                        "from $%.2f to $%.2f in %d minutes!",
                        coin_id, abs(pct_change), threshold_pct,
                        float(older_price), float(latest_price), lookback_minutes,
                    )

    finally:
        cur.close()
        conn.close()

    import json
    ti.xcom_push(key="alerts", value=json.dumps(alerts))
    ti.xcom_push(key="has_alerts", value=len(alerts) > 0)
    log.info("Price check complete: %d alert(s) generated", len(alerts))


def _branch_on_alerts(ti, **context):
    """Branch: fire alert task or skip."""
    has_alerts = ti.xcom_pull(task_ids="check_price_drops", key="has_alerts")
    if has_alerts:
        return "fire_alert"
    return "no_alert"


def _fire_alert(ti, **context):
    """Log the alert details. Extend with email/Slack/webhook as needed."""
    import json

    alerts_json = ti.xcom_pull(task_ids="check_price_drops", key="alerts")
    alerts = json.loads(alerts_json) if alerts_json else []

    for alert in alerts:
        log.critical(
            "═══════════════════════════════════════════════════\n"
            "🚨 PRICE DROP ALERT\n"
            "  Coin      : %s\n"
            "  Drop      : %.2f%%\n"
            "  Price     : $%.2f → $%.2f\n"
            "  Window    : %d minutes\n"
            "  Threshold : %.1f%%\n"
            "  Time      : %s\n"
            "═══════════════════════════════════════════════════",
            alert["coin_id"], abs(alert["pct_change"]),
            alert["previous_price"], alert["current_price"],
            alert["lookback_minutes"], alert["threshold"],
            alert["latest_time"],
        )

    # Optionally send email if SMTP is configured
    try:
        from airflow.utils.email import send_email

        subject = f"🚨 Crypto Alert: {len(alerts)} price drop(s) detected!"
        body = "<h2>Price Drop Alerts</h2><ul>"
        for a in alerts:
            body += (
                f"<li><strong>{a['coin_id'].upper()}</strong>: "
                f"dropped {abs(a['pct_change']):.2f}% "
                f"(${a['previous_price']:.2f} → ${a['current_price']:.2f})</li>"
            )
        body += "</ul>"

        from airflow.models import Variable
        alert_email = Variable.get("alert_email", default_var="")
        if alert_email:
            send_email(to=alert_email, subject=subject, html_content=body)
            log.info("Alert email sent to %s", alert_email)
        else:
            log.info("No alert_email Variable set — skipping email notification")
    except Exception as e:
        log.warning("Email notification failed (SMTP may not be configured): %s", e)


# ══════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════

with DAG(
    dag_id="crypto_alerting",
    default_args=default_args,
    description="Monitor crypto prices for significant drops (e.g. BTC > 5% in 1h)",
    schedule_interval="*/30 * * * *",  # every 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "alerting", "monitoring"],
) as dag:

    check_prices = PythonOperator(
        task_id="check_price_drops",
        python_callable=_check_price_drops,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_alerts",
        python_callable=_branch_on_alerts,
    )

    fire_alert = PythonOperator(
        task_id="fire_alert",
        python_callable=_fire_alert,
    )

    no_alert = EmptyOperator(
        task_id="no_alert",
    )

    check_prices >> branch >> [fire_alert, no_alert]
