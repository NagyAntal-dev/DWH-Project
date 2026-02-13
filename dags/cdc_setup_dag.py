"""
CDC Setup DAG — One-time PostgreSQL Logical Replication Setup

Trigger : manual only (schedule=None)
Purpose : Create the logical replication subscription on the DW database
          connecting to the OLTP publication. This must run AFTER both
          databases are fully initialized.

Idempotent — safely skips if the subscription already exists.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "crypto-dwh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _setup_cdc_subscription(**context):
    """
    Create the logical replication subscription on DW
    pointing to the OLTP publication.
    """
    import os
    import psycopg2

    dw_conn = psycopg2.connect(
        host=os.environ["DW_DB_HOST"],
        port=int(os.environ.get("DW_DB_PORT", 5432)),
        dbname=os.environ["DW_DB_NAME"],
        user=os.environ["DW_DB_USER"],
        password=os.environ["DW_DB_PASSWORD"],
    )
    dw_conn.autocommit = True  # DDL + subscription commands need autocommit
    cur = dw_conn.cursor()

    try:
        # Check if subscription already exists
        cur.execute("""
            SELECT 1 FROM pg_subscription WHERE subname = 'cdc_raw_subscription'
        """)
        if cur.fetchone():
            log.info("Subscription 'cdc_raw_subscription' already exists — skipping.")
            return

        # Build connection string for OLTP
        oltp_host = os.environ["OLTP_DB_HOST"]
        oltp_port = os.environ.get("OLTP_DB_PORT", "5432")
        oltp_db = os.environ["OLTP_DB_NAME"]
        oltp_user = os.environ["OLTP_DB_USER"]
        oltp_pass = os.environ["OLTP_DB_PASSWORD"]

        conninfo = (
            f"host={oltp_host} port={oltp_port} dbname={oltp_db} "
            f"user={oltp_user} password={oltp_pass}"
        )

        cur.execute(f"""
            CREATE SUBSCRIPTION cdc_raw_subscription
            CONNECTION '{conninfo}'
            PUBLICATION cdc_raw_publication
            WITH (copy_data = true, create_slot = true)
        """)
        log.info("CDC subscription created successfully ✅")

    finally:
        cur.close()
        dw_conn.close()


def _verify_cdc(**context):
    """Verify that the subscription is active and replicating."""
    import os
    import psycopg2

    dw_conn = psycopg2.connect(
        host=os.environ["DW_DB_HOST"],
        port=int(os.environ.get("DW_DB_PORT", 5432)),
        dbname=os.environ["DW_DB_NAME"],
        user=os.environ["DW_DB_USER"],
        password=os.environ["DW_DB_PASSWORD"],
    )
    cur = dw_conn.cursor()

    try:
        cur.execute("""
            SELECT subname, subenabled, subconninfo
            FROM pg_subscription
            WHERE subname = 'cdc_raw_subscription'
        """)
        row = cur.fetchone()
        if row:
            log.info("CDC Subscription: name=%s, enabled=%s", row[0], row[1])
        else:
            raise RuntimeError("CDC subscription not found — setup may have failed!")

        # Check replication state
        cur.execute("""
            SELECT srsubid, srrelid::regclass, srsubstate
            FROM pg_subscription_rel
        """)
        for rel in cur.fetchall():
            log.info("  Replicated table: %s, state=%s", rel[1], rel[2])

    finally:
        cur.close()
        dw_conn.close()


# ══════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════

with DAG(
    dag_id="cdc_setup",
    default_args=default_args,
    description="One-time setup: PostgreSQL logical replication OLTP → DW",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "cdc", "setup"],
) as dag:

    setup = PythonOperator(
        task_id="setup_cdc_subscription",
        python_callable=_setup_cdc_subscription,
    )

    verify = PythonOperator(
        task_id="verify_cdc",
        python_callable=_verify_cdc,
    )

    setup >> verify
