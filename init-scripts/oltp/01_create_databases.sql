-- ============================================================
-- Create the Airflow metadata database
-- (Runs as the OLTP superuser during container init)
-- ============================================================

CREATE DATABASE airflow_meta
    OWNER oltp_user
    ENCODING 'UTF8';
