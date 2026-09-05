from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import requests
import psycopg2
import os
import logging
from psycopg2.extras import execute_values

# -----------------------------
# CONFIG
# -----------------------------
LOCAL_TZ = pendulum.timezone("America/New_York")

API_URL = "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true"
API_KEY = os.getenv("MARKET_API_KEY")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

logger = logging.getLogger("airflow.task")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_market_summary (
    trade_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    vwap NUMERIC,
    trade_count INT,
    timestamp BIGINT,
    PRIMARY KEY (trade_date, ticker)
);

CREATE TABLE IF NOT EXISTS ticker_identity_boundaries (
    ticker VARCHAR(20) PRIMARY KEY,
    valid_from DATE NOT NULL,
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ticker_identity_boundaries (ticker, valid_from, reason)
VALUES
    ('BNY', DATE '2026-05-21', 'Ticker reused; previous security ended 2026-02-06'),
    ('SPCX', DATE '2026-06-12', 'Ticker reused; previous security ended 2026-04-06')
ON CONFLICT (ticker) DO UPDATE SET
    valid_from = EXCLUDED.valid_from,
    reason = EXCLUDED.reason,
    updated_at = now();
"""

# Refresh existing rows so reruns can apply corrected/adjusted market data.
INSERT_SQL = """
INSERT INTO daily_market_summary
(trade_date, ticker, open, high, low, close, volume, vwap, trade_count, timestamp)
VALUES %s
ON CONFLICT (trade_date, ticker) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    vwap = EXCLUDED.vwap,
    trade_count = EXCLUDED.trade_count,
    timestamp = EXCLUDED.timestamp
"""

# -----------------------------
# DB helpers
# -----------------------------
def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    conn.close()

# -----------------------------
# Core logic
# -----------------------------
def _fetch_grouped_for_date(session: requests.Session, trade_date: str):
    headers = {"Authorization": f"Bearer {API_KEY}"}
    url = API_URL.format(date=trade_date)
    resp = session.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json().get("results", []) or []

def _load_identity_boundaries(conn) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker, valid_from::text FROM ticker_identity_boundaries")
        return dict(cur.fetchall())


def _bulk_insert(conn, trade_date: str, records: list[dict], boundaries: dict[str, str]):
    rows = []
    for r in records:
        ticker = r.get("T")
        valid_from = boundaries.get(ticker)
        if valid_from and trade_date < valid_from:
            continue
        # expected keys from grouped endpoint: T,o,h,l,c,v,vw,n,t
        rows.append(
            (
                trade_date,
                ticker,
                r.get("o"),
                r.get("h"),
                r.get("l"),
                r.get("c"),
                r.get("v"),
                r.get("vw"),
                r.get("n"),
                r.get("t"),
            )
        )

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows, page_size=5000)
    conn.commit()
    return len(rows)

def fetch_today(**context):
    if not API_KEY:
        raise ValueError("MARKET_API_KEY is not set")

    # Optional override via manual trigger config: {"trade_date": "2026-08-25"}
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    trade_date_override = conf.get("trade_date")

    if trade_date_override:
        trade_date = pendulum.parse(trade_date_override).in_timezone(LOCAL_TZ).to_date_string()
    else:
        trade_date = pendulum.now(LOCAL_TZ).to_date_string()

    logger.info("Fetching grouped daily data for %s (NY)", trade_date)

    session = requests.Session()
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        boundaries = _load_identity_boundaries(conn)
        try:
            records = _fetch_grouped_for_date(session, trade_date)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                logger.error(
                    "Massive API refused %s (likely requested before end of day / plan restriction): %s",
                    trade_date, e.response.text,
                )
            raise

        if not records:
            logger.warning("No records for %s (market holiday, weekend, or data not yet available).", trade_date)
            return

        inserted = _bulk_insert(conn, trade_date, records, boundaries)
        logger.info("Loaded %s: records=%d inserted=%d", trade_date, len(records), inserted)

    finally:
        conn.close()

# -----------------------------
# DAG
# -----------------------------
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="daily_market_summary_today",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 12, 26, tz=LOCAL_TZ),
    schedule=None,   # on-demand only; trigger manually after market close
    catchup=False,
    max_active_runs=1,
    tags=["production-support", "market-data", "stocks", "on-demand"],
) as dag:
    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    fetch_today_task = PythonOperator(
        task_id="fetch_today",
        python_callable=fetch_today,
    )

    create_table_task >> fetch_today_task
