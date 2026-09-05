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

# Refresh existing rows because adjusted historical prices can change after
# splits and other corporate actions.
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

def backfill_last_four_years(**context):
    if not API_KEY:
        raise ValueError("MARKET_API_KEY is not set")

    # Optional override via manual trigger config:
    # {"end_date":"2026-02-14"}  OR {"start_date":"2024-02-15","end_date":"2026-02-14"}
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}

    end_date = conf.get("end_date")
    start_date = conf.get("start_date")

    # Default end_date = yesterday in NY (safer than "today" which may be incomplete)
    if not end_date:
        end = pendulum.now(LOCAL_TZ).subtract(days=1).date()
    else:
        end = pendulum.parse(end_date).in_timezone(LOCAL_TZ).date()

    # Default start_date = end - 4 years
    if not start_date:
        start = (pendulum.datetime(end.year, end.month, end.day, tz=LOCAL_TZ)
                 .subtract(years=4)).date()
    else:
        start = pendulum.parse(start_date).in_timezone(LOCAL_TZ).date()

    if start > end:
        raise ValueError(f"start_date {start} cannot be after end_date {end}")

    logger.info("Backfill range (NY): %s -> %s", start, end)

    session = requests.Session()
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        boundaries = _load_identity_boundaries(conn)
        logger.info("Loaded %d ticker identity boundaries", len(boundaries))
        total_days = 0
        total_rows = 0
        skipped_days = 0

        d = pendulum.date(start.year, start.month, start.day)
        end_p = pendulum.date(end.year, end.month, end.day)

        while d <= end_p:
            # Skip weekends fast (Massive will likely return empty anyway)
            if d.day_of_week in (pendulum.SATURDAY, pendulum.SUNDAY):
                skipped_days += 1
                d = d.add(days=1)
                continue

            trade_date = d.to_date_string()

            try:
                records = _fetch_grouped_for_date(session, trade_date)
            except Exception as e:
                # Fail the DAG so Airflow retries rather than silently losing a day
                logger.error("API fetch failed for %s: %s", trade_date, e)
                raise

            if not records:
                # market holiday or date not available yet; do NOT fail
                logger.warning("No records for %s (holiday or unavailable). Skipping.", trade_date)
                skipped_days += 1
                d = d.add(days=1)
                continue

            inserted = _bulk_insert(conn, trade_date, records, boundaries)
            total_days += 1
            total_rows += inserted

            logger.info("Loaded %s: records=%d inserted=%d", trade_date, len(records), inserted)
            d = d.add(days=1)

        logger.info(
            "Backfill completed: loaded_days=%d skipped_days=%d total_inserted_rows=%d",
            total_days, skipped_days, total_rows
        )

    finally:
        conn.close()

# -----------------------------
# DAG
# -----------------------------
default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="daily_market_summary_manual_4y",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 12, 26, tz=LOCAL_TZ),
    schedule_interval=None,   # Airflow <=2.3 style
    catchup=False,
    max_active_runs=1,
    tags=["production-support", "market-data", "stocks", "backfill"],
) as dag:
    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    backfill_task = PythonOperator(
        task_id="backfill_last_four_years",
        python_callable=backfill_last_four_years,
    )

    create_table_task >> backfill_task
