from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import os
import logging
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
import psycopg2
import pendulum

logger = logging.getLogger("airflow.task")

# ----------------------------
# CONFIG
# ----------------------------
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

API_KEY = os.getenv("MARKET_API_KEY")

# 3-hour candles keep the overnight/pre-market pull lightweight (few bars per ticker).
MASSIVE_URL = (
    "https://api.massive.com/v2/aggs/ticker/{ticker}/range/3/hour/{start}/{end}"
    "?adjusted=true&limit=500&sort=asc"
)

LOCAL_TZ = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------
# Helpers
# ----------------------------
def ensure_limit(url: str, limit: int = 500) -> str:
    if not url:
        return url
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q["limit"] = str(limit)
    q.setdefault("sort", "asc")
    return urlunparse(u._replace(query=urlencode(q)))

def get_all_active_tickers(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker
            FROM tickers_info
            WHERE active = true
            ORDER BY ticker
            """
        )
        return [row[0] for row in cur.fetchall()]

def previous_session_close(now_et: pendulum.DateTime) -> pendulum.DateTime:
    """Most recent prior trading day at 16:00 ET (skips weekends)."""
    prev_day = now_et.subtract(days=1)
    while prev_day.day_of_week in (pendulum.SATURDAY, pendulum.SUNDAY):
        prev_day = prev_day.subtract(days=1)
    return prev_day.at(16, 0, 0)

INSERT_SQL = """
INSERT INTO intraday_data
(ticker, ts, open, high, low, close, volume, vwap, trades)
VALUES (%s, to_timestamp(%s/1000.0), %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker, ts) DO UPDATE
SET open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume,
    vwap   = EXCLUDED.vwap,
    trades = EXCLUDED.trades
"""

# ----------------------------
# Main
# ----------------------------
def fetch_overnight_bars(**kwargs):
    if not API_KEY:
        raise ValueError("MARKET_API_KEY is not set")

    now_et = pendulum.now(LOCAL_TZ)
    start_dt = previous_session_close(now_et)  # prior trading day 4:00 PM ET
    end_dt = now_et

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    conn = psycopg2.connect(**DB_CONFIG)
    tickers = get_all_active_tickers(conn)
    logger.info(
        "Overnight scan tickers=%d window=[%s,%s)",
        len(tickers), start_dt.to_iso8601_string(), end_dt.to_iso8601_string()
    )

    cur = conn.cursor()
    headers = {"Authorization": f"Bearer {API_KEY}"}

    for ticker in tickers:
        url = MASSIVE_URL.format(ticker=ticker, start=start_ms, end=end_ms)
        url = ensure_limit(url, 500)

        inserted_for_ticker = 0
        pages = 0

        while url:
            pages += 1
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code != 200:
                logger.warning(
                    "ticker=%s window=[%s,%s) status=%s body=%s",
                    ticker, start_ms, end_ms, resp.status_code, resp.text[:200]
                )
                conn.rollback()
                break

            data = resp.json()
            rows = data.get("results", []) or []
            next_url = data.get("next_url")
            url = ensure_limit(next_url, 500) if next_url else None

            if not rows:
                continue

            for r in rows:
                cur.execute(
                    INSERT_SQL,
                    (
                        ticker,
                        r["t"],
                        r.get("o"),
                        r.get("h"),
                        r.get("l"),
                        r.get("c"),
                        r.get("v"),
                        r.get("vw"),
                        r.get("n"),
                    ),
                )

            conn.commit()
            inserted_for_ticker += len(rows)

        logger.info(
            "ticker=%s inserted=%d pages=%d",
            ticker, inserted_for_ticker, pages
        )

    cur.close()
    conn.close()

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="overnight_volume_scan",
    default_args=default_args,
    description="Fetch 3-hour candles covering prior close (4pm ET) through pre-market, all active tickers",
    start_date=pendulum.datetime(2025, 12, 18, tz=LOCAL_TZ),
    schedule="0 5,8 * * 1-5",  # 5:00 AM and 8:00 AM ET, Mon-Fri
    catchup=False,
    max_active_runs=1,
    tags=["production-support", "risk-data", "market_data", "overnight"],
) as dag:

    task_fetch_overnight = PythonOperator(
        task_id="fetch_overnight_bars",
        python_callable=fetch_overnight_bars,
    )

    task_fetch_overnight
