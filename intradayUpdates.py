from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import requests
import os
import logging
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import pendulum

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

API_KEY = os.getenv("MARKET_API_KEY")

MASSIVE_URL = (
    "https://api.massive.com/v2/aggs/ticker/{ticker}/range/10/minute/{start}/{end}"
    "?adjusted=true&limit=500&sort=asc"
)

logger = logging.getLogger("airflow.task")

def ensure_limit(url: str, limit: int = 500) -> str:
    if not url:
        return url
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q["limit"] = str(limit)
    q.setdefault("sort", "asc")
    return urlunparse(u._replace(query=urlencode(q)))

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

def fetch_intraday_sp500_qqq(**kwargs):
    if not API_KEY:
        raise ValueError("MARKET_API_KEY is not set")

    start_ms = int(kwargs["data_interval_start"].timestamp() * 1000)
    end_ms = int(kwargs["data_interval_end"].timestamp() * 1000)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT ticker
        FROM tickers_info
        WHERE sp500 = true OR qqq = true
        """
    )
    tickers = [r[0] for r in cur.fetchall()]

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
                        r.get("vw"),  # vwap
                        r.get("n"),   # number of trades
                    ),
                )

            conn.commit()
            inserted_for_ticker += len(rows)

        logger.info(
            "ticker=%s inserted=%d window=[%s,%s) pages=%d",
            ticker, inserted_for_ticker, start_ms, end_ms, pages
        )

    cur.close()
    conn.close()


local_tz = pendulum.timezone("America/New_York")

dag = DAG(
    dag_id="intraday_sp500_qqq_10min",
    start_date=datetime(2025, 12, 18, tzinfo=local_tz),
    schedule="*/10 8-16 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["production-support", "risk-data", "intraday"],
)

PythonOperator(
    task_id="fetch_intraday",
    python_callable=fetch_intraday_sp500_qqq,
    dag=dag,
)
