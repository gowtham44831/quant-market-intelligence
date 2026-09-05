from __future__ import annotations

import os
import hashlib
import logging
from datetime import timedelta, date
from urllib.parse import urlparse, parse_qs

import pendulum
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
LOCAL_TZ = pendulum.timezone("America/New_York")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

MASSIVE_API_KEY = os.getenv("MARKET_API_KEY")
NEWS_PER_DAY = int(os.getenv("NEWS_PER_DAY", "5"))          # keep 5 titles/day/ticker
MAX_TICKERS = int(os.getenv("NEWS_MAX_TICKERS", "0"))       # 0 => all tickers
BACKFILL_DAYS = int(os.getenv("NEWS_BACKFILL_DAYS", "90"))  # default: last 90 days

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ticker_news_daily (
          trade_date      date NOT NULL,          -- ET date
          ticker          text NOT NULL,
          rank            int  NOT NULL,          -- 1..5 newest-first for the ET day
          published_utc   timestamptz NOT NULL,
          title           text NOT NULL,
          title_hash      text NOT NULL,
          inserted_at     timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (trade_date, ticker, rank)
        );
        """)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ticker_news_daily_dedupe
        ON ticker_news_daily (trade_date, ticker, title_hash);
        """)
    conn.commit()

def fetch_sp500_qqq_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ticker
            FROM tickers_info
            WHERE sp500 = TRUE OR qqq = TRUE
            ORDER BY ticker
        """)
        rows = [r[0] for r in cur.fetchall()]
        if MAX_TICKERS and MAX_TICKERS > 0:
            return rows[:MAX_TICKERS]
        return rows

def existing_count_for_day(conn, trade_date_et: date, ticker: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM ticker_news_daily
            WHERE trade_date = %s AND ticker = %s
        """, (trade_date_et, ticker))
        return int(cur.fetchone()[0])

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def normalize_title(title: str) -> str:
    return " ".join((title or "").strip().lower().split())

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def et_day_bounds_utc(trade_date_et: date) -> tuple[str, str]:
    """
    Returns UTC ISO8601 strings for the ET day window:
      [start_of_day_ET, start_of_next_day_ET)
    """
    start_et = pendulum.datetime(trade_date_et.year, trade_date_et.month, trade_date_et.day, tz=LOCAL_TZ)
    end_et = start_et.add(days=1)
    return (
        start_et.in_timezone("UTC").to_iso8601_string(),
        end_et.in_timezone("UTC").to_iso8601_string(),
    )

# -----------------------------------------------------------------------------
# Massive pagination + filter compatibility
# -----------------------------------------------------------------------------
def _call_massive_list_ticker_news(client, params: dict):
    """
    Massive docs show query keys like: published_utc.gte / published_utc.lt
    Some SDKs accept underscores: published_utc_gte / published_utc_lt
    This helper tries underscore first, then dot-keys via **dict expansion.
    """
    # 1) Try underscore version
    try:
        return client.list_ticker_news(**params)
    except TypeError:
        # 2) Convert underscore keys to dot keys if present
        dot_params = dict(params)
        if "published_utc_gte" in dot_params:
            dot_params["published_utc.gte"] = dot_params.pop("published_utc_gte")
        if "published_utc_gt" in dot_params:
            dot_params["published_utc.gt"] = dot_params.pop("published_utc_gt")
        if "published_utc_lte" in dot_params:
            dot_params["published_utc.lte"] = dot_params.pop("published_utc_lte")
        if "published_utc_lt" in dot_params:
            dot_params["published_utc.lt"] = dot_params.pop("published_utc_lt")

        return client.list_ticker_news(**dot_params)

def iter_massive_news(client, base_params: dict, max_items: int):
    """
    Robust iterator for Massive list_ticker_news supporting:
    - Iterator/generator of objects (most SDKs)
    - Page dict with 'results' and 'next_url' (cursor) style

    Stops after max_items items yielded.
    """
    params = dict(base_params)
    yielded = 0

    resp = _call_massive_list_ticker_news(client, params)

    # Case A: SDK yields objects (iterable, not dict)
    if not isinstance(resp, dict):
        for item in resp:
            yield item
            yielded += 1
            if yielded >= max_items:
                return
        return

    # Case B: dict page response
    while True:
        results = resp.get("results") or []
        for item in results:
            yield item
            yielded += 1
            if yielded >= max_items:
                return

        next_url = resp.get("next_url")
        if not next_url:
            return

        cursor = parse_qs(urlparse(next_url).query).get("cursor", [None])[0]
        if not cursor:
            return

        params["cursor"] = cursor
        resp = _call_massive_list_ticker_news(client, params)

# -----------------------------------------------------------------------------
# Core fetch+store
# -----------------------------------------------------------------------------
def fetch_and_store_daily5_for_ticker_date(client, conn, ticker: str, trade_date_et: date) -> int:
    """
    Ensures up to NEWS_PER_DAY rows for (ticker, trade_date_et).
    If already >= NEWS_PER_DAY, skips.
    Uses Massive published_utc filters for that day window and keeps newest 5.
    """
    cnt = existing_count_for_day(conn, trade_date_et, ticker)
    if cnt >= NEWS_PER_DAY:
        return 0

    start_utc, end_utc = et_day_bounds_utc(trade_date_et)

    # Fetch newest-first within ET-day window; only need up to 5
    params = {
        "ticker": ticker,
        "order": "desc",
        "sort": "published_utc",
        "limit": NEWS_PER_DAY,                 # page size (also ok for non-paginated)
        "published_utc_gte": start_utc,        # will fallback to published_utc.gte if needed
        "published_utc_lt": end_utc,           # will fallback to published_utc.lt if needed
    }

    # Lazy import to avoid Broken DAG at parse-time if package missing
    from massive.rest.models import TickerNews  # noqa

    rows = []
    rank = 0

    for it in iter_massive_news(client, params, max_items=NEWS_PER_DAY):
        if not isinstance(it, TickerNews):
            # Some SDKs may return dicts; handle that too
            if isinstance(it, dict):
                title = it.get("title")
                published_utc = it.get("published_utc") or it.get("publishedUtc")
            else:
                continue
        else:
            title = getattr(it, "title", None)
            published_utc = getattr(it, "published_utc", None)

        if not title or not published_utc:
            continue

        rank += 1
        if rank > NEWS_PER_DAY:
            break

        title_hash = sha1(f"{ticker}|{trade_date_et.isoformat()}|{published_utc}|{normalize_title(title)}")
        rows.append((trade_date_et, ticker, rank, published_utc, title, title_hash))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ticker_news_daily
              (trade_date, ticker, rank, published_utc, title, title_hash)
            VALUES %s
            ON CONFLICT (trade_date, ticker, rank) DO NOTHING
            """,
            rows,
            page_size=1000,
        )
    conn.commit()
    return len(rows)

# -----------------------------------------------------------------------------
# Airflow task callable (same for incremental & backfill)
# -----------------------------------------------------------------------------
def run_news_daily5(mode: str, **context):
    if not MASSIVE_API_KEY:
        raise ValueError("MASSIVE_API_KEY env var is missing")

    mode = (mode or "incremental").lower()
    if mode not in ("incremental", "backfill"):
        raise ValueError(f"Invalid mode={mode}. Use incremental|backfill")

    # Lazy import to avoid Broken DAG if massive not installed at parse time
    from massive import RESTClient  # noqa

    conn = get_conn()
    try:
        ensure_table(conn)
        tickers = fetch_sp500_qqq_tickers(conn)

        client = RESTClient(MASSIVE_API_KEY)

        today_et = pendulum.now(LOCAL_TZ).date()

        if mode == "backfill":
            start_date_et = today_et - timedelta(days=BACKFILL_DAYS)
            dates = []
            d = start_date_et
            while d <= today_et:
                dates.append(d)
                d = d + timedelta(days=1)
            logger.info("Backfill: dates %s .. %s (%d days)", dates[0], dates[-1], len(dates))
        else:
            dates = [today_et]
            logger.info("Incremental: date %s", today_et)

        total_inserted = 0

        for di, trade_date_et in enumerate(dates, start=1):
            inserted_for_date = 0
            for ti, ticker in enumerate(tickers, start=1):
                try:
                    inserted = fetch_and_store_daily5_for_ticker_date(client, conn, ticker, trade_date_et)
                    inserted_for_date += inserted
                except Exception as e:
                    logger.warning("Fetch failed ticker=%s date=%s err=%s", ticker, trade_date_et, e)

                if ti % 50 == 0:
                    logger.info("Date %d/%d: processed %d/%d tickers...", di, len(dates), ti, len(tickers))

            logger.info("Date=%s inserted_rows=%d", trade_date_et, inserted_for_date)
            total_inserted += inserted_for_date

        logger.info("DONE mode=%s total_inserted=%d", mode, total_inserted)

    finally:
        conn.close()

# -----------------------------------------------------------------------------
# DAGs
# -----------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 1) Incremental: runs twice daily at 12:30 & 15:30 ET Mon-Fri
with DAG(
    dag_id="news_daily5_incremental_1230_1530_et",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule="30 12,15 * * 1-5",  # 12:30 PM and 3:30 PM ET
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["news", "massive", "incremental", "daily5"],
) as dag:

    run_incremental = PythonOperator(
        task_id="run_news_daily5_incremental",
        python_callable=run_news_daily5,
        op_kwargs={"mode": "incremental"},
    )
