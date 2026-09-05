from __future__ import annotations

import os
import time
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
    "password": os.getenv("POSTGRES_PASSWORD"),
}

MASSIVE_API_KEY = os.getenv("MARKET_API_KEY")

NEWS_PER_DAY = int(os.getenv("NEWS_PER_DAY", "5"))                 # keep 5 titles/day/ticker
MAX_TICKERS = int(os.getenv("NEWS_MAX_TICKERS", "0"))              # 0 => all tickers
BACKFILL_DAYS = int(os.getenv("NEWS_BACKFILL_DAYS", "365"))        # last 365 days
MODE_DEFAULT = os.getenv("NEWS_MODE", "backfill")                  # incremental|backfill

# Reliability knobs
REQUEST_TIMEOUT_SEC = float(os.getenv("NEWS_REQUEST_TIMEOUT_SEC", "20"))  # increase from 10
API_RETRY_ATTEMPTS = int(os.getenv("NEWS_API_RETRY_ATTEMPTS", "4"))
API_RETRY_BACKOFF_SEC = float(os.getenv("NEWS_API_RETRY_BACKOFF_SEC", "3.0"))
API_RETRY_BACKOFF_MAX_SEC = float(os.getenv("NEWS_API_RETRY_BACKOFF_MAX_SEC", "45.0"))

# checkpointing + pacing
CHECKPOINT_EVERY_TICKERS = int(os.getenv("NEWS_CHECKPOINT_EVERY", "25"))
SLEEP_BETWEEN_TICKERS_SEC = float(os.getenv("NEWS_SLEEP_SEC", "0.0"))

# optional: limit work per DAG run (helps avoid very long tasks)
MAX_TICKERS_PER_RUN = int(os.getenv("NEWS_MAX_TICKERS_PER_RUN", "0"))  # 0 => no cap
MAX_FAILURES_PER_RUN = int(os.getenv("NEWS_MAX_FAILURES_PER_RUN", "500"))  # safety

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def ensure_tables(conn):
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

        # Checkpoint table so backfills resume after SIGTERM / retries
        cur.execute("""
        CREATE TABLE IF NOT EXISTS news_backfill_checkpoint (
          job_name        text PRIMARY KEY,
          trade_date      date NOT NULL,
          ticker_index    int  NOT NULL,
          updated_at      timestamptz NOT NULL DEFAULT now()
        );
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

def get_checkpoint(conn, job_name: str) -> tuple[date, int] | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT trade_date, ticker_index
            FROM news_backfill_checkpoint
            WHERE job_name = %s
        """, (job_name,))
        row = cur.fetchone()
        if not row:
            return None
        return row[0], int(row[1])

def save_checkpoint(conn, job_name: str, trade_date: date, ticker_index: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO news_backfill_checkpoint (job_name, trade_date, ticker_index)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_name)
            DO UPDATE SET
              trade_date = EXCLUDED.trade_date,
              ticker_index = EXCLUDED.ticker_index,
              updated_at = now()
        """, (job_name, trade_date, ticker_index))
    conn.commit()

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def normalize_title(title: str) -> str:
    return " ".join((title or "").strip().lower().split())

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def et_day_bounds_utc(trade_date_et: date) -> tuple[str, str]:
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
    Tries underscore keys first. Some Massive SDKs accept dot-keys.
    Also enforces timeout if the underlying client supports it.
    """
    # If Massive client supports a timeout kwarg, include it.
    # If it doesn't, TypeError will be handled.
    p = dict(params)
    p["timeout"] = REQUEST_TIMEOUT_SEC  # best effort

    try:
        return client.list_ticker_news(**p)
    except TypeError:
        # remove timeout if unsupported
        p.pop("timeout", None)

        dot_params = dict(p)
        if "published_utc_gte" in dot_params:
            dot_params["published_utc.gte"] = dot_params.pop("published_utc_gte")
        if "published_utc_gt" in dot_params:
            dot_params["published_utc.gt"] = dot_params.pop("published_utc_gt")
        if "published_utc_lte" in dot_params:
            dot_params["published_utc.lte"] = dot_params.pop("published_utc_lte")
        if "published_utc_lt" in dot_params:
            dot_params["published_utc.lt"] = dot_params.pop("published_utc_lt")

        return client.list_ticker_news(**dot_params)

def iter_massive_news_with_retries(client, base_params: dict, max_items: int):
    """
    Same behavior as before, but with retry/backoff around the Massive call.
    Prevents a single slow ticker from stalling the entire Airflow task.
    """
    params = dict(base_params)
    yielded = 0

    def call_with_retry(p: dict):
        attempt = 0
        backoff = API_RETRY_BACKOFF_SEC
        last_err = None
        while attempt < API_RETRY_ATTEMPTS:
            try:
                return _call_massive_list_ticker_news(client, p)
            except Exception as e:
                last_err = e
                attempt += 1
                sleep_s = min(backoff, API_RETRY_BACKOFF_MAX_SEC)
                logger.warning("Massive call failed attempt=%d/%d err=%s sleep=%.1fs", attempt, API_RETRY_ATTEMPTS, e, sleep_s)
                time.sleep(sleep_s)
                backoff *= 2
        raise last_err  # type: ignore

    resp = call_with_retry(params)

    # Case A: iterable SDK response
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
        resp = call_with_retry(params)

# -----------------------------------------------------------------------------
# Core fetch+store
# -----------------------------------------------------------------------------
def fetch_and_store_daily5_for_ticker_date(client, conn, ticker: str, trade_date_et: date) -> int:
    cnt = existing_count_for_day(conn, trade_date_et, ticker)
    if cnt >= NEWS_PER_DAY:
        return 0

    start_utc, end_utc = et_day_bounds_utc(trade_date_et)
    params = {
        "ticker": ticker,
        "order": "desc",
        "sort": "published_utc",
        "limit": NEWS_PER_DAY,
        "published_utc_gte": start_utc,
        "published_utc_lt": end_utc,
    }

    rows = []
    rank = 0

    for it in iter_massive_news_with_retries(client, params, max_items=NEWS_PER_DAY):
        if isinstance(it, dict):
            title = it.get("title")
            published_utc = it.get("published_utc") or it.get("publishedUtc")
        else:
            title = getattr(it, "title", None)
            published_utc = getattr(it, "published_utc", None)

        if not title or not published_utc:
            continue

        rank += 1
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
# Airflow task callable (incremental & resumable backfill)
# -----------------------------------------------------------------------------
def run_news_daily5(mode: str = MODE_DEFAULT, **_):
    if not MASSIVE_API_KEY:
        raise ValueError("MARKET_API_KEY env var is missing")

    mode = (mode or "incremental").lower()
    if mode not in ("incremental", "backfill"):
        raise ValueError(f"Invalid mode={mode}. Use incremental|backfill")

    # Lazy import to avoid Broken DAG if massive not installed at parse time
    from massive import RESTClient  # noqa

    conn = get_conn()
    try:
        ensure_tables(conn)
        tickers = fetch_sp500_qqq_tickers(conn)
        client = RESTClient(MASSIVE_API_KEY)

        today_et = pendulum.now(LOCAL_TZ).date()

        if mode == "incremental":
            dates = [today_et]
            job_name = "news_incremental"
        else:
            start_date_et = today_et - timedelta(days=BACKFILL_DAYS)
            dates = []
            d = start_date_et
            while d <= today_et:
                dates.append(d)
                d = d + timedelta(days=1)
            job_name = f"news_backfill_{BACKFILL_DAYS}d"

        logger.info("Mode=%s job_name=%s dates=%s..%s (%d) tickers=%d",
                    mode, job_name, dates[0], dates[-1], len(dates), len(tickers))

        # Resume checkpoint only for backfill
        start_di = 0
        start_ti = 0
        if mode == "backfill":
            ck = get_checkpoint(conn, job_name)
            if ck is not None:
                ck_date, ck_idx = ck
                if ck_date in dates:
                    start_di = dates.index(ck_date)
                    start_ti = ck_idx
                    logger.info("Resuming from checkpoint: date=%s ticker_index=%d", ck_date, ck_idx)

        total_inserted = 0
        failures = 0
        processed_tickers_this_run = 0

        for di in range(start_di, len(dates)):
            trade_date_et = dates[di]
            inserted_for_date = 0

            # Resume ticker index on the first resumed day; otherwise start at 0
            ti_start = start_ti if di == start_di else 0

            for ti in range(ti_start, len(tickers)):
                ticker = tickers[ti]

                try:
                    inserted = fetch_and_store_daily5_for_ticker_date(client, conn, ticker, trade_date_et)
                    inserted_for_date += inserted
                except Exception as e:
                    # don't bubble; keep going
                    failures += 1
                    logger.warning("Fetch failed ticker=%s date=%s err=%s failures=%d", ticker, trade_date_et, e, failures)
                    if failures >= MAX_FAILURES_PER_RUN:
                        logger.error("Too many failures in one run (%d). Stopping early.", failures)
                        save_checkpoint(conn, job_name, trade_date_et, ti)
                        return

                processed_tickers_this_run += 1
                if SLEEP_BETWEEN_TICKERS_SEC > 0:
                    time.sleep(SLEEP_BETWEEN_TICKERS_SEC)

                # checkpoint every N tickers
                if mode == "backfill" and (ti % CHECKPOINT_EVERY_TICKERS == 0):
                    save_checkpoint(conn, job_name, trade_date_et, ti)

                if (ti + 1) % 50 == 0:
                    logger.info("Date %d/%d: processed %d/%d tickers (inserted_today=%d, failures=%d)",
                                di + 1, len(dates), ti + 1, len(tickers), inserted_for_date, failures)

                # optional cap: make each run bounded
                if MAX_TICKERS_PER_RUN > 0 and processed_tickers_this_run >= MAX_TICKERS_PER_RUN:
                    logger.info("Reached NEWS_MAX_TICKERS_PER_RUN=%d. Saving checkpoint and exiting.", MAX_TICKERS_PER_RUN)
                    if mode == "backfill":
                        save_checkpoint(conn, job_name, trade_date_et, ti + 1)  # resume at next ticker
                    return

            logger.info("Date=%s inserted_rows=%d", trade_date_et, inserted_for_date)
            total_inserted += inserted_for_date

            # date finished => set checkpoint to next day start
            if mode == "backfill":
                next_date = dates[di + 1] if (di + 1) < len(dates) else trade_date_et
                save_checkpoint(conn, job_name, next_date, 0)

        logger.info("DONE mode=%s total_inserted=%d failures=%d", mode, total_inserted, failures)

    finally:
        conn.close()

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="news_daily5_backfill_manual",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["news", "massive", "backfill", "daily5"],
) as dag:

    run_backfill = PythonOperator(
        task_id="run_news_daily5_backfill",
        python_callable=run_news_daily5,
        op_kwargs={"mode": "backfill"},
        # Keep task from being killed by "too long" operator timeout (tune as needed)
        execution_timeout=timedelta(hours=12),
    )
