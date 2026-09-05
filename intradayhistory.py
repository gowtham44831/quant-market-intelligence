from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import time
import random
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import psycopg2
import psycopg2.extras

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

BASE_URL = (
    "https://api.massive.com/v2/aggs/ticker/{ticker}/range/10/minute/{start}/{end}"
    "?adjusted=true&limit=500&sort=asc"
)

# Defaults are conservative for daily runs; can override via dag_run.conf
DEFAULT_LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "365"))
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "30"))
LIMIT_PER_PAGE = int(os.getenv("LIMIT_PER_PAGE", "500"))
COMMIT_EVERY_PAGES = int(os.getenv("COMMIT_EVERY_PAGES", "10"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------
# HTTP session with retries
# ----------------------------
def make_session():
    retry = Retry(
        total=8,
        connect=5,
        read=5,
        status=8,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

def safe_get(url, headers):
    return SESSION.get(url, headers=headers, timeout=(10, 120))

def throttle(base_sleep=0.08):
    time.sleep(base_sleep + random.random() * 0.15)

def ensure_limit(url: str, limit: int = 500) -> str:
    if not url:
        return url
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q["limit"] = str(limit)
    q.setdefault("sort", "asc")
    return urlunparse(u._replace(query=urlencode(q)))

# ----------------------------
# DB + tickers
# ----------------------------
def get_sp500_qqq_tickers():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ticker
        FROM tickers_info
        WHERE sp500 = TRUE OR qqq = TRUE
        """
    )
    tickers = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return tickers

INSERT_SQL = """
INSERT INTO intraday_data
(ticker, ts, open, high, low, close, volume, vwap, trades)
VALUES %s
ON CONFLICT (ticker, ts) DO UPDATE
SET open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume,
    vwap   = EXCLUDED.vwap,
    trades = EXCLUDED.trades
"""

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = (8, 0)    # 8:00 AM ET
MARKET_CLOSE = (16, 0)  # 4:00 PM ET

def is_regular_market_hours(ts_utc: datetime) -> bool:
    local = ts_utc.astimezone(MARKET_TZ)
    if local.weekday() >= 5:
        return False
    open_t = local.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    close_t = local.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    return open_t <= local < close_t

def insert_bars_page(cur, ticker, bars):
    if not bars:
        return 0

    rows = []
    for b in bars:
        ts = datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc)
        if not is_regular_market_hours(ts):
            continue
        rows.append(
            (
                ticker,
                ts,
                b.get("o"),
                b.get("h"),
                b.get("l"),
                b.get("c"),
                b.get("v"),
                b.get("vw"),
                b.get("n"),
            )
        )

    psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=500)
    return len(rows)

# ----------------------------
# Chunking
# ----------------------------
def iter_chunks(start_dt, end_dt, chunk_days=5):
    cur = start_dt
    while cur < end_dt:
        nxt = min(cur + timedelta(days=chunk_days), end_dt)
        yield cur, nxt
        cur = nxt

def chunk_already_loaded(conn, ticker: str, start_dt: datetime, end_dt: datetime) -> bool:
    """
    Fast check: if we already have any bars in this window, skip refetch.
    This prevents re-downloading 1 year repeatedly.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM intraday_data
            WHERE ticker = %s AND ts >= %s AND ts < %s
            LIMIT 1
            """,
            (ticker, start_dt, end_dt),
        )
        return cur.fetchone() is not None

def stream_fetch_and_store_ticker_range(conn, ticker, start_dt, end_dt, headers):
    url = BASE_URL.format(
        ticker=ticker,
        start=int(start_dt.timestamp() * 1000),
        end=int(end_dt.timestamp() * 1000),
    )
    url = ensure_limit(url, LIMIT_PER_PAGE)

    total_upserted = 0
    pages = 0
    cur = conn.cursor()

    try:
        while url:
            throttle()

            resp = safe_get(url, headers=headers)
            if resp.status_code != 200:
                logger.warning("Non-200 status=%s ticker=%s url=%s", resp.status_code, ticker, url)
                break

            data = resp.json()
            bars = data.get("results", []) or []
            next_url = data.get("next_url")

            upserted = insert_bars_page(cur, ticker, bars)
            total_upserted += upserted
            pages += 1

            if pages % COMMIT_EVERY_PAGES == 0:
                conn.commit()

            logger.info(
                "Ticker=%s range=%s..%s page=%d page_bars=%d upserted=%d total=%d next=%s",
                ticker,
                start_dt.date(),
                end_dt.date(),
                pages,
                len(bars),
                upserted,
                total_upserted,
                bool(next_url),
            )

            url = ensure_limit(next_url, LIMIT_PER_PAGE) if next_url else None

        conn.commit()

    finally:
        cur.close()

    return total_upserted

# ----------------------------
# Main
# ----------------------------
def process_ticker(ticker, start_dt, end_dt, headers):
    conn = psycopg2.connect(**DB_CONFIG)
    inserted_total = 0
    try:
        for c_start, c_end in iter_chunks(start_dt, end_dt, chunk_days=CHUNK_DAYS):
            # Skip if already loaded (prevents repeated 1-year re-download)
            if chunk_already_loaded(conn, ticker, c_start, c_end):
                logger.info("Skip ticker=%s chunk=%s..%s (already loaded)",
                            ticker, c_start.date(), c_end.date())
                continue

            inserted_total += stream_fetch_and_store_ticker_range(
                conn, ticker, c_start, c_end, headers
            )

        logger.info("Done ticker=%s total_upserted=%d", ticker, inserted_total)

    except requests.exceptions.ReadTimeout:
        conn.rollback()
        logger.exception("ReadTimeout ticker=%s (skipping)", ticker)

    except Exception:
        conn.rollback()
        logger.exception("Error ticker=%s (skipping)", ticker)

    finally:
        conn.close()

    return ticker, inserted_total

def fetch_and_store_all_tickers(**context):
    if not API_KEY:
        raise ValueError("MARKET_API_KEY is not set")

    # Allow manual override:
    # Trigger DAG with: {"lookback_days": 365}
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    lookback_days = int(conf.get("lookback_days", DEFAULT_LOOKBACK_DAYS))

    headers = {"Authorization": f"Bearer {API_KEY}"}

    # Prefer "yesterday" end time to avoid partial current day
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=lookback_days)

    tickers = get_sp500_qqq_tickers()
    logger.info(
        "Tickers=%d lookback_days=%d chunk_days=%d limit=%d workers=%d",
        len(tickers), lookback_days, CHUNK_DAYS, LIMIT_PER_PAGE, MAX_WORKERS
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_ticker, ticker, start_dt, end_dt, headers): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                future.result()
            except Exception:
                logger.exception("Unhandled error for ticker=%s", ticker)

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="fetch_sp500_qqq_historical_data",
    default_args=default_args,
    description="Fetch 10-min bars for SP500 & QQQ tickers (paged, upsert, skip already-loaded chunks)",
    start_date=datetime(2025, 12, 24),
    schedule_interval=None,   # daily incremental by default (2 days unless overridden)
    catchup=False,
    tags=["market_data"],
    max_active_runs=1,
) as dag:

    task_fetch_store = PythonOperator(
        task_id="fetch_and_store_bars",
        python_callable=fetch_and_store_all_tickers,
    )

    task_fetch_store
