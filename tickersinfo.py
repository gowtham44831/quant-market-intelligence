"""
Airflow DAG: tickers_massive_sp500_qqq

- Pulls tickers from Massive API (paginated)
- Pulls SP500 + Nasdaq-100 (QQQ) constituents & weights from SlickCharts
  (uses requests + browser-like headers to avoid 403)
- Upserts into Postgres (tickers_info) with sp500/qqq flags + weights

Env:
  MARKET_API_KEY = Massive API key
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
from io import StringIO

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# -----------------------------
# Config
# -----------------------------
MASSIVE_URL = "https://api.massive.com/v3/reference/tickers?limit=100"
API_KEY = os.getenv("MARKET_API_KEY")

SP500_URL = "https://www.slickcharts.com/sp500"
QQQ_URL = "https://www.slickcharts.com/nasdaq100"
SP500_SECTOR_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

SECTOR_TO_ETF = {
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Financials": "XLF",
    "Health Care": "XLV",
    "Industrials": "XLI",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Information Technology": "XLK",
    "Utilities": "XLU",
}

DB_CONFIG = {
    "host": os.getenv("PGHOST", "postgres"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "stocks"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

logger = logging.getLogger("airflow.task")

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive",
}

# -----------------------------
# Helpers
# -----------------------------
def _read_first_table_from_url(url: str, session: requests.Session) -> pd.DataFrame:
    resp = session.get(url, headers=BROWSER_HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text))
    if not tables:
        raise ValueError(f"No HTML tables found at {url}")
    return tables[0]


def fetch_sp500_qqq():
    """
    Returns:
      sp500_weights: dict[ticker -> float]
      qqq_weights: dict[ticker -> float]
    """
    sp500_weights, qqq_weights, sectors = {}, {}, {}
    session = requests.Session()

    try:
        sp_df = _read_first_table_from_url(SP500_URL, session)
        qqq_df = _read_first_table_from_url(QQQ_URL, session)

        for _, row in sp_df.iterrows():
            sym = str(row.get("Symbol", "")).strip().upper()
            if not sym:
                continue
            w = float(str(row.get("Weight", "0")).replace("%", "").strip())
            sp500_weights[sym] = w

        for _, row in qqq_df.iterrows():
            sym = str(row.get("Symbol", "")).strip().upper()
            if not sym:
                continue
            w = float(str(row.get("Weight", "0")).replace("%", "").strip())
            qqq_weights[sym] = w

        logger.info("Fetched weights: sp500=%d qqq=%d", len(sp500_weights), len(qqq_weights))
    except Exception as e:
        logger.error("Error fetching SP500/QQQ from SlickCharts: %s", e)

    # Sector metadata is useful context, but its independent source must never
    # prevent index membership and weights from refreshing.
    try:
        sector_df = _read_first_table_from_url(SP500_SECTOR_URL, session)
        for _, row in sector_df.iterrows():
            sym = str(row.get("Symbol", "")).strip().upper().replace(".", "-")
            sector = str(row.get("GICS Sector", "")).strip()
            sector_etf = SECTOR_TO_ETF.get(sector)
            if sym and sector_etf:
                sectors[sym] = (sector, sector_etf)
        logger.info("Fetched sector mappings: %d", len(sectors))
    except Exception as e:
        logger.error("Error fetching S&P 500 sector mappings: %s", e)

    return sp500_weights, qqq_weights, sectors


def fetch_massive_tickers_batch():
    if not API_KEY:
        raise RuntimeError("MARKET_API_KEY env var is not set")

    url = MASSIVE_URL
    headers = {"Authorization": f"Bearer {API_KEY}"}

    while url:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", []) or []
        next_url = data.get("next_url")

        logger.info("Fetched %d tickers from Massive; next_url=%s", len(results), next_url)
        yield results
        url = next_url


def _dedupe_rows_by_ticker(rows):
    """
    rows: list[tuple] where rows[i][0] is ticker
    Keep last occurrence of each ticker within the same bulk insert.
    """
    dedup = {}
    for r in rows:
        dedup[r[0]] = r
    return list(dedup.values())


# -----------------------------
# Main load
# -----------------------------
def load_tickers_to_postgres(**kwargs):
    sp500_weights, qqq_weights, sectors = fetch_sp500_qqq()

    insert_sql = """
        INSERT INTO tickers_info
        (ticker, name, market, locale, primary_exchange, type, active, currency_name, cik,
         composite_figi, share_class_figi, sp500, sp500_weight, qqq, qqq_weight, last_updated_utc)
        VALUES %s
        ON CONFLICT (ticker) DO UPDATE
        SET name=EXCLUDED.name,
            market=EXCLUDED.market,
            locale=EXCLUDED.locale,
            primary_exchange=EXCLUDED.primary_exchange,
            type=EXCLUDED.type,
            active=EXCLUDED.active,
            currency_name=EXCLUDED.currency_name,
            cik=EXCLUDED.cik,
            composite_figi=EXCLUDED.composite_figi,
            share_class_figi=EXCLUDED.share_class_figi,
            sp500=EXCLUDED.sp500,
            sp500_weight=EXCLUDED.sp500_weight,
            qqq=EXCLUDED.qqq,
            qqq_weight=EXCLUDED.qqq_weight,
            last_updated_utc=EXCLUDED.last_updated_utc
    """

    conn = None
    cur = None
    total_rows = 0

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticker_sector_map (
                ticker varchar(20) PRIMARY KEY,
                sector text NOT NULL,
                sector_etf varchar(10) NOT NULL,
                source text,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
        """)
        if sectors:
            execute_values(
                cur,
                """
                INSERT INTO ticker_sector_map (ticker, sector, sector_etf, source)
                VALUES %s
                ON CONFLICT (ticker) DO UPDATE SET
                    sector = EXCLUDED.sector,
                    sector_etf = EXCLUDED.sector_etf,
                    source = EXCLUDED.source,
                    updated_at = now()
                """,
                [(ticker, sector, etf, "Wikipedia S&P 500 GICS")
                 for ticker, (sector, etf) in sectors.items()],
                page_size=1000,
            )
        conn.commit()

        for batch in fetch_massive_tickers_batch():
            rows = []
            for r in batch:
                ticker_symbol = (r.get("ticker") or "").strip().upper()
                if not ticker_symbol:
                    continue

                rows.append(
                    (
                        ticker_symbol,
                        r.get("name"),
                        r.get("market"),
                        r.get("locale"),
                        r.get("primary_exchange"),
                        r.get("type"),
                        r.get("active"),
                        r.get("currency_name"),
                        r.get("cik"),
                        r.get("composite_figi"),
                        r.get("share_class_figi"),
                        ticker_symbol in sp500_weights,
                        float(sp500_weights.get(ticker_symbol, 0.0)),
                        ticker_symbol in qqq_weights,
                        float(qqq_weights.get(ticker_symbol, 0.0)),
                        r.get("last_updated_utc"),
                    )
                )

            if not rows:
                continue

            before = len(rows)
            rows = _dedupe_rows_by_ticker(rows)
            after = len(rows)
            if after < before:
                logger.warning("Removed %d duplicate tickers within batch (before=%d after=%d)",
                               before - after, before, after)

            execute_values(cur, insert_sql, rows, page_size=1000)
            conn.commit()

            total_rows += len(rows)
            logger.info("Upserted %d rows in this batch (total=%d)", len(rows), total_rows)

        logger.info("Completed tickers load. Total rows upserted=%d", total_rows)

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Error loading tickers into Postgres: %s", e)
        raise

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


# -----------------------------
# DAG
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="tickers_massive_sp500_qqq",
    default_args=default_args,
    description="Fetch Massive tickers + SP500/QQQ weights and upsert into Postgres",
    schedule="0 9 * * 0",  # Sun at 09:00
    start_date=datetime(2025, 12, 20, 9, 0),
    catchup=False,
    max_active_runs=1,
    tags=["production", "reference-data", "weekly"],
)

fetch_task = PythonOperator(
    task_id="fetch_and_load_tickers",
    python_callable=load_tickers_to_postgres,
    dag=dag,
)

fetch_task
