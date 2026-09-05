from __future__ import annotations

import os
import logging
from datetime import timedelta, date

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

# Backfill chunk size (days). Choose 7 (week), 30 (month-ish), 365 (year).
BATCH_DAYS = int(os.getenv("NEWS_BACKFILL_BATCH_DAYS", "365"))

# Force rebuild existing daily features (default false => idempotent "missing-only")
FORCE_REBUILD = os.getenv("NEWS_FORCE_REBUILD", "false").lower() == "true"

# Optional date bounds (ET dates): YYYY-MM-DD
START_DATE_ET = os.getenv("NEWS_BACKFILL_START_DATE_ET", "").strip()
END_DATE_ET = os.getenv("NEWS_BACKFILL_END_DATE_ET", "").strip()

NEG_THRESH = float(os.getenv("NEWS_NEG_THRESH", "-0.2"))
POS_THRESH = float(os.getenv("NEWS_POS_THRESH", "0.2"))

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_tables(conn):
    """
    Source:
      ticker_news_daily(trade_date, ticker, rank, published_utc, title, title_hash)

    Target:
      ticker_news_features_daily(trade_date, ticker, vader_* etc.)
    """
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ticker_news_daily (
          trade_date      date NOT NULL,          -- ET trade date
          ticker          text NOT NULL,
          rank            int  NOT NULL,          -- 1..5 (newest-first for the day)
          published_utc   timestamptz NOT NULL,
          title           text NOT NULL,
          title_hash      text NOT NULL,
          inserted_at     timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (trade_date, ticker, rank)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ticker_news_features_daily (
          trade_date              date NOT NULL,     -- ET date
          ticker                  text NOT NULL,
          news_count              int NOT NULL,
          vader_mean              double precision NOT NULL,
          vader_min               double precision NOT NULL,
          vader_max               double precision NOT NULL,
          neg_count               int NOT NULL,
          pos_count               int NOT NULL,
          last_published_utc      timestamptz,
          updated_at              timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (trade_date, ticker)
        );
        """)

    conn.commit()


def parse_date_or_none(s: str) -> date | None:
    if not s:
        return None
    return pendulum.parse(s).date()


def get_bounds_from_source(conn) -> tuple[date | None, date | None]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(trade_date), MAX(trade_date)
            FROM ticker_news_daily
        """)
        r = cur.fetchone()
        return (r[0], r[1])


def daterange_chunks(start: date, end: date, step_days: int):
    d = start
    while d <= end:
        chunk_end = min(end, d + timedelta(days=step_days - 1))
        yield d, chunk_end
        d = chunk_end + timedelta(days=1)


# -----------------------------------------------------------------------------
# Main backfill task
# -----------------------------------------------------------------------------
def backfill_from_ticker_news_daily(**context):
    """
    Manual backfill:
      - Reads full history from ticker_news_daily in date chunks
      - Computes VADER per title, aggregates per (trade_date, ticker)
      - Upserts into ticker_news_features_daily

    Idempotency:
      - If FORCE_REBUILD=false (default), it only computes rows missing in target
      - If FORCE_REBUILD=true, it recomputes and overwrites existing rows
    """
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    conn = get_conn()
    try:
        ensure_tables(conn)

        src_min, src_max = get_bounds_from_source(conn)
        if src_min is None or src_max is None:
            logger.info("ticker_news_daily is empty; nothing to backfill.")
            return

        cfg_start = parse_date_or_none(START_DATE_ET)
        cfg_end = parse_date_or_none(END_DATE_ET)

        start_et = cfg_start or src_min
        end_et = cfg_end or src_max

        if start_et > end_et:
            logger.info("Invalid bounds: start_et=%s end_et=%s. Nothing to do.", start_et, end_et)
            return

        logger.info(
            "Backfill from ticker_news_daily: %s..%s BATCH_DAYS=%d FORCE_REBUILD=%s",
            start_et, end_et, BATCH_DAYS, FORCE_REBUILD
        )

        # Process chunk-by-chunk to keep memory bounded
        for chunk_start, chunk_end in daterange_chunks(start_et, end_et, BATCH_DAYS):
            logger.info("Chunk %s..%s", chunk_start, chunk_end)

            # Pull titles for the chunk
            # rank is already 1..5 newest-first per day, so no need for window functions
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date, ticker, published_utc, title
                    FROM ticker_news_daily
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date ASC, ticker ASC, rank ASC
                    """,
                    (chunk_start, chunk_end),
                )
                rows = cur.fetchall()

            if not rows:
                logger.info("No source rows in this chunk.")
                continue

            # Group: (trade_date, ticker) -> list[(published_utc, title)]
            grouped: dict[tuple[date, str], list[tuple[object, str]]] = {}
            for trade_date, ticker, published_utc, title in rows:
                grouped.setdefault((trade_date, ticker), []).append((published_utc, title))

            # If missing-only mode, filter out keys already present
            if not FORCE_REBUILD:
                keys = list(grouped.keys())
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        SELECT trade_date, ticker
                        FROM ticker_news_features_daily
                        WHERE (trade_date, ticker) IN (%s)
                        """,
                        [(k[0], k[1]) for k in keys],
                        page_size=5000,
                    )
                    existing = set(cur.fetchall())

                grouped = {k: v for k, v in grouped.items() if k not in existing}

            if not grouped:
                logger.info("Chunk %s..%s: no missing rows to compute.", chunk_start, chunk_end)
                continue

            out_rows = []
            for (trade_date, ticker), items in grouped.items():
                compounds = []
                neg = 0
                pos = 0
                last_pub = None

                for published_utc, title in items:
                    score = float(analyzer.polarity_scores(title or "")["compound"])
                    compounds.append(score)
                    if score <= NEG_THRESH:
                        neg += 1
                    if score >= POS_THRESH:
                        pos += 1
                    if last_pub is None or published_utc > last_pub:
                        last_pub = published_utc

                n = len(compounds)
                if n == 0:
                    continue

                out_rows.append(
                    (
                        trade_date,
                        ticker,
                        int(n),
                        float(sum(compounds) / n),
                        float(min(compounds)),
                        float(max(compounds)),
                        int(neg),
                        int(pos),
                        last_pub,
                    )
                )

            if not out_rows:
                logger.info("Chunk %s..%s: computed 0 rows (unexpected).", chunk_start, chunk_end)
                continue

            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO ticker_news_features_daily
                      (trade_date, ticker,
                       news_count, vader_mean, vader_min, vader_max,
                       neg_count, pos_count, last_published_utc)
                    VALUES %s
                    ON CONFLICT (trade_date, ticker)
                    DO UPDATE SET
                      news_count = EXCLUDED.news_count,
                      vader_mean = EXCLUDED.vader_mean,
                      vader_min = EXCLUDED.vader_min,
                      vader_max = EXCLUDED.vader_max,
                      neg_count = EXCLUDED.neg_count,
                      pos_count = EXCLUDED.pos_count,
                      last_published_utc = EXCLUDED.last_published_utc,
                      updated_at = NOW()
                    """,
                    out_rows,
                    page_size=2000,
                )

            conn.commit()
            logger.info("Chunk %s..%s: upserted %d rows", chunk_start, chunk_end, len(out_rows))

        logger.info("DONE backfill_from_ticker_news_daily.")

    finally:
        conn.close()


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ticker_news_features_daily_backfill_manual",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule=None,  # ✅ manual trigger only
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["news", "vader", "backfill", "manual", "daily_features"],
) as dag:

    backfill_task = PythonOperator(
        task_id="backfill_from_ticker_news_daily",
        python_callable=backfill_from_ticker_news_daily,
    )
