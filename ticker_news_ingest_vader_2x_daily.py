from __future__ import annotations

import os
import logging
from datetime import timedelta, datetime, timezone

import pendulum
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

LOCAL_TZ = pendulum.timezone("America/New_York")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# How many days back to consider when computing snapshot (rolling lookback)
LOOKBACK_DAYS = int(os.getenv("NEWS_LOOKBACK_DAYS", "7"))

NEG_THRESH = float(os.getenv("NEWS_NEG_THRESH", "-0.2"))
POS_THRESH = float(os.getenv("NEWS_POS_THRESH", "0.2"))

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def ensure_tables(conn):
    """
    Source: ticker_news_daily (already exists in your DB)
    Outputs:
      - ticker_news_features_daily (for ML joins)
      - ticker_news_features_snapshot (for realtime)
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
          trade_date              date NOT NULL,
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

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ticker_news_features_snapshot (
          snapshot_utc            timestamptz NOT NULL,
          ticker                  text NOT NULL,
          news_count              int NOT NULL,
          vader_mean              double precision NOT NULL,
          vader_min               double precision NOT NULL,
          vader_max               double precision NOT NULL,
          neg_count               int NOT NULL,
          pos_count               int NOT NULL,
          last_title_age_minutes  double precision,
          created_at              timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (snapshot_utc, ticker)
        );
        """)
    conn.commit()

# -----------------------------------------------------------------------------
# Main task
# -----------------------------------------------------------------------------
def score_from_ticker_news_daily(**context):
    """
    Reads from ticker_news_daily (ET trade_date + rank 1..5),
    computes VADER from titles, writes:
      1) ticker_news_features_daily for all trade_dates in lookback window
      2) ticker_news_features_snapshot for current run (latest trade_date per ticker)
    """
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    snapshot_utc = datetime.now(timezone.utc)
    window_start_et = pendulum.now("America/New_York").date() - timedelta(days=LOOKBACK_DAYS)

    conn = get_conn()
    try:
        ensure_tables(conn)

        # Pull last N days from ticker_news_daily
        df = pd.read_sql(
            """
            SELECT trade_date, ticker, rank, published_utc, title
            FROM ticker_news_daily
            WHERE trade_date >= %s
            ORDER BY trade_date DESC, ticker ASC, rank ASC
            """,
            conn,
            params=(window_start_et,),
        )

        if df.empty:
            logger.info("ticker_news_daily has no rows for trade_date >= %s", window_start_et)
            return

        # Score each title
        df["vader_compound"] = df["title"].astype(str).map(
            lambda t: float(analyzer.polarity_scores(t or "")["compound"])
        )

        # --------------------------
        # (1) DAILY FEATURES (per trade_date, ticker)
        # --------------------------
        daily_agg = (
            df.groupby(["trade_date", "ticker"])["vader_compound"]
            .agg(["count", "mean", "min", "max"])
            .reset_index()
        )
        daily_agg.rename(
            columns={"count": "news_count", "mean": "vader_mean", "min": "vader_min", "max": "vader_max"},
            inplace=True,
        )

        def daily_extras(g: pd.DataFrame):
            neg = int((g["vader_compound"] <= NEG_THRESH).sum())
            pos = int((g["vader_compound"] >= POS_THRESH).sum())
            last_pub = g["published_utc"].max()
            return pd.Series({"neg_count": neg, "pos_count": pos, "last_published_utc": last_pub})

        daily_extra = df.groupby(["trade_date", "ticker"]).apply(daily_extras).reset_index()
        daily_out = daily_agg.merge(daily_extra, on=["trade_date", "ticker"], how="left")

        daily_rows = [
            (
                r.trade_date,
                r.ticker,
                int(r.news_count),
                float(r.vader_mean),
                float(r.vader_min),
                float(r.vader_max),
                int(r.neg_count),
                int(r.pos_count),
                r.last_published_utc,
            )
            for r in daily_out.itertuples(index=False)
        ]

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
                daily_rows,
                page_size=2000,
            )
        conn.commit()
        logger.info("Upserted daily rows: %d (window_start_et=%s)", len(daily_rows), window_start_et)

        # --------------------------
        # (2) SNAPSHOT FEATURES (per ticker for THIS run)
        # Use the latest trade_date available in df for each ticker
        # --------------------------
        latest_trade_date = df["trade_date"].max()

        df_latest_day = df[df["trade_date"] == latest_trade_date].copy()
        if df_latest_day.empty:
            logger.info("No rows for latest_trade_date=%s", latest_trade_date)
            return

        snap_agg = (
            df_latest_day.groupby("ticker")["vader_compound"]
            .agg(["count", "mean", "min", "max"])
            .reset_index()
        )
        snap_agg.rename(
            columns={"count": "news_count", "mean": "vader_mean", "min": "vader_min", "max": "vader_max"},
            inplace=True,
        )

        def snap_extras(g: pd.DataFrame):
            neg = int((g["vader_compound"] <= NEG_THRESH).sum())
            pos = int((g["vader_compound"] >= POS_THRESH).sum())
            latest_pub = g["published_utc"].max()
            age_minutes = (
                pendulum.instance(snapshot_utc).in_timezone("UTC")
                - pendulum.instance(latest_pub).in_timezone("UTC")
            ).total_minutes()
            return pd.Series({"neg_count": neg, "pos_count": pos, "last_title_age_minutes": float(age_minutes)})

        snap_extra = df_latest_day.groupby("ticker").apply(snap_extras).reset_index()
        snap_out = snap_agg.merge(snap_extra, on="ticker", how="left")

        snapshot_rows = [
            (
                snapshot_utc,
                r.ticker,
                int(r.news_count),
                float(r.vader_mean),
                float(r.vader_min),
                float(r.vader_max),
                int(r.neg_count),
                int(r.pos_count),
                None if pd.isna(r.last_title_age_minutes) else float(r.last_title_age_minutes),
            )
            for r in snap_out.itertuples(index=False)
        ]

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO ticker_news_features_snapshot
                  (snapshot_utc, ticker,
                   news_count, vader_mean, vader_min, vader_max,
                   neg_count, pos_count, last_title_age_minutes)
                VALUES %s
                ON CONFLICT (snapshot_utc, ticker)
                DO UPDATE SET
                   news_count = EXCLUDED.news_count,
                   vader_mean = EXCLUDED.vader_mean,
                   vader_min = EXCLUDED.vader_min,
                   vader_max = EXCLUDED.vader_max,
                   neg_count = EXCLUDED.neg_count,
                   pos_count = EXCLUDED.pos_count,
                   last_title_age_minutes = EXCLUDED.last_title_age_minutes,
                   created_at = NOW()
                """,
                snapshot_rows,
                page_size=2000,
            )
        conn.commit()
        logger.info("Inserted snapshot rows: %d (latest_trade_date=%s)", len(snapshot_rows), latest_trade_date)

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
    dag_id="ticker_news_score_from_daily_9_12_2_et",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule="0 9,12,14 * * 1-5",  # 9:00, 12:00, 14:00 ET Mon-Fri
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["news", "vader", "db_only", "daily", "snapshot"],
) as dag:

    run = PythonOperator(
        task_id="score_from_ticker_news_daily",
        python_callable=score_from_ticker_news_daily,
    )
