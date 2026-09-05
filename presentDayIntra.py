from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import logging
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}



API_KEY = os.getenv("MARKET_API_KEY")
BASE_URL = "https://api.massive.com/v2/aggs/ticker/{ticker}/range/10/minute/{start}/{end}?adjusted=true"

eastern = ZoneInfo("America/New_York")
now_est = datetime.now(tz=eastern)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def get_sp500_qqq_tickers():
    """Query tickers_info table for SP500 and QQQ tickers"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker FROM tickers_info
        WHERE sp500 = TRUE OR qqq = TRUE
    """)
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tickers

def fetch_historical_bars(ticker, start_date, end_date):
    """Fetch 10-min bars from Massive API for given ticker and date range"""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    print("Current time in EST/EDT:", now_est)
    url = BASE_URL.format(
        ticker=ticker,
        start = now_est.strftime("%Y-%m-%d"),
        end = now_est.strftime("%Y-%m-%d"),
    )
    bars = []
    while url:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        bars.extend(data.get("results", []))
        url = data.get("next_url")
        logger.info("Fetched %d bars for %s, next_url=%s", len(data.get("results", [])), ticker, url)
    return bars

def save_bars_to_postgres(ticker, bars):
    """Insert 10-min bars into historical_data table"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO intraday_data
        (ticker, ts, open, high, low, close, volume, vwap, trades)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ticker, ts) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            vwap = EXCLUDED.vwap,
            trades = EXCLUDED.trades
    """
    for bar in bars:
        ts = datetime.fromtimestamp(bar["t"] / 1000)
        cursor.execute(insert_sql, (
            ticker, ts, bar["o"], bar["h"], bar["l"], bar["c"], bar["v"], bar["vw"], bar["n"]
        ))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Saved %d bars for %s", len(bars), ticker)

def fetch_and_store_all_tickers(**kwargs):
    """Driver function to fetch bars for all tickers"""
    end_date = datetime.utcnow()
    start_date = datetime.utcnow()
    tickers = get_sp500_qqq_tickers()

    for ticker in tickers:
        logger.info("Processing ticker: %s", ticker)
        bars = fetch_historical_bars(ticker, start_date, end_date)
        if bars:
            save_bars_to_postgres(ticker, bars)
        else:
            logger.info("No bars returned for %s", ticker)

with DAG(
    dag_id="fetch_sp500_qqq_presentdata",
    default_args=default_args,
    description="Fetch 10-min historical bars for SP500 & QQQ tickers for present day",
    start_date=datetime(2025, 12, 7),
    schedule_interval="@daily",
    catchup=False,
    tags=["market_data"]
) as dag:

    task_fetch_store = PythonOperator(
        task_id="fetch_and_store_bars",
        python_callable=fetch_and_store_all_tickers,
        provide_context=True
    )

task_fetch_store
