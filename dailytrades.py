from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import logging
import pendulum


local_tz = pendulum.timezone("America/New_York")
# -----------------------------
# Configuration from ENV
# -----------------------------
API_URL = "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true"
API_KEY = os.getenv("MARKET_API_KEY")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

logger = logging.getLogger("airflow.task")



def fetch_and_load(ds=None, **kwargs):
    trade_date = ds
    if not API_KEY:
        print("MARKET_API_KEY is not set!")
        raise ValueError("MARKET_API_KEY is not set")

    url = API_URL.format(date=trade_date)
    headers = {"Authorization": f"Bearer {API_KEY}"}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        records = response.json().get("results", [])
        logger.info("Fetched %d records from API for %s", len(records), trade_date)
    except Exception as e:
        logger.exception("Error fetching API data")
        raise

    if not records:
        logger.warning("No records found for %s", trade_date)
        raise ValueError(f"No records found for {trade_date}")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_sql = """
        INSERT INTO daily_market_summary
        (trade_date, ticker, open, high, low, close, volume, vwap, trade_count, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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

        for r in records:
            cursor.execute(
                insert_sql,
                (
                    trade_date,
                    r["T"],
                    r.get("o"),
                    r.get("h"),
                    r.get("l"),
                    r.get("c"),
                    r.get("v"),
                    r.get("vw"),
                    r.get("n"),
                    r.get("t")
                )
            )

        conn.commit()
        logger.info("Inserted %d records for %s", len(records), trade_date)
    except Exception as e:
        logger.exception("Error inserting data into Postgres")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    "spy_daily_scan",
    default_args=default_args,
    description="Fetch daily market data and compute SPY confidence",
    schedule_interval="30 16 * * 1-5",  # 4:30 PM Mon–Fri in DAG timezone
    start_date=pendulum.datetime(2025, 12, 23, 16, 30, tz=local_tz),  # ✅ explicit time
    catchup=False,
    tags=["production", "market-data", "daily"],
)

# -----------------------------
# Tasks
# -----------------------------
fetch_task = PythonOperator(
    task_id='fetch_and_load_daily_data',
    python_callable=fetch_and_load,
    dag=dag
)
