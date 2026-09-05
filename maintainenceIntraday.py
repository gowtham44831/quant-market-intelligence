from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import logging
import os

# PostgreSQL connection details
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


logger = logging.getLogger(__name__)

def delete_old_intraday_data():
    """Delete intraday_data rows older than 120 days"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql = """
        DELETE FROM public.intraday_data
        WHERE ts < CURRENT_DATE - INTERVAL '120 days'
        """
        cursor.execute(sql)
        deleted_rows = cursor.rowcount
        conn.commit()
        logger.info("Deleted %d intraday rows older than 120 days", deleted_rows)
    except Exception as e:
        logger.error("Error deleting old intraday data: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()

def delete_old_daily_summary():
    """Delete daily_market_summary rows older than 5 years"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql = """
        DELETE FROM public.daily_market_summary
        WHERE trade_date < CURRENT_DATE - INTERVAL '5 years'
        """
        cursor.execute(sql)
        deleted_rows = cursor.rowcount
        conn.commit()
        logger.info("Deleted %d daily market summary rows older than 5 years", deleted_rows)
    except Exception as e:
        logger.error("Error deleting old daily market summary data: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='cleanup_market_data',
    default_args=default_args,
    description='Cleanup old intraday and daily market summary data',
    schedule='0 0 * * *',  # daily at UTC 00:00
    start_date=datetime(2025, 12, 19),  # start tomorrow
    catchup=False,
    tags=['production-support', 'maintenance']
) as dag:

    delete_intraday = PythonOperator(
        task_id='delete_old_intraday',
        python_callable=delete_old_intraday_data
    )

    delete_daily_summary = PythonOperator(
        task_id='delete_old_daily_summary',
        python_callable=delete_old_daily_summary
    )

    # Run both tasks in parallel
    delete_intraday >> delete_daily_summary
