from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os
import logging

# -----------------------------
# Default args
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# -----------------------------
# DAG Definition
# -----------------------------
dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='Test Postgres connection from Airflow',
    schedule_interval=None,  # manual trigger
    start_date=datetime(2025, 12, 15),
    catchup=False,
    tags=['production-support', 'database', 'diagnostic']
)

# -----------------------------
# Task: Test Connection
# -----------------------------
def test_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            dbname=os.getenv("POSTGRES_DB", "trading"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432))
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        print(f"Postgres connection successful, test query result: {result}")
        cursor.close()
        conn.close()
    except Exception as e:
        raise Exception(f"Postgres connection failed: {e}")

# -----------------------------
# PythonOperator
# -----------------------------
test_postgres_task = PythonOperator(
    task_id='test_postgres_connection_task',
    python_callable=test_connection,
    dag=dag
)
