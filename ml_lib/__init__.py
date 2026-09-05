"""
Shared, non-DAG helpers for the ML pipelines.

This package deliberately contains no DAG definitions. Airflow puts the DAGs
folder on sys.path, so modules here are importable from any DAG file as
`from ml_lib.<module> import ...`.
"""
