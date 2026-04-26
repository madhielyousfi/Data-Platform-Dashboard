"""
Data Platform - Apache Airflow DAG
Orchestrates the full ETL pipeline.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

BASE_PATH = "/home/dados/Documents/Data Platform/data-platform"

with DAG(
    dag_id="data_platform_pipeline",
    default_args=default_args,
    description="Data Platform ETL Pipeline",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "data-platform"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command=f"echo '[DAG] Starting pipeline at $(date)'",
    )

    with TaskGroup("ingestion") as ingestion_group:
        generate_data = BashOperator(
            task_id="generate_sample_data",
            bash_command=f"cd {BASE_PATH} && python3 ingestion/generate_sample.py",
        )

        load_raw = BashOperator(
            task_id="load_raw",
            bash_command=f"cd {BASE_PATH} && python3 ingestion/load_raw.py",
        )

    with TaskGroup("processing") as processing_group:
        clean_data = BashOperator(
            task_id="clean",
            bash_command=f"cd {BASE_PATH} && python3 processing/clean.py",
        )

        transform = BashOperator(
            task_id="transform",
            bash_command=f"cd {BASE_PATH} && python3 processing/transform.py",
        )

        aggregate = BashOperator(
            task_id="aggregate",
            bash_command=f"cd {BASE_PATH} && python3 processing/aggregate.py",
        )

    with TaskGroup("warehouse") as warehouse_group:
        load_warehouse = BashOperator(
            task_id="load_warehouse",
            bash_command=f"cd {BASE_PATH} && python3 warehouse/load_postgres.py",
        )

    with TaskGroup("api") as api_group:
        notify_api = BashOperator(
            task_id="notify_api",
            bash_command=f"echo '[DAG] API data updated'",
        )

    end = BashOperator(
        task_id="end",
        bash_command=f"echo '[DAG] Pipeline complete at $(date)'",
    )

    # Task dependencies
    start >> generate_data >> load_raw >> clean_data >> transform >> aggregate >> load_warehouse >> notify_api >> end