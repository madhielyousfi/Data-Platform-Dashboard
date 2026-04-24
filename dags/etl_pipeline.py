"""
ETL Pipeline DAG - orchestrates the full data platform pipeline.
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

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Data platform ETL pipeline",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "data-platform"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo '[DAG] Starting ETL pipeline at $(date)'",
    )

    with TaskGroup("ingestion") as ingestion_group:
        extract_raw = BashOperator(
            task_id="extract_raw",
            bash_command="cd /home/dados/Documents/Data Platform && python3 ingestion/load_raw.py",
        )

    with TaskGroup("processing") as processing_group:
        transform_bronze_silver = BashOperator(
            task_id="transform_bronze_silver",
            bash_command="cd /home/dados/Documents/Data Platform && python3 processing/clean.py",
        )

        transform_silver_gold = BashOperator(
            task_id="transform_silver_gold",
            bash_command="cd /home/dados/Documents/Data Platform && python3 processing/aggregate.py",
        )

    with TaskGroup("warehouse") as warehouse_group:
        load_warehouse = BashOperator(
            task_id="load_warehouse",
            bash_command="cd /home/dados/Documents/Data Platform && python3 warehouse/load_warehouse.py",
        )

    end = BashOperator(
        task_id="end",
        bash_command="echo '[DAG] ETL pipeline complete at $(date)'",
    )

    # Task dependencies
    start >> extract_raw >> transform_bronze_silver >> transform_silver_gold >> load_warehouse >> end