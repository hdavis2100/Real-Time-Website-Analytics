from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="poker_batch_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    tags=["poker", "batch"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_placeholder_batch_job = BashOperator(
        task_id="run_placeholder_batch_job",
        bash_command="python /opt/airflow/jobs/kafka_to_raw.py",
    )

    end = EmptyOperator(task_id="end")

    start >> run_placeholder_batch_job >> end