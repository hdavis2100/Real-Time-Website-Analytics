from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from poker_pipeline_common import (
    DEFAULT_ACTIONS_TOPIC,
    DEFAULT_HAND_SUMMARIES_TOPIC,
    DEFAULT_KAFKA_BROKERS,
    KAFKA_STREAM_CATCHUP_DAG_ID,
    PROJECT_ROOT,
    STREAM_CHECKPOINT_PATH,
    run_spark_job,
)


def _conf_value(context: dict, key: str, default=None):
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None) and key in dag_run.conf:
        return dag_run.conf[key]
    return default


def run_kafka_snowflake_catchup(**context) -> None:
    run_spark_job(
        str(PROJECT_ROOT / "jobs" / "stream_kafka.py"),
        [
            "--brokers",
            str(_conf_value(context, "kafka_brokers", DEFAULT_KAFKA_BROKERS)),
            "--topic",
            str(_conf_value(context, "actions_topic", DEFAULT_ACTIONS_TOPIC)),
            "--summaries-topic",
            str(_conf_value(context, "hand_summaries_topic", DEFAULT_HAND_SUMMARIES_TOPIC)),
            "--starting-offsets",
            str(_conf_value(context, "starting_offsets", "earliest")),
            "--checkpoint-path",
            str(_conf_value(context, "checkpoint_path", STREAM_CHECKPOINT_PATH)),
            "--available-now",
        ],
    )


with DAG(
    dag_id=KAFKA_STREAM_CATCHUP_DAG_ID,
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["poker", "kafka", "snowflake", "streaming"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_kafka_events = PythonOperator(
        task_id="ingest_kafka_events",
        python_callable=run_kafka_snowflake_catchup,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_kafka_events >> end
