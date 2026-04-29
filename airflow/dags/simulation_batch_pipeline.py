from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from poker_pipeline_common import (
    DEFAULT_ACTIONS_TOPIC,
    DEFAULT_HAND_SUMMARIES_TOPIC,
    DEFAULT_KAFKA_BROKERS,
    PROJECT_ROOT,
    SIMULATION_BATCH_DAG_ID,
    STREAM_CHECKPOINT_PATH,
    run_python_module,
    run_spark_job,
)


def _conf_value(context: dict, key: str, default=None):
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None) and key in dag_run.conf:
        return dag_run.conf[key]
    return default


def run_demo_simulation(**context) -> None:
    args = [
        "--hands",
        str(_conf_value(context, "hands", 200)),
        "--seed",
        str(_conf_value(context, "seed", 42)),
        "--simulation-run-id",
        context["run_id"],
        "--table-id",
        str(_conf_value(context, "table_id", "airflow_demo_table")),
        "--context",
        str(_conf_value(context, "context", "balanced aggressive value bettor")),
        "--publish-kafka",
        "--no-backend-write",
        "--kafka-brokers",
        str(_conf_value(context, "kafka_brokers", DEFAULT_KAFKA_BROKERS)),
        "--actions-topic",
        str(_conf_value(context, "actions_topic", DEFAULT_ACTIONS_TOPIC)),
        "--summaries-topic",
        str(_conf_value(context, "hand_summaries_topic", DEFAULT_HAND_SUMMARIES_TOPIC)),
    ]
    run_python_module("simulator.run_simulation", args)


def refresh_metrics_and_curated_tables(**context) -> None:
    checkpoint = str(_conf_value(context, "checkpoint_path", STREAM_CHECKPOINT_PATH))
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
            checkpoint,
            "--available-now",
        ],
    )


with DAG(
    dag_id=SIMULATION_BATCH_DAG_ID,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["poker", "simulation", "streaming"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_persona_simulation = PythonOperator(
        task_id="run_persona_simulation",
        python_callable=run_demo_simulation,
    )

    ingest_and_build_live_metrics = PythonOperator(
        task_id="ingest_and_build_live_metrics",
        python_callable=refresh_metrics_and_curated_tables,
    )

    end = EmptyOperator(task_id="end")

    start >> run_persona_simulation >> ingest_and_build_live_metrics >> end
