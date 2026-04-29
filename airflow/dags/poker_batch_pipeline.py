from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from poker_pipeline_common import (
    DEFAULT_PHH_SOURCE,
    HISTORICAL_INGEST_DAG_ID,
    run_python_module,
)


def _conf_value(context: dict, key: str, default=None):
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None) and key in dag_run.conf:
        return dag_run.conf[key]
    return default


def load_historical_phh(**context) -> None:
    source = _conf_value(context, "source", DEFAULT_PHH_SOURCE)
    source_text = str(source).lower().strip()
    if not source_text.endswith(".phhs") and "handhq" not in source_text:
        raise ValueError("Historical ingest only supports HandHQ .phhs sources")

    cache_dir = _conf_value(context, "cache_dir")
    file_limit = _conf_value(context, "file_limit")
    force_download = _conf_value(context, "force_download", False)

    args = [
        "--input",
        str(source),
        "--source-run-id",
        context["run_id"],
        "--table-id",
        str(_conf_value(context, "table_id", "historical_phh_6max")),
        "--dataset",
        str(_conf_value(context, "dataset", "handhq_historical")),
        "--worker-count",
        str(_conf_value(context, "worker_count", 4)),
        "--target-rows-per-chunk",
        str(_conf_value(context, "target_rows_per_chunk", 100000)),
        "--write",
    ]
    stage_prefix = _conf_value(context, "stage_prefix")
    spool_dir = _conf_value(context, "spool_dir")
    if cache_dir:
        args += ["--cache-dir", str(cache_dir)]
    if file_limit:
        args += ["--file-limit", str(file_limit)]
    if stage_prefix:
        args += ["--stage-prefix", str(stage_prefix)]
    if spool_dir:
        args += ["--spool-dir", str(spool_dir)]
    if force_download:
        args.append("--force-download")
    run_python_module("historical_ingest.handhq_backfill", args)


with DAG(
    dag_id=HISTORICAL_INGEST_DAG_ID,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["poker", "historical", "action_level"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_historical_phh = PythonOperator(
        task_id="ingest_historical_phh",
        python_callable=load_historical_phh,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_historical_phh >> end
