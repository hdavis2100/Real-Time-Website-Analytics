from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from poker_pipeline_common import PLAYER_PROFILE_TRAINING_DAG_ID, run_python_module
from profiles.constants import FEATURE_VERSION


def _conf_value(context: dict, key: str, default=None):
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None) and key in dag_run.conf:
        return dag_run.conf[key]
    return default


def train_player_profiles(**context) -> None:
    build_args = [
        "--training-dataset",
        str(_conf_value(context, "training_dataset", "all_historical_humans")),
        "--feature-version",
        str(_conf_value(context, "feature_version", FEATURE_VERSION)),
        "--min-hands",
        str(_conf_value(context, "min_hands", 200)),
    ]
    source_dataset = _conf_value(context, "source_dataset")
    if source_dataset:
        build_args += ["--source-dataset", str(source_dataset)]

    train_args = [
        "--training-dataset",
        str(_conf_value(context, "training_dataset", "all_historical_humans")),
        "--feature-version",
        str(_conf_value(context, "feature_version", FEATURE_VERSION)),
        "--min-hands",
        str(_conf_value(context, "min_hands", 200)),
        "--model-run-id",
        str(_conf_value(context, "model_run_id", f"profile-model-{context['run_id']}")),
    ]
    if _conf_value(context, "activate", True) is False:
        train_args.append("--no-activate")

    run_python_module("profiles.build_features", build_args)
    run_python_module("profiles.train_model", train_args)


with DAG(
    dag_id=PLAYER_PROFILE_TRAINING_DAG_ID,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["poker", "profiles", "ml"],
) as dag:
    start = EmptyOperator(task_id="start")

    refresh_player_profiles = PythonOperator(
        task_id="refresh_player_profiles",
        python_callable=train_player_profiles,
    )

    end = EmptyOperator(task_id="end")

    start >> refresh_player_profiles >> end
