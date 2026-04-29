from __future__ import annotations

"""Shared constants and helpers for the action-level Airflow pipelines."""

from pathlib import Path
import os
import site
import subprocess

from poker_platform.env import getenv_path, getenv_stripped


BOOTSTRAP_DAG_ID = "poker_pipeline_bootstrap"
HISTORICAL_INGEST_DAG_ID = "historical_ingest_pipeline"
PLAYER_PROFILE_TRAINING_DAG_ID = "player_profile_training_pipeline"
SIMULATION_BATCH_DAG_ID = "simulation_batch_pipeline"
KAFKA_STREAM_CATCHUP_DAG_ID = "kafka_stream_snowflake_catchup"

PROJECT_ROOT = getenv_path("POKER_PLATFORM_PROJECT_ROOT", "/opt/project") or Path("/opt/project")
SQL_DIR = PROJECT_ROOT / "sql"
DEFAULT_HISTORICAL_FIXTURE_PATH = Path(
    getenv_stripped(
        "HISTORICAL_FIXTURE_PATH",
        PROJECT_ROOT / "sample_data" / "historical" / "handhq_6max_sample.phhs",
    )
    or (PROJECT_ROOT / "sample_data" / "historical" / "handhq_6max_sample.phhs")
)
DEFAULT_PHH_SOURCE = getenv_stripped("PHH_DATASET_SOURCE", str(DEFAULT_HISTORICAL_FIXTURE_PATH)) or str(DEFAULT_HISTORICAL_FIXTURE_PATH)
DEFAULT_KAFKA_BROKERS = getenv_stripped("KAFKA_BROKERS", "kafka:29092") or "kafka:29092"
DEFAULT_ACTIONS_TOPIC = getenv_stripped("KAFKA_TOPIC_ACTIONS", "poker.actions") or "poker.actions"
DEFAULT_HAND_SUMMARIES_TOPIC = (
    getenv_stripped("KAFKA_TOPIC_HAND_SUMMARIES", "poker.hand_summaries") or "poker.hand_summaries"
)
STREAM_CHECKPOINT_PATH = (
    getenv_stripped("STREAM_CHECKPOINT_PATH", str(PROJECT_ROOT / "runtime" / "stream_checkpoints" / "poker"))
    or str(PROJECT_ROOT / "runtime" / "stream_checkpoints" / "poker")
)
SPARK_SUBMIT_BIN = getenv_stripped("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit") or "/opt/spark/bin/spark-submit"
SPARK_MASTER_URL = getenv_stripped("SPARK_MASTER_URL", "local[*]") or "local[*]"
SPARK_KAFKA_PACKAGE = (
    getenv_stripped(
        "SPARK_KAFKA_PACKAGE",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8",
    )
    or "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
)
SPARK_DRIVER_MEMORY = getenv_stripped("SPARK_DRIVER_MEMORY", "2g") or "2g"
SPARK_DRIVER_MAX_RESULT_SIZE = getenv_stripped("SPARK_DRIVER_MAX_RESULT_SIZE", "1g") or "1g"
PYTHON_BIN = getenv_stripped("PYTHON_BIN", "python") or "python"
if "PYTHON_BIN" not in os.environ:
    PYTHON_BIN = "python" if os.name == "nt" else "python3"


def get_bootstrap_sql() -> list[str]:
    ddl_path = SQL_DIR / "ddl" / "snowflake" / "create_tables.sql"
    text = ddl_path.read_text(encoding="utf-8")
    return [statement.strip() for statement in text.split(";") if statement.strip()]


def run_python_module(module_name: str, args: list[str]) -> None:
    env = dict(os.environ)
    pythonpath_parts = _project_pythonpath_parts(env)
    env["PYTHONPATH"] = ":".join(part for part in pythonpath_parts if part)
    subprocess.run(
        [PYTHON_BIN, "-m", module_name, *args],
        check=True,
        env=env,
    )


def _project_pythonpath_parts(env: dict[str, str]) -> list[str]:
    pythonpath_parts: list[str] = []
    user_site = site.getusersitepackages()
    if user_site:
        pythonpath_parts.append(user_site)
    pythonpath_parts.append(str(PROJECT_ROOT))
    pythonpath_parts.append(str(PROJECT_ROOT / "historical"))
    pythonpath_parts.append(str(PROJECT_ROOT / "ml_model"))
    pythonpath_parts.append(str(PROJECT_ROOT / "simulation"))
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)
    return pythonpath_parts


def run_spark_job(script_path: str, args: list[str]) -> None:
    env = dict(os.environ)
    env["PYTHONPATH"] = ":".join(part for part in _project_pythonpath_parts(env) if part)
    subprocess.run(
        [
            SPARK_SUBMIT_BIN,
            "--master",
            SPARK_MASTER_URL,
            "--conf",
            f"spark.driver.memory={SPARK_DRIVER_MEMORY}",
            "--conf",
            f"spark.driver.maxResultSize={SPARK_DRIVER_MAX_RESULT_SIZE}",
            "--packages",
            SPARK_KAFKA_PACKAGE,
            script_path,
            *args,
        ],
        check=True,
        env=env,
    )
