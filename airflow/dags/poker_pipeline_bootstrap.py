from datetime import datetime

"""Manual/bootstrap DAG for one-time Snowflake object setup and warehouse tuning."""

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from poker_pipeline_common import BOOTSTRAP_DAG_ID, get_bootstrap_sql
from snowflake_sql_operator import SnowflakeSqlOperator


with DAG(
    dag_id=BOOTSTRAP_DAG_ID,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["poker", "batch", "bootstrap"],
) as dag:
    start = EmptyOperator(task_id="start")

    initialize_snowflake_objects = SnowflakeSqlOperator(
        task_id="initialize_snowflake_objects",
        sql=get_bootstrap_sql(),
        snowflake_conn_id="snowflake_default",
    )

    end = EmptyOperator(task_id="end")

    start >> initialize_snowflake_objects >> end
