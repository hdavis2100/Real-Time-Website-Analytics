from __future__ import annotations

from pathlib import Path
import sys
import types
import uuid

import pandas as pd

import poker_platform.storage as storage_module
from poker_platform.config import PlatformConfig
from poker_platform.storage import SnowflakeWarehouse


class _FakeCursor:
    def __init__(
        self,
        executed: list[str],
        description_rows: list[tuple[str, str]] | None = None,
        *,
        executemany_error: Exception | None = None,
    ) -> None:
        self.executed = executed
        self.description_rows = description_rows or []
        self.description = None
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.executemany_error = executemany_error

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, statement: str, *args, **kwargs) -> None:
        self.executed.append(statement)
        if statement.startswith("DESC TABLE"):
            self.description = [("name",), ("type",)]
        else:
            self.description = None

    def executemany(self, statement: str, rows: list[tuple[object, ...]]) -> None:
        self.executed.append(statement)
        self.executemany_calls.append((statement, list(rows)))
        if self.executemany_error is not None:
            raise self.executemany_error

    def fetchall(self):
        return self.description_rows


class _FakeConnection:
    def __init__(
        self,
        description_rows: list[tuple[str, str]] | None = None,
        *,
        executemany_error: Exception | None = None,
    ) -> None:
        self.executed: list[str] = []
        self.description_rows = description_rows or []
        self.executemany_error = executemany_error
        self.cursors: list[_FakeCursor] = []

    def cursor(self) -> _FakeCursor:
        cursor = _FakeCursor(
            self.executed,
            self.description_rows,
            executemany_error=self.executemany_error,
        )
        self.cursors.append(cursor)
        return cursor


def _config() -> PlatformConfig:
    return PlatformConfig(
        runtime_dir=Path("runtime"),
        sql_dir=Path("sql"),
        schema_version="1",
        kafka_brokers="localhost:9092",
        kafka_simulation_requests_topic="poker.simulation_requests",
        kafka_actions_topic="poker.actions",
        kafka_hand_summaries_topic="poker.hand_summaries",
        kafka_simulation_events_topic="poker.simulation_events",
        historical_fixture_path=Path("sample_data/historical/handhq_6max_sample.phhs"),
        stream_window_duration="5 minutes",
        stream_slide_duration="1 minute",
        stream_watermark="10 minutes",
        stream_checkpoint_path=Path("runtime/stream_checkpoints/poker_actions"),
    )


def test_snowflake_ensure_schema_creates_and_uses_schema_before_tables(monkeypatch) -> None:
    warehouse = SnowflakeWarehouse(_config())
    fake_connection = _FakeConnection()
    settings = {
        "SNOWFLAKE_DATABASE": "FINCH_DB",
        "SNOWFLAKE_SCHEMA": "FINCH_SCHEMA_CODEX_VALIDATION",
    }

    monkeypatch.setattr(warehouse, "_connect", lambda: fake_connection)
    monkeypatch.setattr(warehouse, "_get_setting", lambda name: settings[name])

    warehouse.ensure_schema()

    assert fake_connection.executed[:3] == [
        'USE DATABASE "FINCH_DB"',
        'CREATE SCHEMA IF NOT EXISTS "FINCH_SCHEMA_CODEX_VALIDATION"',
        'USE SCHEMA "FINCH_SCHEMA_CODEX_VALIDATION"',
    ]
    assert any(statement.startswith("CREATE TABLE IF NOT EXISTS RAW_HANDS") for statement in fake_connection.executed)
    assert any(
        statement == "ALTER TABLE RAW_ACTIONS ADD COLUMN IF NOT EXISTS USER_ID STRING"
        for statement in fake_connection.executed
    )


def test_snowflake_stage_copy_and_merge_helpers_emit_expected_sql(monkeypatch, tmp_path) -> None:
    warehouse = SnowflakeWarehouse(_config())
    fake_connection = _FakeConnection()

    monkeypatch.setattr(warehouse, "_connect", lambda: fake_connection)

    parquet_path = tmp_path / "hands_000001.parquet"
    parquet_path.write_bytes(b"parquet")

    warehouse.put_file_to_stage("HISTORICAL_HANDHQ_STAGE", parquet_path, stage_prefix="run_1/chunk_1")
    warehouse.copy_stage_into_table("HISTORICAL_HANDHQ_STAGE", "HISTORICAL_HANDHQ_LANDING_HANDS", stage_prefix="run_1/chunk_1/hands_000001.parquet")
    warehouse.merge_table_into_table("RAW_HANDS", "HISTORICAL_HANDHQ_LANDING_HANDS", key_columns=["hand_id", "source_run_id", "source_type"])

    assert fake_connection.executed[0].startswith("PUT file://")
    assert "@HISTORICAL_HANDHQ_STAGE/run_1/chunk_1" in fake_connection.executed[0]
    assert fake_connection.executed[1].startswith("COPY INTO HISTORICAL_HANDHQ_LANDING_HANDS")
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in fake_connection.executed[1]
    assert fake_connection.executed[2].startswith("MERGE INTO RAW_HANDS AS target")


def test_snowflake_small_dataframe_writes_use_executemany(monkeypatch) -> None:
    warehouse = SnowflakeWarehouse(_config())
    fake_connection = _FakeConnection(
        description_rows=[
            ("HAND_ID", "TEXT"),
            ("ACTION_INDEX", "NUMBER"),
        ]
    )

    pandas_tools = types.ModuleType("snowflake.connector.pandas_tools")
    pandas_tools.write_pandas = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", pandas_tools)
    monkeypatch.setattr(warehouse, "_connect", lambda: fake_connection)

    warehouse.write_dataframe(
        "RAW_ACTIONS",
        pd.DataFrame(
            [
                {"hand_id": "hand_1", "action_index": 1},
                {"hand_id": "hand_2", "action_index": 2},
            ]
        ),
    )

    assert any(statement.startswith("DESC TABLE RAW_ACTIONS") for statement in fake_connection.executed)
    executemany_calls = [
        call
        for cursor in fake_connection.cursors
        for call in cursor.executemany_calls
    ]
    assert len(executemany_calls) == 1
    statement, rows = executemany_calls[0]
    assert statement.startswith("INSERT INTO RAW_ACTIONS (HAND_ID, ACTION_INDEX) VALUES")
    assert rows == [("hand_1", 1), ("hand_2", 2)]


def test_snowflake_small_dataframe_falls_back_to_execute_when_rewrite_is_unsupported(monkeypatch) -> None:
    warehouse = SnowflakeWarehouse(_config())
    fake_connection = _FakeConnection(
        description_rows=[
            ("SIMULATION_RUN_ID", "TEXT"),
            ("STATUS", "TEXT"),
        ],
        executemany_error=RuntimeError("252001: Failed to rewrite multi-row insert"),
    )

    pandas_tools = types.ModuleType("snowflake.connector.pandas_tools")
    pandas_tools.write_pandas = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", pandas_tools)
    monkeypatch.setattr(warehouse, "_connect", lambda: fake_connection)

    warehouse.write_dataframe(
        "RAW_SIMULATION_RUNS",
        pd.DataFrame([{"simulation_run_id": "run_1", "status": "queued"}]),
    )

    execute_statements = [statement for statement in fake_connection.executed if statement.startswith("INSERT INTO RAW_SIMULATION_RUNS")]
    assert execute_statements == [
        "INSERT INTO RAW_SIMULATION_RUNS (SIMULATION_RUN_ID, STATUS) VALUES (%s, %s)",
        "INSERT INTO RAW_SIMULATION_RUNS (SIMULATION_RUN_ID, STATUS) VALUES (%s, %s)",
    ]


def test_snowflake_small_dataframe_uses_select_for_variant_columns(monkeypatch) -> None:
    warehouse = SnowflakeWarehouse(_config())
    fake_connection = _FakeConnection(
        description_rows=[
            ("STATUS", "TEXT"),
            ("CONFIG_JSON", "VARIANT"),
        ]
    )

    pandas_tools = types.ModuleType("snowflake.connector.pandas_tools")
    pandas_tools.write_pandas = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", pandas_tools)
    monkeypatch.setattr(warehouse, "_connect", lambda: fake_connection)

    warehouse.write_dataframe(
        "RAW_SIMULATION_RUNS",
        pd.DataFrame(
            [
                {
                    "status": "queued",
                    "config_json": {"decision_backend": "heuristic"},
                }
            ]
        ),
    )

    insert_statements = [statement for statement in fake_connection.executed if statement.startswith("INSERT INTO RAW_SIMULATION_RUNS")]
    assert insert_statements == [
        "INSERT INTO RAW_SIMULATION_RUNS (STATUS, CONFIG_JSON) SELECT %s, PARSE_JSON(%s)"
    ]
    executemany_calls = [
        call
        for cursor in fake_connection.cursors
        for call in cursor.executemany_calls
    ]
    assert executemany_calls == []


def test_snowflake_upsert_dataframe_supports_temp_tables(monkeypatch) -> None:
    warehouse = SnowflakeWarehouse(_config())
    fake_connection = _FakeConnection(
        description_rows=[
            ("HAND_ID", "TEXT"),
            ("ACTION_INDEX", "NUMBER"),
        ]
    )
    merge_calls: list[tuple[str, str, list[str]]] = []

    pandas_tools = types.ModuleType("snowflake.connector.pandas_tools")
    pandas_tools.write_pandas = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", pandas_tools)
    monkeypatch.setattr(warehouse, "_connect", lambda: fake_connection)
    monkeypatch.setattr(
        storage_module.uuid,
        "uuid4",
        lambda: uuid.UUID("12345678123456781234567812345678"),
    )
    monkeypatch.setattr(
        warehouse,
        "merge_table_into_table",
        lambda table, source_table, *, key_columns: merge_calls.append((table, source_table, key_columns)),
    )

    warehouse.upsert_dataframe(
        "RAW_ACTIONS",
        pd.DataFrame(
            [
                {"hand_id": "hand_1", "action_index": 1},
                {"hand_id": "hand_2", "action_index": 2},
            ]
        ),
        key_columns=["hand_id", "action_index"],
        force_bulk=False,
    )

    temp_table = "RAW_ACTIONS__TMP_12345678"
    assert any(statement == f"CREATE TEMP TABLE {temp_table} LIKE RAW_ACTIONS" for statement in fake_connection.executed)
    assert any(statement == f"DESC TABLE {temp_table}" for statement in fake_connection.executed)
    executemany_calls = [
        call
        for cursor in fake_connection.cursors
        for call in cursor.executemany_calls
    ]
    assert any(statement.startswith(f"INSERT INTO {temp_table} (HAND_ID, ACTION_INDEX) VALUES") for statement, _ in executemany_calls)
    assert merge_calls == [("RAW_ACTIONS", temp_table, ["hand_id", "action_index"])]
    assert any(statement == f"DROP TABLE IF EXISTS {temp_table}" for statement in fake_connection.executed)


def test_merge_table_into_table_accepts_unmapped_temp_source(monkeypatch) -> None:
    warehouse = SnowflakeWarehouse(_config())
    captured: list[tuple[str, str, list[str]]] = []

    monkeypatch.setattr(
        warehouse,
        "merge_query_into_table",
        lambda table, query, *, key_columns: captured.append((table, query, key_columns)),
    )

    warehouse.merge_table_into_table(
        "RAW_ACTIONS",
        "RAW_ACTIONS__TMP_12345678",
        key_columns=["hand_id", "action_index"],
    )

    assert len(captured) == 1
    table, query, key_columns = captured[0]
    assert table == "RAW_ACTIONS"
    assert key_columns == ["hand_id", "action_index"]
    assert query.startswith("SELECT ")
    assert "HAND_ID" in query
    assert "ACTION_INDEX" in query
    assert query.endswith(" FROM RAW_ACTIONS__TMP_12345678")
