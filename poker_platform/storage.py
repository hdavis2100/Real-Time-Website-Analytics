from __future__ import annotations

"""Snowflake warehouse abstraction for the poker analytics platform."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Iterable
import uuid

import pandas as pd

from poker_platform.config import PlatformConfig, get_config
from poker_platform.env import getenv_stripped, resolve_repo_path


TABLE_NAMES = {
    "raw_hands": "RAW_HANDS",
    "raw_actions": "RAW_ACTIONS",
    "raw_players": "RAW_PLAYERS",
    "raw_simulation_runs": "RAW_SIMULATION_RUNS",
    "simulation_run_player_summaries": "SIMULATION_RUN_PLAYER_SUMMARIES",
    "curated_hands": "CURATED_HANDS",
    "curated_actions": "CURATED_ACTIONS",
    "curated_players": "CURATED_PLAYERS",
    "historical_handhq_landing_hands": "HISTORICAL_HANDHQ_LANDING_HANDS",
    "historical_handhq_landing_actions": "HISTORICAL_HANDHQ_LANDING_ACTIONS",
    "historical_handhq_landing_players": "HISTORICAL_HANDHQ_LANDING_PLAYERS",
    "historical_handhq_ingest_runs": "HISTORICAL_HANDHQ_INGEST_RUNS",
    "historical_handhq_ingest_files": "HISTORICAL_HANDHQ_INGEST_FILES",
    "live_agent_metrics": "LIVE_AGENT_METRICS",
    "live_profile_assignments": "LIVE_PROFILE_ASSIGNMENTS",
    "dashboard_summaries": "DASHBOARD_SUMMARIES",
    "profile_player_hand_features": "PROFILE_PLAYER_HAND_FEATURES",
    "profile_player_training_features": "PROFILE_PLAYER_TRAINING_FEATURES",
    "profile_model_runs": "PROFILE_MODEL_RUNS",
    "profile_model_features": "PROFILE_MODEL_FEATURES",
    "profile_cluster_centroids": "PROFILE_CLUSTER_CENTROIDS",
    "profile_cluster_prototypes": "PROFILE_CLUSTER_PROTOTYPES",
    "profile_historical_assignments": "PROFILE_HISTORICAL_ASSIGNMENTS",
    "profile_session_features": "PROFILE_SESSION_FEATURES",
    "profile_session_results": "PROFILE_SESSION_RESULTS",
}

PROFILE_RATE_FEATURE_COLUMNS = [
    "vpip_rate",
    "pfr_rate",
    "aggression_frequency",
    "call_preference",
    "flop_cbet_rate",
    "turn_barrel_rate",
    "river_seen_rate",
    "all_in_rate",
    "avg_preflop_raise_bb",
    "avg_postflop_bet_pot_ratio",
    "late_position_vpip_rate",
    "blind_defend_rate",
    "short_stack_aggression_rate",
    "deep_stack_looseness",
]

PROFILE_COUNT_COLUMNS = [
    "aggressive_actions",
    "passive_actions",
    "flop_cbet_opportunities",
    "turn_barrel_opportunities",
    "late_position_hands",
    "blind_position_hands",
    "short_stack_hands",
    "deep_stack_hands",
]

TABLE_SCHEMAS = {
    "RAW_HANDS": [
        "hand_id",
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_seat",
        "table_id",
        "button_seat",
        "big_blind_bb",
        "small_blind_bb",
        "total_pot_bb",
        "board_cards",
        "started_at",
        "finished_at",
        "payload_version",
        "raw_lineage",
    ],
    "RAW_ACTIONS": [
        "hand_id",
        "action_index",
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_seat",
        "is_hero_player",
        "table_id",
        "street",
        "player_id",
        "agent_id",
        "seat",
        "position",
        "action_type",
        "amount_bb",
        "pot_before_bb",
        "pot_after_bb",
        "to_call_bb",
        "effective_stack_bb",
        "players_remaining",
        "board_cards_visible",
        "hole_cards_visible",
        "is_all_in",
        "event_ts",
        "backend_type",
        "persona_name",
        "persona_text",
        "payload_version",
        "raw_lineage",
    ],
    "RAW_PLAYERS": [
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_seat",
        "is_hero_player",
        "table_id",
        "hand_id",
        "player_id",
        "agent_id",
        "seat",
        "player_name",
        "stack_start_bb",
        "stack_end_bb",
        "hole_cards",
        "result_bb",
        "backend_type",
        "persona_name",
        "persona_text",
        "made_showdown",
        "showdown_hand_category",
        "showdown_hand_score",
        "payload_version",
        "raw_lineage",
    ],
    "RAW_SIMULATION_RUNS": [
        "simulation_run_id",
        "table_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "backend_type",
        "status",
        "request_mode",
        "requested_at",
        "started_at",
        "finished_at",
        "hand_count",
        "seed",
        "hero_context",
        "hero_seat",
        "model_name",
        "error_message",
        "published_actions",
        "published_hand_summaries",
        "config_json",
    ],
    "SIMULATION_RUN_PLAYER_SUMMARIES": [
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "is_hero_player",
        "seat",
        "player_id",
        "agent_id",
        "persona_name",
        "backend_type",
        "hands_played",
        "total_bb_won",
        "avg_bb_per_hand",
        "bb_per_100",
        "stack_reset_count",
        "llm_decision_count",
        "heuristic_decision_count",
        "llm_fallback_count",
        "final_rank",
        "updated_at",
    ],
    "CURATED_HANDS": [
        "hand_id",
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_seat",
        "table_id",
        "button_seat",
        "big_blind_bb",
        "small_blind_bb",
        "total_pot_bb",
        "board_cards",
        "started_at",
        "finished_at",
        "payload_version",
        "raw_lineage",
    ],
    "CURATED_ACTIONS": [
        "hand_id",
        "action_index",
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_seat",
        "is_hero_player",
        "table_id",
        "street",
        "player_id",
        "agent_id",
        "seat",
        "position",
        "action_type",
        "amount_bb",
        "pot_before_bb",
        "pot_after_bb",
        "to_call_bb",
        "effective_stack_bb",
        "players_remaining",
        "board_cards_visible",
        "hole_cards_visible",
        "is_all_in",
        "event_ts",
        "backend_type",
        "persona_name",
        "persona_text",
        "payload_version",
        "raw_lineage",
    ],
    "CURATED_PLAYERS": [
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_seat",
        "is_hero_player",
        "table_id",
        "hand_id",
        "player_id",
        "agent_id",
        "seat",
        "player_name",
        "stack_start_bb",
        "stack_end_bb",
        "hole_cards",
        "result_bb",
        "backend_type",
        "persona_name",
        "persona_text",
        "made_showdown",
        "showdown_hand_category",
        "showdown_hand_score",
        "payload_version",
        "raw_lineage",
    ],
    "HISTORICAL_HANDHQ_LANDING_HANDS": [
        "hand_id",
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "table_id",
        "button_seat",
        "big_blind_bb",
        "small_blind_bb",
        "total_pot_bb",
        "board_cards",
        "started_at",
        "finished_at",
        "payload_version",
        "raw_lineage",
    ],
    "HISTORICAL_HANDHQ_LANDING_ACTIONS": [
        "hand_id",
        "action_index",
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "table_id",
        "street",
        "player_id",
        "agent_id",
        "seat",
        "position",
        "action_type",
        "amount_bb",
        "pot_before_bb",
        "pot_after_bb",
        "to_call_bb",
        "effective_stack_bb",
        "players_remaining",
        "board_cards_visible",
        "hole_cards_visible",
        "is_all_in",
        "event_ts",
        "backend_type",
        "persona_name",
        "persona_text",
        "payload_version",
        "raw_lineage",
    ],
    "HISTORICAL_HANDHQ_LANDING_PLAYERS": [
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "table_id",
        "hand_id",
        "player_id",
        "agent_id",
        "seat",
        "player_name",
        "stack_start_bb",
        "stack_end_bb",
        "hole_cards",
        "result_bb",
        "persona_name",
        "persona_text",
        "payload_version",
        "raw_lineage",
    ],
    "HISTORICAL_HANDHQ_INGEST_RUNS": [
        "source_run_id",
        "source_uri",
        "source_dataset",
        "stage_name",
        "stage_prefix",
        "status",
        "started_at",
        "finished_at",
        "discovered_files",
        "skipped_files",
        "processed_files",
        "failed_files",
        "rejected_files",
        "loaded_hands",
        "rejected_hands",
        "raw_hand_rows",
        "raw_player_rows",
        "raw_action_rows",
        "message",
    ],
    "HISTORICAL_HANDHQ_INGEST_FILES": [
        "content_hash",
        "source_run_id",
        "source_file",
        "source_dataset",
        "status",
        "file_size_bytes",
        "discovered_at",
        "updated_at",
        "hands_seen",
        "hands_loaded",
        "hands_rejected",
        "raw_hand_rows",
        "raw_player_rows",
        "raw_action_rows",
        "last_error",
    ],
    "LIVE_AGENT_METRICS": [
        "metric_window_start",
        "metric_window_end",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "is_hero_player",
        "agent_id",
        "player_id",
        "persona_name",
        "actions_per_second",
        "hands_per_second",
        "vpip",
        "pfr",
        "aggression_frequency",
        "cbet_rate",
        "all_in_rate",
        "bb_won",
        "observed_hands",
        "observed_actions",
        "updated_at",
    ],
    "LIVE_PROFILE_ASSIGNMENTS": [
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "is_hero_player",
        "agent_id",
        "player_id",
        "nearest_cluster_id",
        "nearest_cluster_label",
        "distance_to_centroid",
        "assigned_at",
    ],
    "DASHBOARD_SUMMARIES": [
        "summary_key",
        "summary_scope",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "summary_json",
        "updated_at",
    ],
    "PROFILE_PLAYER_HAND_FEATURES": [
        "training_dataset",
        "feature_version",
        "source_type",
        "source_dataset",
        "source_run_id",
        "simulation_run_id",
        "table_id",
        "hand_id",
        "player_id",
        "agent_id",
        "position",
        "stack_start_bb",
        "preflop_vpip",
        "preflop_pfr",
        "aggressive_actions",
        "passive_actions",
        "preflop_aggressive_amount_total_bb",
        "preflop_aggressive_action_count",
        "postflop_aggressive_bet_pot_ratio_sum",
        "postflop_aggressive_bet_count",
        "saw_flop",
        "saw_turn",
        "saw_river",
        "all_in",
        "cbet_opportunity_flop",
        "flop_cbet",
        "turn_barrel_opportunity",
        "turn_barrel",
        "late_position_hand",
        "late_position_vpip",
        "blind_position_hand",
        "blind_defend",
        "short_stack_hand",
        "short_stack_aggressive_actions",
        "short_stack_passive_actions",
        "deep_stack_hand",
        "deep_stack_vpip",
        "updated_at",
    ],
    "PROFILE_PLAYER_TRAINING_FEATURES": [
        "training_dataset",
        "feature_version",
        "player_id",
        "source_dataset",
        "hands_played",
        *PROFILE_COUNT_COLUMNS,
        *PROFILE_RATE_FEATURE_COLUMNS,
        "eligible_for_training",
        "updated_at",
    ],
    "PROFILE_MODEL_RUNS": [
        "model_run_id",
        "created_at",
        "activated_at",
        "active",
        "algorithm",
        "cluster_count",
        "training_dataset",
        "feature_version",
        "training_player_count",
        "min_hand_threshold",
        "parameters_json",
        "selection_metrics_json",
        "status",
    ],
    "PROFILE_MODEL_FEATURES": [
        "model_run_id",
        "feature_name",
        "feature_order",
        "feature_version",
        "scaler_mean",
        "scaler_scale",
        "global_mean",
        "updated_at",
    ],
    "PROFILE_CLUSTER_CENTROIDS": [
        "model_run_id",
        "cluster_id",
        "cluster_label",
        "summary_text",
        "cluster_size",
        "cluster_share",
        "centroid_json",
        "updated_at",
    ],
    "PROFILE_CLUSTER_PROTOTYPES": [
        "model_run_id",
        "cluster_id",
        "prototype_rank",
        "player_id",
        "source_dataset",
        "hands_played",
        "distance_to_centroid",
        "prototype_json",
        "updated_at",
    ],
    "PROFILE_HISTORICAL_ASSIGNMENTS": [
        "model_run_id",
        "player_id",
        "source_dataset",
        "cluster_id",
        "cluster_label",
        "distance_to_centroid",
        "hands_played",
        "assigned_at",
    ],
    "PROFILE_SESSION_FEATURES": [
        "model_run_id",
        "feature_version",
        "source_type",
        "source_dataset",
        "source_run_id",
        "simulation_run_id",
        "subject_id",
        "player_id",
        "agent_id",
        "hands_observed",
        *PROFILE_COUNT_COLUMNS,
        *PROFILE_RATE_FEATURE_COLUMNS,
        "scored_at",
    ],
    "PROFILE_SESSION_RESULTS": [
        "model_run_id",
        "feature_version",
        "source_type",
        "source_dataset",
        "source_run_id",
        "simulation_run_id",
        "subject_id",
        "player_id",
        "agent_id",
        "hands_observed",
        "status",
        "cluster_id",
        "cluster_label",
        "confidence_score",
        "summary_text",
        "feature_vector",
        "top_feature_deltas",
        "prototype_matches",
        "scored_at",
    ],
}


_UNMAPPED_TABLE_NAME_PATTERN = re.compile(r"^[A-Z0-9_$.]+$")


def _normalize_table_name(table: str, *, allow_unmapped: bool = False) -> str:
    upper = table.upper()
    if upper in TABLE_SCHEMAS:
        return upper
    if table in TABLE_NAMES:
        return TABLE_NAMES[table]
    if allow_unmapped and _UNMAPPED_TABLE_NAME_PATTERN.match(upper):
        return upper
    raise KeyError(f"Unknown table name: {table}")


def _schema_columns(table: str) -> list[str]:
    return TABLE_SCHEMAS[_normalize_table_name(table)]


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _prepare_frame_for_storage(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    prepared = frame.copy()
    prepared.columns = [str(column).lower() for column in prepared.columns]
    for column in prepared.columns:
        if prepared[column].dtype != "object":
            continue
        prepared[column] = prepared[column].apply(_serialize_cell)
    return prepared


def _serialize_cell(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return json.dumps(list(value))
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return value


def _dbapi_cell(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (list, tuple, dict, set)):
        return json.dumps(list(value) if isinstance(value, set) else value, default=str)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            value = value.tz_convert("UTC").tz_localize(None)
        return value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _format_sql_value(value: Any) -> str:
    normalized = _dbapi_cell(value)
    if normalized is None:
        return "NULL"
    if isinstance(normalized, bool):
        return "TRUE" if normalized else "FALSE"
    if isinstance(normalized, (int, float)) and not isinstance(normalized, bool):
        return str(normalized)
    escaped = str(normalized).replace("'", "''")
    return f"'{escaped}'"


def _build_where_clause(
    *,
    filters_eq: dict[str, Any] | None = None,
    filters_in: dict[str, Iterable[Any]] | None = None,
    is_null: dict[str, bool] | None = None,
) -> str:
    clauses: list[str] = []
    for column, value in (filters_eq or {}).items():
        column_name = column.upper()
        if value is None:
            clauses.append(f"{column_name} IS NULL")
        else:
            clauses.append(f"{column_name} = {_format_sql_value(value)}")
    for column, values in (filters_in or {}).items():
        unique_values = [value for value in dict.fromkeys(list(values)) if value is not None]
        if not unique_values:
            continue
        joined = ", ".join(_format_sql_value(value) for value in unique_values)
        clauses.append(f"{column.upper()} IN ({joined})")
    for column, should_be_null in (is_null or {}).items():
        clauses.append(f"{column.upper()} IS {'NULL' if should_be_null else 'NOT NULL'}")
    if not clauses:
        return ""
    return " WHERE " + " AND ".join(clauses)


@dataclass
class SnowflakeWarehouse:
    config: PlatformConfig
    small_write_threshold = 5000
    _conn: Any | None = field(default=None, init=False, repr=False)

    def _get_setting(self, name: str) -> str:
        value = getenv_stripped(name)
        if not value:
            raise RuntimeError(f"Missing Snowflake setting: {name}")
        return value

    def _private_key_path(self) -> Path:
        configured = self._get_setting("SNOWFLAKE_PRIVATE_KEY_FILE")
        resolved = resolve_repo_path(configured)
        if resolved is None:
            raise RuntimeError("Missing Snowflake private key path")
        if resolved.exists():
            return resolved

        fallback = resolve_repo_path(Path("secrets") / "snowflake" / Path(configured).name)
        if fallback and fallback.exists():
            return fallback

        raise RuntimeError(f"Snowflake private key file does not exist: {configured}")

    def _open_connection(self):
        try:
            import snowflake.connector
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Snowflake backend requested but snowflake-connector-python is not installed"
            ) from exc

        required = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PRIVATE_KEY_FILE",
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_ROLE",
        ]
        missing = [name for name in required if not getenv_stripped(name)]
        if missing:
            raise RuntimeError(f"Missing Snowflake settings: {', '.join(missing)}")

        with self._private_key_path().open("rb") as handle:
            key_bytes = handle.read()

        from cryptography.hazmat.primitives import serialization

        passphrase = getenv_stripped("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        password = passphrase.encode() if passphrase else None
        try:
            private_key = serialization.load_pem_private_key(
                key_bytes,
                password=password,
            )
        except TypeError:
            # Some local dev keys are intentionally unencrypted even when a
            # passphrase env var is present for containerized deployments.
            private_key = serialization.load_pem_private_key(
                key_bytes,
                password=None,
            )
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            account=self._get_setting("SNOWFLAKE_ACCOUNT"),
            user=self._get_setting("SNOWFLAKE_USER"),
            private_key=private_key_der,
            database=self._get_setting("SNOWFLAKE_DATABASE"),
            schema=self._get_setting("SNOWFLAKE_SCHEMA"),
            warehouse=self._get_setting("SNOWFLAKE_WAREHOUSE"),
            role=self._get_setting("SNOWFLAKE_ROLE"),
            login_timeout=20,
            network_timeout=60,
            socket_timeout=60,
        )

    def _connect(self):
        if self._conn is None:
            self._conn = self._open_connection()
        return self._conn

    def ensure_schema(self) -> None:
        ddl_path = self.config.sql_dir / "ddl" / "snowflake" / "create_tables.sql"
        conn = self._connect()
        with conn.cursor() as cursor:
            database = self._quote_identifier(self._get_setting("SNOWFLAKE_DATABASE"))
            schema = self._quote_identifier(self._get_setting("SNOWFLAKE_SCHEMA"))
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cursor.execute(f"USE SCHEMA {schema}")
            for statement in _read_sql(ddl_path).split(";"):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)

    def execute(self, sql: str) -> None:
        conn = self._connect()
        with conn.cursor() as cursor:
            cursor.execute(sql)

    def query_df(self, sql: str) -> pd.DataFrame:
        conn = self._connect()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [description[0].lower() for description in (cursor.description or ())]
        return pd.DataFrame(rows, columns=columns)

    def load_table(
        self,
        table: str,
        *,
        filters_eq: dict[str, Any] | None = None,
        filters_in: dict[str, Iterable[Any]] | None = None,
        is_null: dict[str, bool] | None = None,
        order_by: list[str | tuple[str, bool]] | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        table_name = _normalize_table_name(table)
        sql = f"SELECT * FROM {table_name}"
        sql += _build_where_clause(filters_eq=filters_eq, filters_in=filters_in, is_null=is_null)
        if order_by:
            fragments: list[str] = []
            for item in order_by:
                if isinstance(item, tuple):
                    column, ascending = item
                else:
                    column, ascending = item, True
                fragments.append(f"{column.upper()} {'ASC' if ascending else 'DESC'}")
            sql += " ORDER BY " + ", ".join(fragments)
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return self.query_df(sql)

    def count_rows(
        self,
        table: str,
        *,
        filters_eq: dict[str, Any] | None = None,
        filters_in: dict[str, Iterable[Any]] | None = None,
        is_null: dict[str, bool] | None = None,
    ) -> int:
        table_name = _normalize_table_name(table)
        sql = f"SELECT COUNT(*) AS row_count FROM {table_name}"
        sql += _build_where_clause(filters_eq=filters_eq, filters_in=filters_in, is_null=is_null)
        frame = self.query_df(sql)
        if frame.empty:
            return 0
        return int(frame.iloc[0]["row_count"])

    def delete_rows(
        self,
        table: str,
        *,
        filters_eq: dict[str, Any] | None = None,
        filters_in: dict[str, Iterable[Any]] | None = None,
        is_null: dict[str, bool] | None = None,
    ) -> None:
        table_name = _normalize_table_name(table)
        where_clause = _build_where_clause(filters_eq=filters_eq, filters_in=filters_in, is_null=is_null)
        if not where_clause:
            self.clear_table(table_name)
            return
        self.execute(f"DELETE FROM {table_name}{where_clause}")

    def clear_table(self, table: str) -> None:
        self.execute(f"DELETE FROM {_normalize_table_name(table)}")

    def truncate_table(self, table: str) -> None:
        self.execute(f"TRUNCATE TABLE {_normalize_table_name(table)}")

    def delete_matching_keys(
        self,
        table: str,
        frame: pd.DataFrame,
        key_columns: list[str],
        *,
        chunk_size: int = 500,
    ) -> None:
        """Delete rows whose composite keys match rows in ``frame``.

        This keeps replayed Spark micro-batches idempotent without deleting a
        whole simulation run or hand when only a subset of events is present.
        """

        if frame.empty or not key_columns:
            return
        missing = [column for column in key_columns if column not in frame.columns]
        if missing:
            raise RuntimeError(f"Cannot delete matching keys from {table}; missing columns: {missing}")
        table_name = _normalize_table_name(table)
        keys = frame[key_columns].drop_duplicates().reset_index(drop=True)
        for start in range(0, len(keys), chunk_size):
            chunk = keys.iloc[start : start + chunk_size]
            clauses = []
            for _, row in chunk.iterrows():
                parts: list[str] = []
                for column in key_columns:
                    value = _dbapi_cell(row[column])
                    if value is None:
                        parts.append(f"{column.upper()} IS NULL")
                    else:
                        parts.append(f"{column.upper()} = {_format_sql_value(value)}")
                clauses.append("(" + " AND ".join(parts) + ")")
            if clauses:
                self.execute(f"DELETE FROM {table_name} WHERE " + " OR ".join(clauses))

    def write_dataframe(self, table: str, frame: pd.DataFrame, mode: str = "append", *, force_bulk: bool = False) -> None:
        try:
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Snowflake backend requested but pandas_tools is unavailable"
            ) from exc

        prepared = _prepare_frame_for_storage(frame)
        conn = self._connect()
        resolved_table = _normalize_table_name(table, allow_unmapped=True)
        if mode == "replace":
            self.clear_table(resolved_table)
        with conn.cursor() as cursor:
            cursor.execute(f"DESC TABLE {resolved_table}")
            table_descriptions = cursor.fetchall()
        table_columns = [row[0] for row in table_descriptions]
        normalized_columns = {column.upper(): column for column in table_columns}
        normalized_types = {row[0].upper(): str(row[1]).upper() for row in table_descriptions}
        selected_columns = [
            column for column in prepared.columns if column.upper() in normalized_columns
        ]
        if not selected_columns:
            raise RuntimeError(f"No matching columns between frame and table {table}")

        snowflake_frame = prepared[selected_columns].rename(
            columns={column: normalized_columns[column.upper()] for column in selected_columns}
        )

        if len(snowflake_frame) <= self.small_write_threshold and not force_bulk:
            insert_fragments = []
            has_variant_columns = False
            for column in snowflake_frame.columns:
                if "VARIANT" in normalized_types.get(column.upper(), ""):
                    insert_fragments.append("PARSE_JSON(%s)")
                    has_variant_columns = True
                else:
                    insert_fragments.append("%s")
            placeholders = ", ".join(insert_fragments)
            column_list = ", ".join(snowflake_frame.columns)
            rows = [
                tuple(_dbapi_cell(value) for value in row)
                for row in snowflake_frame.itertuples(index=False, name=None)
            ]
            if rows:
                with conn.cursor() as cursor:
                    if has_variant_columns:
                        insert_sql = f"INSERT INTO {resolved_table} ({column_list}) SELECT {placeholders}"
                        for row in rows:
                            cursor.execute(insert_sql, row)
                    else:
                        insert_sql = f"INSERT INTO {resolved_table} ({column_list}) VALUES ({placeholders})"
                        try:
                            cursor.executemany(insert_sql, rows)
                        except Exception as exc:
                            if len(rows) == 1 or "Failed to rewrite multi-row insert" in str(exc):
                                for row in rows:
                                    cursor.execute(insert_sql, row)
                            else:
                                raise
            return

        write_pandas(
            conn,
            snowflake_frame,
            table_name=resolved_table,
            auto_create_table=False,
            quote_identifiers=False,
            use_logical_type=True,
        )

    def upsert_dataframe(
        self,
        table: str,
        frame: pd.DataFrame,
        *,
        key_columns: list[str],
        force_bulk: bool = True,
    ) -> None:
        if frame.empty:
            return

        target_table = _normalize_table_name(table)
        temp_table = f"{target_table}__TMP_{uuid.uuid4().hex[:8].upper()}"
        self.execute(f"CREATE TEMP TABLE {temp_table} LIKE {target_table}")
        try:
            self.write_dataframe(temp_table, frame, force_bulk=force_bulk)
            self.merge_table_into_table(table, temp_table, key_columns=key_columns)
        finally:
            try:
                self.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except Exception:
                pass

    def put_file_to_stage(
        self,
        stage_name: str,
        local_path: str | Path,
        *,
        stage_prefix: str | None = None,
        auto_compress: bool = False,
        overwrite: bool = True,
    ) -> None:
        path_obj = Path(local_path).expanduser().resolve()
        stage_location = f"@{stage_name}"
        if stage_prefix:
            stage_location += f"/{stage_prefix.strip('/')}"
        path_sql = str(path_obj).replace("\\", "/").replace("'", "\\'")
        self.execute(
            " ".join(
                [
                    f"PUT file://{path_sql}",
                    stage_location,
                    f"AUTO_COMPRESS = {'TRUE' if auto_compress else 'FALSE'}",
                    f"OVERWRITE = {'TRUE' if overwrite else 'FALSE'}",
                ]
            )
        )

    def copy_stage_into_table(
        self,
        stage_name: str,
        table: str,
        *,
        stage_prefix: str | None = None,
        purge: bool = True,
    ) -> None:
        table_name = _normalize_table_name(table)
        stage_location = f"@{stage_name}"
        if stage_prefix:
            stage_location += f"/{stage_prefix.strip('/')}"
        sql = (
            f"COPY INTO {table_name} FROM {stage_location} "
            "FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE) "
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        )
        if purge:
            sql += " PURGE = TRUE"
        self.execute(sql)

    def merge_query_into_table(
        self,
        table: str,
        source_query: str,
        *,
        key_columns: list[str],
        source_alias: str = "source",
    ) -> None:
        target_table = _normalize_table_name(table)
        columns = _schema_columns(table)
        key_lookup = {column.upper() for column in key_columns}
        mutable_columns = [column for column in columns if column.upper() not in key_lookup]
        on_clause = " AND ".join(
            f"target.{column.upper()} = {source_alias}.{column.upper()}"
            for column in key_columns
        )
        update_clause = ", ".join(
            f"{column.upper()} = {source_alias}.{column.upper()}"
            for column in mutable_columns
        )
        insert_columns = ", ".join(column.upper() for column in columns)
        insert_values = ", ".join(f"{source_alias}.{column.upper()}" for column in columns)
        sql = (
            f"MERGE INTO {target_table} AS target "
            f"USING ({source_query}) AS {source_alias} "
            f"ON {on_clause} "
        )
        if update_clause:
            sql += f"WHEN MATCHED THEN UPDATE SET {update_clause} "
        sql += f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        self.execute(sql)

    def merge_table_into_table(
        self,
        table: str,
        source_table: str,
        *,
        key_columns: list[str],
    ) -> None:
        source_name = _normalize_table_name(source_table, allow_unmapped=True)
        columns = ", ".join(column.upper() for column in _schema_columns(table))
        self.merge_query_into_table(table, f"SELECT {columns} FROM {source_name}", key_columns=key_columns)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __del__(self) -> None:  # pragma: no cover - best effort cleanup
        try:
            self.close()
        except Exception:
            pass

    @staticmethod
    def _quote_identifier(value: str) -> str:
        escaped = value.replace('"', '""')
        return f'"{escaped}"'


def get_warehouse(config: PlatformConfig | None = None) -> SnowflakeWarehouse:
    return SnowflakeWarehouse(config or get_config())


def bootstrap_backend(config: PlatformConfig | None = None) -> SnowflakeWarehouse:
    warehouse = get_warehouse(config)
    warehouse.ensure_schema()
    return warehouse
