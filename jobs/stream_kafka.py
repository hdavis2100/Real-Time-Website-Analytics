from __future__ import annotations

"""Spark/Kafka live-metrics job for action-level poker events."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import time
from typing import Any

import pandas as pd

from poker_platform.config import get_config
from poker_platform.event_contracts import raw_action_row_from_event, raw_hand_row_from_summary_event
from poker_platform.redis_live import RedisLiveStore, get_live_store
from poker_platform.storage import TABLE_SCHEMAS, bootstrap_backend, get_warehouse
from simulator.cards import best_five_of_seven


AGGRESSIVE_ACTIONS = {"bet", "raise"}
PASSIVE_ACTIONS = {"call"}
VOLUNTARY_PREFLOP_ACTIONS = {"call", "bet", "raise"}
SHOWDOWN_HAND_CATEGORIES = {
    8: "straight_flush",
    7: "four_of_a_kind",
    6: "full_house",
    5: "flush",
    4: "straight",
    3: "three_of_a_kind",
    2: "two_pair",
    1: "one_pair",
    0: "high_card",
}
PROFILE_COLUMNS = [
    "vpip",
    "pfr",
    "aggression_frequency",
    "cbet_rate",
    "all_in_rate",
]
ACTION_EVENT_KEYS = [
    "hand_id",
    "action_index",
    "source_type",
    "source_run_id",
    "simulation_run_id",
]
HAND_SUMMARY_KEYS = [
    "hand_id",
    "source_type",
    "source_run_id",
    "simulation_run_id",
]
PLAYER_RESULT_KEYS = [
    "hand_id",
    "player_id",
    "source_type",
    "source_run_id",
    "simulation_run_id",
]
PLAYER_SUMMARY_KEYS = [
    "simulation_run_id",
    "player_id",
]
SIMULATION_RUN_KEYS = [
    "simulation_run_id",
]
SIMULATION_RUN_COLUMNS = [column.lower() for column in TABLE_SCHEMAS["RAW_SIMULATION_RUNS"]]
PLAYER_SUMMARY_COLUMNS = [
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
]
TERMINAL_STATUSES = {"completed", "failed"}
EXECUTION_FINISHED_STATUSES = TERMINAL_STATUSES | {"finalizing"}
LIVE_LEADERBOARD_MAX_ROWS = 100
logger = logging.getLogger(__name__)
_ACTIVE_CENTROIDS_CACHE: dict[str, Any] = {
    "loaded_at": 0.0,
    "centroids": pd.DataFrame(),
}


def _warehouse_or_default(warehouse=None):
    if warehouse is not None:
        return warehouse
    if os.getenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP") == "1":
        return get_warehouse(get_config())
    return bootstrap_backend()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return int(default)
    try:
        return int(str(raw).strip())
    except ValueError:
        return int(default)


def _env_optional_positive_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return None
    try:
        value = int(str(raw).strip())
    except ValueError:
        return None
    return value if value > 0 else None


def _stream_shuffle_partitions() -> int:
    return max(1, _env_int("SPARK_SQL_SHUFFLE_PARTITIONS", 32))


def _stream_max_offsets_per_trigger() -> int | None:
    return _env_optional_positive_int("STREAM_MAX_OFFSETS_PER_TRIGGER")


def _stream_snowflake_write_chunk_size() -> int:
    return max(1000, _env_int("STREAM_SNOWFLAKE_WRITE_CHUNK_SIZE", 10000))


def _stream_active_centroids_cache_ttl_seconds() -> int:
    return max(0, _env_int("STREAM_ACTIVE_CENTROIDS_CACHE_TTL_SECONDS", 60))


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []


def _json_object(value: Any) -> dict[str, Any]:
    current = value
    for _ in range(4):
        if current is None:
            return {}
        if isinstance(current, dict):
            return current
        if not isinstance(current, str):
            return {}
        text = current.strip()
        if not text:
            return {}
        try:
            current = json.loads(text)
        except json.JSONDecodeError:
            return {}
    return {}


def _coerce_string_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    for column in columns:
        if column in result.columns:
            result[column] = result[column].astype("string")
    return result


def _mapping_str(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(key): str(item) for key, item in parsed.items()}
    return {}


def _mapping_float(value: Any) -> dict[str, float]:
    if isinstance(value, dict):
        return {str(key): float(item) for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(key): float(item) for key, item in parsed.items()}
    return {}


def _mapping_int(value: Any) -> dict[str, int]:
    if isinstance(value, dict):
        return {str(key): int(item) for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(key): int(item) for key, item in parsed.items()}
    return {}


def _mapping_cards(value: Any) -> dict[str, list[str]]:
    if isinstance(value, dict):
        return {str(key): _json_list(item) for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(key): _json_list(item) for key, item in parsed.items()}
    return {}


def _metric_lookup(row: pd.Series | dict[str, Any], *names: str) -> float | None:
    for name in names:
        if name in row and pd.notna(row[name]):
            return float(row[name])
    return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _lineage_lookup(value: Any, key: str) -> str | None:
    payload = _json_object(value)
    if not payload:
        return None
    result = payload.get(key)
    if result is None:
        return None
    text = str(result).strip()
    return text or None


def _status_from_event(event_type: Any, status: Any) -> str:
    normalized_status = str(status or "").strip().lower()
    if normalized_status:
        return normalized_status
    normalized_type = str(event_type or "").strip().lower()
    if normalized_type == "simulation_requested":
        return "queued"
    if normalized_type == "simulation_started":
        return "running"
    if normalized_type == "simulation_completed":
        return "completed"
    if normalized_type == "simulation_failed":
        return "failed"
    return ""


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(missing, bool):
        return missing
    return False


def _coerce_naive_timestamp(value: Any) -> datetime | None:
    if _is_missing(value):
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.tz_convert("UTC").tz_localize(None).to_pydatetime()


def _as_int_or_none(value: Any) -> int | None:
    if _is_missing(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _existing_run_rows_by_id(warehouse, run_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not run_ids:
        return {}
    try:
        frame = warehouse.load_table(
            "RAW_SIMULATION_RUNS",
            filters_in={"simulation_run_id": run_ids},
            order_by=[("finished_at", False), ("started_at", False), ("requested_at", False)],
        )
    except Exception:
        return {}
    if frame.empty or "simulation_run_id" not in frame.columns:
        return {}
    normalized = frame.copy()
    normalized.columns = [str(column).lower() for column in normalized.columns]
    normalized = normalized.drop_duplicates(subset=["simulation_run_id"], keep="first")
    return {
        str(row["simulation_run_id"]): row.to_dict()
        for _, row in normalized.iterrows()
        if not _is_missing(row.get("simulation_run_id"))
    }


def _set_if_present(row: dict[str, Any], key: str, value: Any) -> None:
    if not _is_missing(value):
        row[key] = value


def _set_max_int_if_present(row: dict[str, Any], key: str, value: Any) -> None:
    parsed = _as_int_or_none(value)
    if parsed is None:
        return
    current = _as_int_or_none(row.get(key))
    row[key] = parsed if current is None else max(current, parsed)


def _simulation_events_to_run_frame(
    simulation_events: pd.DataFrame,
    *,
    warehouse=None,
) -> pd.DataFrame:
    if simulation_events.empty:
        return pd.DataFrame(columns=SIMULATION_RUN_COLUMNS)

    events = simulation_events.copy()
    events.columns = [str(column).lower() for column in events.columns]
    if "simulation_run_id" not in events.columns:
        return pd.DataFrame(columns=SIMULATION_RUN_COLUMNS)

    events = events[events["simulation_run_id"].notna()].copy()
    if events.empty:
        return pd.DataFrame(columns=SIMULATION_RUN_COLUMNS)

    if "event_ts" not in events.columns:
        events["event_ts"] = pd.NaT
    if "requested_at" not in events.columns:
        events["requested_at"] = pd.NaT
    events["_event_sort_ts"] = events.apply(
        lambda row: _coerce_naive_timestamp(row.get("event_ts"))
        or _coerce_naive_timestamp(row.get("requested_at")),
        axis=1,
    )
    events = events.sort_values(["simulation_run_id", "_event_sort_ts"], na_position="last")

    run_ids = sorted(
        {
            str(run_id).strip()
            for run_id in events["simulation_run_id"].astype(str).tolist()
            if str(run_id).strip()
        }
    )
    existing_by_id = _existing_run_rows_by_id(warehouse, run_ids) if warehouse is not None else {}
    rows_by_id: dict[str, dict[str, Any]] = {
        run_id: {
            column: existing_by_id.get(run_id, {}).get(column)
            for column in SIMULATION_RUN_COLUMNS
        }
        for run_id in run_ids
    }

    for _, event in events.iterrows():
        run_id = str(event.get("simulation_run_id") or "").strip()
        if not run_id:
            continue
        row = rows_by_id.setdefault(run_id, {column: None for column in SIMULATION_RUN_COLUMNS})
        row["simulation_run_id"] = run_id

        event_type = str(event.get("event_type") or "").strip().lower()
        status = _status_from_event(event_type, event.get("status"))
        event_ts = _coerce_naive_timestamp(event.get("event_ts"))
        requested_at = _coerce_naive_timestamp(event.get("requested_at"))

        for field in [
            "table_id",
            "user_id",
            "decision_backend",
            "hero_context_hash",
            "backend_type",
            "request_mode",
            "model_name",
            "error_message",
        ]:
            _set_if_present(row, field, event.get(field))
        _set_if_present(row, "hero_context", event.get("hero_context"))
        _set_max_int_if_present(row, "hand_count", event.get("hand_count"))
        _set_max_int_if_present(row, "seed", event.get("seed"))
        _set_max_int_if_present(row, "hero_seat", event.get("hero_seat"))
        _set_max_int_if_present(row, "published_actions", event.get("published_actions"))
        _set_max_int_if_present(row, "published_hand_summaries", event.get("published_hand_summaries"))
        if not _is_missing(event.get("config_json")):
            row["config_json"] = event.get("config_json")

        if status:
            row["status"] = status
        if event_type == "simulation_requested":
            row["requested_at"] = requested_at or event_ts or row.get("requested_at")
        elif event_type in {"simulation_started", "simulation_progress"}:
            row["started_at"] = row.get("started_at") or event_ts
        if status in EXECUTION_FINISHED_STATUSES:
            row["finished_at"] = event_ts or row.get("finished_at")

        for field in ["requested_at", "started_at", "finished_at"]:
            explicit_value = _coerce_naive_timestamp(event.get(field))
            if explicit_value is not None:
                row[field] = explicit_value

    output_rows = []
    for run_id in run_ids:
        row = rows_by_id[run_id]
        for timestamp_field in ["requested_at", "started_at", "finished_at"]:
            row[timestamp_field] = _coerce_naive_timestamp(row.get(timestamp_field))
        output_rows.append({column: row.get(column) for column in SIMULATION_RUN_COLUMNS})
    return pd.DataFrame(output_rows, columns=SIMULATION_RUN_COLUMNS)


def persist_simulation_run_metadata(
    warehouse,
    simulation_events: pd.DataFrame,
) -> pd.DataFrame:
    run_frame = _simulation_events_to_run_frame(simulation_events, warehouse=warehouse)
    if run_frame.empty:
        return run_frame
    _replace_rows(warehouse, "RAW_SIMULATION_RUNS", run_frame, SIMULATION_RUN_KEYS)
    return run_frame


def build_simulation_run_player_summaries(
    player_frame: pd.DataFrame,
    *,
    action_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if player_frame.empty:
        return pd.DataFrame(columns=PLAYER_SUMMARY_COLUMNS)

    players = player_frame.copy()
    players = players[players["simulation_run_id"].notna() & players["player_id"].notna()].copy()
    if players.empty:
        return pd.DataFrame(columns=PLAYER_SUMMARY_COLUMNS)

    players = _coerce_string_columns(
        players,
        [
            "simulation_run_id",
            "user_id",
            "decision_backend",
            "hero_context_hash",
            "player_id",
            "agent_id",
            "persona_name",
            "backend_type",
            "hand_id",
        ],
    )
    for column in ["result_bb", "stack_start_bb", "stack_end_bb"]:
        if column not in players.columns:
            players[column] = pd.NA
        players[column] = pd.to_numeric(players[column], errors="coerce")
    if "is_hero_player" not in players.columns:
        players["is_hero_player"] = False
    players["is_hero_player"] = players["is_hero_player"].fillna(False).astype(bool)
    if "seat" not in players.columns:
        players["seat"] = pd.NA
    players["seat"] = pd.to_numeric(players["seat"], errors="coerce")

    base_group_cols = [
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
    ]
    grouped = (
        players.groupby(base_group_cols, dropna=False)
        .agg(
            hands_played=("hand_id", "nunique"),
            total_bb_won=("result_bb", "sum"),
            avg_bb_per_hand=("result_bb", "mean"),
        )
        .reset_index()
    )
    grouped["hands_played"] = grouped["hands_played"].fillna(0).astype(int)
    grouped["total_bb_won"] = grouped["total_bb_won"].fillna(0.0).round(4)
    grouped["avg_bb_per_hand"] = grouped["avg_bb_per_hand"].fillna(0.0).round(4)
    grouped["bb_per_100"] = grouped.apply(
        lambda row: round(
            100.0 * float(row["total_bb_won"]) / max(1, int(row["hands_played"])),
            4,
        ),
        axis=1,
    )

    reset_working = players.sort_values(
        ["simulation_run_id", "player_id", "hand_id"],
        ascending=[True, True, True],
        na_position="last",
    ).copy()
    reset_working["previous_stack_end_bb"] = reset_working.groupby(
        ["simulation_run_id", "player_id"],
        dropna=False,
    )["stack_end_bb"].shift(1)
    reset_working["stack_reset_flag"] = (
        reset_working["previous_stack_end_bb"].notna()
        & reset_working["stack_start_bb"].notna()
        & ((reset_working["stack_start_bb"] - reset_working["previous_stack_end_bb"]).abs() > 1e-9)
    ).astype(int)
    reset_counts = (
        reset_working.groupby(["simulation_run_id", "player_id"], dropna=False)
        .agg(stack_reset_count=("stack_reset_flag", "sum"))
        .reset_index()
    )
    grouped = grouped.merge(
        reset_counts,
        on=["simulation_run_id", "player_id"],
        how="left",
    )

    if action_frame is not None and not action_frame.empty:
        actions = action_frame.copy()
        actions = actions[actions["simulation_run_id"].notna() & actions["player_id"].notna()].copy()
        if not actions.empty:
            actions = _coerce_string_columns(actions, ["simulation_run_id", "player_id"])
            if "raw_lineage" not in actions.columns:
                actions["raw_lineage"] = None
            actions["decision_source"] = actions["raw_lineage"].apply(
                lambda value: str(_lineage_lookup(value, "decision_source") or "").strip().lower()
            )
            decision_counts = (
                actions.groupby(["simulation_run_id", "player_id"], dropna=False)
                .agg(
                    llm_decision_count=("decision_source", lambda series: int((series == "llm").sum())),
                    heuristic_decision_count=(
                        "decision_source",
                        lambda series: int((series == "heuristic").sum()),
                    ),
                    llm_fallback_count=(
                        "decision_source",
                        lambda series: int((series == "heuristic_fallback").sum()),
                    ),
                )
                .reset_index()
            )
            grouped = grouped.merge(
                decision_counts,
                on=["simulation_run_id", "player_id"],
                how="left",
            )

    for column in [
        "stack_reset_count",
        "llm_decision_count",
        "heuristic_decision_count",
        "llm_fallback_count",
    ]:
        if column not in grouped.columns:
            grouped[column] = 0
        grouped[column] = grouped[column].fillna(0).astype(int)

    grouped = grouped.sort_values(
        ["simulation_run_id", "total_bb_won", "seat", "player_id"],
        ascending=[True, False, True, True],
        na_position="last",
    ).reset_index(drop=True)
    grouped["final_rank"] = grouped.groupby("simulation_run_id", dropna=False).cumcount() + 1
    grouped["updated_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    for column in PLAYER_SUMMARY_COLUMNS:
        if column not in grouped.columns:
            grouped[column] = pd.NA
    return grouped[PLAYER_SUMMARY_COLUMNS]


def refresh_simulation_run_player_summaries(
    simulation_run_ids: list[str],
    *,
    warehouse=None,
) -> pd.DataFrame:
    normalized_run_ids = sorted({str(run_id).strip() for run_id in simulation_run_ids if str(run_id).strip()})
    warehouse = _warehouse_or_default(warehouse)
    if not normalized_run_ids:
        return pd.DataFrame(columns=PLAYER_SUMMARY_COLUMNS)

    player_frame = warehouse.load_table(
        "CURATED_PLAYERS",
        filters_in={"simulation_run_id": normalized_run_ids},
        order_by=[("simulation_run_id", True), ("hand_id", True), ("seat", True)],
    )
    action_frame = warehouse.load_table(
        "CURATED_ACTIONS",
        filters_in={"simulation_run_id": normalized_run_ids},
        order_by=[("simulation_run_id", True), ("hand_id", True), ("action_index", True)],
    )
    summary_frame = build_simulation_run_player_summaries(
        player_frame,
        action_frame=action_frame,
    )
    warehouse.delete_rows(
        "SIMULATION_RUN_PLAYER_SUMMARIES",
        filters_in={"simulation_run_id": normalized_run_ids},
    )
    if not summary_frame.empty:
        warehouse.write_dataframe(
            "SIMULATION_RUN_PLAYER_SUMMARIES",
            summary_frame,
            force_bulk=True,
        )
    return summary_frame


def _run_ids_from_frame(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "simulation_run_id" not in frame.columns:
        return []
    return sorted(
        {
            str(run_id).strip()
            for run_id in frame["simulation_run_id"].dropna().astype(str).tolist()
            if str(run_id).strip()
        }
    )


def _terminal_run_ids_from_frame(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "simulation_run_id" not in frame.columns:
        return []
    run_ids: set[str] = set()
    for row in frame.to_dict(orient="records"):
        run_id = str(row.get("simulation_run_id") or "").strip()
        if not run_id:
            continue
        if _status_from_event(row.get("event_type"), row.get("status")) in EXECUTION_FINISHED_STATUSES:
            run_ids.add(run_id)
    return sorted(run_ids)


def _execution_finished_run_ids_from_registry(
    warehouse,
    simulation_run_ids: list[str],
) -> list[str]:
    normalized_run_ids = sorted({str(run_id).strip() for run_id in simulation_run_ids if str(run_id).strip()})
    if not normalized_run_ids:
        return []
    run_frame = warehouse.load_table(
        "RAW_SIMULATION_RUNS",
        filters_in={"simulation_run_id": normalized_run_ids},
        order_by=[("finished_at", False), ("started_at", False), ("requested_at", False)],
    )
    if run_frame.empty or "simulation_run_id" not in run_frame.columns or "status" not in run_frame.columns:
        return []
    working = _coerce_string_columns(run_frame, ["simulation_run_id", "status"]).copy()
    working = working.drop_duplicates(subset=["simulation_run_id"], keep="first")
    working["status"] = working["status"].fillna("").astype(str).str.strip().str.lower()
    return sorted(
        {
            str(row["simulation_run_id"]).strip()
            for _, row in working.iterrows()
            if str(row["simulation_run_id"]).strip() and row["status"] in EXECUTION_FINISHED_STATUSES
        }
    )


def _showdown_strength_from_cards(board_cards: Any, hole_cards: Any) -> tuple[int, tuple[int, ...]] | None:
    board = tuple(_json_list(board_cards))
    hole = tuple(_json_list(hole_cards))
    if len(board) < 5 or len(hole) != 2:
        return None
    return best_five_of_seven(board + hole)


def _showdown_hand_category_from_cards(board_cards: Any, hole_cards: Any, made_showdown: Any) -> str | None:
    if not _boolish(made_showdown):
        return None
    strength = _showdown_strength_from_cards(board_cards, hole_cards)
    if strength is None:
        return None
    return SHOWDOWN_HAND_CATEGORIES.get(int(strength[0]), "unknown")


def _showdown_hand_score_from_cards(board_cards: Any, hole_cards: Any, made_showdown: Any) -> float | None:
    if not _boolish(made_showdown):
        return None
    strength = _showdown_strength_from_cards(board_cards, hole_cards)
    if strength is None:
        return None
    rank, tiebreak = strength
    components = [int(rank), *[int(value) for value in tiebreak], 0, 0, 0, 0, 0]
    score = 0
    for value in components[:6]:
        score = (score * 15) + value
    return float(score)


def compute_live_metrics(
    actions: pd.DataFrame,
    *,
    player_results: pd.DataFrame | None = None,
    window_minutes: int = 5,
    window_end: datetime | None = None,
) -> pd.DataFrame:
    columns = [
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
    ]
    if actions.empty:
        return pd.DataFrame(columns=columns)

    frame = actions.copy()
    frame["event_ts"] = pd.to_datetime(frame["event_ts"], utc=True, errors="coerce")
    frame["board_cards_visible"] = frame["board_cards_visible"].apply(_json_list)
    frame["hole_cards_visible"] = frame["hole_cards_visible"].apply(_json_list)
    frame = _coerce_string_columns(
        frame,
        [
            "simulation_run_id",
            "user_id",
            "decision_backend",
            "hero_context_hash",
            "agent_id",
            "player_id",
            "persona_name",
            "hand_id",
        ],
    )
    if "is_hero_player" not in frame.columns:
        frame["is_hero_player"] = False
    frame = frame.dropna(subset=["event_ts"]).reset_index(drop=True)
    if frame.empty:
        return pd.DataFrame(columns=columns)

    window_end_ts = pd.Timestamp(window_end or frame["event_ts"].max())
    if window_end_ts.tzinfo is None:
        window_end_ts = window_end_ts.tz_localize("UTC")
    else:
        window_end_ts = window_end_ts.tz_convert("UTC")
    window_start_ts = window_end_ts - pd.Timedelta(minutes=window_minutes)
    frame = frame[(frame["event_ts"] >= window_start_ts) & (frame["event_ts"] <= window_end_ts)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)

    frame["is_preflop"] = frame["street"] == "preflop"
    frame["is_flop"] = frame["street"] == "flop"
    frame["is_aggressive"] = frame["action_type"].isin(AGGRESSIVE_ACTIONS)
    frame["is_passive"] = frame["action_type"].isin(PASSIVE_ACTIONS)
    frame["is_vpip_action"] = frame["is_preflop"] & frame["action_type"].isin(VOLUNTARY_PREFLOP_ACTIONS)
    frame["is_pfr_action"] = frame["is_preflop"] & frame["action_type"].isin(AGGRESSIVE_ACTIONS)
    group_cols = [
        column
        for column in [
            "simulation_run_id",
            "user_id",
            "decision_backend",
            "hero_context_hash",
            "is_hero_player",
            "agent_id",
            "player_id",
            "persona_name",
        ]
        if column in frame.columns
    ]

    hand_features = (
        frame.groupby(group_cols + ["hand_id"], dropna=False)
        .agg(
            vpip=("is_vpip_action", "max"),
            pfr=("is_pfr_action", "max"),
            aggression_actions=("is_aggressive", "sum"),
            passive_actions=("is_passive", "sum"),
            all_in=("is_all_in", "max"),
            saw_flop=("street", lambda series: bool(series.isin(["flop", "turn", "river", "showdown"]).any())),
        )
        .reset_index()
    )

    first_flop_aggressor = (
        frame[frame["is_flop"] & frame["is_aggressive"]]
        .sort_values(["hand_id", "action_index"])
        .drop_duplicates(["hand_id"], keep="first")[["hand_id", "player_id"]]
        .rename(columns={"player_id": "flop_aggressor_player_id"})
    )
    hand_features = hand_features.merge(first_flop_aggressor, on="hand_id", how="left")
    hand_features["is_cbet"] = (
        hand_features["pfr"]
        & hand_features["saw_flop"]
        & (hand_features["player_id"] == hand_features["flop_aggressor_player_id"])
    )
    hand_features.drop(columns=["flop_aggressor_player_id"], inplace=True)

    grouped = (
        hand_features.groupby(group_cols, dropna=False)
        .agg(
            observed_hands=("hand_id", "nunique"),
            vpip=("vpip", "mean"),
            pfr=("pfr", "mean"),
            cbet_rate=("is_cbet", "mean"),
            all_in_rate=("all_in", "mean"),
            aggression_actions=("aggression_actions", "sum"),
            passive_actions=("passive_actions", "sum"),
        )
        .reset_index()
    )

    action_counts = frame.groupby(group_cols, dropna=False).size().reset_index(name="observed_actions")
    grouped = grouped.merge(action_counts, on=group_cols, how="left")
    grouped["observed_actions"] = grouped["observed_actions"].fillna(0).astype(int)

    window_seconds = max(window_minutes * 60, 1)
    grouped["actions_per_second"] = grouped["observed_actions"] / window_seconds
    grouped["hands_per_second"] = grouped["observed_hands"] / window_seconds
    grouped["aggression_frequency"] = grouped.apply(
        lambda row: float(row["aggression_actions"])
        / float(row["aggression_actions"] + row["passive_actions"])
        if (row["aggression_actions"] + row["passive_actions"]) > 0
        else 0.0,
        axis=1,
    )
    grouped.drop(columns=["aggression_actions", "passive_actions"], inplace=True)

    if player_results is not None and not player_results.empty:
        results = _coerce_string_columns(
            player_results,
            [
                "simulation_run_id",
                "user_id",
                "decision_backend",
                "hero_context_hash",
                "agent_id",
                "player_id",
                "persona_name",
                "hand_id",
            ],
        )
        if "is_hero_player" not in results.columns:
            results["is_hero_player"] = False
        if "result_bb" in results.columns:
            results["result_bb"] = pd.to_numeric(results["result_bb"], errors="coerce")
            bb_group_cols = [column for column in group_cols if column in results.columns]
            bb_won = (
                results.groupby(bb_group_cols, dropna=False)
                .agg(bb_won=("result_bb", "sum"))
                .reset_index()
            )
            grouped = grouped.merge(bb_won, on=bb_group_cols, how="left")
        else:
            grouped["bb_won"] = pd.NA
    else:
        grouped["bb_won"] = pd.NA

    grouped["metric_window_start"] = window_start_ts.tz_convert("UTC").tz_localize(None)
    grouped["metric_window_end"] = window_end_ts.tz_convert("UTC").tz_localize(None)
    grouped["updated_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    for column in columns:
        if column not in grouped.columns:
            grouped[column] = pd.NA
    return grouped[columns].sort_values(["simulation_run_id", "agent_id"], na_position="last").reset_index(drop=True)


def assign_live_profiles(live_metrics: pd.DataFrame, centroids: pd.DataFrame) -> pd.DataFrame:
    columns = [
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
    ]
    if live_metrics.empty or centroids.empty:
        return pd.DataFrame(columns=columns)

    centroid_frame = pd.DataFrame(_normalized_centroid_rows(centroids)).dropna(subset=["cluster_id"])
    if centroid_frame.empty:
        return pd.DataFrame(columns=columns)

    assignments: list[dict[str, Any]] = []
    assigned_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for _, metric_row in live_metrics.iterrows():
        best_match: dict[str, Any] | None = None
        for _, centroid_row in centroid_frame.iterrows():
            distance = sum(
                (float(metric_row[column]) - float(centroid_row[column])) ** 2
                for column in PROFILE_COLUMNS
            ) ** 0.5
            candidate = {
                "simulation_run_id": metric_row["simulation_run_id"],
                "user_id": metric_row.get("user_id"),
                "decision_backend": metric_row.get("decision_backend"),
                "hero_context_hash": metric_row.get("hero_context_hash"),
                "is_hero_player": metric_row.get("is_hero_player", False),
                "agent_id": metric_row["agent_id"],
                "player_id": metric_row["player_id"],
                "nearest_cluster_id": centroid_row["cluster_id"],
                "nearest_cluster_label": centroid_row["cluster_label"],
                "distance_to_centroid": float(distance),
                "assigned_at": assigned_at,
            }
            if best_match is None or candidate["distance_to_centroid"] < best_match["distance_to_centroid"]:
                best_match = candidate
        if best_match is not None:
            assignments.append(best_match)
    return pd.DataFrame(assignments, columns=columns)


def write_live_outputs(
    live_metrics: pd.DataFrame,
    assignments: pd.DataFrame,
    *,
    warehouse=None,
) -> None:
    warehouse = _warehouse_or_default(warehouse)
    if not live_metrics.empty:
        run_ids = sorted(live_metrics["simulation_run_id"].dropna().astype(str).unique())
        if run_ids:
            warehouse.delete_rows("LIVE_AGENT_METRICS", filters_in={"simulation_run_id": run_ids})
        warehouse.write_dataframe("LIVE_AGENT_METRICS", live_metrics, force_bulk=True)
    if not assignments.empty:
        run_ids = sorted(assignments["simulation_run_id"].dropna().astype(str).unique())
        if run_ids:
            warehouse.delete_rows("LIVE_PROFILE_ASSIGNMENTS", filters_in={"simulation_run_id": run_ids})
        warehouse.write_dataframe("LIVE_PROFILE_ASSIGNMENTS", assignments, force_bulk=True)


def process_frames(
    action_frame: pd.DataFrame,
    hand_frame: pd.DataFrame,
    player_frame: pd.DataFrame,
    *,
    simulation_event_frame: pd.DataFrame | None = None,
    window_minutes: int = 5,
    warehouse=None,
    live_store: RedisLiveStore | None = None,
) -> dict[str, int]:
    warehouse = _warehouse_or_default(warehouse)
    simulation_event_frame = simulation_event_frame if simulation_event_frame is not None else pd.DataFrame()

    _replace_rows(warehouse, "RAW_ACTIONS", action_frame, ACTION_EVENT_KEYS)
    _replace_rows(warehouse, "CURATED_ACTIONS", action_frame, ACTION_EVENT_KEYS)
    _replace_rows(warehouse, "RAW_HANDS", hand_frame, HAND_SUMMARY_KEYS)
    _replace_rows(warehouse, "CURATED_HANDS", hand_frame, HAND_SUMMARY_KEYS)
    _replace_rows(warehouse, "RAW_PLAYERS", player_frame, PLAYER_RESULT_KEYS)
    _replace_rows(warehouse, "CURATED_PLAYERS", player_frame, PLAYER_RESULT_KEYS)
    simulation_run_frame = persist_simulation_run_metadata(
        warehouse,
        simulation_event_frame,
    )

    touched_run_ids = sorted(
        set(_run_ids_from_frame(action_frame))
        | set(_run_ids_from_frame(hand_frame))
        | set(_run_ids_from_frame(player_frame))
        | set(_run_ids_from_frame(simulation_event_frame))
    )
    terminal_run_ids = sorted(
        set(_terminal_run_ids_from_frame(simulation_event_frame))
        | set(_execution_finished_run_ids_from_registry(warehouse, touched_run_ids))
    )
    player_summary_frame = (
        refresh_simulation_run_player_summaries(
            terminal_run_ids,
            warehouse=warehouse,
        )
        if terminal_run_ids
        else pd.DataFrame(columns=PLAYER_SUMMARY_COLUMNS)
    )

    live_actions = action_frame.copy()
    if not live_actions.empty:
        live_actions = live_actions[live_actions["source_type"].astype(str) == "simulated"]
        if "simulation_run_id" in live_actions.columns:
            live_actions = live_actions[live_actions["simulation_run_id"].notna()]

    live_players = player_frame.copy()
    if not live_players.empty:
        live_players = live_players[live_players["source_type"].astype(str) == "simulated"]
        if "simulation_run_id" in live_players.columns:
            live_players = live_players[live_players["simulation_run_id"].notna()]

    live_metrics = compute_live_metrics(
        live_actions,
        player_results=live_players,
        window_minutes=window_minutes,
    )
    assignments = assign_live_profiles(live_metrics, _load_active_centroids(warehouse))
    write_live_outputs(live_metrics, assignments, warehouse=warehouse)

    dashboard_summary_rows: list[dict[str, Any]] = []
    if live_store is not None:
        dashboard_summary_rows = materialize_live_dashboard(
            live_store,
            live_players=live_players,
            live_metrics=live_metrics,
            assignments=assignments,
            simulation_events=simulation_event_frame,
        )
        if dashboard_summary_rows:
            summary_frame = pd.DataFrame(dashboard_summary_rows)
            warehouse.delete_matching_keys("DASHBOARD_SUMMARIES", summary_frame, ["summary_key"])
            warehouse.write_dataframe("DASHBOARD_SUMMARIES", summary_frame, force_bulk=True)

    return {
        "action_rows": int(len(action_frame)),
        "hand_rows": int(len(hand_frame)),
        "player_rows": int(len(player_frame)),
        "player_summary_rows": int(len(player_summary_frame)),
        "simulation_run_rows": int(len(simulation_run_frame)),
        "simulation_event_rows": int(len(simulation_event_frame)),
        "live_metric_rows": int(len(live_metrics)),
        "live_assignment_rows": int(len(assignments)),
        "dashboard_summary_rows": int(len(dashboard_summary_rows)),
    }


def process_event_batch(
    action_payloads: list[dict[str, Any]],
    summary_payloads: list[dict[str, Any]],
    *,
    simulation_event_payloads: list[dict[str, Any]] | None = None,
    window_minutes: int = 5,
    warehouse=None,
    live_store: RedisLiveStore | None = None,
) -> dict[str, int]:
    action_frame = _action_payloads_to_frame(action_payloads)
    hand_frame = _summary_payloads_to_hand_frame(summary_payloads)
    player_frame = _summary_payloads_to_player_frame(summary_payloads)
    event_frame = _simulation_event_payloads_to_frame(simulation_event_payloads or [])
    return process_frames(
        action_frame,
        hand_frame,
        player_frame,
        simulation_event_frame=event_frame,
        window_minutes=window_minutes,
        warehouse=warehouse,
        live_store=live_store,
    )


def materialize_live_dashboard(
    live_store: RedisLiveStore,
    *,
    live_players: pd.DataFrame,
    live_metrics: pd.DataFrame,
    assignments: pd.DataFrame,
    simulation_events: pd.DataFrame,
) -> list[dict[str, Any]]:
    touched_run_ids: set[str] = set()
    dashboard_rows: list[dict[str, Any]] = []

    if not simulation_events.empty:
        events = simulation_events.copy()
        events["event_ts"] = pd.to_datetime(events["event_ts"], utc=True, errors="coerce")
        events = events.sort_values(["event_ts", "simulation_run_id"], na_position="last")
        for event in events.to_dict(orient="records"):
            run_id = str(event.get("simulation_run_id") or "")
            if not run_id:
                continue
            status = _status_from_event(event.get("event_type"), event.get("status"))
            event_ts = event.get("event_ts")
            meta = {
                "simulation_run_id": run_id,
                "table_id": event.get("table_id"),
                "user_id": event.get("user_id"),
                "decision_backend": event.get("decision_backend"),
                "hero_context_hash": event.get("hero_context_hash"),
                "hero_context_preview": event.get("hero_context_preview"),
                "model_name": event.get("model_name"),
                "backend_type": event.get("backend_type"),
                "status": status,
                "requested_at": event.get("requested_at") or (event_ts if status == "queued" else None),
                "started_at": event.get("started_at") or (event_ts if status == "running" else None),
                "finished_at": event.get("finished_at") or (event_ts if status in EXECUTION_FINISHED_STATUSES else None),
                "updated_at": event_ts,
                "hand_count": event.get("hand_count"),
                "published_actions": event.get("published_actions"),
                "published_hand_summaries": event.get("published_hand_summaries"),
                "error_message": event.get("error_message"),
            }
            live_store.set_run_meta(meta)
            touched_run_ids.add(run_id)

    if not live_players.empty:
        for run_id, run_frame in live_players.groupby("simulation_run_id", dropna=False):
            if pd.isna(run_id):
                continue
            run_id_str = str(run_id)
            touched_run_ids.add(run_id_str)
            rows = run_frame.to_dict(orient="records")
            run_meta = live_store.get_run_meta(run_id_str)
            aggregate_rows = _sync_live_player_state(
                live_store,
                rows,
                run_meta_lookup={run_id_str: run_meta},
            ).get(run_id_str, [])
            profit_rows, bb_rows, high_hand_rows, hero_rows = _build_run_leaderboards_from_aggregate_rows(aggregate_rows)
            live_store.replace_run_leaderboard(run_id_str, "profit", profit_rows, score_field="total_bb_won")
            live_store.replace_run_leaderboard(run_id_str, "bb_per_100", bb_rows, score_field="bb_per_100")
            live_store.replace_run_leaderboard(
                run_id_str,
                "high_hand",
                high_hand_rows,
                score_field="showdown_hand_score",
            )
            dashboard_rows.extend(
                [
                    _summary_row(
                        summary_key=f"run:{run_id_str}:leaderboard:profit",
                        summary_scope="run_profit_leaderboard",
                        rows=profit_rows,
                        run_meta=run_meta,
                    ),
                    _summary_row(
                        summary_key=f"run:{run_id_str}:leaderboard:bb_per_100",
                        summary_scope="run_bb_per_100_leaderboard",
                        rows=bb_rows,
                        run_meta=run_meta,
                    ),
                    _summary_row(
                        summary_key=f"run:{run_id_str}:leaderboard:high_hand",
                        summary_scope="run_high_hand_leaderboard",
                        rows=high_hand_rows,
                        run_meta=run_meta,
                    ),
                    _summary_row(
                        summary_key=f"run:{run_id_str}:leaderboard:hero_context",
                        summary_scope="run_hero_context_leaderboard",
                        rows=hero_rows,
                        run_meta=run_meta,
                    ),
                ]
            )

    if not live_metrics.empty or not assignments.empty:
        metric_frame = live_metrics.copy() if not live_metrics.empty else pd.DataFrame()
        assignment_frame = assignments.copy() if not assignments.empty else pd.DataFrame()
        metric_run_ids = sorted(
            {
                str(run_id)
                for run_id in metric_frame.get("simulation_run_id", pd.Series(dtype="string")).dropna().astype(str).tolist()
            }
            | {
                str(run_id)
                for run_id in assignment_frame.get("simulation_run_id", pd.Series(dtype="string")).dropna().astype(str).tolist()
            }
        )
        for run_id in metric_run_ids:
            touched_run_ids.add(run_id)
            run_metrics = metric_frame[metric_frame["simulation_run_id"].astype(str) == run_id] if not metric_frame.empty else pd.DataFrame()
            run_assignments = (
                assignment_frame[assignment_frame["simulation_run_id"].astype(str) == run_id]
                if not assignment_frame.empty
                else pd.DataFrame()
            )
            run_meta = live_store.get_run_meta(run_id)
            agent_rows = _merge_agent_rows(run_metrics, run_assignments, run_meta)
            live_store.replace_run_agents(run_id, agent_rows)

    terminal_events = []
    if not simulation_events.empty:
        for event in simulation_events.to_dict(orient="records"):
            run_id = str(event.get("simulation_run_id") or "")
            if not run_id:
                continue
            status = _status_from_event(event.get("event_type"), event.get("status"))
            if status in EXECUTION_FINISHED_STATUSES:
                terminal_events.append((run_id, event.get("user_id")))

    for run_id, user_id in terminal_events:
        live_store.expire_run(run_id, user_id=None if user_id is None else str(user_id))

    active_run_ids = live_store.list_active_runs()
    global_profit_rows, global_high_hand_rows, global_hero_rows = _global_leaderboard_rows_from_run_leaderboards(
        live_store,
        active_run_ids,
    )
    live_store.replace_global_leaderboard("profit", global_profit_rows, score_field="total_bb_won")
    live_store.replace_global_leaderboard("high_hand", global_high_hand_rows, score_field="showdown_hand_score")
    live_store.replace_global_leaderboard("hero_context", global_hero_rows, score_field="bb_per_100")
    dashboard_rows.extend(
        [
            _summary_row(
                summary_key="global:leaderboard:profit",
                summary_scope="global_profit_leaderboard",
                rows=global_profit_rows,
                run_meta={},
            ),
            _summary_row(
                summary_key="global:leaderboard:high_hand",
                summary_scope="global_high_hand_leaderboard",
                rows=global_high_hand_rows,
                run_meta={},
            ),
            _summary_row(
                summary_key="global:leaderboard:hero_context",
                summary_scope="global_hero_context_leaderboard",
                rows=global_hero_rows,
                run_meta={},
            ),
        ]
    )
    return dashboard_rows


def _summary_row(
    *,
    summary_key: str,
    summary_scope: str,
    rows: list[dict[str, Any]],
    run_meta: dict[str, Any],
) -> dict[str, Any]:
    return {
        "summary_key": summary_key,
        "summary_scope": summary_scope,
        "user_id": run_meta.get("user_id"),
        "decision_backend": run_meta.get("decision_backend"),
        "hero_context_hash": run_meta.get("hero_context_hash"),
        "summary_json": rows,
        "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def _sort_leaderboard_rows(
    rows: list[dict[str, Any]],
    score_field: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("simulation_run_id") or ""),
            str(row.get("player_id") or ""),
            str(row.get("agent_id") or ""),
        )
        score = pd.to_numeric(pd.Series([row.get(score_field)]), errors="coerce").iloc[0]
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = dict(row)
            continue
        existing_score = pd.to_numeric(pd.Series([existing.get(score_field)]), errors="coerce").iloc[0]
        if pd.isna(existing_score) or (pd.notna(score) and float(score) > float(existing_score)):
            deduped[key] = dict(row)
    sorted_rows = sorted(
        deduped.values(),
        key=lambda item: float(pd.to_numeric(pd.Series([item.get(score_field)]), errors="coerce").fillna(-1e18).iloc[0]),
        reverse=True,
    )
    return sorted_rows[:limit]


def _player_aggregate_key(row: dict[str, Any]) -> str:
    return f"{str(row.get('player_id') or '')}|{str(row.get('agent_id') or '')}"


def _enrich_aggregate_rows_with_meta(
    aggregate_rows: list[dict[str, Any]],
    run_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in aggregate_rows:
        payload = dict(row)
        payload["model_name"] = run_meta.get("model_name")
        payload["hero_context_preview"] = run_meta.get("hero_context_preview")
        payload["status"] = run_meta.get("status")
        payload["aggregate_key"] = str(payload.get("aggregate_key") or _player_aggregate_key(payload))
        enriched.append(payload)
    return enriched


def _build_run_player_aggregates(player_rows: list[dict[str, Any]], run_meta: dict[str, Any]) -> list[dict[str, Any]]:
    if not player_rows:
        return []
    frame = pd.DataFrame(player_rows)
    if frame.empty:
        return []
    frame = _coerce_string_columns(
        frame,
        [
            "simulation_run_id",
            "user_id",
            "decision_backend",
            "hero_context_hash",
            "player_id",
            "agent_id",
            "persona_name",
            "backend_type",
            "showdown_hand_category",
        ],
    )
    frame["result_bb"] = pd.to_numeric(frame.get("result_bb"), errors="coerce").fillna(0.0)
    frame["showdown_hand_score"] = pd.to_numeric(frame.get("showdown_hand_score"), errors="coerce")
    if "is_hero_player" not in frame.columns:
        frame["is_hero_player"] = False
    base_group_cols = [
        column
        for column in [
            "simulation_run_id",
            "user_id",
            "decision_backend",
            "hero_context_hash",
            "is_hero_player",
            "player_id",
            "agent_id",
            "persona_name",
            "backend_type",
            "seat",
        ]
        if column in frame.columns
    ]
    if not base_group_cols:
        return []
    high_hand_lookup = (
        frame.sort_values("showdown_hand_score", na_position="first")
        .drop_duplicates(base_group_cols, keep="last")[
            base_group_cols
            + [column for column in ["showdown_hand_category", "showdown_hand_score"] if column in frame.columns]
        ]
    )
    grouped = (
        frame.groupby(base_group_cols, dropna=False)
        .agg(
            hands_played=("hand_id", "nunique"),
            total_bb_won=("result_bb", "sum"),
            showdown_hand_score=("showdown_hand_score", "max"),
        )
        .reset_index()
    )
    grouped = grouped.merge(high_hand_lookup, on=base_group_cols + ["showdown_hand_score"], how="left")
    grouped["bb_per_100"] = grouped.apply(
        lambda row: 100.0 * float(row["total_bb_won"]) / max(1, int(row["hands_played"])),
        axis=1,
    )
    aggregate_rows = grouped.to_dict(orient="records")
    for row in aggregate_rows:
        row["aggregate_key"] = _player_aggregate_key(row)
    return _enrich_aggregate_rows_with_meta(aggregate_rows, run_meta)


def _build_run_leaderboards_from_aggregate_rows(
    aggregate_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if not aggregate_rows:
        return [], [], [], []
    profit_rows = sorted(
        (dict(row) for row in aggregate_rows),
        key=lambda item: (float(_metric_lookup(item, "total_bb_won") or 0.0), float(_metric_lookup(item, "bb_per_100") or 0.0)),
        reverse=True,
    )
    bb_rows = sorted(
        (dict(row) for row in aggregate_rows),
        key=lambda item: (float(_metric_lookup(item, "bb_per_100") or 0.0), float(_metric_lookup(item, "total_bb_won") or 0.0)),
        reverse=True,
    )
    high_hand_rows = sorted(
        (
            dict(row)
            for row in aggregate_rows
            if _metric_lookup(row, "showdown_hand_score") is not None
        ),
        key=lambda item: (float(_metric_lookup(item, "showdown_hand_score") or 0.0), float(_metric_lookup(item, "total_bb_won") or 0.0)),
        reverse=True,
    )
    hero_rows = sorted(
        (dict(row) for row in aggregate_rows if _boolish(row.get("is_hero_player"))),
        key=lambda item: (float(_metric_lookup(item, "bb_per_100") or 0.0), float(_metric_lookup(item, "total_bb_won") or 0.0)),
        reverse=True,
    )
    return profit_rows, bb_rows, high_hand_rows, hero_rows


def _build_run_leaderboards(
    player_rows: list[dict[str, Any]],
    run_meta: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return _build_run_leaderboards_from_aggregate_rows(_build_run_player_aggregates(player_rows, run_meta))


def _global_leaderboard_rows_from_run_leaderboards(
    live_store: RedisLiveStore,
    active_run_ids: list[str],
    *,
    topn_per_run: int = LIVE_LEADERBOARD_MAX_ROWS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    global_profit_rows: list[dict[str, Any]] = []
    global_high_hand_rows: list[dict[str, Any]] = []
    global_hero_rows: list[dict[str, Any]] = []
    for run_id in active_run_ids:
        global_profit_rows.extend(live_store.get_run_leaderboard(run_id, "profit", topn=topn_per_run))
        global_high_hand_rows.extend(live_store.get_run_leaderboard(run_id, "high_hand", topn=topn_per_run))
        global_hero_rows.extend(
            row
            for row in live_store.get_run_leaderboard(run_id, "bb_per_100", topn=topn_per_run)
            if _boolish(row.get("is_hero_player"))
        )
    return (
        _sort_leaderboard_rows(global_profit_rows, "total_bb_won"),
        _sort_leaderboard_rows(global_high_hand_rows, "showdown_hand_score"),
        _sort_leaderboard_rows(global_hero_rows, "bb_per_100"),
    )


def _merge_agent_rows(
    metrics: pd.DataFrame,
    assignments: pd.DataFrame,
    run_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    if metrics.empty and assignments.empty:
        return []
    metrics = metrics.copy() if not metrics.empty else pd.DataFrame()
    assignments = assignments.copy() if not assignments.empty else pd.DataFrame()
    assignment_lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    if not assignments.empty:
        for row in assignments.to_dict(orient="records"):
            key = (
                str(row.get("simulation_run_id") or ""),
                str(row.get("agent_id") or ""),
                str(row.get("player_id") or ""),
            )
            assignment_lookup[key] = row
    merged: list[dict[str, Any]] = []
    if not metrics.empty:
        for row in metrics.to_dict(orient="records"):
            key = (
                str(row.get("simulation_run_id") or ""),
                str(row.get("agent_id") or ""),
                str(row.get("player_id") or ""),
            )
            payload = dict(row)
            payload.update(assignment_lookup.get(key, {}))
            payload["model_name"] = run_meta.get("model_name")
            payload["hero_context_preview"] = run_meta.get("hero_context_preview")
            payload["status"] = run_meta.get("status")
            merged.append(payload)
    return merged


def _action_payloads_to_frame(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    if not payloads:
        return pd.DataFrame()
    return pd.DataFrame([raw_action_row_from_event(payload) for payload in payloads])


def _summary_payloads_to_hand_frame(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    if not payloads:
        return pd.DataFrame()
    return pd.DataFrame([raw_hand_row_from_summary_event(payload) for payload in payloads])


def _summary_payloads_to_player_frame(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        starting = _mapping_float(payload.get("starting_stacks_bb"))
        ending = _mapping_float(payload.get("final_stacks_bb"))
        collections = _mapping_float(payload.get("collections_bb"))
        personas = _mapping_str(payload.get("player_personas"))
        agent_ids = _mapping_str(payload.get("player_agent_ids") or payload.get("agent_ids"))
        seats = _mapping_int(payload.get("player_seats") or payload.get("seats"))
        backend_types = _mapping_str(payload.get("backend_types"))
        hole_cards_lookup = _mapping_cards(payload.get("player_hole_cards"))
        showdown_player_ids = set(_json_list(payload.get("showdown_player_ids")))
        board_cards = _json_list(payload.get("board_cards"))

        player_ids = sorted(
            set(starting)
            | set(ending)
            | set(collections)
            | set(personas)
            | set(agent_ids)
            | set(seats)
            | set(backend_types)
            | set(hole_cards_lookup)
        )
        for player_id in player_ids:
            stack_start_bb = starting.get(player_id)
            stack_end_bb = ending.get(player_id)
            collection_bb = collections.get(player_id)
            result_bb = None
            if stack_start_bb is not None and stack_end_bb is not None:
                result_bb = round(float(stack_end_bb) - float(stack_start_bb), 4)
            elif collection_bb is not None:
                result_bb = float(collection_bb)
            hole_cards = hole_cards_lookup.get(player_id, [])
            made_showdown = player_id in showdown_player_ids
            rows.append(
                {
                    "source_run_id": payload.get("source_run_id"),
                    "source_type": payload.get("source_type"),
                    "source_dataset": payload.get("source_dataset"),
                    "simulation_run_id": payload.get("simulation_run_id"),
                    "user_id": payload.get("user_id"),
                    "decision_backend": payload.get("decision_backend"),
                    "hero_context_hash": payload.get("hero_context_hash"),
                    "hero_seat": payload.get("hero_seat"),
                    "is_hero_player": int(payload.get("hero_seat") or 0) == int(seats.get(player_id) or 0),
                    "table_id": payload.get("table_id"),
                    "hand_id": payload.get("hand_id"),
                    "player_id": player_id,
                    "agent_id": agent_ids.get(player_id),
                    "seat": seats.get(player_id),
                    "player_name": player_id,
                    "stack_start_bb": stack_start_bb,
                    "stack_end_bb": stack_end_bb,
                    "hole_cards": hole_cards,
                    "result_bb": result_bb,
                    "backend_type": backend_types.get(player_id),
                    "persona_name": personas.get(player_id),
                    "persona_text": None,
                    "made_showdown": made_showdown,
                    "showdown_hand_category": _showdown_hand_category_from_cards(board_cards, hole_cards, made_showdown),
                    "showdown_hand_score": _showdown_hand_score_from_cards(board_cards, hole_cards, made_showdown),
                    "payload_version": payload.get("payload_version", "1"),
                    "raw_lineage": {
                        "collection_bb": collection_bb,
                        "backend_type": backend_types.get(player_id),
                        "winner_player_ids": payload.get("winner_player_ids") or [],
                    },
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _simulation_event_payloads_to_frame(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    if not payloads:
        return pd.DataFrame(
            columns=[
                "event_type",
                "simulation_run_id",
                "table_id",
                "status",
                "event_ts",
                "requested_at",
                "user_id",
                "decision_backend",
                "hero_context_hash",
                "hero_context_preview",
                "backend_type",
                "model_name",
                "hand_count",
                "published_actions",
                "published_hand_summaries",
                "error_message",
            ]
        )
    frame = pd.DataFrame(payloads)
    if "event_ts" in frame.columns:
        frame["event_ts"] = pd.to_datetime(frame["event_ts"], utc=True, errors="coerce").dt.tz_localize(None)
    else:
        frame["event_ts"] = pd.NaT
    if "requested_at" in frame.columns:
        frame["requested_at"] = pd.to_datetime(frame["requested_at"], utc=True, errors="coerce").dt.tz_localize(None)
    return frame


def _replace_rows(warehouse, table: str, frame: pd.DataFrame, key_columns: list[str]) -> None:
    if frame.empty:
        return
    usable_keys = [column for column in key_columns if column in frame.columns]
    deduped = (
        frame.drop_duplicates(usable_keys, keep="last").reset_index(drop=True)
        if usable_keys
        else frame.reset_index(drop=True).copy()
    )
    if usable_keys and hasattr(warehouse, "upsert_dataframe"):
        warehouse.upsert_dataframe(table, deduped, key_columns=usable_keys, force_bulk=True)
        return
    if usable_keys:
        warehouse.delete_matching_keys(table, deduped, usable_keys)
    warehouse.write_dataframe(table, deduped, force_bulk=True)


def _load_active_centroids(warehouse) -> pd.DataFrame:
    cache_ttl = _stream_active_centroids_cache_ttl_seconds()
    cached = _ACTIVE_CENTROIDS_CACHE.get("centroids")
    loaded_at = float(_ACTIVE_CENTROIDS_CACHE.get("loaded_at") or 0.0)
    if cache_ttl > 0 and isinstance(cached, pd.DataFrame) and (time.monotonic() - loaded_at) < cache_ttl:
        return cached

    runs = warehouse.load_table(
        "PROFILE_MODEL_RUNS",
        filters_eq={"active": True},
        order_by=[("activated_at", False), ("created_at", False)],
        limit=1,
    )
    if not runs.empty and "model_run_id" in runs.columns:
        centroids = warehouse.load_table(
            "PROFILE_CLUSTER_CENTROIDS",
            filters_eq={"model_run_id": str(runs.iloc[0]["model_run_id"])},
            order_by=[("cluster_id", True)],
        )
        _ACTIVE_CENTROIDS_CACHE["loaded_at"] = time.monotonic()
        _ACTIVE_CENTROIDS_CACHE["centroids"] = centroids
        return centroids

    centroids = warehouse.load_table(
        "PROFILE_CLUSTER_CENTROIDS",
        order_by=[("updated_at", False), ("cluster_id", True)],
    )
    if centroids.empty or "model_run_id" not in centroids.columns:
        _ACTIVE_CENTROIDS_CACHE["loaded_at"] = time.monotonic()
        _ACTIVE_CENTROIDS_CACHE["centroids"] = centroids
        return centroids

    latest_model_run_id = str(centroids.iloc[0]["model_run_id"])
    filtered = centroids[centroids["model_run_id"].astype(str) == latest_model_run_id].reset_index(drop=True)
    _ACTIVE_CENTROIDS_CACHE["loaded_at"] = time.monotonic()
    _ACTIVE_CENTROIDS_CACHE["centroids"] = filtered
    return filtered


def _normalized_centroid_rows(centroids: pd.DataFrame) -> list[dict[str, Any]]:
    if centroids.empty:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in centroids.iterrows():
        metric_payload = _json_object(row.get("centroid_json"))
        rows.append(
            {
                "cluster_id": row.get("cluster_id"),
                "cluster_label": row.get("cluster_label"),
                "vpip": _metric_lookup(metric_payload, "vpip_rate", "vpip") or 0.0,
                "pfr": _metric_lookup(metric_payload, "pfr_rate", "pfr") or 0.0,
                "aggression_frequency": _metric_lookup(metric_payload, "aggression_frequency") or 0.0,
                "cbet_rate": _metric_lookup(metric_payload, "flop_cbet_rate", "cbet_rate") or 0.0,
                "all_in_rate": _metric_lookup(metric_payload, "all_in_rate") or 0.0,
            }
        )
    return rows


def _spark_imports():
    try:
        from pyspark.sql import SparkSession, Window
        from pyspark.sql import functions as F
        from pyspark.sql import types as T
    except ImportError as exc:  # pragma: no cover - optional path
        raise RuntimeError("pyspark is required for the live metrics stream") from exc
    return SparkSession, Window, F, T


def _spark_schemas():
    _SparkSession, _Window, _F, T = _spark_imports()
    action_schema = T.StructType(
        [
            T.StructField("payload_version", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("source_type", T.StringType(), True),
            T.StructField("source_dataset", T.StringType(), True),
            T.StructField("source_run_id", T.StringType(), True),
            T.StructField("simulation_run_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("decision_backend", T.StringType(), True),
            T.StructField("hero_context_hash", T.StringType(), True),
            T.StructField("hero_seat", T.IntegerType(), True),
            T.StructField("is_hero_player", T.BooleanType(), True),
            T.StructField("table_id", T.StringType(), True),
            T.StructField("hand_id", T.StringType(), True),
            T.StructField("action_index", T.IntegerType(), True),
            T.StructField("street", T.StringType(), True),
            T.StructField("player_id", T.StringType(), True),
            T.StructField("agent_id", T.StringType(), True),
            T.StructField("seat", T.IntegerType(), True),
            T.StructField("position", T.StringType(), True),
            T.StructField("action_type", T.StringType(), True),
            T.StructField("amount_bb", T.DoubleType(), True),
            T.StructField("pot_before_bb", T.DoubleType(), True),
            T.StructField("pot_after_bb", T.DoubleType(), True),
            T.StructField("to_call_bb", T.DoubleType(), True),
            T.StructField("effective_stack_bb", T.DoubleType(), True),
            T.StructField("players_remaining", T.IntegerType(), True),
            T.StructField("board_cards_visible", T.ArrayType(T.StringType()), True),
            T.StructField("hole_cards_visible", T.ArrayType(T.StringType()), True),
            T.StructField("is_all_in", T.BooleanType(), True),
            T.StructField("event_ts", T.StringType(), True),
            T.StructField("backend_type", T.StringType(), True),
            T.StructField("persona_name", T.StringType(), True),
            T.StructField("persona_text", T.StringType(), True),
            T.StructField("raw_lineage", T.MapType(T.StringType(), T.StringType()), True),
        ]
    )
    hand_schema = T.StructType(
        [
            T.StructField("payload_version", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("source_type", T.StringType(), True),
            T.StructField("source_dataset", T.StringType(), True),
            T.StructField("source_run_id", T.StringType(), True),
            T.StructField("simulation_run_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("decision_backend", T.StringType(), True),
            T.StructField("hero_context_hash", T.StringType(), True),
            T.StructField("hero_seat", T.IntegerType(), True),
            T.StructField("table_id", T.StringType(), True),
            T.StructField("hand_id", T.StringType(), True),
            T.StructField("button_seat", T.IntegerType(), True),
            T.StructField("small_blind_bb", T.DoubleType(), True),
            T.StructField("big_blind_bb", T.DoubleType(), True),
            T.StructField("board_cards", T.ArrayType(T.StringType()), True),
            T.StructField("started_at", T.StringType(), True),
            T.StructField("finished_at", T.StringType(), True),
            T.StructField("winner_player_ids", T.ArrayType(T.StringType()), True),
            T.StructField("total_pot_bb", T.DoubleType(), True),
            T.StructField("rake_bb", T.DoubleType(), True),
            T.StructField("starting_stacks_bb", T.MapType(T.StringType(), T.DoubleType()), True),
            T.StructField("final_stacks_bb", T.MapType(T.StringType(), T.DoubleType()), True),
            T.StructField("collections_bb", T.MapType(T.StringType(), T.DoubleType()), True),
            T.StructField("player_personas", T.MapType(T.StringType(), T.StringType()), True),
            T.StructField("agent_ids", T.MapType(T.StringType(), T.StringType()), True),
            T.StructField("player_agent_ids", T.MapType(T.StringType(), T.StringType()), True),
            T.StructField("seats", T.MapType(T.StringType(), T.IntegerType()), True),
            T.StructField("player_seats", T.MapType(T.StringType(), T.IntegerType()), True),
            T.StructField("showdown_player_ids", T.ArrayType(T.StringType()), True),
            T.StructField("player_hole_cards", T.MapType(T.StringType(), T.ArrayType(T.StringType())), True),
            T.StructField("backend_types", T.MapType(T.StringType(), T.StringType()), True),
            T.StructField("backend_type", T.StringType(), True),
            T.StructField("raw_lineage", T.MapType(T.StringType(), T.StringType()), True),
        ]
    )
    simulation_event_schema = T.StructType(
        [
            T.StructField("event_type", T.StringType(), True),
            T.StructField("simulation_run_id", T.StringType(), True),
            T.StructField("table_id", T.StringType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("event_ts", T.StringType(), True),
            T.StructField("requested_at", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("decision_backend", T.StringType(), True),
            T.StructField("hero_context_hash", T.StringType(), True),
            T.StructField("hero_context_preview", T.StringType(), True),
            T.StructField("backend_type", T.StringType(), True),
            T.StructField("model_name", T.StringType(), True),
            T.StructField("request_mode", T.StringType(), True),
            T.StructField("hand_count", T.IntegerType(), True),
            T.StructField("seed", T.IntegerType(), True),
            T.StructField("hero_seat", T.IntegerType(), True),
            T.StructField("published_actions", T.IntegerType(), True),
            T.StructField("published_hand_summaries", T.IntegerType(), True),
            T.StructField("error_message", T.StringType(), True),
        ]
    )
    return action_schema, hand_schema, simulation_event_schema


def _spark_metric_schema():
    _SparkSession, _Window, _F, T = _spark_imports()
    return T.StructType(
        [
            T.StructField("metric_window_start", T.TimestampType(), True),
            T.StructField("metric_window_end", T.TimestampType(), True),
            T.StructField("simulation_run_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("decision_backend", T.StringType(), True),
            T.StructField("hero_context_hash", T.StringType(), True),
            T.StructField("is_hero_player", T.BooleanType(), True),
            T.StructField("agent_id", T.StringType(), True),
            T.StructField("player_id", T.StringType(), True),
            T.StructField("persona_name", T.StringType(), True),
            T.StructField("actions_per_second", T.DoubleType(), True),
            T.StructField("hands_per_second", T.DoubleType(), True),
            T.StructField("vpip", T.DoubleType(), True),
            T.StructField("pfr", T.DoubleType(), True),
            T.StructField("aggression_frequency", T.DoubleType(), True),
            T.StructField("cbet_rate", T.DoubleType(), True),
            T.StructField("all_in_rate", T.DoubleType(), True),
            T.StructField("bb_won", T.DoubleType(), True),
            T.StructField("observed_hands", T.LongType(), True),
            T.StructField("observed_actions", T.LongType(), True),
            T.StructField("updated_at", T.TimestampType(), True),
        ]
    )


def _spark_assignment_schema():
    _SparkSession, _Window, _F, T = _spark_imports()
    return T.StructType(
        [
            T.StructField("simulation_run_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("decision_backend", T.StringType(), True),
            T.StructField("hero_context_hash", T.StringType(), True),
            T.StructField("is_hero_player", T.BooleanType(), True),
            T.StructField("agent_id", T.StringType(), True),
            T.StructField("player_id", T.StringType(), True),
            T.StructField("nearest_cluster_id", T.IntegerType(), True),
            T.StructField("nearest_cluster_label", T.StringType(), True),
            T.StructField("distance_to_centroid", T.DoubleType(), True),
            T.StructField("assigned_at", T.TimestampType(), True),
        ]
    )


def _spark_player_schema():
    _SparkSession, _Window, _F, T = _spark_imports()
    return T.StructType(
        [
            T.StructField("source_run_id", T.StringType(), True),
            T.StructField("source_type", T.StringType(), True),
            T.StructField("source_dataset", T.StringType(), True),
            T.StructField("simulation_run_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("decision_backend", T.StringType(), True),
            T.StructField("hero_context_hash", T.StringType(), True),
            T.StructField("hero_seat", T.IntegerType(), True),
            T.StructField("is_hero_player", T.BooleanType(), True),
            T.StructField("table_id", T.StringType(), True),
            T.StructField("hand_id", T.StringType(), True),
            T.StructField("player_id", T.StringType(), True),
            T.StructField("agent_id", T.StringType(), True),
            T.StructField("seat", T.IntegerType(), True),
            T.StructField("player_name", T.StringType(), True),
            T.StructField("stack_start_bb", T.DoubleType(), True),
            T.StructField("stack_end_bb", T.DoubleType(), True),
            T.StructField("hole_cards", T.ArrayType(T.StringType()), True),
            T.StructField("result_bb", T.DoubleType(), True),
            T.StructField("backend_type", T.StringType(), True),
            T.StructField("persona_name", T.StringType(), True),
            T.StructField("persona_text", T.StringType(), True),
            T.StructField("made_showdown", T.BooleanType(), True),
            T.StructField("showdown_hand_category", T.StringType(), True),
            T.StructField("showdown_hand_score", T.DoubleType(), True),
            T.StructField("payload_version", T.StringType(), True),
            T.StructField(
                "raw_lineage",
                T.StructType(
                    [
                        T.StructField("collection_bb", T.DoubleType(), True),
                        T.StructField("backend_type", T.StringType(), True),
                        T.StructField("winner_player_ids", T.ArrayType(T.StringType()), True),
                    ]
                ),
                True,
            ),
        ]
    )


def _spark_player_rows_frame(spark, rows: list[dict[str, Any]]):
    schema = _spark_player_schema()
    if not rows:
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(rows, schema=schema)


def _spark_frame_has_rows(frame) -> bool:
    is_empty = getattr(frame, "isEmpty", None)
    if callable(is_empty):
        return not bool(is_empty())
    return bool(frame.take(1))


def _spark_collect_records(frame) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in frame.toLocalIterator()]


def _spark_collect_run_ids(frame) -> list[str]:
    _SparkSession, _Window, F, _T = _spark_imports()
    if not _spark_frame_has_rows(frame) or "simulation_run_id" not in frame.columns:
        return []
    rows = frame.select("simulation_run_id").where(F.col("simulation_run_id").isNotNull()).distinct().collect()
    return sorted({str(row["simulation_run_id"] or "").strip() for row in rows if str(row["simulation_run_id"] or "").strip()})


def _spark_collect_terminal_run_ids(frame) -> list[str]:
    _SparkSession, _Window, F, _T = _spark_imports()
    if not _spark_frame_has_rows(frame) or "simulation_run_id" not in frame.columns:
        return []
    rows = frame.select("simulation_run_id", "event_type", "status").collect()
    terminal_run_ids: set[str] = set()
    for row in rows:
        run_id = str(row["simulation_run_id"] or "").strip()
        if not run_id:
            continue
        if _status_from_event(row["event_type"], row["status"]) in EXECUTION_FINISHED_STATUSES:
            terminal_run_ids.add(run_id)
    return sorted(terminal_run_ids)


def _spark_row_count(frame) -> int:
    return int(frame.count()) if _spark_frame_has_rows(frame) else 0


def _spark_stream_rows(frame, *, min_partitions: int = 32):
    repartition_count = max(int(min_partitions), int(frame.rdd.getNumPartitions()))
    writable = frame.repartition(repartition_count) if repartition_count > 1 else frame
    return writable.toLocalIterator(prefetchPartitions=False)


def _spark_parse_topic_frame(batch_df, *, topic_name: str, schema):
    _SparkSession, _Window, F, _T = _spark_imports()
    return (
        batch_df.filter(F.col("topic") == topic_name)
        .select(F.from_json(F.col("raw_value"), schema).alias("payload"))
        .filter(F.col("payload").isNotNull())
        .select("payload.*")
    )


def _spark_action_output_frame(action_events_df):
    _SparkSession, _Window, F, _T = _spark_imports()
    return action_events_df.select(
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
        F.coalesce(F.col("is_hero_player"), F.lit(False)).alias("is_hero_player"),
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
        F.coalesce(F.col("is_all_in"), F.lit(False)).alias("is_all_in"),
        F.to_timestamp("event_ts").alias("event_ts"),
        "backend_type",
        "persona_name",
        "persona_text",
        "payload_version",
        "raw_lineage",
    )


def _spark_hand_output_frame(hand_events_df):
    _SparkSession, _Window, F, _T = _spark_imports()
    return hand_events_df.select(
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
        F.to_timestamp("started_at").alias("started_at"),
        F.to_timestamp("finished_at").alias("finished_at"),
        "payload_version",
        "raw_lineage",
    )


def _spark_player_output_frame(spark, hand_events_df):
    _SparkSession, _Window, F, T = _spark_imports()
    if not _spark_frame_has_rows(hand_events_df):
        return spark.createDataFrame([], schema=_spark_player_schema())

    showdown_category_udf = F.udf(_showdown_hand_category_from_cards, T.StringType())
    showdown_score_udf = F.udf(_showdown_hand_score_from_cards, T.DoubleType())
    empty_array = F.expr("cast(array() as array<string>)")

    frame = (
        hand_events_df.select(
            "*",
            F.coalesce(F.col("player_agent_ids"), F.col("agent_ids")).alias("_agent_ids_map"),
            F.coalesce(F.col("player_seats"), F.col("seats")).alias("_seat_map"),
        )
        .withColumn(
            "_player_ids",
            F.array_distinct(
                F.flatten(
                    F.array(
                        F.coalesce(F.map_keys("starting_stacks_bb"), empty_array),
                        F.coalesce(F.map_keys("final_stacks_bb"), empty_array),
                        F.coalesce(F.map_keys("collections_bb"), empty_array),
                        F.coalesce(F.map_keys("player_personas"), empty_array),
                        F.coalesce(F.map_keys("_agent_ids_map"), empty_array),
                        F.coalesce(F.map_keys("_seat_map"), empty_array),
                        F.coalesce(F.map_keys("backend_types"), empty_array),
                        F.coalesce(F.map_keys("player_hole_cards"), empty_array),
                    )
                )
            ),
        )
        .withColumn("player_id", F.explode_outer("_player_ids"))
        .where(F.col("player_id").isNotNull())
    )

    stack_start = F.element_at(F.col("starting_stacks_bb"), F.col("player_id"))
    stack_end = F.element_at(F.col("final_stacks_bb"), F.col("player_id"))
    collection_bb = F.element_at(F.col("collections_bb"), F.col("player_id"))
    seat = F.element_at(F.col("_seat_map"), F.col("player_id"))
    hole_cards = F.element_at(F.col("player_hole_cards"), F.col("player_id"))
    made_showdown = F.array_contains(F.coalesce(F.col("showdown_player_ids"), empty_array), F.col("player_id"))

    return frame.select(
        "source_run_id",
        "source_type",
        "source_dataset",
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_seat",
        F.when(F.col("hero_seat").isNull() | seat.isNull(), F.lit(False))
        .otherwise(F.col("hero_seat").cast("int") == seat.cast("int"))
        .alias("is_hero_player"),
        "table_id",
        "hand_id",
        "player_id",
        F.element_at(F.col("_agent_ids_map"), F.col("player_id")).alias("agent_id"),
        seat.alias("seat"),
        F.col("player_id").alias("player_name"),
        stack_start.alias("stack_start_bb"),
        stack_end.alias("stack_end_bb"),
        hole_cards.alias("hole_cards"),
        F.when(stack_start.isNotNull() & stack_end.isNotNull(), F.round(stack_end - stack_start, 4))
        .otherwise(collection_bb)
        .alias("result_bb"),
        F.coalesce(F.element_at(F.col("backend_types"), F.col("player_id")), F.col("backend_type")).alias("backend_type"),
        F.element_at(F.col("player_personas"), F.col("player_id")).alias("persona_name"),
        F.lit(None).cast("string").alias("persona_text"),
        made_showdown.alias("made_showdown"),
        showdown_category_udf(F.col("board_cards"), hole_cards, made_showdown).alias("showdown_hand_category"),
        showdown_score_udf(F.col("board_cards"), hole_cards, made_showdown).alias("showdown_hand_score"),
        "payload_version",
        F.struct(
            collection_bb.alias("collection_bb"),
            F.coalesce(F.element_at(F.col("backend_types"), F.col("player_id")), F.col("backend_type")).alias("backend_type"),
            F.col("winner_player_ids").alias("winner_player_ids"),
        ).alias("raw_lineage"),
    )


def _spark_simulation_event_output_frame(simulation_events_df):
    _SparkSession, _Window, F, _T = _spark_imports()
    return simulation_events_df.select(
        "event_type",
        "simulation_run_id",
        "table_id",
        "status",
        F.to_timestamp("event_ts").alias("event_ts"),
        F.to_timestamp("requested_at").alias("requested_at"),
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "hero_context_preview",
        "backend_type",
        "model_name",
        "request_mode",
        "hand_count",
        "seed",
        "hero_seat",
        "published_actions",
        "published_hand_summaries",
        "error_message",
    )


def _spark_compute_live_metrics(spark, actions_df, player_results_df, *, window_minutes: int):
    _SparkSession, Window, F, T = _spark_imports()
    if not _spark_frame_has_rows(actions_df):
        return spark.createDataFrame([], schema=_spark_metric_schema())

    frame = (
        actions_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("is_hero_player", F.coalesce(F.col("is_hero_player"), F.lit(False)))
        .where(F.col("event_ts").isNotNull())
    )
    if not _spark_frame_has_rows(frame):
        return spark.createDataFrame([], schema=_spark_metric_schema())

    window_end = frame.agg(F.max("event_ts").alias("window_end")).collect()[0]["window_end"]
    if window_end is None:
        return spark.createDataFrame([], schema=_spark_metric_schema())
    window_start = window_end - timedelta(minutes=window_minutes)
    frame = frame.where((F.col("event_ts") >= F.lit(window_start)) & (F.col("event_ts") <= F.lit(window_end)))
    if not _spark_frame_has_rows(frame):
        return spark.createDataFrame([], schema=_spark_metric_schema())

    group_cols = [
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "is_hero_player",
        "agent_id",
        "player_id",
        "persona_name",
    ]
    prepared = (
        frame.withColumn("is_preflop", F.col("street") == F.lit("preflop"))
        .withColumn("is_flop", F.col("street") == F.lit("flop"))
        .withColumn("is_aggressive", F.col("action_type").isin(*sorted(AGGRESSIVE_ACTIONS)))
        .withColumn("is_passive", F.col("action_type").isin(*sorted(PASSIVE_ACTIONS)))
        .withColumn("is_vpip_action", (F.col("street") == F.lit("preflop")) & F.col("action_type").isin(*sorted(VOLUNTARY_PREFLOP_ACTIONS)))
        .withColumn("is_pfr_action", (F.col("street") == F.lit("preflop")) & F.col("action_type").isin(*sorted(AGGRESSIVE_ACTIONS)))
    )

    hand_features = prepared.groupBy(*(group_cols + ["hand_id"])).agg(
        F.max(F.col("is_vpip_action").cast("int")).alias("vpip"),
        F.max(F.col("is_pfr_action").cast("int")).alias("pfr"),
        F.sum(F.col("is_aggressive").cast("int")).alias("aggression_actions"),
        F.sum(F.col("is_passive").cast("int")).alias("passive_actions"),
        F.max(F.col("is_all_in").cast("int")).alias("all_in"),
        F.max(F.when(F.col("street").isin("flop", "turn", "river", "showdown"), F.lit(1)).otherwise(F.lit(0))).alias("saw_flop"),
    )
    first_flop_window = Window.partitionBy("hand_id").orderBy(F.col("action_index").asc())
    first_flop_aggressor = (
        prepared.where(F.col("is_flop") & F.col("is_aggressive"))
        .withColumn("_rn", F.row_number().over(first_flop_window))
        .where(F.col("_rn") == 1)
        .select("hand_id", F.col("player_id").alias("flop_aggressor_player_id"))
    )
    hand_features = (
        hand_features.join(first_flop_aggressor, on="hand_id", how="left")
        .withColumn(
            "is_cbet",
            (F.col("pfr") > 0) & (F.col("saw_flop") > 0) & (F.col("player_id") == F.col("flop_aggressor_player_id")),
        )
        .drop("flop_aggressor_player_id")
    )
    grouped = hand_features.groupBy(*group_cols).agg(
        F.countDistinct("hand_id").alias("observed_hands"),
        F.avg("vpip").alias("vpip"),
        F.avg("pfr").alias("pfr"),
        F.avg(F.col("is_cbet").cast("double")).alias("cbet_rate"),
        F.avg("all_in").alias("all_in_rate"),
        F.sum("aggression_actions").alias("aggression_actions"),
        F.sum("passive_actions").alias("passive_actions"),
    )
    action_counts = prepared.groupBy(*group_cols).count().withColumnRenamed("count", "observed_actions")
    grouped = grouped.join(action_counts, on=group_cols, how="left")
    grouped = grouped.withColumn(
        "aggression_frequency",
        F.when(
            (F.col("aggression_actions") + F.col("passive_actions")) > 0,
            F.col("aggression_actions") / (F.col("aggression_actions") + F.col("passive_actions")),
        ).otherwise(F.lit(0.0)),
    )

    if _spark_frame_has_rows(player_results_df):
        results = player_results_df.select(
            *[column for column in group_cols if column in player_results_df.columns],
            F.col("result_bb").cast("double").alias("result_bb"),
        )
        bb_won = results.groupBy(*group_cols).agg(F.sum("result_bb").alias("bb_won"))
        grouped = grouped.join(bb_won, on=group_cols, how="left")
    else:
        grouped = grouped.withColumn("bb_won", F.lit(None).cast("double"))

    window_seconds = float(max(window_minutes * 60, 1))
    return (
        grouped.withColumn("actions_per_second", F.col("observed_actions") / F.lit(window_seconds))
        .withColumn("hands_per_second", F.col("observed_hands") / F.lit(window_seconds))
        .withColumn("metric_window_start", F.lit(window_start))
        .withColumn("metric_window_end", F.lit(window_end))
        .withColumn("updated_at", F.current_timestamp())
        .select(*_spark_metric_schema().fieldNames())
    )


def _spark_assign_live_profiles(spark, live_metrics_df, centroids: pd.DataFrame):
    _SparkSession, Window, F, T = _spark_imports()
    if not _spark_frame_has_rows(live_metrics_df):
        return spark.createDataFrame([], schema=_spark_assignment_schema())
    centroid_rows = _normalized_centroid_rows(centroids)
    if not centroid_rows:
        return spark.createDataFrame([], schema=_spark_assignment_schema())

    centroid_df = spark.createDataFrame(
        [
            {
                "nearest_cluster_id": int(row["cluster_id"]),
                "nearest_cluster_label": row["cluster_label"],
                "centroid_vpip": float(row["vpip"]),
                "centroid_pfr": float(row["pfr"]),
                "centroid_aggression_frequency": float(row["aggression_frequency"]),
                "centroid_cbet_rate": float(row["cbet_rate"]),
                "centroid_all_in_rate": float(row["all_in_rate"]),
            }
            for row in centroid_rows
            if row.get("cluster_id") is not None
        ]
    )
    if not _spark_frame_has_rows(centroid_df):
        return spark.createDataFrame([], schema=_spark_assignment_schema())

    distance = (
        F.pow(F.coalesce(F.col("vpip"), F.lit(0.0)) - F.col("centroid_vpip"), 2)
        + F.pow(F.coalesce(F.col("pfr"), F.lit(0.0)) - F.col("centroid_pfr"), 2)
        + F.pow(F.coalesce(F.col("aggression_frequency"), F.lit(0.0)) - F.col("centroid_aggression_frequency"), 2)
        + F.pow(F.coalesce(F.col("cbet_rate"), F.lit(0.0)) - F.col("centroid_cbet_rate"), 2)
        + F.pow(F.coalesce(F.col("all_in_rate"), F.lit(0.0)) - F.col("centroid_all_in_rate"), 2)
    )
    ranked = (
        live_metrics_df.crossJoin(F.broadcast(centroid_df))
        .withColumn("distance_to_centroid", F.sqrt(distance))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("simulation_run_id", "agent_id", "player_id").orderBy(
                    F.col("distance_to_centroid").asc(),
                    F.col("nearest_cluster_id").asc(),
                )
            ),
        )
        .where(F.col("_rn") == 1)
    )
    return ranked.select(
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
        F.current_timestamp().alias("assigned_at"),
    )


def _spark_grouped_player_results(player_df):
    _SparkSession, Window, F, T = _spark_imports()
    prepared = (
        player_df.withColumn("result_bb", F.col("result_bb").cast("double"))
        .withColumn("showdown_hand_score", F.col("showdown_hand_score").cast("double"))
        .withColumn(
            "showdown_hand_score",
            F.when(F.isnan("showdown_hand_score"), F.lit(None).cast("double")).otherwise(F.col("showdown_hand_score")),
        )
        .withColumn("is_hero_player", F.coalesce(F.col("is_hero_player").cast("boolean"), F.lit(False)))
    )
    if not _spark_frame_has_rows(prepared):
        return prepared

    base_group_cols = [
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "is_hero_player",
        "player_id",
        "agent_id",
        "persona_name",
        "backend_type",
        "seat",
    ]
    high_hand_lookup = (
        prepared.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(*base_group_cols).orderBy(F.col("showdown_hand_score").desc_nulls_last())
            ),
        )
        .where(F.col("_rn") == 1)
        .select(
            *base_group_cols,
            F.col("showdown_hand_category").alias("_showdown_hand_category"),
            F.col("showdown_hand_score").alias("_lookup_showdown_hand_score"),
        )
    )
    grouped = prepared.groupBy(*base_group_cols).agg(
        F.countDistinct("hand_id").alias("hands_played"),
        F.sum(F.coalesce(F.col("result_bb"), F.lit(0.0))).alias("total_bb_won"),
        F.max("showdown_hand_score").alias("showdown_hand_score"),
    )
    return (
        grouped.join(high_hand_lookup, on=base_group_cols, how="left")
        .withColumn(
            "showdown_hand_category",
            F.when(F.col("showdown_hand_score") == F.col("_lookup_showdown_hand_score"), F.col("_showdown_hand_category")),
        )
        .withColumn(
            "bb_per_100",
            F.when(F.col("hands_played") > 0, F.lit(100.0) * F.col("total_bb_won") / F.col("hands_played")).otherwise(F.lit(0.0)),
        )
        .drop("_showdown_hand_category", "_lookup_showdown_hand_score")
    )


def _spark_rank_rows(frame, *, order_columns, partition_columns: list[str] | None = None, limit: int = 100):
    _SparkSession, Window, F, T = _spark_imports()
    if partition_columns:
        return (
            frame.withColumn("_rn", F.row_number().over(Window.partitionBy(*partition_columns).orderBy(*order_columns)))
            .where(F.col("_rn") <= int(limit))
            .drop("_rn")
        )
    return frame.orderBy(*order_columns).limit(int(limit))


def _write_spark_frame(warehouse, table: str, frame, key_columns: list[str], *, chunk_size: int | None = None) -> int:
    if not _spark_frame_has_rows(frame):
        return 0
    resolved_chunk_size = int(chunk_size or _stream_snowflake_write_chunk_size())
    usable_keys = [column for column in key_columns if column in frame.columns]
    writable = frame.dropDuplicates(usable_keys) if usable_keys else frame
    total_rows = 0
    batch: list[dict[str, Any]] = []
    for row in _spark_stream_rows(writable):
        batch.append(row.asDict(recursive=True))
        if len(batch) >= resolved_chunk_size:
            chunk_frame = pd.DataFrame(batch)
            _replace_rows(warehouse, table, chunk_frame, key_columns)
            total_rows += len(chunk_frame)
            batch = []
    if batch:
        chunk_frame = pd.DataFrame(batch)
        _replace_rows(warehouse, table, chunk_frame, key_columns)
        total_rows += len(chunk_frame)
    return total_rows


def _replace_dashboard_summaries(warehouse, summary_rows: list[dict[str, Any]]) -> None:
    if not summary_rows:
        return
    summary_frame = pd.DataFrame(summary_rows)
    warehouse.delete_matching_keys("DASHBOARD_SUMMARIES", summary_frame, ["summary_key"])
    warehouse.write_dataframe("DASHBOARD_SUMMARIES", summary_frame, force_bulk=True)


def _apply_simulation_event_rows(live_store: RedisLiveStore, event_rows: list[dict[str, Any]]) -> None:
    for event in event_rows:
        run_id = str(event.get("simulation_run_id") or "")
        if not run_id:
            continue
        status = _status_from_event(event.get("event_type"), event.get("status"))
        event_ts = event.get("event_ts")
        meta = {
            "simulation_run_id": run_id,
            "table_id": event.get("table_id"),
            "user_id": event.get("user_id"),
            "decision_backend": event.get("decision_backend"),
            "hero_context_hash": event.get("hero_context_hash"),
            "hero_context_preview": event.get("hero_context_preview"),
            "model_name": event.get("model_name"),
            "backend_type": event.get("backend_type"),
            "status": status,
            "requested_at": event.get("requested_at") or (event_ts if status == "queued" else None),
            "started_at": event.get("started_at") or (event_ts if status == "running" else None),
            "finished_at": event.get("finished_at") or (event_ts if status in EXECUTION_FINISHED_STATUSES else None),
            "updated_at": event_ts,
            "hand_count": event.get("hand_count"),
            "published_actions": event.get("published_actions"),
            "published_hand_summaries": event.get("published_hand_summaries"),
            "error_message": event.get("error_message"),
        }
        live_store.set_run_meta(meta)
        if status in EXECUTION_FINISHED_STATUSES:
            user_id = event.get("user_id")
            live_store.expire_run(run_id, user_id=None if user_id is None else str(user_id))


def _merge_run_player_aggregate_row(
    existing: dict[str, Any] | None,
    player_row: dict[str, Any],
    run_meta: dict[str, Any],
) -> dict[str, Any]:
    aggregate = dict(existing or {})
    aggregate["aggregate_key"] = str(aggregate.get("aggregate_key") or _player_aggregate_key(player_row))
    for field in [
        "simulation_run_id",
        "user_id",
        "decision_backend",
        "hero_context_hash",
        "is_hero_player",
        "player_id",
        "agent_id",
        "persona_name",
        "backend_type",
        "seat",
    ]:
        aggregate[field] = player_row.get(field)
    hands_played = int(aggregate.get("hands_played") or 0) + 1
    total_bb_won = float(_metric_lookup(aggregate, "total_bb_won") or 0.0) + float(
        _metric_lookup(player_row, "result_bb") or 0.0
    )
    aggregate["hands_played"] = hands_played
    aggregate["total_bb_won"] = round(total_bb_won, 4)
    aggregate["bb_per_100"] = round(100.0 * total_bb_won / max(1, hands_played), 4)

    showdown_score = _metric_lookup(player_row, "showdown_hand_score")
    current_showdown_score = _metric_lookup(aggregate, "showdown_hand_score")
    if showdown_score is not None and (
        current_showdown_score is None or float(showdown_score) > float(current_showdown_score)
    ):
        aggregate["showdown_hand_score"] = float(showdown_score)
        aggregate["showdown_hand_category"] = player_row.get("showdown_hand_category")
    elif "showdown_hand_score" not in aggregate:
        aggregate["showdown_hand_score"] = None
        aggregate["showdown_hand_category"] = None

    aggregate["model_name"] = run_meta.get("model_name")
    aggregate["hero_context_preview"] = run_meta.get("hero_context_preview")
    aggregate["status"] = run_meta.get("status")
    return aggregate


def _refresh_run_player_aggregate_state(
    live_store: RedisLiveStore,
    run_id: str,
    *,
    run_meta: dict[str, Any],
    inserted_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    aggregate_suffix = f"run:{run_id}:state:player_aggregates"
    existing_rows = live_store.list_json_hash(aggregate_suffix)
    if not existing_rows:
        raw_rows = live_store.list_json_hash(f"run:{run_id}:state:player_hands")
        aggregate_rows = _build_run_player_aggregates(raw_rows, run_meta)
        live_store.replace_json_hash(aggregate_suffix, aggregate_rows, key_field="aggregate_key")
        return aggregate_rows

    aggregate_lookup = {
        str(row.get("aggregate_key") or _player_aggregate_key(row)): dict(row)
        for row in existing_rows
    }
    for row in inserted_rows or []:
        key = _player_aggregate_key(row)
        aggregate_lookup[key] = _merge_run_player_aggregate_row(aggregate_lookup.get(key), row, run_meta)
    aggregate_rows = _enrich_aggregate_rows_with_meta(list(aggregate_lookup.values()), run_meta)
    live_store.replace_json_hash(aggregate_suffix, aggregate_rows, key_field="aggregate_key")
    return aggregate_rows


def _sync_live_player_state(
    live_store: RedisLiveStore,
    player_rows: list[dict[str, Any]],
    *,
    run_meta_lookup: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped = _group_records_by_run(player_rows)
    inserted_by_run: dict[str, list[dict[str, Any]]] = {}
    for run_id, rows in grouped.items():
        for row in rows:
            if live_store.insert_json_hash_if_absent(
                f"run:{run_id}:state:player_hands",
                f"{row.get('hand_id')}|{row.get('player_id')}",
                row,
            ):
                inserted_by_run.setdefault(run_id, []).append(row)

    aggregate_rows_by_run: dict[str, list[dict[str, Any]]] = {}
    for run_id, rows in grouped.items():
        aggregate_rows_by_run[run_id] = _refresh_run_player_aggregate_state(
            live_store,
            run_id,
            run_meta=run_meta_lookup.get(run_id, {}),
            inserted_rows=inserted_by_run.get(run_id, []),
        )
    return aggregate_rows_by_run


def _group_records_by_run(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        run_id = str(row.get("simulation_run_id") or "")
        if not run_id:
            continue
        grouped.setdefault(run_id, []).append(row)
    return grouped


def _record_run_ids(rows: list[dict[str, Any]]) -> set[str]:
    return {
        str(row.get("simulation_run_id") or "").strip()
        for row in rows
        if str(row.get("simulation_run_id") or "").strip()
    }


def _replace_run_agents_from_records(live_store: RedisLiveStore, agent_rows: list[dict[str, Any]]) -> None:
    for run_id, rows in _group_records_by_run(agent_rows).items():
        live_store.replace_run_agents(run_id, rows)


def _materialize_spark_dashboard_outputs(
    spark,
    *,
    warehouse,
    live_store: RedisLiveStore,
    live_players_df,
    live_metrics_df,
    assignments_df,
    simulation_event_df,
    touched_run_ids: list[str],
    live_player_records: list[dict[str, Any]] | None = None,
    simulation_event_records: list[dict[str, Any]] | None = None,
    live_metric_records: list[dict[str, Any]] | None = None,
    assignment_records: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    _SparkSession, _Window, F, _T = _spark_imports()
    summary_rows: list[dict[str, Any]] = []

    event_rows = (
        simulation_event_records
        if simulation_event_records is not None
        else (
            _spark_collect_records(simulation_event_df.orderBy(F.col("event_ts").asc_nulls_last(), F.col("simulation_run_id")))
            if _spark_frame_has_rows(simulation_event_df)
            else []
        )
    )
    if event_rows:
        _apply_simulation_event_rows(live_store, event_rows)

    run_meta_lookup = {run_id: live_store.get_run_meta(run_id) for run_id in touched_run_ids}
    aggregate_rows_by_run = (
        _sync_live_player_state(
            live_store,
            live_player_records if live_player_records is not None else _spark_collect_records(live_players_df),
            run_meta_lookup=run_meta_lookup,
        )
        if (
            (live_player_records is not None and bool(live_player_records))
            or (live_player_records is None and _spark_frame_has_rows(live_players_df))
        )
        else {}
    )

    metric_rows = (
        live_metric_records
        if live_metric_records is not None
        else (_spark_collect_records(live_metrics_df) if _spark_frame_has_rows(live_metrics_df) else [])
    )
    assignment_rows = (
        assignment_records
        if assignment_records is not None
        else (_spark_collect_records(assignments_df) if _spark_frame_has_rows(assignments_df) else [])
    )
    if metric_rows or assignment_rows:
        metrics_frame = pd.DataFrame(metric_rows)
        assignments_frame = pd.DataFrame(assignment_rows)
        metric_run_ids = sorted(_record_run_ids(metric_rows) | _record_run_ids(assignment_rows))
        agent_rows: list[dict[str, Any]] = []
        for run_id in metric_run_ids:
            run_metrics = (
                metrics_frame[metrics_frame["simulation_run_id"].astype(str) == run_id]
                if not metrics_frame.empty and "simulation_run_id" in metrics_frame.columns
                else pd.DataFrame()
            )
            run_assignments = (
                assignments_frame[assignments_frame["simulation_run_id"].astype(str) == run_id]
                if not assignments_frame.empty and "simulation_run_id" in assignments_frame.columns
                else pd.DataFrame()
            )
            agent_rows.extend(_merge_agent_rows(run_metrics, run_assignments, run_meta_lookup.get(run_id, {})))
        _replace_run_agents_from_records(live_store, agent_rows)

    for run_id in touched_run_ids:
        run_meta = run_meta_lookup.get(run_id, {})
        aggregate_rows = aggregate_rows_by_run.get(run_id)
        if aggregate_rows is None:
            aggregate_rows = _refresh_run_player_aggregate_state(
                live_store,
                run_id,
                run_meta=run_meta,
                inserted_rows=[],
            )
        profit_rows, bb_rows, high_rows, hero_rows = _build_run_leaderboards_from_aggregate_rows(aggregate_rows)
        profit_rows = profit_rows[:LIVE_LEADERBOARD_MAX_ROWS]
        bb_rows = bb_rows[:LIVE_LEADERBOARD_MAX_ROWS]
        high_rows = high_rows[:LIVE_LEADERBOARD_MAX_ROWS]
        hero_rows = hero_rows[:LIVE_LEADERBOARD_MAX_ROWS]
        live_store.replace_run_leaderboard(run_id, "profit", profit_rows, score_field="total_bb_won")
        live_store.replace_run_leaderboard(run_id, "bb_per_100", bb_rows, score_field="bb_per_100")
        live_store.replace_run_leaderboard(run_id, "high_hand", high_rows, score_field="showdown_hand_score")
        summary_rows.extend(
            [
                _summary_row(
                    summary_key=f"run:{run_id}:leaderboard:profit",
                    summary_scope="run_profit_leaderboard",
                    rows=profit_rows,
                    run_meta=run_meta,
                ),
                _summary_row(
                    summary_key=f"run:{run_id}:leaderboard:bb_per_100",
                    summary_scope="run_bb_per_100_leaderboard",
                    rows=bb_rows,
                    run_meta=run_meta,
                ),
                _summary_row(
                    summary_key=f"run:{run_id}:leaderboard:high_hand",
                    summary_scope="run_high_hand_leaderboard",
                    rows=high_rows,
                    run_meta=run_meta,
                ),
                _summary_row(
                    summary_key=f"run:{run_id}:leaderboard:hero_context",
                    summary_scope="run_hero_context_leaderboard",
                    rows=hero_rows,
                    run_meta=run_meta,
                ),
            ]
        )

    global_profit_rows, global_high_rows, global_hero_rows = _global_leaderboard_rows_from_run_leaderboards(
        live_store,
        live_store.list_active_runs(),
    )
    live_store.replace_global_leaderboard("profit", global_profit_rows, score_field="total_bb_won")
    live_store.replace_global_leaderboard("high_hand", global_high_rows, score_field="showdown_hand_score")
    live_store.replace_global_leaderboard("hero_context", global_hero_rows, score_field="bb_per_100")
    summary_rows.extend(
        [
            _summary_row(
                summary_key="global:leaderboard:profit",
                summary_scope="global_profit_leaderboard",
                rows=global_profit_rows,
                run_meta={},
            ),
            _summary_row(
                summary_key="global:leaderboard:high_hand",
                summary_scope="global_high_hand_leaderboard",
                rows=global_high_rows,
                run_meta={},
            ),
            _summary_row(
                summary_key="global:leaderboard:hero_context",
                summary_scope="global_hero_context_leaderboard",
                rows=global_hero_rows,
                run_meta={},
            ),
        ]
    )
    _replace_dashboard_summaries(warehouse, summary_rows)
    return summary_rows


def run_spark_kafka_stream(
    *,
    brokers: str,
    topic: str,
    summaries_topic: str,
    events_topic: str,
    window_minutes: int,
    starting_offsets: str,
    checkpoint_path: str,
    available_now: bool = False,
) -> None:
    SparkSession, _Window, F, _T = _spark_imports()
    action_schema, hand_schema, simulation_event_schema = _spark_schemas()

    spark = (
        SparkSession.builder.appName("poker_action_live_metrics")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.api.mode", "classic")
        .config("spark.sql.shuffle.partitions", str(_stream_shuffle_partitions()))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    topic_list = ",".join(dict.fromkeys([topic, summaries_topic, events_topic]))
    source_reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic_list)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    max_offsets_per_trigger = _stream_max_offsets_per_trigger()
    if max_offsets_per_trigger is not None:
        source_reader = source_reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
    source_df = (
        source_reader.load()
        .selectExpr("topic", "partition", "offset", "CAST(value AS STRING) AS raw_value")
    )

    warehouse = _warehouse_or_default()
    live_store = get_live_store(get_config())

    def process_batch(batch_df, batch_id: int) -> None:
        cached_frames = []
        try:
            batch_df = batch_df.cache()
            cached_frames.append(batch_df)
            if not _spark_frame_has_rows(batch_df):
                return

            offset_rows = [
                row.asDict(recursive=True)
                for row in batch_df.groupBy("topic", "partition")
                .agg(
                    F.min("offset").alias("min_offset"),
                    F.max("offset").alias("max_offset"),
                    F.count("*").alias("message_count"),
                )
                .orderBy("topic", "partition")
                .collect()
            ]

            action_events_df = _spark_parse_topic_frame(batch_df, topic_name=topic, schema=action_schema)
            hand_events_df = _spark_parse_topic_frame(batch_df, topic_name=summaries_topic, schema=hand_schema).cache()
            simulation_events_df = _spark_parse_topic_frame(
                batch_df,
                topic_name=events_topic,
                schema=simulation_event_schema,
            )
            cached_frames.append(hand_events_df)

            action_frame = _spark_action_output_frame(action_events_df).cache()
            hand_frame = _spark_hand_output_frame(hand_events_df).cache()
            player_frame = _spark_player_output_frame(spark, hand_events_df).cache()
            simulation_event_frame = _spark_simulation_event_output_frame(simulation_events_df).cache()
            cached_frames.extend([action_frame, hand_frame, player_frame, simulation_event_frame])

            action_rows = _write_spark_frame(warehouse, "RAW_ACTIONS", action_frame, ACTION_EVENT_KEYS)
            _write_spark_frame(warehouse, "CURATED_ACTIONS", action_frame, ACTION_EVENT_KEYS)
            hand_rows = _write_spark_frame(warehouse, "RAW_HANDS", hand_frame, HAND_SUMMARY_KEYS)
            _write_spark_frame(warehouse, "CURATED_HANDS", hand_frame, HAND_SUMMARY_KEYS)
            player_rows = _write_spark_frame(warehouse, "RAW_PLAYERS", player_frame, PLAYER_RESULT_KEYS)
            _write_spark_frame(warehouse, "CURATED_PLAYERS", player_frame, PLAYER_RESULT_KEYS)
            simulation_event_rows = _spark_row_count(simulation_event_frame)
            simulation_event_records = (
                _spark_collect_records(
                    simulation_event_frame.orderBy(F.col("event_ts").asc_nulls_last(), F.col("simulation_run_id"))
                )
                if simulation_event_rows
                else []
            )
            simulation_run_rows = (
                len(
                    persist_simulation_run_metadata(
                        warehouse,
                        pd.DataFrame(simulation_event_records),
                    )
                )
                if simulation_event_records
                else 0
            )
            touched_run_ids = sorted(
                set(_spark_collect_run_ids(action_frame))
                | set(_spark_collect_run_ids(hand_frame))
                | set(_spark_collect_run_ids(player_frame))
                | set(_spark_collect_run_ids(simulation_event_frame))
            )
            terminal_run_ids = sorted(
                set(_spark_collect_terminal_run_ids(simulation_event_frame))
                | set(_execution_finished_run_ids_from_registry(warehouse, touched_run_ids))
            )
            player_summary_rows = int(
                len(
                    refresh_simulation_run_player_summaries(
                        terminal_run_ids,
                        warehouse=warehouse,
                    )
                )
            ) if terminal_run_ids else 0

            live_actions_df = action_frame.where(
                (F.col("source_type") == F.lit("simulated")) & F.col("simulation_run_id").isNotNull()
            ).cache()
            live_players_df = player_frame.where(
                (F.col("source_type") == F.lit("simulated")) & F.col("simulation_run_id").isNotNull()
            ).cache()
            cached_frames.extend([live_actions_df, live_players_df])

            live_metrics_df = _spark_compute_live_metrics(
                spark,
                live_actions_df,
                live_players_df,
                window_minutes=window_minutes,
            ).cache()
            assignments_df = _spark_assign_live_profiles(spark, live_metrics_df, _load_active_centroids(warehouse)).cache()
            cached_frames.extend([live_metrics_df, assignments_df])

            live_metric_records = _spark_collect_records(live_metrics_df) if _spark_frame_has_rows(live_metrics_df) else []
            assignment_records = _spark_collect_records(assignments_df) if _spark_frame_has_rows(assignments_df) else []
            write_live_outputs(
                pd.DataFrame(live_metric_records),
                pd.DataFrame(assignment_records),
                warehouse=warehouse,
            )
            live_metric_rows = len(live_metric_records)
            live_assignment_rows = len(assignment_records)
            live_player_records = _spark_collect_records(live_players_df) if _spark_frame_has_rows(live_players_df) else []

            touched_run_ids = sorted(
                _record_run_ids(simulation_event_records)
                | _record_run_ids(live_player_records)
                | _record_run_ids(live_metric_records)
                | _record_run_ids(assignment_records)
                | set(touched_run_ids)
            )
            dashboard_summary_rows = _materialize_spark_dashboard_outputs(
                spark,
                warehouse=warehouse,
                live_store=live_store,
                live_players_df=live_players_df,
                live_metrics_df=live_metrics_df,
                assignments_df=assignments_df,
                simulation_event_df=simulation_event_frame,
                touched_run_ids=touched_run_ids,
                live_player_records=live_player_records,
                simulation_event_records=simulation_event_records,
                live_metric_records=live_metric_records,
                assignment_records=assignment_records,
            )

            print(
                json.dumps(
                    {
                        "message": "spark_stream_batch_processed",
                        "batch_id": int(batch_id),
                        "offsets": offset_rows,
                        "touched_run_ids": touched_run_ids,
                        "action_rows": action_rows,
                        "hand_rows": hand_rows,
                        "player_rows": player_rows,
                        "player_summary_rows": player_summary_rows,
                        "simulation_run_rows": simulation_run_rows,
                        "simulation_event_rows": simulation_event_rows,
                        "live_metric_rows": live_metric_rows,
                        "live_assignment_rows": live_assignment_rows,
                        "dashboard_summary_rows": len(dashboard_summary_rows),
                    },
                    default=str,
                ),
                flush=True,
            )
        finally:
            for frame in reversed(cached_frames):
                frame.unpersist(blocking=False)

    writer = source_df.writeStream.foreachBatch(process_batch).option(
        "checkpointLocation",
        checkpoint_path,
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()


def main(argv: list[str] | None = None) -> int:
    config = get_config()
    parser = argparse.ArgumentParser(description="Compute live action-level metrics from Kafka using Spark.")
    parser.add_argument("--brokers", default=config.kafka_brokers)
    parser.add_argument("--topic", default=config.kafka_actions_topic)
    parser.add_argument("--summaries-topic", default=config.kafka_hand_summaries_topic)
    parser.add_argument("--events-topic", default=config.kafka_simulation_events_topic)
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--checkpoint-path", default=str(config.stream_checkpoint_path))
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args(argv)

    run_spark_kafka_stream(
        brokers=args.brokers,
        topic=args.topic,
        summaries_topic=args.summaries_topic,
        events_topic=args.events_topic,
        window_minutes=args.window_minutes,
        starting_offsets=args.starting_offsets,
        checkpoint_path=args.checkpoint_path,
        available_now=args.available_now,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
