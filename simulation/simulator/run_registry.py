from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from typing import Any, Mapping

import pandas as pd

from poker_platform.config import get_config
from poker_platform.storage import bootstrap_backend
from poker_platform.storage import get_warehouse

from .llm_policy import DEFAULT_MODEL, DEFAULT_REASONING_EFFORT
from .session_metadata import hero_context_hash as compute_hero_context_hash
from .session_metadata import normalize_decision_backend


RUN_TABLE = "RAW_SIMULATION_RUNS"
SUMMARY_TABLE = "SIMULATION_RUN_PLAYER_SUMMARIES"


def _warehouse_or_default(warehouse=None):
    if warehouse is not None:
        return warehouse
    if os.getenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP") == "1":
        return get_warehouse(get_config())
    return bootstrap_backend()


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _python_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            return value.tz_convert("UTC").tz_localize(None).to_pydatetime()
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _json_maybe(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return value
    return value


def _serialize_json(value: Any) -> Any:
    value = _python_value(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat() + "Z"
    if isinstance(value, pd.Timestamp):
        return _serialize_json(value.to_pydatetime())
    if isinstance(value, dict):
        return {str(key): _serialize_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_json(item) for item in value]
    return value


def _datetime_maybe(value: Any) -> datetime | None:
    value = _python_value(value)
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        normalized = normalized.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    return None


def _row_to_record(row: Mapping[str, Any]) -> dict[str, Any]:
    record = {str(key): _python_value(value) for key, value in row.items()}
    record["config_json"] = _json_maybe(record.get("config_json")) or {}
    return record


def _backend_config(decision_backend: str) -> dict[str, Any]:
    normalized = normalize_decision_backend(decision_backend)
    if normalized == "heuristic":
        return {
            "decision_backend": "heuristic",
            "backend_type": "heuristic_persona",
            "model_name": None,
            "reasoning_effort": None,
            "always_model_postflop": False,
            "gating_profile": None,
        }
    return {
        "decision_backend": "llm",
        "backend_type": "llm_gated_nano",
        "model_name": DEFAULT_MODEL,
        "reasoning_effort": DEFAULT_REASONING_EFFORT,
        "always_model_postflop": True,
        "gating_profile": "conservative",
    }


def _replace_run_record(record: Mapping[str, Any], *, warehouse=None) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    simulation_run_id = str(record["simulation_run_id"])
    frame = pd.DataFrame([record])
    if hasattr(warehouse, "upsert_dataframe"):
        warehouse.upsert_dataframe(
            RUN_TABLE,
            frame,
            key_columns=["simulation_run_id"],
            force_bulk=False,
        )
    else:
        warehouse.delete_rows(RUN_TABLE, filters_eq={"simulation_run_id": simulation_run_id})
        warehouse.write_dataframe(RUN_TABLE, frame)
    return get_run_record(simulation_run_id, warehouse=warehouse) or dict(record)


def get_run_record(simulation_run_id: str, *, warehouse=None) -> dict[str, Any] | None:
    warehouse = _warehouse_or_default(warehouse)
    frame = warehouse.load_table(
        RUN_TABLE,
        filters_eq={"simulation_run_id": simulation_run_id},
        order_by=[("requested_at", False), ("started_at", False), ("finished_at", False)],
        limit=1,
    )
    if frame.empty:
        return None
    return _row_to_record(frame.iloc[0].to_dict())


def list_player_summaries(simulation_run_id: str, *, warehouse=None) -> list[dict[str, Any]]:
    warehouse = _warehouse_or_default(warehouse)
    frame = warehouse.load_table(
        SUMMARY_TABLE,
        filters_eq={"simulation_run_id": simulation_run_id},
        order_by=[("final_rank", True), ("seat", True)],
    )
    if frame.empty:
        return []
    records = []
    for row in frame.to_dict(orient="records"):
        normalized = {str(key): _python_value(value) for key, value in row.items()}
        records.append(normalized)
    return records


def queue_run_record(payload: Mapping[str, Any], *, warehouse=None) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    simulation_run_id = str(payload["simulation_run_id"])
    existing = get_run_record(simulation_run_id, warehouse=warehouse)
    if existing is not None:
        raise ValueError(f"simulation_run_id already exists: {simulation_run_id}")
    backend_config = _backend_config(payload.get("decision_backend") or payload.get("backend_type"))
    resolved_hero_context_hash = payload.get("hero_context_hash") or compute_hero_context_hash(payload.get("context"))

    record = {
        "simulation_run_id": simulation_run_id,
        "table_id": str(payload.get("table_id") or "table_1"),
        "user_id": payload.get("user_id"),
        "decision_backend": backend_config["decision_backend"],
        "hero_context_hash": resolved_hero_context_hash,
        "backend_type": backend_config["backend_type"],
        "status": "queued",
        "request_mode": "context",
        "requested_at": _datetime_maybe(payload.get("requested_at")) or _utcnow_naive(),
        "started_at": None,
        "finished_at": None,
        "hand_count": int(payload.get("hand_count") or 500),
        "seed": int(payload.get("seed") or 42),
        "hero_context": str(payload.get("context") or ""),
        "hero_seat": int(payload.get("hero_seat") or 1),
        "model_name": payload.get("model_name") if payload.get("model_name") is not None else backend_config["model_name"],
        "error_message": None,
        "published_actions": 0,
        "published_hand_summaries": 0,
        "config_json": {
            "small_blind_bb": float(payload.get("small_blind_bb") or 0.5),
            "big_blind_bb": float(payload.get("big_blind_bb") or 1.0),
            "starting_stack_bb": float(payload.get("starting_stack_bb") or 100.0),
            "decision_backend": backend_config["decision_backend"],
            "hero_context_hash": resolved_hero_context_hash,
            "model_name": payload.get("model_name") if payload.get("model_name") is not None else backend_config["model_name"],
            "reasoning_effort": (
                payload.get("reasoning_effort")
                if payload.get("reasoning_effort") is not None
                else backend_config["reasoning_effort"]
            ),
            "always_model_postflop": backend_config["always_model_postflop"],
            "gating_profile": backend_config["gating_profile"],
            "villain_personas": ["tag", "lag", "calling_station", "nit", "maniac"],
        },
    }
    return _replace_run_record(record, warehouse=warehouse)


def update_run_record(
    simulation_run_id: str,
    *,
    warehouse=None,
    **updates: Any,
) -> dict[str, Any]:
    warehouse = _warehouse_or_default(warehouse)
    existing = get_run_record(simulation_run_id, warehouse=warehouse)
    if existing is None:
        raise ValueError(f"Simulation run does not exist: {simulation_run_id}")
    record = dict(existing)
    record.update(updates)
    if "config_json" in updates and isinstance(existing.get("config_json"), dict) and isinstance(updates["config_json"], dict):
        merged = dict(existing["config_json"])
        merged.update(updates["config_json"])
        record["config_json"] = merged
    return _replace_run_record(record, warehouse=warehouse)


def replace_player_summaries(
    simulation_run_id: str,
    summaries: list[Mapping[str, Any]] | pd.DataFrame,
    *,
    warehouse=None,
) -> list[dict[str, Any]]:
    warehouse = _warehouse_or_default(warehouse)
    warehouse.delete_rows(SUMMARY_TABLE, filters_eq={"simulation_run_id": simulation_run_id})
    if isinstance(summaries, pd.DataFrame):
        frame = summaries.copy()
    else:
        frame = pd.DataFrame(list(summaries))
    if frame.empty:
        return []
    warehouse.write_dataframe(SUMMARY_TABLE, frame)
    return list_player_summaries(simulation_run_id, warehouse=warehouse)


def get_run_status_payload(simulation_run_id: str, *, warehouse=None) -> dict[str, Any] | None:
    warehouse = _warehouse_or_default(warehouse)
    record = get_run_record(simulation_run_id, warehouse=warehouse)
    if record is None:
        return None
    player_summaries = list_player_summaries(simulation_run_id, warehouse=warehouse)
    payload = {**record, "player_summaries": player_summaries}
    if player_summaries:
        payload["result"] = {
            "performance": [
                {
                    "agent_id": item.get("agent_id"),
                    "player_id": item.get("player_id"),
                    "persona_name": item.get("persona_name"),
                    "hands_played": item.get("hands_played"),
                    "total_bb_won": item.get("total_bb_won"),
                    "avg_bb_per_hand": item.get("avg_bb_per_hand"),
                    "bb_per_100": item.get("bb_per_100"),
                    "final_rank": item.get("final_rank"),
                }
                for item in player_summaries
            ]
        }
    return payload


def _print_json(payload: Any) -> None:
    print(json.dumps(_serialize_json(payload), indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage durable simulation run records.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enqueue_parser = subparsers.add_parser("enqueue")
    enqueue_parser.add_argument("--payload-json", required=True)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--simulation-run-id", required=True)

    fail_parser = subparsers.add_parser("fail")
    fail_parser.add_argument("--simulation-run-id", required=True)
    fail_parser.add_argument("--error-message", required=True)

    subparsers.add_parser("bootstrap")

    args = parser.parse_args(argv)

    if args.command == "enqueue":
        payload = json.loads(args.payload_json)
        _print_json(queue_run_record(payload))
        return 0
    if args.command == "status":
        payload = get_run_status_payload(args.simulation_run_id)
        if payload is None:
            _print_json({"found": False, "simulation_run_id": args.simulation_run_id})
            return 0
        _print_json(payload)
        return 0
    if args.command == "fail":
        payload = update_run_record(
            args.simulation_run_id,
            status="failed",
            error_message=args.error_message,
            finished_at=_utcnow_naive(),
        )
        _print_json(payload)
        return 0
    if args.command == "bootstrap":
        bootstrap_backend()
        _print_json({"ok": True})
        return 0
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
