from __future__ import annotations

import argparse
from contextlib import suppress
from datetime import datetime, timezone
import json
import os
from queue import Queue
import signal
import threading
import traceback
from typing import Any
from urllib.parse import quote

import requests
from poker_platform.config import get_config
from poker_platform.kafka_utils import build_producer, publish_records
from poker_platform.storage import bootstrap_backend

from .llm_policy import DEFAULT_MODEL, DEFAULT_REASONING_EFFORT
from .run_registry import get_run_record, queue_run_record, replace_player_summaries, update_run_record
from .run_simulation import build_context_matchup_players, simulate_and_record
from .session_metadata import hero_context_hash as compute_hero_context_hash
from .session_metadata import hero_context_preview
from .session_metadata import normalize_decision_backend

_APP_RUN_MISSING = object()


class SimulationRunHalted(RuntimeError):
    def __init__(self, simulation_run_id: str, *, status: str, reason: str) -> None:
        super().__init__(f"simulation {simulation_run_id} halted: {reason}")
        self.simulation_run_id = simulation_run_id
        self.status = status
        self.reason = reason


def _log_worker_warning(message: str) -> None:
    print(f"[simulation-worker] {message}", flush=True)


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _bool_payload_value(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _env_positive_int(name: str) -> int | None:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _progress_interval_hands(decision_backend: str | None) -> int:
    normalized = normalize_decision_backend(decision_backend)
    backend_override = (
        _env_positive_int("SIMULATION_PROGRESS_INTERVAL_HANDS_LLM")
        if normalized == "llm"
        else _env_positive_int("SIMULATION_PROGRESS_INTERVAL_HANDS_HEURISTIC")
    )
    global_override = _env_positive_int("SIMULATION_PROGRESS_INTERVAL_HANDS")
    if backend_override is not None:
        return backend_override
    if global_override is not None:
        return global_override
    return 1 if normalized == "llm" else 5


def _fallback_existing_record(payload: dict[str, Any]) -> dict[str, Any]:
    backend_settings = _backend_settings(payload, {})
    resolved_context = str(payload.get("context") or "")
    reset_stacks_each_hand = _bool_payload_value(
        payload.get("reset_stacks_each_hand"),
        default=True,
    )
    return {
        "simulation_run_id": str(payload["simulation_run_id"]),
        "table_id": str(payload.get("table_id") or "table_1"),
        "user_id": payload.get("user_id"),
        "status": "queued",
        "decision_backend": backend_settings["decision_backend"],
        "hero_context_hash": payload.get("hero_context_hash")
        or compute_hero_context_hash(resolved_context),
        "backend_type": backend_settings["backend_type"],
        "request_mode": "context",
        "requested_at": payload.get("requested_at"),
        "started_at": None,
        "finished_at": None,
        "hand_count": int(payload.get("hand_count") or 500),
        "seed": int(payload.get("seed") or 42),
        "hero_context": resolved_context,
        "hero_seat": int(payload.get("hero_seat") or 1),
        "model_name": payload.get("model_name")
        if payload.get("model_name") is not None
        else backend_settings["model_name"],
        "error_message": None,
        "published_actions": 0,
        "published_hand_summaries": 0,
        "config_json": {
            "small_blind_bb": float(payload.get("small_blind_bb") or 0.5),
            "big_blind_bb": float(payload.get("big_blind_bb") or 1.0),
            "starting_stack_bb": float(payload.get("starting_stack_bb") or 100.0),
            "decision_backend": backend_settings["decision_backend"],
            "hero_context_hash": payload.get("hero_context_hash")
            or compute_hero_context_hash(resolved_context),
            "model_name": payload.get("model_name")
            if payload.get("model_name") is not None
            else backend_settings["model_name"],
            "reasoning_effort": (
                payload.get("reasoning_effort")
                if payload.get("reasoning_effort") is not None
                else backend_settings["reasoning_effort"]
            ),
            "reset_stacks_each_hand": reset_stacks_each_hand,
            "always_model_postflop": backend_settings["always_model_postflop"],
            "gating_profile": backend_settings["gating_profile"],
        },
    }


def _normalize_app_run_status(run: dict[str, Any] | None) -> str | None:
    if not isinstance(run, dict):
        return None
    status = str(run.get("status") or "").strip().lower()
    if status in {"running", "finalizing", "completed", "failed"}:
        return status
    if run.get("completed_at") or run.get("finished_at"):
        return "finalizing"
    if run.get("started_at"):
        return "running"
    if int(run.get("published_actions") or 0) > 0:
        return "running"
    if int(run.get("published_hand_summaries") or 0) > 0:
        return "running"
    if isinstance(run.get("player_summaries"), list) and run["player_summaries"]:
        return "running"
    return status or None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _timestamps_match(left: Any, right: Any, *, tolerance_seconds: float = 1.0) -> bool:
    left_ts = _parse_timestamp(left)
    right_ts = _parse_timestamp(right)
    if left_ts is None or right_ts is None:
        return False
    return abs(left_ts.timestamp() - right_ts.timestamp()) <= tolerance_seconds


def _request_matches_app_run(payload: dict[str, Any], app_run: dict[str, Any]) -> bool:
    request_payload = app_run.get("request_payload")
    if not isinstance(request_payload, dict):
        request_payload = {}

    payload_requested_at = payload.get("requested_at")
    app_requested_at = app_run.get("requested_at") or request_payload.get("requested_at")
    if payload_requested_at or app_requested_at:
        if not _timestamps_match(payload_requested_at, app_requested_at):
            return False

    string_fields = ["user_id", "decision_backend", "hero_context_hash"]
    for field in string_fields:
        payload_value = str(payload.get(field) or "").strip()
        app_value = str(app_run.get(field) or request_payload.get(field) or "").strip()
        if payload_value and app_value and payload_value != app_value:
            return False

    integer_fields = ["hand_count", "seed", "hero_seat"]
    for field in integer_fields:
        payload_value = payload.get(field)
        app_value = app_run.get(field)
        if app_value is None:
            app_value = request_payload.get(field)
        if payload_value is None or app_value is None:
            continue
        try:
            if int(payload_value) != int(app_value):
                return False
        except (TypeError, ValueError):
            return False

    return True


def _raise_if_app_run_halted(payload: dict[str, Any]) -> None:
    simulation_run_id = str(payload.get("simulation_run_id") or "").strip()
    if not simulation_run_id:
        return
    app_run = _best_effort_get_app_run_record(simulation_run_id)
    if app_run is _APP_RUN_MISSING:
        raise SimulationRunHalted(
            simulation_run_id,
            status="missing",
            reason="app_run_not_found",
        )
    if not isinstance(app_run, dict):
        return
    app_status = _normalize_app_run_status(app_run)
    if app_status in {"finalizing", "completed", "failed"}:
        raise SimulationRunHalted(
            simulation_run_id,
            status=app_status,
            reason=f"app_run_{app_status}",
        )
    if not _request_matches_app_run(payload, app_run):
        raise SimulationRunHalted(
            simulation_run_id,
            status=app_status or "queued",
            reason="stale_request",
        )


def _best_effort_get_app_run_record(simulation_run_id: str) -> dict[str, Any] | object | None:
    base_url = str(os.getenv("APP_INTERNAL_BASE_URL") or "").strip()
    if not base_url:
        return None
    url = f"{base_url.rstrip('/')}/api/simulations/{quote(simulation_run_id, safe='')}"
    try:
        response = requests.get(url, timeout=5.0)
        if response.status_code == 404:
            return _APP_RUN_MISSING
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except Exception as exc:  # pragma: no cover - network/runtime concerns
        print(
            f"[simulation-worker] warning: failed to load app run record for {simulation_run_id}: {exc}"
        )
        return None


def _best_effort_get_run_record(
    simulation_run_id: str,
    *,
    warehouse,
) -> dict[str, Any] | None:
    try:
        return get_run_record(simulation_run_id, warehouse=warehouse)
    except Exception as exc:  # pragma: no cover - exercised via worker integration-style tests
        print(
            f"[simulation-worker] warning: failed to load legacy run record for {simulation_run_id}: {exc}"
        )
        return None


def _best_effort_queue_run_record(
    payload: dict[str, Any],
    *,
    warehouse,
) -> dict[str, Any]:
    try:
        return queue_run_record(payload, warehouse=warehouse)
    except Exception as exc:  # pragma: no cover - exercised via worker integration-style tests
        print(
            f"[simulation-worker] warning: failed to queue legacy run record for {payload.get('simulation_run_id')}: {exc}"
        )
        return _fallback_existing_record(payload)


def _best_effort_update_run_record(
    simulation_run_id: str,
    *,
    warehouse,
    fallback: dict[str, Any],
    **updates: Any,
) -> dict[str, Any]:
    try:
        return update_run_record(
            simulation_run_id,
            warehouse=warehouse,
            **updates,
        )
    except Exception as exc:  # pragma: no cover - exercised via worker integration-style tests
        print(
            f"[simulation-worker] warning: failed to update legacy run record for {simulation_run_id}: {exc}"
        )
        record = dict(fallback)
        record.update(updates)
        if (
            "config_json" in updates
            and isinstance(fallback.get("config_json"), dict)
            and isinstance(updates["config_json"], dict)
        ):
            merged = dict(fallback["config_json"])
            merged.update(updates["config_json"])
            record["config_json"] = merged
        return record


def _best_effort_replace_player_summaries(
    simulation_run_id: str,
    summaries: list[dict[str, Any]],
    *,
    warehouse,
) -> list[dict[str, Any]]:
    try:
        return replace_player_summaries(
            simulation_run_id,
            summaries,
            warehouse=warehouse,
        )
    except Exception as exc:  # pragma: no cover - exercised via worker integration-style tests
        print(
            f"[simulation-worker] warning: failed to write legacy player summaries for {simulation_run_id}: {exc}"
        )
        return list(summaries)


def _local_run_record_update(
    fallback: dict[str, Any],
    **updates: Any,
) -> dict[str, Any]:
    record = dict(fallback)
    record.update(updates)
    if (
        "config_json" in updates
        and isinstance(fallback.get("config_json"), dict)
        and isinstance(updates["config_json"], dict)
    ):
        merged = dict(fallback["config_json"])
        merged.update(updates["config_json"])
        record["config_json"] = merged
    return record


def _build_consumer(
    brokers: str,
    topic: str,
    *,
    group_id: str,
    max_poll_interval_ms: int,
):
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "kafka-python is required for the simulation worker; install with pip install -e .[kafka]"
        ) from exc

    return KafkaConsumer(
        topic,
        bootstrap_servers=[broker.strip() for broker in brokers.split(",") if broker.strip()],
        group_id=group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
        max_poll_interval_ms=max(300000, int(max_poll_interval_ms)),
    )


def _is_retriable_commit_error(exc: Exception) -> bool:
    return type(exc).__name__ in {
        "CommitFailedError",
        "IllegalStateError",
        "RebalanceInProgressError",
    }


def _commit_consumer_offsets(consumer) -> None:
    try:
        consumer.commit()
    except Exception as exc:
        if _is_retriable_commit_error(exc):
            _log_worker_warning(
                f"consumer commit skipped after rebalance: {type(exc).__name__}: {exc}"
            )
            return
        raise


def _publish_lifecycle_event(
    producer,
    topic: str,
    *,
    event_type: str,
    simulation_run_id: str,
    table_id: str,
    status: str,
    payload: dict[str, Any] | None = None,
) -> None:
    event = {
        "event_type": event_type,
        "simulation_run_id": simulation_run_id,
        "table_id": table_id,
        "status": status,
        "event_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if payload:
        event.update(payload)
    publish_records(
        producer=producer,
        topic=topic,
        records=[event],
        key_builder=lambda record: str(record["simulation_run_id"]),
    )


def _best_effort_publish_progress(
    *,
    simulation_run_id: str,
    progress: dict[str, Any],
    payload: dict[str, Any],
    running_record: dict[str, Any],
    backend_settings: dict[str, Any],
    resolved_hero_context_hash: str,
    user_id: str | None,
    producer,
    events_topic: str,
    legacy_registry_enabled: bool,
    warehouse,
) -> dict[str, Any]:
    try:
        if legacy_registry_enabled:
            updated_record = _best_effort_update_run_record(
                simulation_run_id,
                warehouse=warehouse,
                fallback=running_record,
                status="running",
                user_id=user_id,
                decision_backend=str(backend_settings["decision_backend"]),
                hero_context_hash=resolved_hero_context_hash,
                backend_type=str(backend_settings["backend_type"]),
                model_name=backend_settings["model_name"],
                published_actions=int(progress.get("published_actions") or 0),
                published_hand_summaries=int(progress.get("published_hand_summaries") or 0),
                config_json=running_record.get("config_json"),
            )
            _best_effort_replace_player_summaries(
                simulation_run_id,
                list(progress.get("player_summaries") or []),
                warehouse=warehouse,
            )
        else:
            updated_record = _local_run_record_update(
                running_record,
                status="running",
                user_id=user_id,
                decision_backend=str(backend_settings["decision_backend"]),
                hero_context_hash=resolved_hero_context_hash,
                backend_type=str(backend_settings["backend_type"]),
                model_name=backend_settings["model_name"],
                published_actions=int(progress.get("published_actions") or 0),
                published_hand_summaries=int(progress.get("published_hand_summaries") or 0),
                player_summaries=list(progress.get("player_summaries") or []),
                config_json=running_record.get("config_json"),
            )

        _publish_lifecycle_event(
            producer,
            events_topic,
            event_type="simulation_progress",
            simulation_run_id=simulation_run_id,
            table_id=str(updated_record.get("table_id") or payload.get("table_id") or "table_1"),
            status="running",
            payload={
                "hand_count": int(payload.get("hand_count") or running_record.get("hand_count") or 500),
                "published_actions": int(progress.get("published_actions") or 0),
                "published_hand_summaries": int(progress.get("published_hand_summaries") or 0),
                "player_summaries": list(progress.get("player_summaries") or []),
                "decision_backend": str(backend_settings["decision_backend"]),
                "backend_type": str(backend_settings["backend_type"]),
                "user_id": user_id,
                "hero_context_hash": resolved_hero_context_hash,
                "hero_context_preview": hero_context_preview(payload.get("context") or running_record.get("hero_context")),
                "model_name": backend_settings["model_name"],
            },
        )
        return updated_record
    except Exception as exc:  # pragma: no cover - best-effort progress publishing
        print(
            f"[simulation-worker] warning: failed to publish progress for {simulation_run_id}: {exc}"
        )
        return running_record


def _backend_settings(payload: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    config_json = existing.get("config_json") or {}
    decision_backend = normalize_decision_backend(
        payload.get("decision_backend")
        or config_json.get("decision_backend")
        or payload.get("backend_type")
        or existing.get("backend_type")
    )
    if decision_backend == "heuristic":
        return {
            "decision_backend": "heuristic",
            "backend_type": "heuristic_persona",
            "model_name": None,
            "reasoning_effort": None,
            "gating_profile": None,
            "always_model_postflop": False,
        }
    return {
        "decision_backend": "llm",
        "backend_type": "llm_gated_nano",
        "model_name": str(payload.get("model_name") or existing.get("model_name") or DEFAULT_MODEL),
        "reasoning_effort": str(payload.get("reasoning_effort") or config_json.get("reasoning_effort") or DEFAULT_REASONING_EFFORT),
        "gating_profile": "conservative",
        "always_model_postflop": True,
    }


def process_request_message(
    payload: dict[str, Any],
    *,
    warehouse,
    producer,
    brokers: str,
    actions_topic: str,
    summaries_topic: str,
    events_topic: str,
    legacy_registry_enabled: bool = False,
) -> dict[str, Any]:
    simulation_run_id = str(payload["simulation_run_id"])
    if legacy_registry_enabled:
        existing = _best_effort_get_run_record(simulation_run_id, warehouse=warehouse)
        if existing is None:
            existing = _best_effort_queue_run_record(payload, warehouse=warehouse)
    else:
        app_run = _best_effort_get_app_run_record(simulation_run_id)
        if app_run is _APP_RUN_MISSING:
            return {
                "simulation_run_id": simulation_run_id,
                "status": "missing",
                "skipped": True,
                "reason": "app_run_not_found",
            }
        if not isinstance(app_run, dict):
            app_run = None
        app_status = _normalize_app_run_status(app_run)
        if app_status in {"running", "finalizing", "completed", "failed"}:
            return {
                "simulation_run_id": simulation_run_id,
                "status": app_status,
                "skipped": True,
            }
        if app_run is not None and not _request_matches_app_run(payload, app_run):
            return {
                "simulation_run_id": simulation_run_id,
                "status": app_status or "queued",
                "skipped": True,
                "reason": "stale_request",
            }
        existing = _fallback_existing_record(payload)
    backend_settings = _backend_settings(payload, existing)
    resolved_hero_context_hash = (
        payload.get("hero_context_hash")
        or existing.get("hero_context_hash")
        or compute_hero_context_hash(payload.get("context") or existing.get("hero_context"))
    )
    user_id = payload.get("user_id") or existing.get("user_id")

    current_status = str(existing.get("status") or "").lower()
    if current_status in {"running", "finalizing", "completed", "failed"}:
        return {"simulation_run_id": simulation_run_id, "status": current_status, "skipped": True}

    run_started_at = _utcnow_naive()
    running_record = (
        _best_effort_update_run_record(
            simulation_run_id,
            warehouse=warehouse,
            fallback=existing,
            status="running",
            started_at=run_started_at,
            finished_at=None,
            error_message=None,
            backend_type=backend_settings["backend_type"],
        )
        if legacy_registry_enabled
        else _local_run_record_update(
            existing,
            status="running",
            started_at=run_started_at,
            finished_at=None,
            error_message=None,
            backend_type=backend_settings["backend_type"],
        )
    )
    _publish_lifecycle_event(
        producer,
        events_topic,
        event_type="simulation_started",
        simulation_run_id=simulation_run_id,
        table_id=str(payload.get("table_id") or "table_1"),
        status="running",
        payload={
            "hand_count": int(payload.get("hand_count") or 500),
            "decision_backend": str(backend_settings["decision_backend"]),
            "backend_type": str(backend_settings["backend_type"]),
            "user_id": user_id,
            "hero_context_hash": resolved_hero_context_hash,
            "hero_context_preview": hero_context_preview(payload.get("context") or existing.get("hero_context")),
            "model_name": backend_settings["model_name"],
        },
    )

    try:
        config_json = existing.get("config_json") or {}
        reset_stacks_each_hand = _bool_payload_value(
            payload.get("reset_stacks_each_hand")
            if "reset_stacks_each_hand" in payload
            else config_json.get("reset_stacks_each_hand"),
            default=True,
        )
        players = build_context_matchup_players(
            str(payload["context"]),
            starting_stack_bb=float(payload.get("starting_stack_bb") or config_json.get("starting_stack_bb") or 100.0),
            hero_seat=int(payload.get("hero_seat") or 1),
            backend_type=str(backend_settings["backend_type"]),
            model_name=backend_settings["model_name"],
            reasoning_effort=backend_settings["reasoning_effort"],
            gating_profile=backend_settings["gating_profile"],
            always_model_postflop=bool(backend_settings["always_model_postflop"]),
            max_model_calls_per_hand=None,
            heuristic_compiler_timeout_seconds=float(payload.get("llm_timeout_seconds") or 15.0),
        )

        def _handle_progress(progress: dict[str, Any]) -> None:
            nonlocal running_record
            if not legacy_registry_enabled:
                _raise_if_app_run_halted(payload)
            running_record = _best_effort_publish_progress(
                simulation_run_id=simulation_run_id,
                progress=progress,
                payload=payload,
                running_record=running_record,
                backend_settings=backend_settings,
                resolved_hero_context_hash=resolved_hero_context_hash,
                user_id=user_id,
                producer=producer,
                events_topic=events_topic,
                legacy_registry_enabled=legacy_registry_enabled,
                warehouse=warehouse,
            )

        result = simulate_and_record(
            hand_count=int(payload.get("hand_count") or existing.get("hand_count") or 500),
            seed=int(payload.get("seed") or existing.get("seed") or 42),
            table_id=str(payload.get("table_id") or existing.get("table_id") or "table_1"),
            simulation_run_id=simulation_run_id,
            starting_stack_bb=float(payload.get("starting_stack_bb") or config_json.get("starting_stack_bb") or 100.0),
            small_blind_bb=float(payload.get("small_blind_bb") or config_json.get("small_blind_bb") or 0.5),
            big_blind_bb=float(payload.get("big_blind_bb") or config_json.get("big_blind_bb") or 1.0),
            players=players,
            publish_kafka=True,
            kafka_brokers=brokers,
            actions_topic=actions_topic,
            summaries_topic=summaries_topic,
            write_backend=False,
            carry_stacks_between_hands=not reset_stacks_each_hand,
            max_model_calls_per_run=0,
            llm_timeout_seconds=float(payload.get("llm_timeout_seconds") or 15.0),
            user_id=user_id,
            decision_backend=str(backend_settings["decision_backend"]),
            hero_context_hash=resolved_hero_context_hash,
            hero_seat=int(payload.get("hero_seat") or running_record.get("hero_seat") or 1),
            run_started_at=run_started_at,
            progress_callback=_handle_progress,
            progress_interval_hands=_progress_interval_hands(
                backend_settings["decision_backend"]
            ),
        )
        if legacy_registry_enabled:
            _best_effort_replace_player_summaries(
                simulation_run_id,
                result["player_summaries"],
                warehouse=warehouse,
            )
            record = _best_effort_update_run_record(
                simulation_run_id,
                warehouse=warehouse,
                fallback=running_record,
                status="finalizing",
                finished_at=_utcnow_naive(),
                user_id=user_id,
                decision_backend=str(backend_settings["decision_backend"]),
                hero_context_hash=resolved_hero_context_hash,
                backend_type=str(backend_settings["backend_type"]),
                model_name=backend_settings["model_name"],
                published_actions=int(result.get("published_actions") or 0),
                published_hand_summaries=int(result.get("published_hand_summaries") or 0),
                config_json={
                    "decision_backend": str(backend_settings["decision_backend"]),
                    "reasoning_effort": backend_settings["reasoning_effort"],
                    "reset_stacks_each_hand": reset_stacks_each_hand,
                    "always_model_postflop": bool(backend_settings["always_model_postflop"]),
                    "gating_profile": backend_settings["gating_profile"],
                },
            )
        else:
            record = _local_run_record_update(
                running_record,
                status="finalizing",
                finished_at=_utcnow_naive(),
                user_id=user_id,
                decision_backend=str(backend_settings["decision_backend"]),
                hero_context_hash=resolved_hero_context_hash,
                backend_type=str(backend_settings["backend_type"]),
                model_name=backend_settings["model_name"],
                published_actions=int(result.get("published_actions") or 0),
                published_hand_summaries=int(result.get("published_hand_summaries") or 0),
                player_summaries=list(result["player_summaries"]),
                config_json={
                    "decision_backend": str(backend_settings["decision_backend"]),
                    "reasoning_effort": backend_settings["reasoning_effort"],
                    "reset_stacks_each_hand": reset_stacks_each_hand,
                    "always_model_postflop": bool(backend_settings["always_model_postflop"]),
                    "gating_profile": backend_settings["gating_profile"],
                },
            )
        _publish_lifecycle_event(
            producer,
            events_topic,
            event_type="simulation_completed",
            simulation_run_id=simulation_run_id,
            table_id=str(record.get("table_id") or payload.get("table_id") or "table_1"),
            status="finalizing",
            payload={
                "hand_count": int(result["hands"]),
                "published_actions": int(result["published_actions"]),
                "published_hand_summaries": int(result["published_hand_summaries"]),
                "player_summaries": list(result["player_summaries"]),
                "profile_session_features": list(
                    result.get("profile_session_features") or []
                ),
                "decision_backend": str(backend_settings["decision_backend"]),
                "backend_type": str(backend_settings["backend_type"]),
                "user_id": user_id,
                "hero_context_hash": resolved_hero_context_hash,
                "hero_context_preview": hero_context_preview(record.get("hero_context")),
                "model_name": backend_settings["model_name"],
            },
        )
        return {"simulation_run_id": simulation_run_id, "status": "finalizing", "result": result}
    except SimulationRunHalted as exc:
        _log_worker_warning(
            f"halted {simulation_run_id}: {exc.reason} ({exc.status})"
        )
        return {
            "simulation_run_id": simulation_run_id,
            "status": exc.status,
            "skipped": True,
            "reason": exc.reason,
        }
    except Exception as exc:
        error_message = str(exc)
        if legacy_registry_enabled:
            _best_effort_update_run_record(
                simulation_run_id,
                warehouse=warehouse,
                fallback=running_record,
                status="failed",
                finished_at=_utcnow_naive(),
                error_message=error_message,
            )
        _publish_lifecycle_event(
            producer,
            events_topic,
            event_type="simulation_failed",
            simulation_run_id=simulation_run_id,
            table_id=str(payload.get("table_id") or existing.get("table_id") or "table_1"),
            status="failed",
            payload={
                "error_message": error_message,
                "decision_backend": str(backend_settings["decision_backend"]),
                "backend_type": str(backend_settings["backend_type"]),
                "user_id": user_id,
                "hero_context_hash": resolved_hero_context_hash,
                "hero_context_preview": hero_context_preview(payload.get("context") or existing.get("hero_context")),
                "model_name": backend_settings["model_name"],
            },
        )
        raise


def _handle_consumer_exception(
    payload: dict[str, Any] | None,
    exc: Exception,
    *,
    warehouse,
    producer,
    events_topic: str,
    legacy_registry_enabled: bool = False,
) -> None:
    def _log_exception() -> None:
        traceback.print_exception(type(exc), exc, exc.__traceback__)

    if not isinstance(payload, dict):
        _log_exception()
        return

    simulation_run_id = str(payload.get("simulation_run_id") or "").strip()
    if not simulation_run_id:
        _log_exception()
        return

    try:
        existing = (
            _best_effort_get_run_record(simulation_run_id, warehouse=warehouse)
            if legacy_registry_enabled
            else None
        ) or _fallback_existing_record(payload)
        current_status = str(existing.get("status") or "").lower()
        if current_status not in {"completed", "failed"}:
            resolved_error = str(exc)
            record = (
                _best_effort_update_run_record(
                    simulation_run_id,
                    warehouse=warehouse,
                    fallback=existing,
                    status="failed",
                    finished_at=_utcnow_naive(),
                    error_message=resolved_error,
                )
                if legacy_registry_enabled
                else _local_run_record_update(
                    existing,
                    status="failed",
                    finished_at=_utcnow_naive(),
                    error_message=resolved_error,
                )
            )
            _publish_lifecycle_event(
                producer,
                events_topic,
                event_type="simulation_failed",
                simulation_run_id=simulation_run_id,
                table_id=str(record.get("table_id") or payload.get("table_id") or "table_1"),
                status="failed",
                payload={
                    "error_message": resolved_error,
                    "decision_backend": payload.get("decision_backend") or existing.get("decision_backend"),
                    "backend_type": payload.get("backend_type") or existing.get("backend_type"),
                    "user_id": payload.get("user_id") or existing.get("user_id"),
                    "hero_context_hash": payload.get("hero_context_hash") or existing.get("hero_context_hash"),
                    "hero_context_preview": hero_context_preview(payload.get("context") or existing.get("hero_context")),
                    "model_name": payload.get("model_name") or existing.get("model_name"),
                },
            )
    finally:
        _log_exception()


def _consume_loop(
    *,
    brokers: str,
    requests_topic: str,
    actions_topic: str,
    summaries_topic: str,
    events_topic: str,
    group_id: str,
    max_poll_interval_ms: int,
    stop_event: threading.Event,
    legacy_registry_enabled: bool = False,
) -> None:
    warehouse = bootstrap_backend() if legacy_registry_enabled else None
    producer = build_producer(brokers)
    consumer = _build_consumer(
        brokers,
        requests_topic,
        group_id=group_id,
        max_poll_interval_ms=max_poll_interval_ms,
    )

    try:
        while not stop_event.is_set():
            batch = consumer.poll(timeout_ms=1000, max_records=1)
            if not batch:
                continue
            for messages in batch.values():
                for message in messages:
                    if stop_event.is_set():
                        break
                    try:
                        process_request_message(
                            message.value,
                            warehouse=warehouse,
                            producer=producer,
                            brokers=brokers,
                            actions_topic=actions_topic,
                            summaries_topic=summaries_topic,
                            events_topic=events_topic,
                            legacy_registry_enabled=legacy_registry_enabled,
                        )
                    except Exception as exc:
                        _handle_consumer_exception(
                            message.value,
                            exc,
                            warehouse=warehouse,
                            producer=producer,
                            events_topic=events_topic,
                            legacy_registry_enabled=legacy_registry_enabled,
                        )
                    finally:
                        _commit_consumer_offsets(consumer)
                if stop_event.is_set():
                    break
    finally:
        with suppress(Exception):
            consumer.close()
        with suppress(Exception):
            producer.close()
        with suppress(Exception):
            if warehouse is not None:
                warehouse.close()


def consume_forever(
    *,
    brokers: str,
    requests_topic: str,
    actions_topic: str,
    summaries_topic: str,
    events_topic: str,
    group_id: str,
    concurrency: int = 1,
    max_poll_interval_ms: int = 1800000,
    legacy_registry_enabled: bool = False,
) -> None:
    worker_count = max(1, int(concurrency))
    stop_event = threading.Event()
    worker_errors: Queue[Exception] = Queue()

    def _stop(*_args):
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(ValueError):
            signal.signal(sig, _stop)

    def _run_consumer() -> None:
        try:
            _consume_loop(
                brokers=brokers,
                requests_topic=requests_topic,
                actions_topic=actions_topic,
                summaries_topic=summaries_topic,
                events_topic=events_topic,
                group_id=group_id,
                max_poll_interval_ms=max_poll_interval_ms,
                stop_event=stop_event,
                legacy_registry_enabled=legacy_registry_enabled,
            )
        except Exception as exc:  # pragma: no cover - fatal startup/runtime failures are integration concerns
            worker_errors.put(exc)
            stop_event.set()

    threads = [
        threading.Thread(
            target=_run_consumer,
            name=f"simulation-worker-{index + 1}",
            daemon=True,
        )
        for index in range(worker_count)
    ]

    for thread in threads:
        thread.start()

    try:
        while any(thread.is_alive() for thread in threads):
            if not worker_errors.empty():
                raise worker_errors.get()
            if stop_event.wait(0.25):
                break
    finally:
        stop_event.set()
        for thread in threads:
            thread.join(timeout=5.0)
        if not worker_errors.empty():
            raise worker_errors.get()


def main(argv: list[str] | None = None) -> int:
    config = get_config()
    parser = argparse.ArgumentParser(description="Consume queued simulation requests and execute runs.")
    parser.add_argument("--brokers", default=config.kafka_brokers)
    parser.add_argument("--requests-topic", default=config.kafka_simulation_requests_topic)
    parser.add_argument("--actions-topic", default=config.kafka_actions_topic)
    parser.add_argument("--summaries-topic", default=config.kafka_hand_summaries_topic)
    parser.add_argument("--events-topic", default=config.kafka_simulation_events_topic)
    parser.add_argument("--group-id", default="poker-simulation-worker")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=config.simulation_worker_concurrency,
        help="Number of Kafka consumers to run inside this worker process.",
    )
    parser.add_argument(
        "--max-poll-interval-ms",
        type=int,
        default=config.simulation_worker_max_poll_interval_ms,
        help="Maximum time a worker thread may spend processing a request before Kafka considers it unhealthy.",
    )
    parser.add_argument(
        "--legacy-run-registry",
        action="store_true",
        default=config.simulation_worker_legacy_registry_enabled,
        help="Enable legacy Snowflake-backed run registry compatibility writes",
    )
    args = parser.parse_args(argv)

    consume_forever(
        brokers=str(args.brokers),
        requests_topic=str(args.requests_topic),
        actions_topic=str(args.actions_topic),
        summaries_topic=str(args.summaries_topic),
        events_topic=str(args.events_topic),
        group_id=str(args.group_id),
        concurrency=max(1, int(args.concurrency)),
        max_poll_interval_ms=max(300000, int(args.max_poll_interval_ms)),
        legacy_registry_enabled=bool(args.legacy_run_registry),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
