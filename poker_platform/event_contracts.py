from __future__ import annotations

"""Shared serializers for Kafka event contracts."""

from datetime import datetime, timezone
import json
from typing import Any, Mapping

from poker_platform.schemas import (
    ActionEvent as ActionEventSchema,
    HandSummaryEvent as HandSummaryEventSchema,
    PAYLOAD_VERSION,
)


def _model_to_json_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    return json.loads(model.json())


def _validate_model(model_cls: Any, payload: Mapping[str, Any]) -> Any:
    if hasattr(model_cls, "model_validate"):
        return model_cls.model_validate(payload)
    return model_cls.parse_obj(payload)


def _coerce_list(value: Any) -> list[str]:
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
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return [str(value)]


def _coerce_datetime(value: Any, *, timestamp_ms: int | float | None = None) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    if timestamp_ms is not None:
        return datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=timezone.utc)
    raise ValueError("A datetime-like value is required for event serialization")


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _coerce_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _coerce_lineage(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {"raw_lineage_text": text}
        if isinstance(parsed, Mapping):
            value = parsed
        else:
            return {"raw_lineage_text": text}
    if isinstance(value, Mapping):
        normalized: dict[str, str] = {}
        for key, item in value.items():
            if isinstance(item, str):
                normalized[str(key)] = item
            elif isinstance(item, (int, float, bool)) or item is None:
                normalized[str(key)] = str(item)
            else:
                normalized[str(key)] = json.dumps(item, default=str)
        return normalized
    return {"raw_lineage_text": str(value)}


def serialize_action_event(record: Mapping[str, Any]) -> dict[str, Any]:
    raw_lineage = _coerce_lineage(record.get("raw_lineage"))
    timestamp_ms = record.get("timestamp_ms")
    if timestamp_ms is not None and "timestamp_ms" not in raw_lineage:
        raw_lineage["timestamp_ms"] = str(timestamp_ms)

    event = ActionEventSchema(
        payload_version=str(record.get("payload_version") or PAYLOAD_VERSION),
        event_type="action",
        source_type=str(record.get("source_type") or "simulated"),
        source_dataset=record.get("source_dataset"),
        source_run_id=record.get("source_run_id"),
        simulation_run_id=record.get("simulation_run_id"),
        user_id=record.get("user_id"),
        decision_backend=record.get("decision_backend"),
        hero_context_hash=record.get("hero_context_hash"),
        hero_seat=None if record.get("hero_seat") is None else _coerce_int(record.get("hero_seat")),
        is_hero_player=bool(record.get("is_hero_player", False)),
        table_id=str(record.get("table_id") or "table_1"),
        hand_id=str(record.get("hand_id")),
        action_index=_coerce_int(record.get("action_index")),
        street=str(record.get("street")),
        player_id=str(record.get("player_id")),
        agent_id=record.get("agent_id"),
        seat=_coerce_int(record.get("seat", record.get("seat_index")), 1),
        position=str(record.get("position") or "UNKNOWN"),
        action_type=str(record.get("action_type")),
        amount_bb=None if record.get("amount_bb") is None else float(record.get("amount_bb")),
        pot_before_bb=_coerce_float(record.get("pot_before_bb")),
        pot_after_bb=_coerce_float(record.get("pot_after_bb")),
        to_call_bb=_coerce_float(record.get("to_call_bb")),
        effective_stack_bb=_coerce_float(record.get("effective_stack_bb")),
        players_remaining=_coerce_int(record.get("players_remaining"), 1),
        board_cards_visible=_coerce_list(record.get("board_cards_visible")),
        hole_cards_visible=_coerce_list(record.get("hole_cards_visible")),
        is_all_in=bool(record.get("is_all_in", False)),
        event_ts=_coerce_datetime(record.get("event_ts"), timestamp_ms=timestamp_ms),
        backend_type=str(record.get("backend_type") or "unknown"),
        persona_name=record.get("persona_name"),
        persona_text=record.get("persona_text"),
        raw_lineage=raw_lineage,
    )
    return _model_to_json_dict(event)


def serialize_hand_summary_event(record: Mapping[str, Any]) -> dict[str, Any]:
    raw_lineage = _coerce_lineage(record.get("raw_lineage"))
    if "runtime_seed" in record and "runtime_seed" not in raw_lineage:
        raw_lineage["runtime_seed"] = str(record.get("runtime_seed"))
    if "small_blind_bb" in record and "small_blind_bb" not in raw_lineage:
        raw_lineage["small_blind_bb"] = str(record.get("small_blind_bb"))
    if "big_blind_bb" in record and "big_blind_bb" not in raw_lineage:
        raw_lineage["big_blind_bb"] = str(record.get("big_blind_bb"))
    if "street_action_counts" in record and "street_action_counts" not in raw_lineage:
        raw_lineage["street_action_counts"] = json.dumps(record.get("street_action_counts"), default=str)
    if "winning_seat_indices" in record and "winning_seat_indices" not in raw_lineage:
        raw_lineage["winning_seat_indices"] = json.dumps(record.get("winning_seat_indices"), default=str)
    if "starting_stacks_bb" in record and "starting_stacks_bb" not in raw_lineage:
        raw_lineage["starting_stacks_bb"] = json.dumps(record.get("starting_stacks_bb"), default=str)
    if "final_stacks_bb" in record and "final_stacks_bb" not in raw_lineage:
        raw_lineage["final_stacks_bb"] = json.dumps(record.get("final_stacks_bb"), default=str)
    if "collections_bb" in record and "collections_bb" not in raw_lineage:
        raw_lineage["collections_bb"] = json.dumps(record.get("collections_bb"), default=str)
    if "player_personas" in record and "player_personas" not in raw_lineage:
        raw_lineage["player_personas"] = json.dumps(record.get("player_personas"), default=str)
    if "player_agent_ids" in record and "player_agent_ids" not in raw_lineage:
        raw_lineage["player_agent_ids"] = json.dumps(record.get("player_agent_ids"), default=str)
    if "player_seats" in record and "player_seats" not in raw_lineage:
        raw_lineage["player_seats"] = json.dumps(record.get("player_seats"), default=str)
    if "backend_types" in record and "backend_types" not in raw_lineage:
        raw_lineage["backend_types"] = json.dumps(record.get("backend_types"), default=str)

    backend_type = record.get("backend_type")
    if backend_type is None:
        backend_types = record.get("backend_types")
        if isinstance(backend_types, Mapping) and backend_types:
            unique = sorted({str(value) for value in backend_types.values()})
            backend_type = unique[0] if len(unique) == 1 else "mixed"
        else:
            backend_type = "unknown"

    event = HandSummaryEventSchema(
        payload_version=str(record.get("payload_version") or PAYLOAD_VERSION),
        event_type="hand_summary",
        source_type=str(record.get("source_type") or "simulated"),
        source_dataset=record.get("source_dataset"),
        source_run_id=record.get("source_run_id"),
        simulation_run_id=record.get("simulation_run_id"),
        user_id=record.get("user_id"),
        decision_backend=record.get("decision_backend"),
        hero_context_hash=record.get("hero_context_hash"),
        hero_seat=None if record.get("hero_seat") is None else _coerce_int(record.get("hero_seat")),
        table_id=str(record.get("table_id") or "table_1"),
        hand_id=str(record.get("hand_id")),
        button_seat=_coerce_int(record.get("button_seat"), 1),
        small_blind_bb=None if record.get("small_blind_bb") is None else _coerce_float(record.get("small_blind_bb")),
        big_blind_bb=None if record.get("big_blind_bb") is None else _coerce_float(record.get("big_blind_bb")),
        board_cards=_coerce_list(record.get("board_cards")),
        started_at=_coerce_datetime(record.get("started_at")),
        finished_at=_coerce_datetime(record.get("finished_at")),
        winner_player_ids=_coerce_list(record.get("winner_player_ids", record.get("winners"))),
        total_pot_bb=_coerce_float(record.get("total_pot_bb", record.get("pot_bb"))),
        rake_bb=_coerce_float(record.get("rake_bb")),
        starting_stacks_bb={str(k): float(v) for k, v in _coerce_lineage(record.get("starting_stacks_bb")).items()} if isinstance(record.get("starting_stacks_bb"), Mapping) else {},
        final_stacks_bb={str(k): float(v) for k, v in _coerce_lineage(record.get("final_stacks_bb")).items()} if isinstance(record.get("final_stacks_bb"), Mapping) else {},
        collections_bb={str(k): float(v) for k, v in _coerce_lineage(record.get("collections_bb")).items()} if isinstance(record.get("collections_bb"), Mapping) else {},
        player_personas={str(k): str(v) for k, v in _coerce_lineage(record.get("player_personas")).items()} if isinstance(record.get("player_personas"), Mapping) else {},
        agent_ids={str(k): str(v) for k, v in _coerce_lineage(record.get("agent_ids", record.get("player_agent_ids"))).items()} if isinstance(record.get("agent_ids", record.get("player_agent_ids")), Mapping) else {},
        player_agent_ids={str(k): str(v) for k, v in _coerce_lineage(record.get("player_agent_ids", record.get("agent_ids"))).items()} if isinstance(record.get("player_agent_ids", record.get("agent_ids")), Mapping) else {},
        seats={str(k): int(v) for k, v in _coerce_lineage(record.get("seats", record.get("player_seats"))).items()} if isinstance(record.get("seats", record.get("player_seats")), Mapping) else {},
        player_seats={str(k): int(v) for k, v in _coerce_lineage(record.get("player_seats", record.get("seats"))).items()} if isinstance(record.get("player_seats", record.get("seats")), Mapping) else {},
        showdown_player_ids=_coerce_list(record.get("showdown_player_ids")),
        player_hole_cards={
            str(player_id): _coerce_list(cards)
            for player_id, cards in (record.get("player_hole_cards") or {}).items()
        }
        if isinstance(record.get("player_hole_cards"), Mapping)
        else {},
        backend_types={str(k): str(v) for k, v in _coerce_lineage(record.get("backend_types")).items()} if isinstance(record.get("backend_types"), Mapping) else {},
        backend_type=str(backend_type),
        raw_lineage=raw_lineage,
    )
    return _model_to_json_dict(event)


def raw_action_row_from_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    event = _model_to_json_dict(_validate_model(ActionEventSchema, payload))
    event["board_cards_visible"] = json.dumps(event["board_cards_visible"])
    event["hole_cards_visible"] = json.dumps(event["hole_cards_visible"])
    event["raw_lineage"] = json.dumps(_coerce_lineage(event["raw_lineage"]))
    return event


def raw_hand_row_from_summary_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    summary = _model_to_json_dict(_validate_model(HandSummaryEventSchema, payload))
    return {
        "hand_id": summary["hand_id"],
        "source_run_id": summary.get("source_run_id"),
        "source_type": summary["source_type"],
        "source_dataset": summary.get("source_dataset"),
        "simulation_run_id": summary.get("simulation_run_id"),
        "user_id": summary.get("user_id"),
        "decision_backend": summary.get("decision_backend"),
        "hero_context_hash": summary.get("hero_context_hash"),
        "hero_seat": summary.get("hero_seat"),
        "table_id": summary["table_id"],
        "button_seat": summary["button_seat"],
        "big_blind_bb": summary.get("big_blind_bb"),
        "small_blind_bb": summary.get("small_blind_bb"),
        "total_pot_bb": summary["total_pot_bb"],
        "board_cards": json.dumps(summary["board_cards"]),
        "started_at": summary["started_at"],
        "finished_at": summary["finished_at"],
        "payload_version": summary["payload_version"],
        "raw_lineage": json.dumps(_coerce_lineage(summary["raw_lineage"])),
    }
