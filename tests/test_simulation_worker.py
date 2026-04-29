from __future__ import annotations

from datetime import datetime
import threading

import pandas as pd
import pytest

import simulator.run_registry as registry
import simulator.worker as worker_module


class _FakeWarehouse:
    def __init__(self) -> None:
        self.tables: dict[str, pd.DataFrame] = {
            "RAW_SIMULATION_RUNS": pd.DataFrame(),
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(),
        }

    def delete_rows(self, table, *, filters_eq=None, **_kwargs) -> None:
        frame = self.tables.get(table, pd.DataFrame()).copy()
        if frame.empty or not filters_eq:
            self.tables[table] = pd.DataFrame(columns=frame.columns)
            return
        mask = pd.Series(True, index=frame.index)
        for column, value in filters_eq.items():
            if column in frame.columns:
                mask &= frame[column] == value
        self.tables[table] = frame.loc[~mask].reset_index(drop=True)

    def write_dataframe(self, table, frame, **_kwargs) -> None:
        existing = self.tables.get(table, pd.DataFrame()).copy()
        if existing.empty:
            self.tables[table] = frame.reset_index(drop=True).copy()
            return
        self.tables[table] = pd.concat([existing, frame], ignore_index=True)

    def load_table(self, table, *, filters_eq=None, order_by=None, limit=None, **_kwargs) -> pd.DataFrame:
        frame = self.tables.get(table, pd.DataFrame()).copy()
        if frame.empty:
            return frame
        if filters_eq:
            for column, value in filters_eq.items():
                if column in frame.columns:
                    frame = frame.loc[frame[column] == value]
        if order_by:
            by: list[str] = []
            ascending: list[bool] = []
            for item in order_by:
                if isinstance(item, tuple):
                    column, is_ascending = item
                else:
                    column, is_ascending = item, True
                if column in frame.columns:
                    by.append(column)
                    ascending.append(is_ascending)
            if by:
                frame = frame.sort_values(by=by, ascending=ascending)
        if limit is not None:
            frame = frame.head(limit)
        return frame.reset_index(drop=True)


class _FailingWarehouse(_FakeWarehouse):
    def load_table(self, table, **kwargs):  # type: ignore[override]
        raise RuntimeError("warehouse unavailable")

    def write_dataframe(self, table, frame, **kwargs):  # type: ignore[override]
        raise RuntimeError("warehouse unavailable")

    def delete_rows(self, table, **kwargs):  # type: ignore[override]
        raise RuntimeError("warehouse unavailable")


def _queued_payload() -> dict[str, object]:
    return {
        "simulation_run_id": "sim_worker_1",
        "table_id": "table_1",
        "hand_count": 500,
        "seed": 42,
        "small_blind_bb": 0.5,
        "big_blind_bb": 1.0,
        "starting_stack_bb": 100.0,
        "context": "balanced aggressive value bettor",
        "hero_seat": 1,
        "decision_backend": "llm",
        "model_name": "gpt-5.4-nano",
        "reasoning_effort": "none",
        "requested_at": "2026-04-19T10:00:00Z",
    }


def test_process_request_message_marks_run_finalizing_and_writes_summaries(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    registry.queue_run_record(_queued_payload(), warehouse=warehouse)
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)
    monkeypatch.setattr(
        worker_module,
        "simulate_and_record",
        lambda **_kwargs: {
            "hands": 500,
            "actions": 2000,
            "published_actions": 2000,
            "published_hand_summaries": 500,
            "player_summaries": [
                {
                    "simulation_run_id": "sim_worker_1",
                    "seat": 1,
                    "player_id": "user_agent",
                    "agent_id": "user_agent",
                    "persona_name": "hero_style",
                    "backend_type": "llm_gated_nano",
                    "hands_played": 500,
                    "total_bb_won": 12.5,
                    "avg_bb_per_hand": 0.025,
                    "bb_per_100": 2.5,
                    "stack_reset_count": 3,
                    "llm_decision_count": 1200,
                    "heuristic_decision_count": 200,
                    "llm_fallback_count": 5,
                    "final_rank": 1,
                    "updated_at": datetime(2026, 4, 19, 10, 5),
                }
            ],
        },
    )

    result = worker_module.process_request_message(
        _queued_payload(),
        warehouse=warehouse,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    assert result["status"] == "finalizing"
    stored = registry.get_run_status_payload("sim_worker_1", warehouse=warehouse)
    assert stored is not None
    assert stored["status"] == "finalizing"
    assert stored["player_summaries"][0]["player_id"] == "user_agent"
    assert [event["event_type"] for event in lifecycle_events] == [
        "simulation_started",
        "simulation_completed",
    ]


def test_process_request_message_publishes_running_progress(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    registry.queue_run_record(_queued_payload(), warehouse=warehouse)
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    def fake_simulate_and_record(**kwargs):
        kwargs["progress_callback"](
            {
                "hands": 50,
                "actions": 200,
                "published_actions": 200,
                "published_hand_summaries": 50,
                "player_summaries": [
                    {
                        "simulation_run_id": "sim_worker_1",
                        "seat": 1,
                        "player_id": "user_agent",
                        "agent_id": "user_agent",
                        "persona_name": "hero_style",
                        "backend_type": "llm_gated_nano",
                        "hands_played": 50,
                        "total_bb_won": 5.0,
                        "avg_bb_per_hand": 0.1,
                        "bb_per_100": 10.0,
                        "stack_reset_count": 0,
                        "llm_decision_count": 120,
                        "heuristic_decision_count": 20,
                        "llm_fallback_count": 1,
                        "showdown_hand_category": "pair",
                        "showdown_hand_score": 100.0,
                        "final_rank": 1,
                        "updated_at": datetime(2026, 4, 19, 10, 2),
                    }
                ],
            }
        )
        return {
            "hands": 500,
            "actions": 2000,
            "published_actions": 2000,
            "published_hand_summaries": 500,
            "player_summaries": [
                {
                    "simulation_run_id": "sim_worker_1",
                    "seat": 1,
                    "player_id": "user_agent",
                    "agent_id": "user_agent",
                    "persona_name": "hero_style",
                    "backend_type": "llm_gated_nano",
                    "hands_played": 500,
                    "total_bb_won": 12.5,
                    "avg_bb_per_hand": 0.025,
                    "bb_per_100": 2.5,
                    "stack_reset_count": 3,
                    "llm_decision_count": 1200,
                    "heuristic_decision_count": 200,
                    "llm_fallback_count": 5,
                    "showdown_hand_category": "two_pair",
                    "showdown_hand_score": 200.0,
                    "final_rank": 1,
                    "updated_at": datetime(2026, 4, 19, 10, 5),
                }
            ],
        }

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)
    monkeypatch.setattr(worker_module, "simulate_and_record", fake_simulate_and_record)

    worker_module.process_request_message(
        _queued_payload(),
        warehouse=warehouse,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    assert [event["event_type"] for event in lifecycle_events] == [
        "simulation_started",
        "simulation_progress",
        "simulation_completed",
    ]
    assert lifecycle_events[1]["published_actions"] == 200
    assert lifecycle_events[1]["player_summaries"][0]["player_id"] == "user_agent"


def test_process_request_message_marks_run_failed_on_exception(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    registry.queue_run_record(_queued_payload(), warehouse=warehouse)
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)
    monkeypatch.setattr(worker_module, "simulate_and_record", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        worker_module.process_request_message(
            _queued_payload(),
            warehouse=warehouse,
            producer=object(),
            brokers="localhost:9092",
            actions_topic="poker.actions",
            summaries_topic="poker.hand_summaries",
            events_topic="poker.simulation_events",
            legacy_registry_enabled=True,
        )

    stored = registry.get_run_status_payload("sim_worker_1", warehouse=warehouse)
    assert stored is not None
    assert stored["status"] == "failed"
    assert stored["error_message"] == "boom"
    assert [event["event_type"] for event in lifecycle_events] == [
        "simulation_started",
        "simulation_failed",
    ]


def test_process_request_message_skips_duplicate_completed_run(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    registry.queue_run_record(_queued_payload(), warehouse=warehouse)
    registry.update_run_record("sim_worker_1", warehouse=warehouse, status="completed")
    monkeypatch.setattr(worker_module, "publish_records", lambda **_kwargs: 0)

    result = worker_module.process_request_message(
        _queued_payload(),
        warehouse=warehouse,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    assert result["skipped"] is True
    assert result["status"] == "completed"


def test_process_request_message_supports_heuristic_backend(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_heuristic",
        "decision_backend": "heuristic",
        "model_name": None,
        "reasoning_effort": None,
    }
    registry.queue_run_record(payload, warehouse=warehouse)
    captured_players: dict[str, object] = {}
    captured_simulation_kwargs: dict[str, object] = {}

    def fake_build_context_matchup_players(*args, **kwargs):
        captured_players.update(kwargs)
        return []

    monkeypatch.setattr(worker_module, "build_context_matchup_players", fake_build_context_matchup_players)
    monkeypatch.setattr(worker_module, "publish_records", lambda **_kwargs: 1)
    def fake_simulate_and_record(**kwargs):
        captured_simulation_kwargs.update(kwargs)
        return {
            "hands": 12,
            "actions": 96,
            "published_actions": 96,
            "published_hand_summaries": 12,
            "player_summaries": [],
        }
    monkeypatch.setattr(
        worker_module,
        "simulate_and_record",
        fake_simulate_and_record,
    )

    result = worker_module.process_request_message(
        payload,
        warehouse=warehouse,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    stored = registry.get_run_status_payload("sim_worker_heuristic", warehouse=warehouse)
    assert result["status"] == "finalizing"
    assert captured_players["backend_type"] == "heuristic_persona"
    assert captured_players["model_name"] is None
    assert captured_players["reasoning_effort"] is None
    assert captured_players["always_model_postflop"] is False
    assert captured_players["heuristic_compiler_timeout_seconds"] == 15.0
    assert captured_simulation_kwargs["carry_stacks_between_hands"] is False
    assert captured_simulation_kwargs["progress_interval_hands"] == 5
    assert stored is not None
    assert captured_simulation_kwargs["run_started_at"] == stored["started_at"]
    assert stored["backend_type"] == "heuristic_persona"
    assert stored["model_name"] is None


def test_process_request_message_uses_per_hand_progress_for_llm_backend(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_llm_progress",
        "decision_backend": "llm",
    }
    registry.queue_run_record(payload, warehouse=warehouse)
    captured_simulation_kwargs: dict[str, object] = {}

    monkeypatch.setattr(worker_module, "build_context_matchup_players", lambda *args, **kwargs: [])
    monkeypatch.setattr(worker_module, "publish_records", lambda **_kwargs: 1)

    def fake_simulate_and_record(**kwargs):
        captured_simulation_kwargs.update(kwargs)
        return {
            "hands": 12,
            "actions": 96,
            "published_actions": 96,
            "published_hand_summaries": 12,
            "player_summaries": [],
        }

    monkeypatch.setattr(worker_module, "simulate_and_record", fake_simulate_and_record)

    result = worker_module.process_request_message(
        payload,
        warehouse=warehouse,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    assert result["status"] == "finalizing"
    assert captured_simulation_kwargs["progress_interval_hands"] == 1


def test_process_request_message_can_preserve_session_stacks_when_requested(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_carry_stacks",
        "reset_stacks_each_hand": False,
    }
    registry.queue_run_record(payload, warehouse=warehouse)
    captured_simulation_kwargs: dict[str, object] = {}

    monkeypatch.setattr(worker_module, "build_context_matchup_players", lambda *args, **kwargs: [])
    monkeypatch.setattr(worker_module, "publish_records", lambda **_kwargs: 1)

    def fake_simulate_and_record(**kwargs):
        captured_simulation_kwargs.update(kwargs)
        return {
            "hands": 12,
            "actions": 96,
            "published_actions": 96,
            "published_hand_summaries": 12,
            "player_summaries": [],
        }

    monkeypatch.setattr(worker_module, "simulate_and_record", fake_simulate_and_record)

    result = worker_module.process_request_message(
        payload,
        warehouse=warehouse,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    stored = registry.get_run_status_payload("sim_worker_carry_stacks", warehouse=warehouse)
    assert result["status"] == "finalizing"
    assert captured_simulation_kwargs["carry_stacks_between_hands"] is True
    assert stored is not None
    assert stored["config_json"]["reset_stacks_each_hand"] is False


def test_handle_consumer_exception_marks_queued_run_failed(monkeypatch) -> None:
    warehouse = _FakeWarehouse()
    payload = _queued_payload()
    registry.queue_run_record(payload, warehouse=warehouse)
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)

    worker_module._handle_consumer_exception(  # type: ignore[attr-defined]
        payload,
        RuntimeError("consumer boom"),
        warehouse=warehouse,
        producer=object(),
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    stored = registry.get_run_status_payload("sim_worker_1", warehouse=warehouse)
    assert stored is not None
    assert stored["status"] == "failed"
    assert stored["error_message"] == "consumer boom"
    assert [event["event_type"] for event in lifecycle_events] == ["simulation_failed"]


def test_process_request_message_continues_when_legacy_registry_is_unavailable(monkeypatch) -> None:
    warehouse = _FailingWarehouse()
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_best_effort",
        "decision_backend": "heuristic",
        "model_name": None,
        "reasoning_effort": None,
    }
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)
    monkeypatch.setattr(worker_module, "build_context_matchup_players", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        worker_module,
        "simulate_and_record",
        lambda **_kwargs: {
            "hands": 12,
            "actions": 96,
            "published_actions": 96,
            "published_hand_summaries": 12,
            "player_summaries": [],
        },
    )

    result = worker_module.process_request_message(
        payload,
        warehouse=warehouse,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    assert result["status"] == "finalizing"
    assert [event["event_type"] for event in lifecycle_events] == [
        "simulation_started",
        "simulation_completed",
    ]


def test_handle_consumer_exception_publishes_failed_event_when_legacy_registry_is_unavailable(monkeypatch) -> None:
    warehouse = _FailingWarehouse()
    payload = _queued_payload()
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)

    worker_module._handle_consumer_exception(  # type: ignore[attr-defined]
        payload,
        RuntimeError("consumer boom"),
        warehouse=warehouse,
        producer=object(),
        events_topic="poker.simulation_events",
        legacy_registry_enabled=True,
    )

    assert [event["event_type"] for event in lifecycle_events] == ["simulation_failed"]
    assert lifecycle_events[0]["error_message"] == "consumer boom"


def test_process_request_message_skips_legacy_registry_calls_when_compatibility_disabled(monkeypatch) -> None:
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_no_legacy",
        "decision_backend": "heuristic",
        "model_name": None,
        "reasoning_effort": None,
    }
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)
    monkeypatch.setattr(worker_module, "build_context_matchup_players", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        worker_module,
        "get_run_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy read should be disabled")),
    )
    monkeypatch.setattr(
        worker_module,
        "queue_run_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy enqueue should be disabled")),
    )
    monkeypatch.setattr(
        worker_module,
        "update_run_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy update should be disabled")),
    )
    monkeypatch.setattr(
        worker_module,
        "replace_player_summaries",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy summary writes should be disabled")),
    )
    monkeypatch.setattr(
        worker_module,
        "simulate_and_record",
        lambda **_kwargs: {
            "hands": 12,
            "actions": 96,
            "published_actions": 96,
            "published_hand_summaries": 12,
            "player_summaries": [],
        },
    )

    result = worker_module.process_request_message(
        payload,
        warehouse=None,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=False,
    )

    assert result["status"] == "finalizing"
    assert [event["event_type"] for event in lifecycle_events] == [
        "simulation_started",
        "simulation_completed",
    ]


def test_process_request_message_skips_replayed_app_run_when_compatibility_disabled(monkeypatch) -> None:
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_app_completed",
    }

    monkeypatch.setattr(
        worker_module,
        "_best_effort_get_app_run_record",
        lambda _simulation_run_id: {
            "simulation_run_id": "sim_worker_app_completed",
            "status": "completed",
            "started_at": "2026-04-22T20:00:00Z",
            "completed_at": "2026-04-22T20:05:00Z",
            "published_actions": 100,
            "published_hand_summaries": 12,
            "player_summaries": [{"player_id": "user_agent"}],
        },
    )
    monkeypatch.setattr(
        worker_module,
        "build_context_matchup_players",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("replayed completed app run should be skipped")
        ),
    )

    result = worker_module.process_request_message(
        payload,
        warehouse=None,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=False,
    )

    assert result == {
        "simulation_run_id": "sim_worker_app_completed",
        "status": "completed",
        "skipped": True,
    }


def test_process_request_message_skips_stale_queued_app_run_once_progress_exists(monkeypatch) -> None:
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_app_running",
    }

    monkeypatch.setattr(
        worker_module,
        "_best_effort_get_app_run_record",
        lambda _simulation_run_id: {
            "simulation_run_id": "sim_worker_app_running",
            "status": "queued",
            "started_at": "2026-04-22T20:00:00Z",
            "completed_at": None,
            "published_actions": 250,
            "published_hand_summaries": 25,
            "player_summaries": [{"player_id": "user_agent"}],
        },
    )
    monkeypatch.setattr(
        worker_module,
        "build_context_matchup_players",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("stale queued app run with progress should be skipped")
        ),
    )

    result = worker_module.process_request_message(
        payload,
        warehouse=None,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=False,
    )

    assert result == {
        "simulation_run_id": "sim_worker_app_running",
        "status": "running",
        "skipped": True,
    }


def test_process_request_message_skips_missing_app_run_when_app_registry_enabled(monkeypatch) -> None:
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_missing_app",
    }

    monkeypatch.setenv("APP_INTERNAL_BASE_URL", "http://app:3000")
    monkeypatch.setattr(
        worker_module,
        "_best_effort_get_app_run_record",
        lambda _run_id: worker_module._APP_RUN_MISSING,  # type: ignore[attr-defined]
    )
    monkeypatch.setattr(
        worker_module,
        "build_context_matchup_players",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("missing app run should be skipped")
        ),
    )

    result = worker_module.process_request_message(
        payload,
        warehouse=None,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=False,
    )

    assert result == {
        "simulation_run_id": "sim_worker_missing_app",
        "status": "missing",
        "skipped": True,
        "reason": "app_run_not_found",
    }


def test_process_request_message_skips_stale_app_request_for_reused_run_id(monkeypatch) -> None:
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_reused",
        "hand_count": 5000,
        "requested_at": "2026-04-19T10:00:00Z",
    }

    monkeypatch.setenv("APP_INTERNAL_BASE_URL", "http://app:3000")
    monkeypatch.setattr(
        worker_module,
        "_best_effort_get_app_run_record",
        lambda _simulation_run_id: {
            "simulation_run_id": "sim_worker_reused",
            "status": "queued",
            "hand_count": 80,
            "seed": 42,
            "hero_seat": 1,
            "decision_backend": "llm",
            "hero_context_hash": payload.get("hero_context_hash"),
            "requested_at": "2026-04-19T10:05:00Z",
            "published_actions": 0,
            "published_hand_summaries": 0,
            "player_summaries": [],
        },
    )
    monkeypatch.setattr(
        worker_module,
        "build_context_matchup_players",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("stale app request should be skipped")
        ),
    )

    result = worker_module.process_request_message(
        payload,
        warehouse=None,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=False,
    )

    assert result == {
        "simulation_run_id": "sim_worker_reused",
        "status": "queued",
        "skipped": True,
        "reason": "stale_request",
    }


def test_process_request_message_halts_when_app_run_is_canceled_during_progress(monkeypatch) -> None:
    payload = {
        **_queued_payload(),
        "simulation_run_id": "sim_worker_cancel",
    }
    lifecycle_events: list[dict] = []
    app_records = iter(
        [
            {
                "simulation_run_id": "sim_worker_cancel",
                "status": "queued",
                "hand_count": 500,
                "seed": 42,
                "hero_seat": 1,
                "decision_backend": "llm",
                "requested_at": payload["requested_at"],
                "published_actions": 0,
                "published_hand_summaries": 0,
                "player_summaries": [],
            },
            {
                "simulation_run_id": "sim_worker_cancel",
                "status": "failed",
                "hand_count": 500,
                "seed": 42,
                "hero_seat": 1,
                "decision_backend": "llm",
                "requested_at": payload["requested_at"],
                "error_message": "Canceled by user",
            },
        ]
    )

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    def fake_simulate_and_record(**kwargs):
        kwargs["progress_callback"](
            {
                "hands": 5,
                "actions": 20,
                "published_actions": 20,
                "published_hand_summaries": 5,
                "player_summaries": [],
            }
        )
        raise AssertionError("canceled run should halt during progress")

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)
    monkeypatch.setattr(
        worker_module,
        "_best_effort_get_app_run_record",
        lambda _simulation_run_id: next(app_records),
    )
    monkeypatch.setattr(worker_module, "build_context_matchup_players", lambda *args, **kwargs: [])
    monkeypatch.setattr(worker_module, "simulate_and_record", fake_simulate_and_record)

    result = worker_module.process_request_message(
        payload,
        warehouse=None,
        producer=object(),
        brokers="localhost:9092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        legacy_registry_enabled=False,
    )

    assert result == {
        "simulation_run_id": "sim_worker_cancel",
        "status": "failed",
        "skipped": True,
        "reason": "app_run_failed",
    }
    assert [event["event_type"] for event in lifecycle_events] == [
        "simulation_started",
    ]


def test_handle_consumer_exception_skips_legacy_registry_calls_when_compatibility_disabled(monkeypatch) -> None:
    payload = _queued_payload()
    lifecycle_events: list[dict] = []

    def fake_publish_records(*, records, **_kwargs):
        lifecycle_events.extend(records)
        return len(records)

    monkeypatch.setattr(worker_module, "publish_records", fake_publish_records)
    monkeypatch.setattr(
        worker_module,
        "get_run_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy read should be disabled")),
    )
    monkeypatch.setattr(
        worker_module,
        "update_run_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy update should be disabled")),
    )

    worker_module._handle_consumer_exception(  # type: ignore[attr-defined]
        payload,
        RuntimeError("consumer boom"),
        warehouse=None,
        producer=object(),
        events_topic="poker.simulation_events",
        legacy_registry_enabled=False,
    )

    assert [event["event_type"] for event in lifecycle_events] == ["simulation_failed"]
    assert lifecycle_events[0]["error_message"] == "consumer boom"


def test_consume_forever_starts_requested_number_of_consumer_threads(monkeypatch) -> None:
    started_threads: list[str] = []
    start_gate = threading.Event()

    def fake_consume_loop(*, stop_event, **_kwargs):
        started_threads.append(threading.current_thread().name)
        if len(started_threads) == 3:
            start_gate.set()
            stop_event.set()
        assert start_gate.wait(timeout=1.0)

    monkeypatch.setattr(worker_module, "_consume_loop", fake_consume_loop)
    monkeypatch.setattr(worker_module.signal, "signal", lambda *_args, **_kwargs: None)

    worker_module.consume_forever(
        brokers="localhost:9092",
        requests_topic="poker.simulation_requests",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        group_id="poker-simulation-worker",
        concurrency=3,
        legacy_registry_enabled=False,
    )

    assert sorted(started_threads) == [
        "simulation-worker-1",
        "simulation-worker-2",
        "simulation-worker-3",
    ]


def test_consume_loop_tolerates_retriable_commit_failures(monkeypatch) -> None:
    processed_payloads: list[dict[str, object]] = []

    class CommitFailedError(Exception):
        pass

    class _FakeMessage:
        def __init__(self, value):
            self.value = value

    class _FakeConsumer:
        def __init__(self, stop_event: threading.Event) -> None:
            self._stop_event = stop_event
            self.poll_calls = 0
            self.commit_calls = 0

        def poll(self, timeout_ms=0, max_records=0):
            self.poll_calls += 1
            if self.poll_calls == 1:
                return {
                    "partition-0": [_FakeMessage({"simulation_run_id": "sim_commit_retry_1"})]
                }
            self._stop_event.set()
            return {}

        def commit(self):
            self.commit_calls += 1
            raise CommitFailedError("consumer rebalanced")

        def close(self):
            return None

    class _FakeProducer:
        def close(self):
            return None

    stop_event = threading.Event()
    fake_consumer = _FakeConsumer(stop_event)

    monkeypatch.setattr(worker_module, "bootstrap_backend", lambda: (_ for _ in ()).throw(AssertionError("legacy registry should be disabled")))
    monkeypatch.setattr(worker_module, "build_producer", lambda _brokers: _FakeProducer())
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda *_args, **_kwargs: fake_consumer,
    )
    monkeypatch.setattr(
        worker_module,
        "process_request_message",
        lambda payload, **_kwargs: processed_payloads.append(payload),
    )

    worker_module._consume_loop(  # type: ignore[attr-defined]
        brokers="localhost:9092",
        requests_topic="poker.simulation_requests",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        events_topic="poker.simulation_events",
        group_id="poker-simulation-worker",
        max_poll_interval_ms=1800000,
        stop_event=stop_event,
        legacy_registry_enabled=False,
    )

    assert processed_payloads == [{"simulation_run_id": "sim_commit_retry_1"}]
    assert fake_consumer.commit_calls == 1
