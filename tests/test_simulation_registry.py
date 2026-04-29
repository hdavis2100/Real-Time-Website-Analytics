from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

import simulator.run_registry as registry


class _FakeWarehouse:
    def __init__(self) -> None:
        self.tables: dict[str, pd.DataFrame] = {
            "RAW_SIMULATION_RUNS": pd.DataFrame(),
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(),
        }
        self.upserts: list[tuple[str, list[str]]] = []

    def delete_rows(self, table, *, filters_eq=None, **_kwargs) -> None:
        frame = self.tables.get(table, pd.DataFrame()).copy()
        if frame.empty or not filters_eq:
            self.tables[table] = pd.DataFrame(columns=frame.columns)
            return
        mask = pd.Series(True, index=frame.index)
        for column, value in filters_eq.items():
            if column not in frame.columns:
                continue
            mask &= frame[column] == value
        self.tables[table] = frame.loc[~mask].reset_index(drop=True)

    def write_dataframe(self, table, frame, **_kwargs) -> None:
        existing = self.tables.get(table, pd.DataFrame()).copy()
        if existing.empty:
            self.tables[table] = frame.reset_index(drop=True).copy()
            return
        self.tables[table] = pd.concat([existing, frame], ignore_index=True)

    def upsert_dataframe(self, table, frame, *, key_columns, **_kwargs) -> None:
        self.upserts.append((table, list(key_columns)))
        existing = self.tables.get(table, pd.DataFrame()).copy()
        incoming = frame.reset_index(drop=True).copy()
        if existing.empty:
            self.tables[table] = incoming
            return

        remaining = existing.copy()
        for _, row in incoming[list(key_columns)].drop_duplicates().iterrows():
            mask = pd.Series(True, index=remaining.index)
            for column in key_columns:
                mask &= remaining[column].astype("object") == row[column]
            remaining = remaining.loc[~mask]
        if remaining.empty:
            self.tables[table] = incoming
        else:
            self.tables[table] = pd.concat([remaining, incoming], ignore_index=True)

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


def test_queue_run_record_and_status_payload_round_trip() -> None:
    warehouse = _FakeWarehouse()
    queued = registry.queue_run_record(
        {
            "simulation_run_id": "sim_registry_1",
            "table_id": "table_1",
            "hand_count": 500,
            "seed": 42,
            "small_blind_bb": 0.5,
            "big_blind_bb": 1.0,
            "starting_stack_bb": 100.0,
            "context": "balanced aggressive value bettor",
            "hero_seat": 2,
            "requested_at": "2026-04-19T10:00:00Z",
        },
        warehouse=warehouse,
    )

    assert queued["status"] == "queued"
    payload = registry.get_run_status_payload("sim_registry_1", warehouse=warehouse)
    assert payload is not None
    assert payload["simulation_run_id"] == "sim_registry_1"
    assert payload["status"] == "queued"
    assert payload["player_summaries"] == []
    assert payload["config_json"]["decision_backend"] == "llm"


def test_queue_run_record_rejects_duplicate_run_ids() -> None:
    warehouse = _FakeWarehouse()
    registry.queue_run_record(
        {
            "simulation_run_id": "sim_registry_dup",
            "context": "tight aggressive",
        },
        warehouse=warehouse,
    )

    with pytest.raises(ValueError, match="already exists"):
        registry.queue_run_record(
            {
                "simulation_run_id": "sim_registry_dup",
                "context": "loose aggressive",
            },
            warehouse=warehouse,
        )


def test_replace_player_summaries_updates_status_payload_result() -> None:
    warehouse = _FakeWarehouse()
    registry.queue_run_record(
        {
            "simulation_run_id": "sim_registry_summary",
            "context": "balanced aggressive",
        },
        warehouse=warehouse,
    )
    registry.replace_player_summaries(
        "sim_registry_summary",
        [
            {
                "simulation_run_id": "sim_registry_summary",
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
        warehouse=warehouse,
    )

    payload = registry.get_run_status_payload("sim_registry_summary", warehouse=warehouse)
    assert payload is not None
    assert payload["result"]["performance"][0]["player_id"] == "user_agent"
    assert payload["player_summaries"][0]["stack_reset_count"] == 3


def test_queue_run_record_supports_heuristic_backend() -> None:
    warehouse = _FakeWarehouse()
    queued = registry.queue_run_record(
        {
            "simulation_run_id": "sim_registry_heuristic",
            "context": "play very tight",
            "decision_backend": "heuristic",
        },
        warehouse=warehouse,
    )

    assert queued["backend_type"] == "heuristic_persona"
    assert queued["model_name"] is None
    assert queued["config_json"]["decision_backend"] == "heuristic"
    assert queued["config_json"]["always_model_postflop"] is False
    assert queued["config_json"]["reasoning_effort"] is None


def test_warehouse_helper_can_skip_schema_bootstrap(monkeypatch) -> None:
    warehouse = _FakeWarehouse()

    monkeypatch.setenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP", "1")
    monkeypatch.setattr(registry, "get_warehouse", lambda *_args, **_kwargs: warehouse)
    monkeypatch.setattr(registry, "get_config", lambda: None)
    monkeypatch.setattr(
        registry,
        "bootstrap_backend",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("bootstrap should be skipped")),
    )

    try:
        assert registry._warehouse_or_default() is warehouse
    finally:
        monkeypatch.delenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP", raising=False)


def test_run_record_updates_use_upsert_when_available() -> None:
    warehouse = _FakeWarehouse()
    registry.queue_run_record(
        {
            "simulation_run_id": "sim_registry_upsert",
            "context": "balanced aggressive",
        },
        warehouse=warehouse,
    )

    updated = registry.update_run_record(
        "sim_registry_upsert",
        warehouse=warehouse,
        status="running",
        started_at=datetime(2026, 4, 20, 12, 0),
    )

    assert updated["status"] == "running"
    assert ("RAW_SIMULATION_RUNS", ["simulation_run_id"]) in warehouse.upserts
