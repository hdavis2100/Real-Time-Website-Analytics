from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

import jobs.stream_kafka as stream_module


class _ExplodingRdd:
    def isEmpty(self):  # pragma: no cover - this should never be called
        raise AssertionError("RDD-based emptiness checks should not be used")


class _FakeSparkFrame:
    def __init__(self, *, empty: bool) -> None:
        self.empty = bool(empty)
        self.rdd = _ExplodingRdd()
        self.take_calls = 0

    def isEmpty(self) -> bool:
        return self.empty

    def take(self, _count: int):
        self.take_calls += 1
        return []


class _FakeSparkRdd:
    def __init__(self, partitions: int) -> None:
        self.partitions = int(partitions)

    def getNumPartitions(self) -> int:
        return self.partitions


class _FakeStreamingFrame:
    def __init__(self, *, partitions: int) -> None:
        self.rdd = _FakeSparkRdd(partitions)
        self.repartition_calls: list[int] = []
        self.prefetch_flags: list[bool] = []

    def repartition(self, partitions: int):
        self.repartition_calls.append(int(partitions))
        self.rdd = _FakeSparkRdd(partitions)
        return self

    def toLocalIterator(self, *, prefetchPartitions: bool):
        self.prefetch_flags.append(bool(prefetchPartitions))
        return iter(())


def _action_rows() -> pd.DataFrame:
    base = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
    return pd.DataFrame(
        [
            {
                "simulation_run_id": "sim_1",
                "agent_id": "a1",
                "player_id": "p1",
                "persona_name": "lag",
                "hand_id": "h1",
                "street": "preflop",
                "action_index": 0,
                "action_type": "raise",
                "is_all_in": False,
                "event_ts": base,
                "board_cards_visible": "[]",
                "hole_cards_visible": "[]",
            },
            {
                "simulation_run_id": "sim_1",
                "agent_id": "a1",
                "player_id": "p1",
                "persona_name": "lag",
                "hand_id": "h1",
                "street": "flop",
                "action_index": 1,
                "action_type": "bet",
                "is_all_in": False,
                "event_ts": base + timedelta(seconds=5),
                "board_cards_visible": '["Ah","Kd","2c"]',
                "hole_cards_visible": "[]",
            },
            {
                "simulation_run_id": "sim_1",
                "agent_id": "a2",
                "player_id": "p2",
                "persona_name": "calling_station",
                "hand_id": "h1",
                "street": "preflop",
                "action_index": 2,
                "action_type": "call",
                "is_all_in": False,
                "event_ts": base + timedelta(seconds=1),
                "board_cards_visible": "[]",
                "hole_cards_visible": "[]",
            },
        ]
    )


def _player_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "simulation_run_id": "sim_1",
                "agent_id": "a1",
                "player_id": "p1",
                "hand_id": "h1",
                "result_bb": 3.5,
            },
            {
                "simulation_run_id": "sim_1",
                "agent_id": "a2",
                "player_id": "p2",
                "hand_id": "h1",
                "result_bb": -3.5,
            },
        ]
    )


class _FakeWarehouse:
    def __init__(self) -> None:
        self.deleted: list[tuple[str, dict]] = []
        self.deleted_matching: list[tuple[str, list[str]]] = []
        self.written: dict[str, pd.DataFrame] = {}
        self.write_kwargs: dict[str, dict] = {}
        self.upserted: list[tuple[str, list[str], dict[str, object]]] = []
        self.model_runs = pd.DataFrame(
            [
                {
                    "model_run_id": "model_1",
                    "active": True,
                    "activated_at": datetime(2026, 4, 17, 12, 10),
                    "created_at": datetime(2026, 4, 17, 12, 0),
                }
            ]
        )
        self.centroids = pd.DataFrame(
            [
                {
                    "model_run_id": "model_1",
                    "cluster_id": 0,
                    "cluster_label": "TAG",
                    "centroid_json": '{"vpip_rate": 0.25, "pfr_rate": 0.2, "aggression_frequency": 0.5, "flop_cbet_rate": 0.45, "all_in_rate": 0.05}',
                },
                {
                    "model_run_id": "model_1",
                    "cluster_id": 1,
                    "cluster_label": "LAG",
                    "centroid_json": '{"vpip_rate": 0.75, "pfr_rate": 0.7, "aggression_frequency": 0.8, "flop_cbet_rate": 0.75, "all_in_rate": 0.2}',
                },
            ]
        )

    def delete_rows(self, table, **kwargs) -> None:
        self.deleted.append((table, kwargs))

    def delete_matching_keys(self, table, frame, key_columns) -> None:
        self.deleted_matching.append((table, list(key_columns)))

    def write_dataframe(self, table, frame, **kwargs) -> None:
        self.written[table] = frame.copy()
        self.write_kwargs[table] = dict(kwargs)

    def upsert_dataframe(self, table, frame, *, key_columns, force_bulk=True) -> None:
        self.written[table] = frame.copy()
        self.upserted.append((table, list(key_columns), {"force_bulk": force_bulk}))

    def load_table(self, table, **_kwargs) -> pd.DataFrame:
        if table == "PROFILE_MODEL_RUNS":
            return self.model_runs.copy()
        if table == "PROFILE_CLUSTER_CENTROIDS":
            return self.centroids.copy()
        return self.written.get(table, pd.DataFrame()).copy()


def test_compute_live_metrics_and_profile_assignment() -> None:
    live_metrics = stream_module.compute_live_metrics(
        _action_rows(),
        player_results=_player_rows(),
        window_minutes=5,
        window_end=datetime(2026, 4, 17, 12, 1, tzinfo=timezone.utc),
    )

    assert len(live_metrics) == 2
    assert {"actions_per_second", "hands_per_second", "vpip", "pfr", "cbet_rate", "bb_won"}.issubset(live_metrics.columns)

    lag_row = live_metrics.loc[live_metrics["agent_id"] == "a1"].iloc[0]
    assert float(lag_row["vpip"]) == 1.0
    assert float(lag_row["pfr"]) == 1.0
    assert float(lag_row["cbet_rate"]) == 1.0
    assert float(lag_row["bb_won"]) == 3.5

    centroids = pd.DataFrame(
        [
            {
                "cluster_id": 0,
                "cluster_label": "TAG",
                "centroid_json": '{"vpip_rate": 0.25, "pfr_rate": 0.2, "aggression_frequency": 0.5, "cbet_rate": 0.45, "all_in_rate": 0.05}',
            },
            {
                "cluster_id": 1,
                "cluster_label": "LAG",
                "centroid_json": '{"vpip_rate": 0.75, "pfr_rate": 0.7, "aggression_frequency": 0.8, "cbet_rate": 0.75, "all_in_rate": 0.2}',
            },
        ]
    )
    assignments = stream_module.assign_live_profiles(live_metrics, centroids)
    assert len(assignments) == 2
    assert "nearest_cluster_label" in assignments.columns


def test_spark_frame_has_rows_uses_dataframe_api_before_rdd() -> None:
    empty_frame = _FakeSparkFrame(empty=True)
    populated_frame = _FakeSparkFrame(empty=False)

    assert stream_module._spark_frame_has_rows(empty_frame) is False
    assert stream_module._spark_frame_has_rows(populated_frame) is True
    assert empty_frame.take_calls == 0
    assert populated_frame.take_calls == 0


def test_spark_stream_rows_repartitions_before_local_iteration() -> None:
    frame = _FakeStreamingFrame(partitions=4)

    rows = list(stream_module._spark_stream_rows(frame, min_partitions=16))

    assert rows == []
    assert frame.repartition_calls == [16]
    assert frame.prefetch_flags == [False]


def test_write_live_outputs_replaces_matching_run_ids(monkeypatch) -> None:
    fake_warehouse = _FakeWarehouse()
    monkeypatch.setattr(stream_module, "bootstrap_backend", lambda: fake_warehouse)

    live_metrics = pd.DataFrame(
        [
            {
                "metric_window_start": datetime(2026, 4, 17, 12, 0),
                "metric_window_end": datetime(2026, 4, 17, 12, 5),
                "simulation_run_id": "sim_1",
                "agent_id": "a1",
                "player_id": "p1",
                "persona_name": "lag",
                "actions_per_second": 0.1,
                "hands_per_second": 0.02,
                "vpip": 1.0,
                "pfr": 1.0,
                "aggression_frequency": 1.0,
                "cbet_rate": 1.0,
                "all_in_rate": 0.0,
                "bb_won": 1.25,
                "observed_hands": 1,
                "observed_actions": 2,
                "updated_at": datetime(2026, 4, 17, 12, 5),
            }
        ]
    )
    assignments = pd.DataFrame(
        [
            {
                "simulation_run_id": "sim_1",
                "agent_id": "a1",
                "player_id": "p1",
                "nearest_cluster_id": 0,
                "nearest_cluster_label": "TAG",
                "distance_to_centroid": 0.1,
                "assigned_at": datetime(2026, 4, 17, 12, 5),
            }
        ]
    )

    stream_module.write_live_outputs(live_metrics, assignments)

    assert ("LIVE_AGENT_METRICS", {"filters_in": {"simulation_run_id": ["sim_1"]}}) in fake_warehouse.deleted
    assert ("LIVE_PROFILE_ASSIGNMENTS", {"filters_in": {"simulation_run_id": ["sim_1"]}}) in fake_warehouse.deleted
    assert "LIVE_AGENT_METRICS" in fake_warehouse.written
    assert "LIVE_PROFILE_ASSIGNMENTS" in fake_warehouse.written
    assert fake_warehouse.write_kwargs["LIVE_AGENT_METRICS"]["force_bulk"] is True
    assert fake_warehouse.write_kwargs["LIVE_PROFILE_ASSIGNMENTS"]["force_bulk"] is True


def test_process_event_batch_materializes_stream_rows_and_live_outputs() -> None:
    fake_warehouse = _FakeWarehouse()
    action_payloads = [
        {
            "event_type": "action",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "action_index": 0,
            "street": "preflop",
            "player_id": "p1",
            "agent_id": "a1",
            "seat": 1,
            "position": "BTN",
            "action_type": "raise",
            "amount_bb": 2.5,
            "pot_before_bb": 1.5,
            "pot_after_bb": 4.0,
            "to_call_bb": 1.0,
            "effective_stack_bb": 100.0,
            "players_remaining": 2,
            "board_cards_visible": [],
            "hole_cards_visible": [],
            "is_all_in": False,
            "event_ts": "2026-04-17T12:00:00Z",
            "backend_type": "heuristic_persona",
            "persona_name": "lag",
            "persona_text": "loose aggressive",
            "raw_lineage": {"decision_source": "heuristic"},
        },
        {
            "event_type": "action",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "action_index": 1,
            "street": "preflop",
            "player_id": "p2",
            "agent_id": "a2",
            "seat": 2,
            "position": "SB",
            "action_type": "call",
            "amount_bb": 2.0,
            "pot_before_bb": 4.0,
            "pot_after_bb": 6.0,
            "to_call_bb": 2.0,
            "effective_stack_bb": 100.0,
            "players_remaining": 2,
            "board_cards_visible": [],
            "hole_cards_visible": [],
            "is_all_in": False,
            "event_ts": "2026-04-17T12:00:01Z",
            "backend_type": "heuristic_persona",
            "persona_name": "calling_station",
            "persona_text": "calls too much",
            "raw_lineage": {"decision_source": "heuristic_fallback"},
        },
        {
            "event_type": "action",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "action_index": 2,
            "street": "flop",
            "player_id": "p1",
            "agent_id": "a1",
            "seat": 1,
            "position": "BTN",
            "action_type": "bet",
            "amount_bb": 3.0,
            "pot_before_bb": 6.0,
            "pot_after_bb": 9.0,
            "to_call_bb": 0.0,
            "effective_stack_bb": 97.5,
            "players_remaining": 2,
            "board_cards_visible": ["Ah", "Kd", "2c"],
            "hole_cards_visible": [],
            "is_all_in": False,
            "event_ts": "2026-04-17T12:00:05Z",
            "backend_type": "heuristic_persona",
            "persona_name": "lag",
            "persona_text": "loose aggressive",
            "raw_lineage": {"decision_source": "llm"},
        },
    ]
    summary_payloads = [
        {
            "event_type": "hand_summary",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "button_seat": 1,
            "small_blind_bb": 0.5,
            "big_blind_bb": 1.0,
            "board_cards": ["Ah", "Kd", "2c"],
            "started_at": "2026-04-17T12:00:00Z",
            "finished_at": "2026-04-17T12:00:08Z",
            "winner_player_ids": ["p1"],
            "total_pot_bb": 9.0,
            "rake_bb": 0.0,
            "starting_stacks_bb": {"p1": 100.0, "p2": 100.0},
            "final_stacks_bb": {"p1": 103.5, "p2": 96.5},
            "collections_bb": {"p1": 9.0},
            "player_personas": {"p1": "lag", "p2": "calling_station"},
            "player_agent_ids": {"p1": "a1", "p2": "a2"},
            "player_seats": {"p1": 1, "p2": 2},
            "backend_types": {"p1": "heuristic_persona", "p2": "heuristic_persona"},
            "raw_lineage": {},
        }
    ]
    simulation_event_payloads = [
        {
            "event_type": "simulation_completed",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "status": "finalizing",
            "event_ts": "2026-04-17T12:00:09Z",
        }
    ]

    result = stream_module.process_event_batch(
        action_payloads,
        summary_payloads,
        simulation_event_payloads=simulation_event_payloads,
        window_minutes=5,
        warehouse=fake_warehouse,
    )

    assert result["action_rows"] == 3
    assert result["hand_rows"] == 1
    assert result["player_rows"] == 2
    assert result["player_summary_rows"] == 2
    assert result["simulation_event_rows"] == 1
    assert result["live_metric_rows"] == 2
    assert result["live_assignment_rows"] == 2
    assert "RAW_ACTIONS" in fake_warehouse.written
    assert "CURATED_HANDS" in fake_warehouse.written
    assert "CURATED_PLAYERS" in fake_warehouse.written
    assert "SIMULATION_RUN_PLAYER_SUMMARIES" in fake_warehouse.written
    assert "LIVE_AGENT_METRICS" in fake_warehouse.written
    assert "LIVE_PROFILE_ASSIGNMENTS" in fake_warehouse.written
    assert ("RAW_ACTIONS", ["hand_id", "action_index", "source_type", "source_run_id", "simulation_run_id"], {"force_bulk": True}) in fake_warehouse.upserted
    assert ("CURATED_HANDS", ["hand_id", "source_type", "source_run_id", "simulation_run_id"], {"force_bulk": True}) in fake_warehouse.upserted
    assert ("CURATED_PLAYERS", ["hand_id", "player_id", "source_type", "source_run_id", "simulation_run_id"], {"force_bulk": True}) in fake_warehouse.upserted
    assert fake_warehouse.write_kwargs["LIVE_AGENT_METRICS"]["force_bulk"] is True
    summary_rows = fake_warehouse.written["SIMULATION_RUN_PLAYER_SUMMARIES"].sort_values("final_rank").reset_index(drop=True)
    assert summary_rows.loc[0, "player_id"] == "p1"
    assert float(summary_rows.loc[0, "total_bb_won"]) == 3.5
    assert float(summary_rows.loc[0, "avg_bb_per_hand"]) == 3.5
    assert float(summary_rows.loc[0, "bb_per_100"]) == 350.0
    assert int(summary_rows.loc[0, "heuristic_decision_count"]) == 1
    assert int(summary_rows.loc[0, "llm_decision_count"]) == 1
    assert int(summary_rows.loc[1, "llm_fallback_count"]) == 1


def test_process_event_batch_persists_simulation_run_metadata_from_lifecycle_events() -> None:
    fake_warehouse = _FakeWarehouse()
    action_payloads: list[dict[str, object]] = []
    summary_payloads: list[dict[str, object]] = []
    simulation_event_payloads = [
        {
            "event_type": "simulation_requested",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "status": "queued",
            "event_ts": "2026-04-17T12:00:00Z",
            "requested_at": "2026-04-17T12:00:00Z",
            "hand_count": 500,
            "request_mode": "context",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "hero_context_hash": "ctx_hash",
            "hero_context_preview": "tight aggressive",
            "backend_type": "heuristic_persona",
            "model_name": None,
        },
        {
            "event_type": "simulation_completed",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "status": "finalizing",
            "event_ts": "2026-04-17T12:10:00Z",
            "hand_count": 500,
            "published_actions": 7200,
            "published_hand_summaries": 500,
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "hero_context_hash": "ctx_hash",
            "backend_type": "heuristic_persona",
        },
    ]

    result = stream_module.process_event_batch(
        action_payloads,
        summary_payloads,
        simulation_event_payloads=simulation_event_payloads,
        warehouse=fake_warehouse,
    )

    assert result["simulation_run_rows"] == 1
    assert ("RAW_SIMULATION_RUNS", ["simulation_run_id"], {"force_bulk": True}) in fake_warehouse.upserted
    run_rows = fake_warehouse.written["RAW_SIMULATION_RUNS"]
    assert run_rows.iloc[0]["simulation_run_id"] == "sim_1"
    assert run_rows.iloc[0]["status"] == "finalizing"
    assert int(run_rows.iloc[0]["published_hand_summaries"]) == 500
    assert pd.notna(run_rows.iloc[0]["requested_at"])
    assert pd.notna(run_rows.iloc[0]["finished_at"])


def test_lineage_lookup_handles_double_encoded_json() -> None:
    raw_lineage = json.dumps(json.dumps({"decision_source": "llm"}))

    assert stream_module._lineage_lookup(raw_lineage, "decision_source") == "llm"


def test_process_event_batch_defers_durable_player_summaries_until_terminal_event() -> None:
    fake_warehouse = _FakeWarehouse()
    action_payloads = [
        {
            "event_type": "action",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "action_index": 0,
            "street": "preflop",
            "player_id": "p1",
            "agent_id": "a1",
            "seat": 1,
            "position": "BTN",
            "action_type": "raise",
            "amount_bb": 2.5,
            "pot_before_bb": 1.5,
            "pot_after_bb": 4.0,
            "to_call_bb": 1.0,
            "effective_stack_bb": 100.0,
            "players_remaining": 2,
            "board_cards_visible": [],
            "hole_cards_visible": [],
            "is_all_in": False,
            "event_ts": "2026-04-17T12:00:00Z",
            "backend_type": "heuristic_persona",
            "persona_name": "lag",
            "persona_text": "loose aggressive",
            "raw_lineage": {"decision_source": "heuristic"},
        }
    ]
    summary_payloads = [
        {
            "event_type": "hand_summary",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "button_seat": 1,
            "small_blind_bb": 0.5,
            "big_blind_bb": 1.0,
            "board_cards": ["Ah", "Kd", "2c"],
            "started_at": "2026-04-17T12:00:00Z",
            "finished_at": "2026-04-17T12:00:08Z",
            "winner_player_ids": ["p1"],
            "total_pot_bb": 9.0,
            "rake_bb": 0.0,
            "starting_stacks_bb": {"p1": 100.0, "p2": 100.0},
            "final_stacks_bb": {"p1": 103.5, "p2": 96.5},
            "collections_bb": {"p1": 9.0},
            "player_personas": {"p1": "lag", "p2": "calling_station"},
            "player_agent_ids": {"p1": "a1", "p2": "a2"},
            "player_seats": {"p1": 1, "p2": 2},
            "backend_types": {"p1": "heuristic_persona", "p2": "heuristic_persona"},
            "raw_lineage": {},
        }
    ]

    result = stream_module.process_event_batch(
        action_payloads,
        summary_payloads,
        warehouse=fake_warehouse,
    )

    assert result["player_summary_rows"] == 0
    assert "SIMULATION_RUN_PLAYER_SUMMARIES" not in fake_warehouse.written


def test_process_event_batch_refreshes_summaries_for_runs_already_marked_finalizing() -> None:
    fake_warehouse = _FakeWarehouse()
    fake_warehouse.written["RAW_SIMULATION_RUNS"] = pd.DataFrame(
        [
            {
                "simulation_run_id": "sim_1",
                "status": "finalizing",
                "finished_at": datetime(2026, 4, 17, 12, 0, 9),
                "started_at": datetime(2026, 4, 17, 12, 0, 1),
                "requested_at": datetime(2026, 4, 17, 12, 0, 0),
            }
        ]
    )
    action_payloads = [
        {
            "event_type": "action",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "action_index": 0,
            "street": "preflop",
            "player_id": "p1",
            "agent_id": "a1",
            "seat": 1,
            "position": "BTN",
            "action_type": "raise",
            "amount_bb": 2.5,
            "pot_before_bb": 1.5,
            "pot_after_bb": 4.0,
            "to_call_bb": 1.0,
            "effective_stack_bb": 100.0,
            "players_remaining": 2,
            "board_cards_visible": [],
            "hole_cards_visible": [],
            "is_all_in": False,
            "event_ts": "2026-04-17T12:00:00Z",
            "backend_type": "heuristic_persona",
            "persona_name": "lag",
            "persona_text": "loose aggressive",
            "raw_lineage": {"decision_source": "heuristic"},
        }
    ]
    summary_payloads = [
        {
            "event_type": "hand_summary",
            "payload_version": "1",
            "source_type": "simulated",
            "source_dataset": "persona_simulation",
            "source_run_id": "sim_1",
            "simulation_run_id": "sim_1",
            "table_id": "table_1",
            "hand_id": "h1",
            "button_seat": 1,
            "small_blind_bb": 0.5,
            "big_blind_bb": 1.0,
            "board_cards": ["Ah", "Kd", "2c"],
            "started_at": "2026-04-17T12:00:00Z",
            "finished_at": "2026-04-17T12:00:08Z",
            "winner_player_ids": ["p1"],
            "total_pot_bb": 9.0,
            "rake_bb": 0.0,
            "starting_stacks_bb": {"p1": 100.0, "p2": 100.0},
            "final_stacks_bb": {"p1": 103.5, "p2": 96.5},
            "collections_bb": {"p1": 9.0},
            "player_personas": {"p1": "lag", "p2": "calling_station"},
            "player_agent_ids": {"p1": "a1", "p2": "a2"},
            "player_seats": {"p1": 1, "p2": 2},
            "backend_types": {"p1": "heuristic_persona", "p2": "heuristic_persona"},
            "raw_lineage": {},
        }
    ]

    result = stream_module.process_event_batch(
        action_payloads,
        summary_payloads,
        warehouse=fake_warehouse,
    )

    assert result["player_summary_rows"] == 2
    assert "SIMULATION_RUN_PLAYER_SUMMARIES" in fake_warehouse.written


def test_execution_finished_run_ids_registry_uses_real_raw_run_columns() -> None:
    captured: dict[str, object] = {}

    class _RecordingWarehouse:
        def load_table(self, table, **kwargs):
            captured["table"] = table
            captured["kwargs"] = kwargs
            return pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_1",
                        "status": "finalizing",
                        "finished_at": datetime(2026, 4, 17, 12, 0, 9),
                        "started_at": datetime(2026, 4, 17, 12, 0, 1),
                        "requested_at": datetime(2026, 4, 17, 12, 0, 0),
                    }
                ]
            )

    run_ids = stream_module._execution_finished_run_ids_from_registry(_RecordingWarehouse(), ["sim_1"])

    assert run_ids == ["sim_1"]
    assert captured["table"] == "RAW_SIMULATION_RUNS"
    assert captured["kwargs"]["order_by"] == [("finished_at", False), ("started_at", False), ("requested_at", False)]


def test_spark_stream_module_avoids_driver_to_pandas_in_live_consumer_path() -> None:
    source = Path("jobs/stream_kafka.py").read_text(encoding="utf-8")
    assert ".toPandas(" not in source


def test_spark_stream_module_tolerates_kafka_data_loss() -> None:
    source = Path("jobs/stream_kafka.py").read_text(encoding="utf-8")
    assert '.option("failOnDataLoss", "false")' in source


def test_spark_player_frame_derives_hero_and_showdown_fields() -> None:
    pyspark = pytest.importorskip("pyspark.sql")
    spark = (
        pyspark.SparkSession.builder.master("local[1]")
        .appName("streaming-metrics-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        hand_events = spark.createDataFrame(
            [
                {
                    "payload_version": "1",
                    "event_type": "hand_summary",
                    "source_type": "simulated",
                    "source_dataset": "fixture_set",
                    "source_run_id": "sim_1",
                    "simulation_run_id": "sim_1",
                    "user_id": "user_1",
                    "decision_backend": "heuristic",
                    "hero_context_hash": "ctx_hash",
                    "hero_seat": 1,
                    "table_id": "table_1",
                    "hand_id": "h1",
                    "button_seat": 6,
                    "small_blind_bb": 0.5,
                    "big_blind_bb": 1.0,
                    "board_cards": ["Ah", "Kd", "Qh", "2c", "3h"],
                    "started_at": "2026-04-17T12:00:00Z",
                    "finished_at": "2026-04-17T12:00:10Z",
                    "winner_player_ids": ["p1"],
                    "total_pot_bb": 12.5,
                    "rake_bb": 0.0,
                    "starting_stacks_bb": {"p1": 100.0, "p2": 100.0},
                    "final_stacks_bb": {"p1": 106.0, "p2": 94.0},
                    "collections_bb": {"p1": 12.5},
                    "player_personas": {"p1": "hero", "p2": "villain"},
                    "agent_ids": {"p1": "a1", "p2": "a2"},
                    "player_agent_ids": {"p1": "a1", "p2": "a2"},
                    "seats": {"p1": 1, "p2": 2},
                    "player_seats": {"p1": 1, "p2": 2},
                    "showdown_player_ids": ["p1", "p2"],
                    "player_hole_cards": {"p1": ["Jh", "Th"], "p2": ["Ad", "Ac"]},
                    "backend_types": {"p1": "heuristic_persona", "p2": "heuristic_persona"},
                    "backend_type": "heuristic_persona",
                    "raw_lineage": {"winner_player_ids": "[\"p1\"]"},
                }
            ]
        )

        player_rows = stream_module._spark_player_output_frame(spark, hand_events).toPandas()

        assert set(player_rows["player_id"]) == {"p1", "p2"}
        hero_row = player_rows.loc[player_rows["player_id"] == "p1"].iloc[0]
        assert bool(hero_row["is_hero_player"]) is True
        assert bool(hero_row["made_showdown"]) is True
        assert hero_row["showdown_hand_category"] == "straight_flush"
        assert float(hero_row["result_bb"]) == 6.0
    finally:
        spark.stop()


def test_spark_player_rows_frame_handles_nullable_live_state_rows() -> None:
    pyspark = pytest.importorskip("pyspark.sql")
    spark = (
        pyspark.SparkSession.builder.master("local[1]")
        .appName("streaming-live-state-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        player_rows = stream_module._spark_player_rows_frame(
            spark,
            [
                {
                    "source_run_id": "sim_1",
                    "source_type": "simulated",
                    "source_dataset": "persona_simulation",
                    "simulation_run_id": "sim_1",
                    "user_id": None,
                    "decision_backend": "heuristic",
                    "hero_context_hash": "ctx_hash",
                    "hero_seat": 5,
                    "is_hero_player": True,
                    "table_id": "table_1",
                    "hand_id": "h1",
                    "player_id": "p1",
                    "agent_id": "a1",
                    "seat": 5,
                    "player_name": None,
                    "stack_start_bb": 100.0,
                    "stack_end_bb": 98.5,
                    "hole_cards": ["Ah", "Kd"],
                    "result_bb": -1.5,
                    "backend_type": None,
                    "persona_name": "hero",
                    "persona_text": None,
                    "made_showdown": False,
                    "showdown_hand_category": None,
                    "showdown_hand_score": None,
                    "payload_version": "1",
                    "raw_lineage": None,
                }
            ],
        )
        grouped = stream_module._spark_grouped_player_results(player_rows).toPandas()

        assert len(grouped) == 1
        assert grouped.iloc[0]["simulation_run_id"] == "sim_1"
        assert grouped.iloc[0]["showdown_hand_category"] is None
        assert pd.isna(grouped.iloc[0]["showdown_hand_score"])
    finally:
        spark.stop()
