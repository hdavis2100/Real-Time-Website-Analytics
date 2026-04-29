from __future__ import annotations

from datetime import datetime
import math
from pathlib import Path

import pandas as pd

import jobs.stream_kafka as stream_module
from dashboard.data_access import load_live_dashboard_data
from poker_platform.config import PlatformConfig
from poker_platform.redis_live import RedisLiveStore


class _FakePipeline:
    def __init__(self, client: "_FakeRedisClient") -> None:
        self.client = client
        self.operations: list[tuple[str, tuple, dict]] = []

    def hset(self, key, mapping=None, **kwargs):
        self.operations.append(("hset", (key,), {"mapping": mapping, **kwargs}))
        return self

    def sadd(self, key, *members):
        self.operations.append(("sadd", (key, *members), {}))
        return self

    def srem(self, key, *members):
        self.operations.append(("srem", (key, *members), {}))
        return self

    def expire(self, key, ttl):
        self.operations.append(("expire", (key, ttl), {}))
        return self

    def delete(self, key):
        self.operations.append(("delete", (key,), {}))
        return self

    def zadd(self, key, mapping):
        self.operations.append(("zadd", (key, mapping), {}))
        return self

    def hsetnx(self, key, field, value):
        self.operations.append(("hsetnx", (key, field, value), {}))
        return self

    def execute(self):
        for name, args, kwargs in self.operations:
            getattr(self.client, name)(*args, **kwargs)
        self.operations.clear()
        return []


class _FakeRedisClient:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.expirations: dict[str, int] = {}

    def pipeline(self):
        return _FakePipeline(self)

    def ping(self):
        return True

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(str(member) for member in members)

    def srem(self, key, *members):
        existing = self.sets.setdefault(key, set())
        for member in members:
            existing.discard(str(member))

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None, **_kwargs):
        if mapping:
            normalized = {str(field): str(value) for field, value in mapping.items()}
            self.hashes.setdefault(key, {}).update(normalized)

    def hsetnx(self, key, field, value):
        target = self.hashes.setdefault(key, {})
        normalized_field = str(field)
        if normalized_field in target:
            return 0
        target[normalized_field] = str(value)
        return 1

    def zadd(self, key, mapping):
        target = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            target[str(member)] = float(score)

    def zrevrange(self, key, start, end):
        members = self.sorted_sets.get(key, {})
        ordered = sorted(members.items(), key=lambda item: (item[1], item[0]), reverse=True)
        if end < 0:
            slice_end = None
        else:
            slice_end = end + 1
        return [member for member, _score in ordered[start:slice_end]]

    def delete(self, key):
        self.hashes.pop(key, None)
        self.sets.pop(key, None)
        self.sorted_sets.pop(key, None)

    def expire(self, key, ttl):
        self.expirations[str(key)] = int(ttl)


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
        redis_url="redis://localhost:6379/0",
        redis_live_ttl_seconds=86400,
        redis_key_prefix="live",
    )


def _store() -> RedisLiveStore:
    return RedisLiveStore(_config(), client=_FakeRedisClient())


def test_materialize_live_dashboard_builds_redis_run_and_global_views() -> None:
    store = _store()
    live_players = pd.DataFrame(
        [
            {
                "simulation_run_id": "sim_1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "hero_context_hash": "ctx_hash",
                "is_hero_player": True,
                "hand_id": "h1",
                "player_id": "p1",
                "agent_id": "a1",
                "persona_name": "hero_profile",
                "backend_type": "llm_gated_nano",
                "seat": 1,
                "result_bb": 4.5,
                "showdown_hand_category": "two_pair",
                "showdown_hand_score": 12345.0,
            },
            {
                "simulation_run_id": "sim_1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "hero_context_hash": "ctx_hash",
                "is_hero_player": False,
                "hand_id": "h1",
                "player_id": "p2",
                "agent_id": "a2",
                "persona_name": "villain",
                "backend_type": "llm_gated_nano",
                "seat": 2,
                "result_bb": -4.5,
                "showdown_hand_category": "one_pair",
                "showdown_hand_score": 11111.0,
            },
        ]
    )
    live_metrics = pd.DataFrame(
        [
            {
                "metric_window_start": datetime(2026, 4, 17, 12, 0),
                "metric_window_end": datetime(2026, 4, 17, 12, 5),
                "simulation_run_id": "sim_1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "hero_context_hash": "ctx_hash",
                "is_hero_player": True,
                "agent_id": "a1",
                "player_id": "p1",
                "persona_name": "hero_profile",
                "actions_per_second": 0.2,
                "hands_per_second": 0.03,
                "vpip": 1.0,
                "pfr": 1.0,
                "aggression_frequency": 1.0,
                "cbet_rate": 1.0,
                "all_in_rate": 0.0,
                "bb_won": 4.5,
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
                "user_id": "user_1",
                "decision_backend": "llm",
                "hero_context_hash": "ctx_hash",
                "is_hero_player": True,
                "agent_id": "a1",
                "player_id": "p1",
                "nearest_cluster_id": 0,
                "nearest_cluster_label": "TAG",
                "distance_to_centroid": 0.08,
                "assigned_at": datetime(2026, 4, 17, 12, 5),
            }
        ]
    )
    simulation_events = pd.DataFrame(
        [
            {
                "event_type": "simulation_requested",
                "simulation_run_id": "sim_1",
                "table_id": "table_1",
                "status": "queued",
                "event_ts": datetime(2026, 4, 17, 11, 59),
                "requested_at": datetime(2026, 4, 17, 11, 59),
                "user_id": "user_1",
                "decision_backend": "llm",
                "hero_context_hash": "ctx_hash",
                "hero_context_preview": "aggressive value bettor",
                "backend_type": "llm_gated_nano",
                "model_name": "gpt-5.4-nano",
            },
            {
                "event_type": "simulation_started",
                "simulation_run_id": "sim_1",
                "table_id": "table_1",
                "status": "running",
                "event_ts": datetime(2026, 4, 17, 12, 0),
                "user_id": "user_1",
                "decision_backend": "llm",
                "hero_context_hash": "ctx_hash",
                "hero_context_preview": "aggressive value bettor",
                "backend_type": "llm_gated_nano",
                "model_name": "gpt-5.4-nano",
            },
        ]
    )

    summary_rows = stream_module.materialize_live_dashboard(
        store,
        live_players=live_players,
        live_metrics=live_metrics,
        assignments=assignments,
        simulation_events=simulation_events,
    )

    assert "sim_1" in store.list_active_runs(user_id="user_1")
    assert store.get_run_meta("sim_1")["decision_backend"] == "llm"
    assert store.get_run_leaderboard("sim_1", "profit", topn=5)[0]["player_id"] == "p1"
    assert store.get_global_leaderboard("hero_context", topn=5)[0]["is_hero_player"] is True

    agent_rows = store.get_run_agents("sim_1")
    assert len(agent_rows) == 1
    assert agent_rows[0]["nearest_cluster_label"] == "TAG"
    assert any(row["summary_key"] == "global:leaderboard:profit" for row in summary_rows)


def test_global_leaderboards_can_be_built_from_run_leaderboards_without_live_state() -> None:
    store = _store()
    store.replace_run_leaderboard(
        "sim_1",
        "profit",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "total_bb_won": 12.0,
            }
        ],
        score_field="total_bb_won",
    )
    store.replace_run_leaderboard(
        "sim_1",
        "high_hand",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "showdown_hand_score": 5000.0,
            }
        ],
        score_field="showdown_hand_score",
    )
    store.replace_run_leaderboard(
        "sim_1",
        "bb_per_100",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "is_hero_player": True,
                "bb_per_100": 45.0,
            },
            {
                "simulation_run_id": "sim_1",
                "player_id": "p2",
                "agent_id": "a2",
                "is_hero_player": False,
                "bb_per_100": 99.0,
            },
        ],
        score_field="bb_per_100",
    )
    store.replace_run_leaderboard(
        "sim_2",
        "profit",
        [
            {
                "simulation_run_id": "sim_2",
                "player_id": "p3",
                "agent_id": "a3",
                "user_id": "user_2",
                "decision_backend": "heuristic",
                "total_bb_won": 18.0,
            }
        ],
        score_field="total_bb_won",
    )
    store.replace_run_leaderboard(
        "sim_2",
        "high_hand",
        [
            {
                "simulation_run_id": "sim_2",
                "player_id": "p3",
                "agent_id": "a3",
                "showdown_hand_score": 9000.0,
            }
        ],
        score_field="showdown_hand_score",
    )
    store.replace_run_leaderboard(
        "sim_2",
        "bb_per_100",
        [
            {
                "simulation_run_id": "sim_2",
                "player_id": "p3",
                "agent_id": "a3",
                "is_hero_player": True,
                "bb_per_100": 30.0,
            }
        ],
        score_field="bb_per_100",
    )

    profit_rows, high_hand_rows, hero_rows = stream_module._global_leaderboard_rows_from_run_leaderboards(
        store,
        ["sim_1", "sim_2"],
    )

    assert [row["simulation_run_id"] for row in profit_rows] == ["sim_2", "sim_1"]
    assert [row["simulation_run_id"] for row in high_hand_rows] == ["sim_2", "sim_1"]
    assert [row["player_id"] for row in hero_rows] == ["p1", "p3"]


def test_sync_live_player_state_builds_idempotent_player_aggregates() -> None:
    store = _store()
    run_meta_lookup = {
        "sim_1": {
            "simulation_run_id": "sim_1",
            "user_id": "user_1",
            "decision_backend": "llm",
            "hero_context_hash": "ctx_hash",
            "hero_context_preview": "preview",
            "model_name": "gpt-5.4-nano",
            "status": "running",
        }
    }
    first_hand = {
        "simulation_run_id": "sim_1",
        "user_id": "user_1",
        "decision_backend": "llm",
        "hero_context_hash": "ctx_hash",
        "is_hero_player": True,
        "hand_id": "h1",
        "player_id": "p1",
        "agent_id": "a1",
        "persona_name": "hero_profile",
        "backend_type": "llm_gated_nano",
        "seat": 1,
        "result_bb": 3.5,
        "showdown_hand_category": "two_pair",
        "showdown_hand_score": 1234.0,
    }
    second_hand = {
        **first_hand,
        "hand_id": "h2",
        "result_bb": 1.5,
        "showdown_hand_category": "flush",
        "showdown_hand_score": 5000.0,
    }

    stream_module._sync_live_player_state(store, [first_hand], run_meta_lookup=run_meta_lookup)
    stream_module._sync_live_player_state(store, [first_hand], run_meta_lookup=run_meta_lookup)
    aggregate_rows_by_run = stream_module._sync_live_player_state(store, [second_hand], run_meta_lookup=run_meta_lookup)

    aggregate_rows = store.list_json_hash("run:sim_1:state:player_aggregates")

    assert len(aggregate_rows) == 1
    assert len(store.list_json_hash("run:sim_1:state:player_hands")) == 2
    assert len(aggregate_rows_by_run["sim_1"]) == 1
    aggregate_row = aggregate_rows[0]
    assert aggregate_row["hands_played"] == 2
    assert float(aggregate_row["total_bb_won"]) == 5.0
    assert float(aggregate_row["bb_per_100"]) == 250.0
    assert float(aggregate_row["showdown_hand_score"]) == 5000.0
    assert aggregate_row["showdown_hand_category"] == "flush"


def test_sync_live_player_state_handles_nullable_user_ids_in_aggregate_rebuild() -> None:
    store = _store()
    run_meta_lookup = {
        "sim_nullable": {
            "simulation_run_id": "sim_nullable",
            "user_id": None,
            "decision_backend": "heuristic",
            "hero_context_hash": "ctx_hash",
            "hero_context_preview": "preview",
            "model_name": None,
            "status": "running",
        }
    }
    player_rows = [
        {
            "simulation_run_id": "sim_nullable",
            "user_id": None,
            "decision_backend": "heuristic",
            "hero_context_hash": "ctx_hash",
            "is_hero_player": True,
            "hand_id": "h1",
            "player_id": "p1",
            "agent_id": "a1",
            "persona_name": "hero_profile",
            "backend_type": "heuristic_persona",
            "seat": 1,
            "result_bb": 2.0,
            "showdown_hand_category": "one_pair",
            "showdown_hand_score": 111.0,
        },
        {
            "simulation_run_id": "sim_nullable",
            "user_id": None,
            "decision_backend": "heuristic",
            "hero_context_hash": "ctx_hash",
            "is_hero_player": True,
            "hand_id": "h2",
            "player_id": "p1",
            "agent_id": "a1",
            "persona_name": "hero_profile",
            "backend_type": "heuristic_persona",
            "seat": 1,
            "result_bb": 4.0,
            "showdown_hand_category": "straight",
            "showdown_hand_score": 444.0,
        },
    ]

    aggregate_rows_by_run = stream_module._sync_live_player_state(
        store,
        player_rows,
        run_meta_lookup=run_meta_lookup,
    )

    assert len(aggregate_rows_by_run["sim_nullable"]) == 1
    aggregate_row = aggregate_rows_by_run["sim_nullable"][0]
    assert aggregate_row["user_id"] is None or str(aggregate_row["user_id"]) in {"", "<NA>"}
    assert float(aggregate_row["total_bb_won"]) == 6.0
    assert aggregate_row["showdown_hand_category"] == "straight"


def test_redis_live_store_skips_nan_sorted_set_scores_and_sanitizes_payloads() -> None:
    store = _store()

    store.replace_global_leaderboard(
        "high_hand",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "showdown_hand_score": math.nan,
                "showdown_hand_category": None,
            },
            {
                "simulation_run_id": "sim_1",
                "player_id": "p2",
                "agent_id": "a2",
                "showdown_hand_score": 1234.0,
                "showdown_hand_category": "two_pair",
            },
        ],
        score_field="showdown_hand_score",
    )
    store.upsert_json_hash(
        "run:sim_1:state:player_hands",
        [
            {
                "simulation_run_id": "sim_1",
                "hand_id": "h1",
                "player_id": "p1",
                "agent_id": "a1",
                "showdown_hand_score": math.nan,
            }
        ],
        key_builder=lambda row: f"{row['hand_id']}|{row['player_id']}",
    )

    high_hand_rows = store.get_global_leaderboard("high_hand")
    state_rows = store.list_json_hash("run:sim_1:state:player_hands")

    assert len(high_hand_rows) == 1
    assert high_hand_rows[0]["player_id"] == "p2"
    assert state_rows[0]["showdown_hand_score"] is None


def test_load_live_dashboard_data_reads_redis_only_and_filters() -> None:
    store = _store()
    store.set_run_meta(
        {
            "simulation_run_id": "sim_1",
            "user_id": "user_1",
            "decision_backend": "llm",
            "hero_context_hash": "ctx_hash",
            "hero_context_preview": "preview",
            "model_name": "gpt-5.4-nano",
            "status": "running",
        }
    )
    store.replace_run_leaderboard(
        "sim_1",
        "profit",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "persona_name": "hero_profile",
                "total_bb_won": 5.0,
            }
        ],
        score_field="total_bb_won",
    )
    store.replace_run_leaderboard(
        "sim_1",
        "bb_per_100",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "persona_name": "hero_profile",
                "is_hero_player": True,
                "bb_per_100": 12.0,
            }
        ],
        score_field="bb_per_100",
    )
    store.replace_run_leaderboard(
        "sim_1",
        "high_hand",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "showdown_hand_category": "flush",
                "showdown_hand_score": 654321.0,
            }
        ],
        score_field="showdown_hand_score",
    )
    store.replace_global_leaderboard(
        "profit",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "total_bb_won": 5.0,
            }
        ],
        score_field="total_bb_won",
    )
    store.replace_global_leaderboard(
        "high_hand",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "showdown_hand_score": 654321.0,
            }
        ],
        score_field="showdown_hand_score",
    )
    store.replace_global_leaderboard(
        "hero_context",
        [
            {
                "simulation_run_id": "sim_1",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "is_hero_player": True,
                "bb_per_100": 12.0,
            }
        ],
        score_field="bb_per_100",
    )
    store.replace_run_agents(
        "sim_1",
        [
            {
                "simulation_run_id": "sim_1",
                "agent_id": "a1",
                "player_id": "p1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "persona_name": "hero_profile",
                "nearest_cluster_label": "TAG",
            }
        ],
    )
    store.upsert_json_hash(
        "run:sim_1:state:player_hands",
        [
            {
                "simulation_run_id": "sim_1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "hand_id": "sim_1_hand_00001",
                "player_id": "p1",
                "agent_id": "a1",
                "is_hero_player": True,
                "result_bb": 1.5,
            },
            {
                "simulation_run_id": "sim_1",
                "user_id": "user_1",
                "decision_backend": "llm",
                "hand_id": "sim_1_hand_00002",
                "player_id": "p1",
                "agent_id": "a1",
                "is_hero_player": True,
                "result_bb": 3.5,
            },
        ],
        key_builder=lambda row: f"{row['hand_id']}|{row['player_id']}",
    )

    result = load_live_dashboard_data(
        simulation_run_id="sim_1",
        user_id="user_1",
        decision_backend="llm",
        live_store=store,
    )

    assert result.available is True
    assert result.selected_run_id == "sim_1"
    assert result.selected_run_meta["status"] == "running"
    assert result.run_profit_leaderboard[0]["player_id"] == "p1"
    assert result.global_hero_context_leaderboard[0]["is_hero_player"] is True
    assert len(result.hero_profit_timeseries) == 2
    assert result.hero_profit_timeseries[-1]["cumulative_bb_won"] == 5.0


def test_load_live_dashboard_data_returns_unavailable_when_redis_is_empty() -> None:
    result = load_live_dashboard_data(live_store=_store())

    assert result.available is False
    assert result.message == "No active simulation runs are available yet."


def test_load_live_dashboard_data_can_read_completed_run_by_id_after_active_cleanup() -> None:
    store = _store()
    store.set_run_meta(
        {
            "simulation_run_id": "sim_done",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "hero_context_hash": "ctx_hash",
            "hero_context_preview": "preview",
            "model_name": "",
            "status": "completed",
        }
    )
    store.replace_run_leaderboard(
        "sim_done",
        "profit",
        [
            {
                "simulation_run_id": "sim_done",
                "player_id": "p1",
                "agent_id": "a1",
                "user_id": "user_1",
                "decision_backend": "heuristic",
                "persona_name": "hero_profile",
                "total_bb_won": 9.0,
            }
        ],
        score_field="total_bb_won",
    )
    store.replace_run_agents(
        "sim_done",
        [
            {
                "simulation_run_id": "sim_done",
                "agent_id": "a1",
                "player_id": "p1",
                "user_id": "user_1",
                "decision_backend": "heuristic",
                "persona_name": "hero_profile",
            }
        ],
    )
    store.expire_run("sim_done", user_id="user_1")

    result = load_live_dashboard_data(
        simulation_run_id="sim_done",
        user_id="user_1",
        decision_backend="heuristic",
        live_store=store,
    )

    assert result.available is True
    assert result.active_run_ids == []
    assert result.selected_run_id == "sim_done"
    assert result.selected_run_meta["status"] == "completed"
    assert result.run_profit_leaderboard[0]["player_id"] == "p1"
    assert result.run_agents[0]["agent_id"] == "a1"
    assert result.global_profit_leaderboard == []


def test_redis_live_store_does_not_reactivate_terminal_runs_from_stale_updates() -> None:
    store = _store()
    store.set_run_meta(
        {
            "simulation_run_id": "sim_done",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "status": "completed",
            "updated_at": "2026-04-20T20:10:00Z",
            "finished_at": "2026-04-20T20:10:00Z",
        }
    )
    store.set_run_meta(
        {
            "simulation_run_id": "sim_done",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "status": "queued",
            "updated_at": "2026-04-20T20:00:00Z",
            "requested_at": "2026-04-20T20:00:00Z",
        }
    )

    meta = store.get_run_meta("sim_done")

    assert meta["status"] == "completed"
    assert meta["finished_at"] == "2026-04-20T20:10:00Z"
    assert store.list_active_runs(user_id="user_1") == []


def test_redis_live_store_does_not_downgrade_nonterminal_status_from_requested_event() -> None:
    store = _store()
    store.set_run_meta(
        {
            "simulation_run_id": "sim_live",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "status": "finalizing",
            "started_at": "2026-04-20T20:05:00Z",
            "finished_at": "2026-04-20T20:10:00Z",
            "updated_at": "2026-04-20T20:10:00Z",
            "published_actions": 180,
            "published_hand_summaries": 24,
        }
    )
    store.set_run_meta(
        {
            "simulation_run_id": "sim_live",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "status": "queued",
            "requested_at": "2026-04-20T20:00:00Z",
            "updated_at": None,
        }
    )

    meta = store.get_run_meta("sim_live")

    assert meta["status"] == "finalizing"
    assert meta["requested_at"] == "2026-04-20T20:00:00Z"
    assert meta["started_at"] == "2026-04-20T20:05:00Z"
    assert meta["finished_at"] == "2026-04-20T20:10:00Z"
    assert meta["published_actions"] == "180"
    assert meta["published_hand_summaries"] == "24"


def test_redis_live_store_resets_state_when_run_id_is_reused() -> None:
    store = _store()
    store.set_run_meta(
        {
            "simulation_run_id": "sim_reused",
            "user_id": "user_1",
            "decision_backend": "llm",
            "hero_context_hash": "old_ctx",
            "status": "finalizing",
            "requested_at": "2026-04-20T20:00:00Z",
            "started_at": "2026-04-20T20:01:00Z",
            "finished_at": "2026-04-20T20:10:00Z",
            "updated_at": "2026-04-20T20:10:00Z",
            "published_actions": 180,
            "published_hand_summaries": 24,
        }
    )
    store.replace_run_leaderboard(
        "sim_reused",
        "profit",
        [
            {
                "simulation_run_id": "sim_reused",
                "player_id": "old_player",
                "total_bb_won": 42.0,
            }
        ],
        score_field="total_bb_won",
    )
    store.upsert_json_hash(
        "run:sim_reused:state:player_hands",
        [
            {
                "simulation_run_id": "sim_reused",
                "hand_id": "sim_reused_hand_00024",
                "player_id": "old_player",
                "result_bb": 42.0,
            }
        ],
        key_builder=lambda row: f"{row['hand_id']}|{row['player_id']}",
    )

    store.set_run_meta(
        {
            "simulation_run_id": "sim_reused",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "hero_context_hash": "new_ctx",
            "status": "queued",
            "requested_at": "2026-04-20T20:30:00Z",
            "updated_at": "2026-04-20T20:30:00Z",
            "published_actions": 0,
            "published_hand_summaries": 0,
        }
    )

    meta = store.get_run_meta("sim_reused")

    assert meta["status"] == "queued"
    assert meta["requested_at"] == "2026-04-20T20:30:00Z"
    assert meta["decision_backend"] == "heuristic"
    assert meta["hero_context_hash"] == "new_ctx"
    assert meta["published_actions"] == "0"
    assert meta["published_hand_summaries"] == "0"
    assert store.get_run_leaderboard("sim_reused", "profit") == []
    assert store.list_json_hash("run:sim_reused:state:player_hands") == []
    assert store.list_active_runs(user_id="user_1") == ["sim_reused"]


def test_load_live_dashboard_data_filters_terminal_runs_left_in_active_index() -> None:
    store = _store()
    store.set_run_meta(
        {
            "simulation_run_id": "sim_done",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "status": "completed",
        }
    )
    store.set_run_meta(
        {
            "simulation_run_id": "sim_live",
            "user_id": "user_1",
            "decision_backend": "heuristic",
            "status": "running",
        }
    )
    store.client.sadd(store._key("runs:active"), "sim_done", "sim_live")
    store.client.sadd(store._key("user:user_1:runs:active"), "sim_done", "sim_live")

    result = load_live_dashboard_data(user_id="user_1", decision_backend="heuristic", live_store=store)

    assert result.available is True
    assert result.active_run_ids == ["sim_live"]
    assert [run["simulation_run_id"] for run in result.active_runs] == ["sim_live"]
    assert result.selected_run_id == "sim_live"
