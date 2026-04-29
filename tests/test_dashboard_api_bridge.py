from __future__ import annotations

import json

import pandas as pd

from dashboard import api_bridge


class _FakeWarehouse:
    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self.tables = {name.upper(): frame.copy() for name, frame in tables.items()}

    def load_table(
        self,
        table: str,
        filters_eq: dict[str, object] | None = None,
        filters_in: dict[str, list[object]] | None = None,
        order_by: list[tuple[str, bool]] | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        frame = self.tables.get(str(table).upper(), pd.DataFrame()).copy()
        for column, value in (filters_eq or {}).items():
            if column in frame.columns:
                frame = frame[frame[column] == value]
        for column, values in (filters_in or {}).items():
            if column in frame.columns:
                frame = frame[frame[column].isin(list(values))]
        if order_by:
            columns = [column for column, _ascending in order_by if column in frame.columns]
            if columns:
                ascending = [ascending for column, ascending in order_by if column in frame.columns]
                frame = frame.sort_values(columns, ascending=ascending)
        if limit is not None:
            frame = frame.head(int(limit))
        return frame.reset_index(drop=True)


def test_load_performance_summary_aggregates_hero_scoped_results() -> None:
    warehouse = _FakeWarehouse(
        {
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_1",
                        "user_id": "user_1",
                        "decision_backend": "llm",
                        "is_hero_player": True,
                        "hands_played": 100,
                        "total_bb_won": 10.5,
                        "bb_per_100": 10.5,
                        "final_rank": 1,
                        "updated_at": "2026-04-20T20:01:00Z",
                    },
                    {
                        "simulation_run_id": "sim_1",
                        "user_id": "user_1",
                        "decision_backend": "llm",
                        "is_hero_player": False,
                        "hands_played": 100,
                        "total_bb_won": -10.5,
                        "bb_per_100": -10.5,
                        "final_rank": 6,
                        "updated_at": "2026-04-20T20:01:00Z",
                    },
                    {
                        "simulation_run_id": "sim_2",
                        "user_id": "user_1",
                        "decision_backend": "llm",
                        "is_hero_player": True,
                        "hands_played": 80,
                        "total_bb_won": -5.0,
                        "bb_per_100": -6.25,
                        "final_rank": 3,
                        "updated_at": "2026-04-20T20:02:00Z",
                    },
                ]
            ),
            "PROFILE_SESSION_RESULTS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_1",
                        "status": "ready",
                        "scored_at": "2026-04-20T20:05:00Z",
                    },
                    {
                        "simulation_run_id": "sim_2",
                        "status": "insufficient_evidence",
                        "scored_at": "2026-04-20T20:06:00Z",
                    },
                    {
                        "simulation_run_id": "sim_other",
                        "status": "ready",
                        "scored_at": "2026-04-20T20:07:00Z",
                    },
                ]
            ),
        }
    )

    payload = api_bridge.load_performance_summary(
        simulation_run_ids=["sim_1", "sim_2"],
        user_id="user_1",
        decision_backend="llm",
        topn=5,
        warehouse=warehouse,
    )

    assert payload["summary"]["run_count"] == 2
    assert payload["summary"]["total_hands"] == 180
    assert payload["summary"]["total_bb_won"] == 5.5
    assert payload["summary"]["first_place_rate"] == 0.5
    assert len(payload["recent_runs"]) == 2
    assert payload["recent_runs"][0]["simulation_run_id"] == "sim_2"
    assert payload["profile_overview"]["ready_profiles"] == 1
    assert payload["profile_overview"]["insufficient_evidence"] == 1
    assert payload["profile_overview"]["other"] == 0


def test_load_simulation_results_builds_completed_run_payload() -> None:
    warehouse = _FakeWarehouse(
        {
            "RAW_SIMULATION_RUNS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_results_1",
                        "status": "completed",
                        "published_actions": 384,
                        "published_hand_summaries": 30,
                        "finished_at": "2026-04-20T20:30:00Z",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "hero_hash_1",
                    }
                ]
            ),
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_results_1",
                        "player_id": "hero_player",
                        "agent_id": "hero_agent",
                        "seat": 1,
                        "is_hero_player": True,
                        "hands_played": 30,
                        "total_bb_won": 12.0,
                        "bb_per_100": 40.0,
                        "final_rank": 1,
                    },
                    {
                        "simulation_run_id": "sim_results_1",
                        "player_id": "villain_player",
                        "agent_id": "villain_agent",
                        "seat": 2,
                        "is_hero_player": False,
                        "hands_played": 30,
                        "total_bb_won": -12.0,
                        "bb_per_100": -40.0,
                        "final_rank": 6,
                    },
                ]
            ),
            "CURATED_PLAYERS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_1",
                        "player_id": "hero_player",
                        "agent_id": "hero_agent",
                        "seat": 1,
                        "is_hero_player": True,
                        "persona_name": "hero_style",
                        "backend_type": "llm_gated_nano",
                        "result_bb": 5.0,
                        "showdown_hand_category": "straight",
                        "showdown_hand_score": 406.0,
                        "made_showdown": True,
                    },
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_1",
                        "player_id": "villain_player",
                        "agent_id": "villain_agent",
                        "seat": 2,
                        "is_hero_player": False,
                        "persona_name": "tag",
                        "backend_type": "heuristic_persona",
                        "result_bb": -5.0,
                        "showdown_hand_category": "pair",
                        "showdown_hand_score": 108.0,
                        "made_showdown": True,
                    },
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_2",
                        "player_id": "hero_player",
                        "agent_id": "hero_agent",
                        "seat": 1,
                        "is_hero_player": True,
                        "persona_name": "hero_style",
                        "backend_type": "llm_gated_nano",
                        "result_bb": 7.0,
                        "showdown_hand_category": None,
                        "showdown_hand_score": None,
                        "made_showdown": False,
                    },
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_2",
                        "player_id": "villain_player",
                        "agent_id": "villain_agent",
                        "seat": 2,
                        "is_hero_player": False,
                        "persona_name": "tag",
                        "backend_type": "heuristic_persona",
                        "result_bb": -7.0,
                        "showdown_hand_category": None,
                        "showdown_hand_score": None,
                        "made_showdown": False,
                    },
                ]
            ),
            "CURATED_HANDS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_1",
                        "finished_at": "2026-04-20T20:00:05Z",
                    },
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_2",
                        "finished_at": "2026-04-20T20:01:05Z",
                    },
                ]
            ),
            "CURATED_ACTIONS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_1",
                        "agent_id": "hero_agent",
                        "player_id": "hero_player",
                        "persona_name": "hero_style",
                        "backend_type": "llm_gated_nano",
                        "is_hero_player": True,
                        "street": "preflop",
                        "action_type": "raise",
                        "action_index": 1,
                        "is_all_in": False,
                        "event_ts": "2026-04-20T20:00:00Z",
                    },
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_1",
                        "agent_id": "villain_agent",
                        "player_id": "villain_player",
                        "persona_name": "tag",
                        "backend_type": "heuristic_persona",
                        "is_hero_player": False,
                        "street": "preflop",
                        "action_type": "fold",
                        "action_index": 2,
                        "is_all_in": False,
                        "event_ts": "2026-04-20T20:00:03Z",
                    },
                    {
                        "simulation_run_id": "sim_results_1",
                        "hand_id": "hand_2",
                        "agent_id": "villain_agent",
                        "player_id": "villain_player",
                        "persona_name": "tag",
                        "backend_type": "heuristic_persona",
                        "is_hero_player": False,
                        "street": "preflop",
                        "action_type": "fold",
                        "action_index": 1,
                        "is_all_in": False,
                        "event_ts": "2026-04-20T20:01:00Z",
                    }
                ]
            ),
            "LIVE_PROFILE_ASSIGNMENTS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_results_1",
                        "agent_id": "hero_agent",
                        "player_id": "hero_player",
                        "nearest_cluster_label": "LAG",
                        "distance_to_centroid": 0.12,
                        "assigned_at": "2026-04-20T20:28:00Z",
                    }
                ]
            ),
        }
    )

    payload = api_bridge.load_simulation_results(
        simulation_run_id="sim_results_1",
        topn=10,
        warehouse=warehouse,
    )

    assert payload["status"] == "ready"
    assert payload["source"] == "snowflake"
    assert payload["run_summary"]["published_actions"] == 384
    assert payload["result"]["summary"]["hero"]["player_id"] == "hero_player"
    assert payload["result"]["summary"]["winner"]["player_id"] == "hero_player"
    assert payload["run_profit_leaderboard"][0]["player_id"] == "hero_player"
    assert payload["run_bb_per_100_leaderboard"][0]["player_id"] == "hero_player"
    assert payload["run_high_hand_leaderboard"][0]["showdown_hand_category"] == "straight"
    assert payload["agent_metrics"][0]["agent_id"] == "hero_agent"
    assert payload["agent_metrics"][0]["bb_won"] == 12.0
    assert payload["agent_metrics"][0]["observed_hands"] == 2
    assert payload["agent_metrics"][0]["vpip"] == 0.5
    assert payload["agent_metrics"][0]["pfr"] == 0.5
    assert payload["profile_assignments"][0]["nearest_cluster_label"] == "LAG"
    assert payload["hero_profit_timeseries"][-1]["cumulative_bb_won"] == 12.0


def test_load_simulation_results_stays_pending_until_player_summaries_exist() -> None:
    warehouse = _FakeWarehouse(
        {
            "RAW_SIMULATION_RUNS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_results_pending_1",
                        "status": "completed",
                        "published_actions": 1200,
                        "published_hand_summaries": 100,
                        "finished_at": "2026-04-20T20:30:00Z",
                        "decision_backend": "heuristic",
                    }
                ]
            ),
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame([]),
            "CURATED_PLAYERS": pd.DataFrame([]),
            "CURATED_ACTIONS": pd.DataFrame([]),
            "LIVE_PROFILE_ASSIGNMENTS": pd.DataFrame([]),
        }
    )

    payload = api_bridge.load_simulation_results(
        simulation_run_id="sim_results_pending_1",
        topn=10,
        warehouse=warehouse,
    )

    assert payload["status"] == "pending"
    assert payload["ready"] is False
    assert payload["message"] == "Final results are still being prepared"
    assert payload["run_summary"]["simulation_run_id"] == "sim_results_pending_1"
    assert payload["player_summaries"] == []
    assert payload["run_profit_leaderboard"] == []


def test_load_simulation_results_scopes_duplicate_run_ids_to_latest_run_context() -> None:
    warehouse = _FakeWarehouse(
        {
            "RAW_SIMULATION_RUNS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "duplicate_run",
                        "status": "completed",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_old",
                        "requested_at": "2026-04-20T20:00:00Z",
                        "finished_at": "2026-04-20T20:10:00Z",
                    },
                    {
                        "simulation_run_id": "duplicate_run",
                        "status": "completed",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_new",
                        "requested_at": "2026-04-21T20:00:00Z",
                        "finished_at": "2026-04-21T20:10:00Z",
                    },
                ]
            ),
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_old",
                        "player_id": "old_hero",
                        "agent_id": "user_agent",
                        "is_hero_player": True,
                        "hands_played": 5000,
                        "total_bb_won": 9000,
                        "bb_per_100": 180,
                        "final_rank": 1,
                    },
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_new",
                        "player_id": "new_hero",
                        "agent_id": "user_agent",
                        "is_hero_player": True,
                        "hands_played": 12,
                        "total_bb_won": 3,
                        "bb_per_100": 25,
                        "final_rank": 2,
                    },
                ]
            ),
            "CURATED_PLAYERS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_old",
                        "hand_id": "old_hand",
                        "player_id": "old_hero",
                        "agent_id": "user_agent",
                        "is_hero_player": True,
                        "result_bb": 9000,
                    },
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_new",
                        "hand_id": "new_hand",
                        "player_id": "new_hero",
                        "agent_id": "user_agent",
                        "is_hero_player": True,
                        "result_bb": 3,
                    },
                ]
            ),
            "CURATED_HANDS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_old",
                        "hand_id": "old_hand",
                        "finished_at": "2026-04-20T20:00:01Z",
                    },
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_new",
                        "hand_id": "new_hand",
                        "finished_at": "2026-04-21T20:00:01Z",
                    },
                ]
            ),
            "CURATED_ACTIONS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_old",
                        "hand_id": "old_hand",
                        "player_id": "old_hero",
                        "agent_id": "user_agent",
                        "is_hero_player": True,
                        "street": "preflop",
                        "action_type": "raise",
                        "action_index": 1,
                        "is_all_in": False,
                        "event_ts": "2026-04-20T20:00:00Z",
                    },
                    {
                        "simulation_run_id": "duplicate_run",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_new",
                        "hand_id": "new_hand",
                        "player_id": "new_hero",
                        "agent_id": "user_agent",
                        "is_hero_player": True,
                        "street": "preflop",
                        "action_type": "call",
                        "action_index": 1,
                        "is_all_in": False,
                        "event_ts": "2026-04-21T20:00:00Z",
                    },
                ]
            ),
            "LIVE_PROFILE_ASSIGNMENTS": pd.DataFrame([]),
        }
    )

    payload = api_bridge.load_simulation_results(
        simulation_run_id="duplicate_run",
        warehouse=warehouse,
    )

    assert payload["status"] == "ready"
    assert payload["run_summary"]["hero_context_hash"] == "ctx_new"
    assert [row["player_id"] for row in payload["player_summaries"]] == ["new_hero"]
    assert payload["hero_profit_timeseries"][-1]["cumulative_bb_won"] == 3.0


def test_load_profile_source_status_requires_actions_and_players() -> None:
    warehouse = _FakeWarehouse(
        {
            "RAW_SIMULATION_RUNS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_profile_pending_1",
                        "status": "completed",
                        "finished_at": "2026-04-20T20:30:00Z",
                    }
                ]
            ),
            "CURATED_ACTIONS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_profile_pending_1",
                        "hand_id": "hand_1",
                        "action_index": 0,
                        "event_ts": "2026-04-20T20:20:00Z",
                    }
                ]
            ),
            "CURATED_PLAYERS": pd.DataFrame([]),
        }
    )

    payload = api_bridge.load_profile_source_status(
        simulation_run_id="sim_profile_pending_1",
        warehouse=warehouse,
    )

    assert payload["status"] == "pending"
    assert payload["ready"] is False
    assert payload["has_actions"] is True
    assert payload["has_players"] is False
    assert payload["message"] == "Profile inputs are still being prepared"


def test_load_profile_source_status_is_ready_without_player_summaries() -> None:
    warehouse = _FakeWarehouse(
        {
            "RAW_SIMULATION_RUNS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_profile_ready_1",
                        "status": "completed",
                        "finished_at": "2026-04-20T20:30:00Z",
                    }
                ]
            ),
            "CURATED_ACTIONS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_profile_ready_1",
                        "hand_id": "hand_1",
                        "action_index": 0,
                        "event_ts": "2026-04-20T20:20:00Z",
                    }
                ]
            ),
            "CURATED_PLAYERS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_profile_ready_1",
                        "hand_id": "hand_1",
                        "player_id": "hero_player",
                        "seat": 1,
                    }
                ]
            ),
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame([]),
        }
    )

    payload = api_bridge.load_profile_source_status(
        simulation_run_id="sim_profile_ready_1",
        warehouse=warehouse,
    )

    assert payload["status"] == "ready"
    assert payload["ready"] is True
    assert payload["has_actions"] is True
    assert payload["has_players"] is True


def test_load_performance_query_filters_and_groups_results() -> None:
    warehouse = _FakeWarehouse(
        {
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_a",
                        "user_id": "user_1",
                        "decision_backend": "llm",
                        "is_hero_player": True,
                        "hands_played": 120,
                        "total_bb_won": 18.0,
                        "bb_per_100": 15.0,
                        "final_rank": 1,
                    },
                    {
                        "simulation_run_id": "sim_b",
                        "user_id": "user_1",
                        "decision_backend": "heuristic",
                        "is_hero_player": True,
                        "hands_played": 40,
                        "total_bb_won": 2.0,
                        "bb_per_100": 5.0,
                        "final_rank": 4,
                    },
                    {
                        "simulation_run_id": "sim_c",
                        "user_id": "user_1",
                        "decision_backend": "llm",
                        "is_hero_player": True,
                        "hands_played": 95,
                        "total_bb_won": 6.0,
                        "bb_per_100": 6.3,
                        "final_rank": 2,
                    },
                ]
            ),
            "PROFILE_SESSION_RESULTS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_a",
                        "status": "ready",
                        "scored_at": "2026-04-20T20:05:00Z",
                    },
                    {
                        "simulation_run_id": "sim_c",
                        "status": "insufficient_evidence",
                        "scored_at": "2026-04-20T20:06:00Z",
                    },
                ]
            ),
        }
    )

    payload = api_bridge.load_performance_query(
        simulation_run_ids=["sim_a", "sim_b", "sim_c"],
        user_id="user_1",
        min_hands_played=90,
        max_final_rank=2,
        topn=10,
        warehouse=warehouse,
    )

    assert payload["status"] == "ready"
    assert payload["matched_run_count"] == 2
    assert payload["summary"]["run_count"] == 2
    assert payload["summary"]["total_hands"] == 215
    assert payload["rows"][0]["simulation_run_id"] == "sim_a"
    assert payload["breakdowns"]["by_backend"][0]["decision_backend"] == "llm"
    assert payload["breakdowns"]["by_finish_rank"][0]["final_rank"] == 1
    assert payload["profile_overview"]["ready_profiles"] == 1
    assert payload["profile_overview"]["insufficient_evidence"] == 1


def test_load_performance_query_builds_grouped_comparisons_and_profile_breakdowns() -> None:
    warehouse = _FakeWarehouse(
        {
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_profile_a",
                        "user_id": "user_2",
                        "decision_backend": "llm",
                        "hero_context_hash": "ctx_hash_a",
                        "persona_name": "River Captain",
                        "player_id": "hero_player_a",
                        "agent_id": "hero_agent_a",
                        "is_hero_player": True,
                        "hands_played": 140,
                        "total_bb_won": 20.0,
                        "bb_per_100": 14.3,
                        "final_rank": 1,
                    },
                    {
                        "simulation_run_id": "sim_profile_b",
                        "user_id": "user_2",
                        "decision_backend": "heuristic",
                        "hero_context_hash": "ctx_hash_b",
                        "persona_name": "River Captain",
                        "player_id": "hero_player_b",
                        "agent_id": "hero_agent_b",
                        "is_hero_player": True,
                        "hands_played": 100,
                        "total_bb_won": 8.0,
                        "bb_per_100": 8.0,
                        "final_rank": 2,
                    },
                ]
            ),
            "PROFILE_SESSION_RESULTS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_profile_a",
                        "player_id": "hero_player_a",
                        "agent_id": "hero_agent_a",
                        "status": "ready",
                        "cluster_label": "LAG",
                        "confidence_score": 0.91,
                        "hands_observed": 140,
                        "scored_at": "2026-04-20T20:10:00Z",
                    },
                    {
                        "simulation_run_id": "sim_profile_b",
                        "player_id": "hero_player_b",
                        "agent_id": "hero_agent_b",
                        "status": "insufficient_evidence",
                        "cluster_label": "TAG",
                        "confidence_score": 0.42,
                        "hands_observed": 100,
                        "scored_at": "2026-04-20T20:11:00Z",
                    },
                ]
            ),
        }
    )

    payload = api_bridge.load_performance_query(
        query={
            "simulation_run_ids": ["sim_profile_a", "sim_profile_b"],
            "user_id": "user_2",
            "group_by": ["decision_backend", "cluster_label"],
            "sort": {"field": "total_bb_won", "direction": "desc"},
            "filters": {
                "cluster_labels": ["LAG", "TAG"],
                "profile_statuses": ["ready", "insufficient_evidence"],
                "min_total_bb_won": 5,
            },
            "topn": 10,
        },
        warehouse=warehouse,
    )

    assert payload["matched_run_count"] == 2
    assert payload["rows"][0]["cluster_label"] == "LAG"
    assert payload["rows"][0]["profile_status"] == "ready"
    assert payload["comparison_rows"][0]["decision_backend"] == "llm"
    assert payload["comparison_rows"][0]["cluster_label"] == "LAG"
    assert payload["comparison_rows"][0]["ready_profiles"] == 1
    assert payload["breakdowns"]["by_profile_status"][0]["profile_status"] == "insufficient_evidence"
    assert payload["breakdowns"]["by_cluster_label"][0]["cluster_label"] == "LAG"
    assert payload["query"]["group_by"] == ["decision_backend", "cluster_label"]


def test_serialize_sanitizes_nan_values_for_json_output() -> None:
    warehouse = _FakeWarehouse(
        {
            "SIMULATION_RUN_PLAYER_SUMMARIES": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_nan_1",
                        "user_id": "user_nan",
                        "decision_backend": "llm",
                        "is_hero_player": True,
                        "hands_played": 120,
                        "total_bb_won": 9.0,
                        "bb_per_100": 7.5,
                        "final_rank": 1,
                    }
                ]
            ),
            "PROFILE_SESSION_RESULTS": pd.DataFrame(
                [
                    {
                        "simulation_run_id": "sim_nan_1",
                        "player_id": "hero_player_nan",
                        "agent_id": "hero_agent_nan",
                        "status": "ready",
                        "cluster_label": "LAG",
                        "confidence_score": float("nan"),
                        "hands_observed": 120,
                        "scored_at": "2026-04-20T20:12:00Z",
                    }
                ]
            ),
        }
    )

    payload = api_bridge.load_performance_query(
        query={
            "simulation_run_ids": ["sim_nan_1"],
            "user_id": "user_nan",
            "filters": {
                "profile_statuses": ["ready"],
            },
            "topn": 10,
        },
        warehouse=warehouse,
    )

    rendered = json.dumps(api_bridge._serialize(payload), allow_nan=False)

    assert '"confidence_score": null' in rendered
