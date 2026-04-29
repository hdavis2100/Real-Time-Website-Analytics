from __future__ import annotations

import dashboard.app as dashboard_app
from dashboard.app import (
    _aggression_chart_frame,
    _profit_chart_frame,
    _resolve_selected_snapshot,
    _normalize_rate_percent,
    _load_live_result_for_ui,
    _live_result_from_payload,
    _display_frame,
    _display_frame_from_rows,
    _frame_from_rows,
    _participant_display_name,
    _resolve_selected_run_id,
    _user_agent_row,
)


def test_resolve_selected_run_id_prefers_existing_choice() -> None:
    active_runs = [
        {"simulation_run_id": "run_2"},
        {"simulation_run_id": "run_1"},
    ]

    assert _resolve_selected_run_id(active_runs, "run_1") == "run_1"


def test_resolve_selected_run_id_falls_back_to_first_active_run() -> None:
    active_runs = [
        {"simulation_run_id": "run_2"},
        {"simulation_run_id": "run_1"},
    ]

    assert _resolve_selected_run_id(active_runs, "missing_run") == "run_2"
    assert _resolve_selected_run_id(active_runs, None) == "run_2"


def test_frame_from_rows_keeps_requested_columns_when_present() -> None:
    frame = _frame_from_rows(
        [
            {
                "simulation_run_id": "run_1",
                "status": "running",
                "user_id": "user_1",
                "extra": "ignore_me",
            }
        ],
        ["simulation_run_id", "status", "user_id"],
    )

    assert list(frame.columns) == ["simulation_run_id", "status", "user_id"]
    assert frame.iloc[0]["simulation_run_id"] == "run_1"


def test_display_frame_prefers_account_name_for_account_column() -> None:
    frame = _frame_from_rows(
        [
            {
                "simulation_run_id": "run_1",
                "user_id": "748f4a71-d1bd-4a5b-825b-5cc3b6da323a",
                "account_name": "Hannah Davis",
            }
        ],
        ["simulation_run_id", "user_id"],
    )

    display_frame = _display_frame(frame)

    assert list(display_frame.columns) == ["Simulation Run", "Account"]
    assert display_frame.iloc[0]["Account"] == "Hannah Davis"


def test_participant_display_name_prefers_persona_labels_for_baselines() -> None:
    assert _participant_display_name({"persona_name": "nit", "player_id": "baseline_2"}) == "NIT"
    assert _participant_display_name({"persona_name": "lag", "agent_id": "baseline_agent_3"}) == "LAG"


def test_participant_display_name_marks_hero_agent() -> None:
    assert _participant_display_name({"is_hero_player": True, "persona_name": "tag"}) == "Your Agent"
    assert _participant_display_name({"player_id": "user_agent"}) == "Your Agent"


def test_display_frame_from_rows_adds_player_label_column() -> None:
    frame = _display_frame_from_rows(
        [
            {
                "player_id": "baseline_4",
                "agent_id": "baseline_agent_4",
                "persona_name": "calling_station",
                "bb_won": 5.0,
            }
        ],
        ["player", "bb_won"],
    )

    assert list(frame.columns) == ["player", "bb_won"]
    assert frame.iloc[0]["player"] == "Calling Station"


def test_normalize_rate_percent_supports_fraction_and_percent_inputs() -> None:
    assert _normalize_rate_percent(0.28) == 28.0
    assert _normalize_rate_percent(28) == 28.0
    assert _normalize_rate_percent(None) is None


def test_user_agent_row_prefers_hero_player() -> None:
    row = _user_agent_row(
        [
            {"player_id": "baseline_1", "persona_name": "nit"},
            {"player_id": "hero_1", "is_hero_player": True, "vpip": 0.24},
        ]
    )

    assert row is not None
    assert row["player_id"] == "hero_1"


def test_user_agent_row_falls_back_to_single_unlabeled_row() -> None:
    row = _user_agent_row(
        [
            {"player_id": "baseline_1", "persona_name": "nit"},
            {"player_id": "hero_1", "persona_name": None, "vpip": 0.24},
        ]
    )

    assert row is not None
    assert row["player_id"] == "hero_1"


def test_aggression_chart_frame_returns_three_metrics_for_user_agent() -> None:
    frame = _aggression_chart_frame(
        {
            "vpip": 0.31,
            "pfr": 0.18,
            "aggression_frequency": 0.42,
        }
    )

    assert list(frame.index) == ["VPIP", "PFR", "Aggression"]
    assert frame.loc["VPIP", "rate_percent"] == 31.0
    assert frame.loc["PFR", "rate_percent"] == 18.0
    assert frame.loc["Aggression", "rate_percent"] == 42.0


def test_profit_chart_frame_orders_points_by_hand_number() -> None:
    frame = _profit_chart_frame(
        [
            {"hand_number": 3, "cumulative_bb_won": 2.5},
            {"hand_number": 1, "cumulative_bb_won": -1.0},
            {"hand_number": 2, "cumulative_bb_won": 0.5},
        ]
    )

    assert list(frame.index) == [1, 2, 3]
    assert frame.loc[3, "cumulative_bb_won"] == 2.5


def test_live_result_from_payload_keeps_dashboard_rows() -> None:
    result = _live_result_from_payload(
        {
            "available": True,
            "source": "api",
            "active_run_ids": ["run_1"],
            "active_runs": [{"simulation_run_id": "run_1", "status": "running"}],
            "selected_run_id": "run_1",
            "selected_run_meta": {"simulation_run_id": "run_1", "status": "running"},
            "run_profit_leaderboard": [{"player_id": "p1"}],
        }
    )

    assert result.available is True
    assert result.source == "api"
    assert result.active_run_ids == ["run_1"]
    assert result.selected_run_id == "run_1"
    assert result.run_profit_leaderboard == [{"player_id": "p1"}]


def test_load_live_result_for_ui_prefers_dashboard_api_payload(monkeypatch) -> None:
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "available": False,
                "source": "api",
                "message": "No active simulation runs are available yet.",
                "active_run_ids": [],
                "active_runs": [],
                "selected_run_id": None,
                "selected_run_meta": None,
            }

    monkeypatch.setattr(
        dashboard_app,
        "_dashboard_live_api_candidates",
        lambda: ["http://app:3000/api/dashboard/live"],
    )
    monkeypatch.setattr(
        dashboard_app.requests,
        "get",
        lambda *args, **kwargs: Response(),
    )
    monkeypatch.setattr(
        dashboard_app,
        "load_live_dashboard_data",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("fallback should not be used")),
    )

    result = _load_live_result_for_ui(
        simulation_run_id=None,
        user_id=None,
        decision_backend=None,
        topn=25,
    )

    assert result.available is False
    assert result.active_runs == []


def test_resolve_selected_snapshot_reuses_overview_when_selected_run_matches(monkeypatch) -> None:
    overview = _live_result_from_payload(
        {
            "available": True,
            "source": "api",
            "active_run_ids": ["run_1"],
            "active_runs": [
                {
                    "simulation_run_id": "run_1",
                    "status": "running",
                    "published_actions": 25,
                    "published_hand_summaries": 2,
                    "updated_at": "2026-04-22T21:00:00Z",
                }
            ],
            "selected_run_id": "run_1",
            "selected_run_meta": {"simulation_run_id": "run_1", "status": "running"},
            "run_profit_leaderboard": [{"player_id": "p1"}],
        }
    )

    monkeypatch.setattr(
        dashboard_app,
        "st",
        type("_FakeStreamlit", (), {"session_state": {}})(),
    )
    monkeypatch.setattr(
        dashboard_app,
        "_load_live_result_cached",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("should not refetch")),
    )

    selected = _resolve_selected_snapshot(
        overview,
        selected_run_id="run_1",
        user_filter=None,
        backend_filter=None,
        topn=25,
    )

    assert selected is overview


def test_resolve_selected_snapshot_refetches_selected_run_payload(monkeypatch) -> None:
    overview = _live_result_from_payload(
        {
            "available": True,
            "source": "api",
            "active_run_ids": ["run_1", "run_2"],
            "active_runs": [
                {"simulation_run_id": "run_1", "status": "running"},
                {
                    "simulation_run_id": "run_2",
                    "status": "running",
                    "published_actions": 80,
                    "published_hand_summaries": 8,
                    "updated_at": "2026-04-22T21:00:00Z",
                },
            ],
            "selected_run_id": "run_1",
            "selected_run_meta": {"simulation_run_id": "run_1", "status": "running"},
        }
    )
    fetched_result = _live_result_from_payload(
        {
            "available": True,
            "source": "api",
            "selected_run_id": "run_2",
            "selected_run_meta": {"simulation_run_id": "run_2", "status": "running"},
            "run_profit_leaderboard": [{"player_id": "fresh"}],
        }
    )
    captured = {}
    monkeypatch.setattr(
        dashboard_app,
        "st",
        type("_FakeStreamlit", (), {"session_state": {}})(),
    )
    def _fake_load_live_result_cached(**kwargs):
        captured["kwargs"] = kwargs
        return fetched_result

    monkeypatch.setattr(
        dashboard_app,
        "_load_live_result_cached",
        _fake_load_live_result_cached,
    )

    selected = _resolve_selected_snapshot(
        overview,
        selected_run_id="run_2",
        user_filter=None,
        backend_filter=None,
        topn=25,
    )

    assert selected is fetched_result
    assert captured["kwargs"]["simulation_run_id"] == "run_2"
