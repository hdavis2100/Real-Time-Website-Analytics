from __future__ import annotations

from dashboard.data_access import load_live_dashboard_data


class _FakeLiveStore:
    def __init__(self) -> None:
        self._run_profit = [
            {
                "simulation_run_id": "run_live_1",
                "player_id": "user_agent",
                "agent_id": "user_agent",
                "is_hero_player": True,
                "status": "running",
                "decision_backend": "heuristic",
                "total_bb_won": 12.5,
            }
        ]
        self._run_agents = [
            {
                "simulation_run_id": "run_live_1",
                "player_id": "user_agent",
                "agent_id": "user_agent",
                "is_hero_player": True,
                "status": "running",
                "decision_backend": "heuristic",
                "vpip": 0.28,
                "pfr": 0.17,
                "aggression_frequency": 0.44,
                "hands_played": 24,
                "observed_actions": 61,
                "total_bb_won": 12.5,
            }
        ]
        self._player_hands = [
            {
                "simulation_run_id": "run_live_1",
                "hand_id": "hand_1",
                "player_id": "user_agent",
                "agent_id": "user_agent",
                "is_hero_player": True,
                "result_bb": 5.0,
            },
            {
                "simulation_run_id": "run_live_1",
                "hand_id": "hand_2",
                "player_id": "user_agent",
                "agent_id": "user_agent",
                "is_hero_player": True,
                "result_bb": 7.5,
            },
        ]

    def list_active_runs(self, *, user_id: str | None = None) -> list[str]:
        return []

    def get_run_meta(self, run_id: str) -> dict[str, object]:
        return {}

    def get_run_leaderboard(self, run_id: str, kind: str, *, topn: int = 25) -> list[dict[str, object]]:
        if run_id != "run_live_1":
            return []
        if kind == "profit":
            return list(self._run_profit)
        return []

    def get_global_leaderboard(self, kind: str, *, topn: int = 25) -> list[dict[str, object]]:
        return []

    def get_run_agents(self, run_id: str) -> list[dict[str, object]]:
        if run_id != "run_live_1":
            return []
        return list(self._run_agents)

    def list_json_hash(self, suffix: str) -> list[dict[str, object]]:
        if suffix == "run:run_live_1:state:player_hands":
            return list(self._player_hands)
        return []


def test_load_live_dashboard_data_synthesizes_selected_run_from_live_rows() -> None:
    result = load_live_dashboard_data(
        simulation_run_id="run_live_1",
        topn=10,
        live_store=_FakeLiveStore(),
    )

    assert result.available is True
    assert result.selected_run_id == "run_live_1"
    assert result.selected_run_meta is not None
    assert result.selected_run_meta["simulation_run_id"] == "run_live_1"
    assert result.selected_run_meta["status"] == "running"
    assert result.selected_run_meta["published_hand_summaries"] == 2
    assert result.selected_run_meta["published_actions"] == 61
    assert len(result.run_agents or []) == 1
    assert result.run_agents[0]["vpip"] == 0.28
    assert result.run_agents[0]["pfr"] == 0.17
    assert result.run_agents[0]["aggression_frequency"] == 0.44
    assert result.run_agents[0]["observed_hands"] == 24
    assert result.run_agents[0]["bb_won"] == 12.5
    assert len(result.hero_profit_timeseries or []) == 2
    assert result.hero_profit_timeseries[-1]["hand_number"] == 24
    assert result.hero_profit_timeseries[-1]["cumulative_bb_won"] == 12.5
