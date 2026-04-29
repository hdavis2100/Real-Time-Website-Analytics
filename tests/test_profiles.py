from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

import numpy as np
import pandas as pd

import profiles.activate_model as activate_model_module
import profiles.relabel_model as relabel_model_module
import profiles.score_session as score_session_module
import profiles.train_model as train_model_module
from profiles.constants import (
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    SIMULATED_POSTFLOP_BET_POT_RATIO_CAP,
    SIMULATED_PREFLOP_RAISE_BB_CAP,
)
from profiles.labels import label_centroid
from profiles.reference_features import (
    aggregate_session_features,
    aggregate_training_features,
    compute_player_hand_features,
)
from profiles.scoring import score_feature_vector


def test_reference_training_and_session_features_share_the_same_vector_contract() -> None:
    actions, players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_1",
        simulation_run_id="sim_run_1",
        player_id="hero_player",
        agent_id="hero_agent",
        source_dataset="fixture_set",
    )

    hand_features = compute_player_hand_features(actions, players)
    training_features = aggregate_training_features(hand_features, min_hands=50)
    session_features = aggregate_session_features(actions, players)

    assert bool(training_features.iloc[0]["eligible_for_training"]) is True
    assert int(training_features.iloc[0]["hands_played"]) == 60
    assert int(session_features.iloc[0]["hands_observed"]) == 60
    assert session_features.iloc[0]["subject_id"] == "hero_agent"
    assert session_features.iloc[0]["agent_id"] == "hero_agent"

    for column in FEATURE_COLUMNS:
        assert float(session_features.iloc[0][column]) == float(training_features.iloc[0][column])


def test_score_feature_vector_prefers_nearest_centroid() -> None:
    artifact = {
        "model_run_id": "model_ready",
        "feature_version": FEATURE_VERSION,
        "feature_columns": FEATURE_COLUMNS,
        "feature_stats": [
            {
                "feature_name": column,
                "feature_order": index + 1,
                "scaler_mean": 0.0,
                "scaler_scale": 1.0,
                "global_mean": 0.2,
            }
            for index, column in enumerate(FEATURE_COLUMNS)
        ],
        "centroids": [
            {
                "cluster_id": 0,
                "cluster_label": "Nit",
                "summary_text": "Nit summary",
                "centroid": {column: 0.0 for column in FEATURE_COLUMNS},
            },
            {
                "cluster_id": 1,
                "cluster_label": "LAG",
                "summary_text": "LAG summary",
                "centroid": {column: 1.0 for column in FEATURE_COLUMNS},
            },
        ],
    }

    scored = score_feature_vector(
        {column: 0.9 for column in FEATURE_COLUMNS} | {"hands_observed": 120},
        artifact,
    )

    assert scored["cluster_id"] == 1
    assert scored["cluster_label"] == "LAG"
    assert scored["confidence_score"] > 0
    assert len(scored["top_feature_deltas"]) == 5
    assert all(delta["feature_name"] in FEATURE_COLUMNS for delta in scored["top_feature_deltas"])


def test_select_cluster_count_rejects_too_small_clusters(monkeypatch) -> None:
    scaled = np.arange(200, dtype=float).reshape(100, 2)

    class _FakeMiniBatchKMeans:
        def __init__(self, *, n_clusters: int, **_kwargs) -> None:
            self.n_clusters = n_clusters

        def fit_predict(self, _scaled: np.ndarray) -> np.ndarray:
            return np.array(_label_map(self.n_clusters), dtype=int)

    silhouette_by_k = {8: 0.41, 10: 0.52, 12: 0.91, 14: 0.49}

    monkeypatch.setattr(train_model_module, "MiniBatchKMeans", _FakeMiniBatchKMeans)
    monkeypatch.setattr(
        train_model_module,
        "silhouette_score",
        lambda _scaled, labels: silhouette_by_k[len(set(labels.tolist()))],
    )

    candidate_results: list[dict[str, float | int | str]] = []
    selected = train_model_module._select_cluster_count(scaled, candidate_results)

    assert selected == 10
    rejected = next(result for result in candidate_results if result["cluster_count"] == 12)
    assert rejected["accepted"] is False


def test_score_completed_session_returns_insufficient_evidence_and_persists_outputs(monkeypatch) -> None:
    actions, players = _build_actions_and_players(
        hand_count=20,
        source_type="simulated",
        source_run_id="sim_run_2",
        simulation_run_id="sim_run_2",
        player_id="hero_player",
        agent_id="hero_agent",
        source_dataset="fixture_set",
    )
    warehouse = _FakeWarehouse(actions=actions, players=players)
    monkeypatch.setattr(score_session_module, "bootstrap_backend", lambda: warehouse)
    monkeypatch.setattr(
        score_session_module,
        "load_active_model_artifact",
        lambda **_kwargs: _ready_artifact("model_for_session"),
    )

    result = score_session_module.score_completed_session(
        simulation_run_id="sim_run_2",
        agent_id="hero_agent",
    )

    assert result["status"] == "insufficient_evidence"
    assert result["model_run_id"] == "model_for_session"
    assert warehouse.tables["PROFILE_SESSION_FEATURES"].shape[0] == 1
    assert warehouse.tables["PROFILE_SESSION_RESULTS"].shape[0] == 1


def test_score_completed_sessions_returns_all_subjects_and_reuses_cached_results(monkeypatch) -> None:
    hero_actions, hero_players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_3",
        simulation_run_id="sim_run_3",
        player_id="hero_player",
        agent_id="hero_agent",
        source_dataset="fixture_set",
    )
    villain_actions, villain_players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_3",
        simulation_run_id="sim_run_3",
        player_id="villain_player",
        agent_id="villain_agent",
        source_dataset="fixture_set",
    )
    warehouse = _FakeWarehouse(
        actions=pd.concat([hero_actions, villain_actions], ignore_index=True),
        players=pd.concat([hero_players, villain_players], ignore_index=True),
    )
    monkeypatch.setattr(score_session_module, "bootstrap_backend", lambda: warehouse)
    monkeypatch.setattr(
        score_session_module,
        "load_active_model_artifact",
        lambda **_kwargs: _ready_artifact("model_for_multi_session"),
    )

    result = score_session_module.score_completed_sessions(simulation_run_id="sim_run_3")

    assert result["status"] == "ready"
    assert result["cached"] is False
    assert len(result["results"]) == 2
    assert warehouse.tables["PROFILE_SESSION_FEATURES"].shape[0] == 2
    assert warehouse.tables["PROFILE_SESSION_RESULTS"].shape[0] == 2

    cached = score_session_module.score_completed_sessions(
        simulation_run_id="sim_run_3",
        cached_only=True,
    )
    assert cached["status"] == "ready"
    assert cached["cached"] is True
    assert len(cached["results"]) == 2


def test_score_completed_sessions_ignores_cached_profiles_from_prior_execution(monkeypatch) -> None:
    actions, players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_reused",
        simulation_run_id="sim_run_reused",
        player_id="old_player",
        agent_id="old_agent",
        source_dataset="fixture_set",
    )
    warehouse = _FakeWarehouse(actions=actions, players=players)
    warehouse.tables["PROFILE_SESSION_RESULTS"] = pd.DataFrame(
        [
            {
                "model_run_id": "model_reused",
                "feature_version": FEATURE_VERSION,
                "source_type": "simulated",
                "source_dataset": "fixture_set",
                "source_run_id": "sim_run_reused",
                "simulation_run_id": "sim_run_reused",
                "subject_id": "old_agent",
                "player_id": "old_player",
                "agent_id": "old_agent",
                "hands_observed": 60,
                "status": "ready",
                "cluster_id": 0,
                "cluster_label": "Old profile",
                "confidence_score": 0.9,
                "summary_text": "old",
                "feature_vector": {},
                "top_feature_deltas": [],
                "prototype_matches": [],
                "scored_at": datetime(2026, 4, 19, 12, 5, 0),
            }
        ]
    )
    monkeypatch.setattr(score_session_module, "bootstrap_backend", lambda: warehouse)
    monkeypatch.setattr(
        score_session_module,
        "load_active_model_artifact",
        lambda **_kwargs: _ready_artifact("model_reused"),
    )

    cached = score_session_module.score_completed_sessions(
        simulation_run_id="sim_run_reused",
        cached_only=True,
        min_event_ts="2026-04-20T12:00:00Z",
    )

    assert cached["status"] == "not_found"


def test_score_completed_sessions_filters_reused_run_inputs_by_execution_time(monkeypatch) -> None:
    old_actions, old_players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_reused_inputs",
        simulation_run_id="sim_run_reused_inputs",
        player_id="old_player",
        agent_id="old_agent",
        source_dataset="fixture_set",
    )
    new_actions, new_players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_reused_inputs",
        simulation_run_id="sim_run_reused_inputs",
        player_id="new_player",
        agent_id="new_agent",
        source_dataset="fixture_set",
    )
    new_actions = new_actions.copy()
    new_players = new_players.copy()
    new_actions["event_ts"] = datetime(2026, 4, 20, 12, 0, 0)
    new_actions["hand_id"] = [f"new_hand_{index}" for index in range(len(new_actions))]
    new_players["hand_id"] = [f"new_hand_{index}" for index in range(len(new_players))]
    warehouse = _FakeWarehouse(
        actions=pd.concat([old_actions, new_actions], ignore_index=True),
        players=pd.concat([old_players, new_players], ignore_index=True),
    )
    monkeypatch.setattr(score_session_module, "bootstrap_backend", lambda: warehouse)
    monkeypatch.setattr(
        score_session_module,
        "load_active_model_artifact",
        lambda **_kwargs: _ready_artifact("model_reused_inputs"),
    )

    result = score_session_module.score_completed_sessions(
        simulation_run_id="sim_run_reused_inputs",
        min_event_ts="2026-04-20T00:00:00Z",
    )

    assert result["status"] == "ready"
    assert result["cached"] is False
    assert [row["player_id"] for row in result["results"]] == ["new_player"]


def test_score_completed_sessions_cached_only_returns_not_found_without_persisting(monkeypatch) -> None:
    actions, players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_4",
        simulation_run_id="sim_run_4",
        player_id="hero_player",
        agent_id="hero_agent",
        source_dataset="fixture_set",
    )
    warehouse = _FakeWarehouse(actions=actions, players=players)
    monkeypatch.setattr(score_session_module, "bootstrap_backend", lambda: warehouse)
    monkeypatch.setattr(
        score_session_module,
        "load_active_model_artifact",
        lambda **_kwargs: _ready_artifact("model_cached_only"),
    )

    result = score_session_module.score_completed_sessions(
        simulation_run_id="sim_run_4",
        cached_only=True,
    )

    assert result["status"] == "not_found"
    assert result["message"] == "No completed profile results were found"
    assert warehouse.tables.get("PROFILE_SESSION_FEATURES", pd.DataFrame()).empty
    assert warehouse.tables.get("PROFILE_SESSION_RESULTS", pd.DataFrame()).empty


def test_score_completed_sessions_can_use_provisional_session_features(monkeypatch) -> None:
    actions, players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_run_provisional",
        simulation_run_id="sim_run_provisional",
        player_id="hero_player",
        agent_id="hero_agent",
        source_dataset="fixture_set",
    )
    session_features = aggregate_session_features(actions, players)
    warehouse = _FakeWarehouse(actions=pd.DataFrame(), players=pd.DataFrame())
    monkeypatch.setattr(score_session_module, "bootstrap_backend", lambda: warehouse)
    monkeypatch.setattr(
        score_session_module,
        "load_active_model_artifact",
        lambda **_kwargs: _ready_artifact("model_provisional"),
    )

    result = score_session_module.score_completed_sessions(
        simulation_run_id="sim_run_provisional",
        session_features_json=session_features.to_json(orient="records", date_format="iso"),
    )

    assert result["status"] == "ready"
    assert result["cached"] is False
    assert len(result["results"]) == 1
    assert result["results"][0]["player_id"] == "hero_player"
    assert warehouse.tables["PROFILE_SESSION_FEATURES"].shape[0] == 1
    assert warehouse.tables["PROFILE_SESSION_RESULTS"].shape[0] == 1


def test_profile_warehouse_helper_can_skip_schema_bootstrap(monkeypatch) -> None:
    warehouse = _FakeWarehouse(actions=pd.DataFrame(), players=pd.DataFrame())

    monkeypatch.setenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP", "1")
    monkeypatch.setattr(score_session_module, "get_warehouse", lambda *_args, **_kwargs: warehouse)
    monkeypatch.setattr(score_session_module, "get_config", lambda: None)
    monkeypatch.setattr(
        score_session_module,
        "bootstrap_backend",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("bootstrap should be skipped")),
    )

    try:
        assert score_session_module._warehouse_or_default() is warehouse
    finally:
        monkeypatch.delenv("POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP", raising=False)


def test_simulated_session_features_clip_unbounded_wager_sizes() -> None:
    actions, players = _build_actions_and_players(
        hand_count=60,
        source_type="simulated",
        source_run_id="sim_clip_1",
        simulation_run_id="sim_clip_1",
        player_id="hero_player",
        agent_id="hero_agent",
        source_dataset="fixture_set",
    )
    actions = actions.copy()
    actions["action_type"] = "raise"
    actions["amount_bb"] = 1_000_000.0
    actions["pot_before_bb"] = 1.5
    actions["pot_after_bb"] = 1_000_001.5
    flop_rows = actions.copy()
    flop_rows["street"] = "flop"
    flop_rows["action_index"] = 1
    flop_rows["action_type"] = "bet"
    flop_rows["amount_bb"] = 50_000.0
    flop_rows["pot_before_bb"] = 1.0
    flop_rows["pot_after_bb"] = 50_001.0
    actions = pd.concat([actions, flop_rows], ignore_index=True)

    session_features = aggregate_session_features(actions, players)

    assert float(session_features.iloc[0]["avg_preflop_raise_bb"]) == SIMULATED_PREFLOP_RAISE_BB_CAP
    assert float(session_features.iloc[0]["avg_postflop_bet_pot_ratio"]) == SIMULATED_POSTFLOP_BET_POT_RATIO_CAP
def test_river_seen_rate_captures_later_street_reach() -> None:
    actions, players = _build_actions_and_players(
        hand_count=4,
        source_type="historical",
        source_run_id="hist_run_1",
        simulation_run_id=None,
        player_id="hero_player",
        agent_id=None,
        source_dataset="fixture_set",
    )
    actions = actions.copy()
    river_rows = pd.DataFrame(
        [
            {
                "hand_id": "hand_0",
                "action_index": 1,
                "source_run_id": "hist_run_1",
                "source_type": "historical",
                "source_dataset": "fixture_set",
                "simulation_run_id": None,
                "table_id": "table_1",
                "street": "river",
                "player_id": "hero_player",
                "agent_id": None,
                "seat": 1,
                "position": "BTN",
                "action_type": "call",
                "amount_bb": 2.0,
                "pot_before_bb": 8.0,
                "pot_after_bb": 10.0,
                "to_call_bb": 2.0,
                "effective_stack_bb": 60.0,
                "players_remaining": 2,
                "board_cards_visible": "[]",
                "hole_cards_visible": "[]",
                "is_all_in": False,
                "event_ts": datetime(2026, 4, 19, 12, 0, 1),
                "backend_type": "historical",
                "persona_name": None,
                "persona_text": None,
                "payload_version": "1",
                "raw_lineage": "{}",
            },
            {
                "hand_id": "hand_1",
                "action_index": 1,
                "source_run_id": "hist_run_1",
                "source_type": "historical",
                "source_dataset": "fixture_set",
                "simulation_run_id": None,
                "table_id": "table_1",
                "street": "river",
                "player_id": "hero_player",
                "agent_id": None,
                "seat": 1,
                "position": "BTN",
                "action_type": "bet",
                "amount_bb": 3.0,
                "pot_before_bb": 9.0,
                "pot_after_bb": 12.0,
                "to_call_bb": 0.0,
                "effective_stack_bb": 58.0,
                "players_remaining": 2,
                "board_cards_visible": "[]",
                "hole_cards_visible": "[]",
                "is_all_in": False,
                "event_ts": datetime(2026, 4, 19, 12, 0, 2),
                "backend_type": "historical",
                "persona_name": None,
                "persona_text": None,
                "payload_version": "1",
                "raw_lineage": "{}",
            },
        ]
    )
    actions = pd.concat([actions, river_rows], ignore_index=True)

    hand_features = compute_player_hand_features(actions, players)
    training_features = aggregate_training_features(hand_features, min_hands=1)

    assert float(training_features.iloc[0]["river_seen_rate"]) == 0.5


def test_label_centroid_uses_river_and_stack_signals() -> None:
    assert (
        label_centroid(
            {
                "vpip_rate": 0.46,
                "pfr_rate": 0.11,
                "aggression_frequency": 0.41,
                "call_preference": 0.59,
                "river_seen_rate": 0.44,
                "blind_defend_rate": 0.31,
                "short_stack_aggression_rate": 0.22,
                "all_in_rate": 0.01,
            }
        )
        == "Calling station"
    )
    assert (
        label_centroid(
            {
                "vpip_rate": 0.27,
                "pfr_rate": 0.15,
                "aggression_frequency": 0.6,
                "call_preference": 0.34,
                "river_seen_rate": 0.3,
                "blind_defend_rate": 0.19,
                "short_stack_aggression_rate": 0.74,
                "all_in_rate": 0.05,
            }
        )
        == "Short-stack jammer"
    )


def test_label_centroid_distinguishes_loose_and_polarized_callers() -> None:
    assert (
        label_centroid(
            {
                "vpip_rate": 0.3366,
                "pfr_rate": 0.0689,
                "aggression_frequency": 0.3635,
                "call_preference": 0.6364,
                "blind_defend_rate": 0.3437,
                "avg_preflop_raise_bb": 5.05,
            }
        )
        == "Loose caller"
    )
    assert (
        label_centroid(
            {
                "vpip_rate": 0.3747,
                "pfr_rate": 0.0608,
                "aggression_frequency": 0.3282,
                "call_preference": 0.6717,
                "blind_defend_rate": 0.3837,
                "avg_preflop_raise_bb": 16.96,
            }
        )
        == "Polarized caller"
    )


def test_label_centroid_distinguishes_turn_barrel_and_loose_regular() -> None:
    assert (
        label_centroid(
            {
                "vpip_rate": 0.2157,
                "pfr_rate": 0.1095,
                "aggression_frequency": 0.5730,
                "call_preference": 0.4269,
                "turn_barrel_rate": 0.5080,
            }
        )
        == "Turn-barrel regular"
    )
    assert (
        label_centroid(
            {
                "vpip_rate": 0.3982,
                "pfr_rate": 0.1576,
                "aggression_frequency": 0.4879,
                "call_preference": 0.5120,
                "blind_defend_rate": 0.4071,
            }
        )
        == "Loose regular"
    )


def test_relabel_profile_model_updates_active_outputs() -> None:
    warehouse = _FakeWarehouse(actions=pd.DataFrame(), players=pd.DataFrame())
    warehouse.tables["PROFILE_MODEL_RUNS"] = pd.DataFrame(
        [
            {
                "model_run_id": "active_model",
                "active": True,
                "status": "active",
                "created_at": datetime(2026, 4, 22, 12, 0, 0),
                "activated_at": datetime(2026, 4, 22, 12, 0, 0),
            }
        ]
    )
    warehouse.tables["PROFILE_CLUSTER_CENTROIDS"] = pd.DataFrame(
        [
            {
                "model_run_id": "active_model",
                "cluster_id": 1,
                "cluster_label": "Balanced regular",
                "summary_text": "old",
                "cluster_size": 10,
                "cluster_share": 0.5,
                "centroid_json": {
                    "vpip_rate": 0.3366,
                    "pfr_rate": 0.0689,
                    "aggression_frequency": 0.3635,
                    "call_preference": 0.6364,
                    "blind_defend_rate": 0.3437,
                    "avg_preflop_raise_bb": 5.05,
                },
                "updated_at": datetime(2026, 4, 22, 12, 0, 0),
            },
            {
                "model_run_id": "active_model",
                "cluster_id": 7,
                "cluster_label": "Balanced regular",
                "summary_text": "old",
                "cluster_size": 10,
                "cluster_share": 0.5,
                "centroid_json": {
                    "vpip_rate": 0.3747,
                    "pfr_rate": 0.0608,
                    "aggression_frequency": 0.3282,
                    "call_preference": 0.6717,
                    "blind_defend_rate": 0.3837,
                    "avg_preflop_raise_bb": 16.96,
                },
                "updated_at": datetime(2026, 4, 22, 12, 0, 0),
            },
        ]
    )
    warehouse.tables["PROFILE_HISTORICAL_ASSIGNMENTS"] = pd.DataFrame(
        [
            {
                "model_run_id": "active_model",
                "player_id": "hist_1",
                "source_dataset": "fixture",
                "cluster_id": 1,
                "cluster_label": "Balanced regular",
                "distance_to_centroid": 0.1,
                "hands_played": 100,
                "assigned_at": datetime(2026, 4, 22, 12, 0, 0),
            }
        ]
    )
    warehouse.tables["PROFILE_SESSION_RESULTS"] = pd.DataFrame(
        [
            {
                "model_run_id": "active_model",
                "feature_version": FEATURE_VERSION,
                "source_type": "simulated",
                "source_dataset": "fixture",
                "source_run_id": "sim_1",
                "simulation_run_id": "sim_1",
                "subject_id": "agent_1",
                "player_id": "player_1",
                "agent_id": "agent_1",
                "hands_observed": 150,
                "status": "ready",
                "cluster_id": 7,
                "cluster_label": "Balanced regular",
                "confidence_score": 0.75,
                "summary_text": "old",
                "feature_vector": {},
                "top_feature_deltas": [],
                "prototype_matches": [],
                "scored_at": datetime(2026, 4, 22, 12, 0, 0),
            }
        ]
    )

    result = relabel_model_module.relabel_profile_model(warehouse=warehouse)

    assert result["status"] == "ready"
    centroid_labels = warehouse.tables["PROFILE_CLUSTER_CENTROIDS"].sort_values("cluster_id")["cluster_label"].tolist()
    assert centroid_labels == ["Loose caller", "Polarized caller"]
    assert warehouse.tables["PROFILE_HISTORICAL_ASSIGNMENTS"].iloc[0]["cluster_label"] == "Loose caller"
    session_row = warehouse.tables["PROFILE_SESSION_RESULTS"].iloc[0]
    assert session_row["cluster_label"] == "Polarized caller"
    assert "unusually large preflop raises" in str(session_row["summary_text"])


class _FakeWarehouse:
    def __init__(self, *, actions: pd.DataFrame, players: pd.DataFrame) -> None:
        self.actions = actions
        self.players = players
        self.tables: dict[str, pd.DataFrame] = {}

    def load_table(self, table: str, *, filters_eq=None, order_by=None, limit=None, **_kwargs) -> pd.DataFrame:
        if table == "CURATED_ACTIONS":
            frame = self.actions.copy()
        elif table == "CURATED_PLAYERS":
            frame = self.players.copy()
        elif table == "PROFILE_CLUSTER_PROTOTYPES":
            frame = self.tables.get(table, pd.DataFrame()).copy()
        elif table == "PROFILE_MODEL_RUNS":
            frame = self.tables.get(table, pd.DataFrame()).copy()
        else:
            frame = self.tables.get(table, pd.DataFrame()).copy()

        if filters_eq:
            for column, value in filters_eq.items():
                if column in frame.columns:
                    frame = frame[frame[column] == value]
        if order_by:
            sort_columns: list[str] = []
            ascending: list[bool] = []
            for item in order_by:
                if isinstance(item, tuple):
                    column, is_ascending = item
                else:
                    column, is_ascending = item, True
                if column in frame.columns:
                    sort_columns.append(column)
                    ascending.append(is_ascending)
            if sort_columns:
                frame = frame.sort_values(sort_columns, ascending=ascending)
        if limit is not None:
            frame = frame.head(limit)
        return frame.reset_index(drop=True)

    def delete_matching_keys(self, table: str, frame: pd.DataFrame, key_columns: Iterable[str]) -> None:
        existing = self.tables.get(table)
        if existing is None or existing.empty:
            return
        remaining = existing.copy()
        for _, row in frame[list(key_columns)].drop_duplicates().iterrows():
            mask = pd.Series(True, index=remaining.index)
            for column in key_columns:
                mask &= remaining[column].astype("object") == row[column]
            remaining = remaining.loc[~mask]
        self.tables[table] = remaining.reset_index(drop=True)

    def write_dataframe(self, table: str, frame: pd.DataFrame, **_kwargs) -> None:
        existing = self.tables.get(table)
        if existing is None or existing.empty:
            self.tables[table] = frame.reset_index(drop=True)
        else:
            self.tables[table] = pd.concat([existing, frame], ignore_index=True)


def _build_actions_and_players(
    *,
    hand_count: int,
    source_type: str,
    source_run_id: str,
    simulation_run_id: str | None,
    player_id: str,
    agent_id: str | None,
    source_dataset: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    actions: list[dict[str, object]] = []
    players: list[dict[str, object]] = []
    for hand_index in range(hand_count):
        hand_id = f"hand_{hand_index}"
        actions.append(
            {
                "hand_id": hand_id,
                "action_index": 0,
                "source_run_id": source_run_id,
                "source_type": source_type,
                "source_dataset": source_dataset,
                "simulation_run_id": simulation_run_id,
                "table_id": "table_1",
                "street": "preflop",
                "player_id": player_id,
                "agent_id": agent_id,
                "seat": 1,
                "position": "BTN",
                "action_type": "call",
                "amount_bb": 1.0,
                "pot_before_bb": 1.5,
                "pot_after_bb": 2.5,
                "to_call_bb": 1.0,
                "effective_stack_bb": 100.0,
                "players_remaining": 6,
                "board_cards_visible": "[]",
                "hole_cards_visible": "[]",
                "is_all_in": False,
                "event_ts": datetime(2026, 4, 19, 12, 0, 0),
                "backend_type": "heuristic_persona",
                "persona_name": None,
                "persona_text": None,
                "payload_version": "1",
                "raw_lineage": "{}",
            }
        )
        players.append(
            {
                "source_run_id": source_run_id,
                "source_type": source_type,
                "source_dataset": source_dataset,
                "simulation_run_id": simulation_run_id,
                "table_id": "table_1",
                "hand_id": hand_id,
                "player_id": player_id,
                "agent_id": agent_id,
                "seat": 1,
                "player_name": player_id,
                "stack_start_bb": 100.0,
                "stack_end_bb": 99.0,
                "hole_cards": "[]",
                "result_bb": -1.0,
                "persona_name": None,
                "persona_text": None,
                "payload_version": "1",
                "raw_lineage": "{}",
            }
        )
    return pd.DataFrame(actions), pd.DataFrame(players)


def _label_map(cluster_count: int) -> list[int]:
    if cluster_count == 8:
        counts = [13, 13, 13, 13, 12, 12, 12, 12]
    elif cluster_count == 10:
        counts = [10] * 10
    elif cluster_count == 12:
        counts = [2, 10, 9, 9, 9, 9, 9, 9, 9, 8, 8, 9]
    elif cluster_count == 14:
        counts = [3, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 10, 10]
    else:  # pragma: no cover - defensive
        raise ValueError(cluster_count)

    labels: list[int] = []
    for cluster_id, count in enumerate(counts):
        labels.extend([cluster_id] * count)
    assert len(labels) == 100
    return labels


def _ready_artifact(model_run_id: str) -> dict[str, object]:
    return {
        "status": "ready",
        "model_run_id": model_run_id,
        "training_dataset": "all_historical_humans",
        "feature_version": FEATURE_VERSION,
        "cluster_count": 1,
        "feature_columns": FEATURE_COLUMNS,
        "feature_stats": [
            {
                "feature_name": column,
                "feature_order": index + 1,
                "scaler_mean": 0.0,
                "scaler_scale": 1.0,
                "global_mean": 0.0,
            }
            for index, column in enumerate(FEATURE_COLUMNS)
        ],
        "centroids": [
            {
                "cluster_id": 0,
                "cluster_label": "Balanced regular",
                "summary_text": "Balanced regular: sits near the middle of the historical population",
                "cluster_size": 100,
                "cluster_share": 1.0,
                "centroid": {column: 0.5 for column in FEATURE_COLUMNS},
            }
        ],
        "prototypes": [],
        "scoring_artifact": {
            "model_run_id": model_run_id,
            "training_dataset": "all_historical_humans",
            "feature_version": FEATURE_VERSION,
            "feature_columns": FEATURE_COLUMNS,
            "feature_stats": [
                {
                    "feature_name": column,
                    "feature_order": index + 1,
                    "scaler_mean": 0.0,
                    "scaler_scale": 1.0,
                    "global_mean": 0.0,
                }
                for index, column in enumerate(FEATURE_COLUMNS)
            ],
            "centroids": [
                {
                    "cluster_id": 0,
                    "cluster_label": "Balanced regular",
                    "summary_text": "Balanced regular: sits near the middle of the historical population",
                    "cluster_size": 100,
                    "cluster_share": 1.0,
                    "centroid": {column: 0.5 for column in FEATURE_COLUMNS},
                }
            ],
        },
    }


def test_train_historical_archetype_model_skips_activation_for_tiny_training_sets(
    monkeypatch,
) -> None:
    warehouse = _FakeWarehouse(actions=pd.DataFrame(), players=pd.DataFrame())
    warehouse.tables["PROFILE_PLAYER_TRAINING_FEATURES"] = pd.DataFrame(
        [
            {
                "training_dataset": "profile_sanity_fixture",
                "feature_version": FEATURE_VERSION,
                "player_id": f"player_{index}",
                "source_dataset": "fixture",
                "hands_played": 250,
                **{column: float(index + 1) / 10.0 for column in FEATURE_COLUMNS},
                "eligible_for_training": True,
                "updated_at": datetime(2026, 4, 21, 12, 0, 0),
            }
            for index in range(6)
        ]
    )
    warehouse.tables["PROFILE_MODEL_RUNS"] = pd.DataFrame(
        [
            {
                "model_run_id": "historical_prod_model",
                "active": True,
                "status": "active",
                "training_player_count": 2469,
            }
        ]
    )
    monkeypatch.setattr(train_model_module, "bootstrap_backend", lambda: warehouse)

    result = train_model_module.train_historical_archetype_model(
        training_dataset="profile_sanity_fixture",
        model_run_id="profile_model_sanity_fixture",
        activate=True,
    )

    assert result["status"] == "ready"
    assert result["active"] is False
    assert "Activation skipped" in str(result["activation_skipped_reason"])

    model_runs = warehouse.tables["PROFILE_MODEL_RUNS"].sort_values("model_run_id").reset_index(drop=True)
    assert bool(model_runs.loc[0, "active"]) is True
    assert bool(model_runs.loc[1, "active"]) is False
    assert model_runs.loc[1, "status"] == "trained"


def test_activate_profile_model_switches_the_active_model() -> None:
    warehouse = _FakeWarehouse(actions=pd.DataFrame(), players=pd.DataFrame())
    warehouse.tables["PROFILE_MODEL_RUNS"] = pd.DataFrame(
        [
            {
                "model_run_id": "profile_model_sanity_fixture",
                "active": True,
                "status": "active",
                "training_player_count": 6,
                "training_dataset": "profile_sanity_fixture",
                "activated_at": datetime(2026, 4, 21, 10, 0, 0),
            },
            {
                "model_run_id": "handhq_full_eval_model_fixture",
                "active": False,
                "status": "trained",
                "training_player_count": 2469,
                "training_dataset": "handhq_full_eval_fixture",
                "activated_at": None,
            },
        ]
    )

    result = activate_model_module.activate_profile_model(
        "handhq_full_eval_model_fixture",
        warehouse=warehouse,
    )

    assert result["status"] == "ready"
    model_runs = warehouse.tables["PROFILE_MODEL_RUNS"].sort_values("model_run_id").reset_index(drop=True)
    assert bool(model_runs.loc[0, "active"]) is True
    assert model_runs.loc[0, "status"] == "active"
    assert bool(model_runs.loc[1, "active"]) is False
    assert model_runs.loc[1, "status"] == "trained"
