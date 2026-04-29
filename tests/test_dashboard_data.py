from __future__ import annotations

import unittest

import pandas as pd

from dashboard.analytics import (
    compare_live_agents_to_clusters,
    compare_personas_to_clusters,
    filter_actions,
    summarize_live_metrics,
    summarize_player_clusters,
)


class DashboardDataTests(unittest.TestCase):
    def test_cluster_summary_and_comparison_helpers(self) -> None:
        training_features = pd.DataFrame(
            [
                {"player_id": "p1", "source_dataset": "hist", "hands_played": 10, "vpip_rate": 0.2, "pfr_rate": 0.15, "aggression_frequency": 0.4, "river_seen_rate": 0.3},
                {"player_id": "p2", "source_dataset": "hist", "hands_played": 20, "vpip_rate": 0.18, "pfr_rate": 0.14, "aggression_frequency": 0.42, "river_seen_rate": 0.28},
                {"player_id": "p3", "source_dataset": "hist", "hands_played": 30, "vpip_rate": 0.33, "pfr_rate": 0.23, "aggression_frequency": 0.6, "river_seen_rate": 0.2},
            ]
        )
        assignments = pd.DataFrame(
            [
                {"player_id": "p1", "source_dataset": "hist", "cluster_id": 0, "cluster_label": "TAG", "model_run_id": "run_1"},
                {"player_id": "p2", "source_dataset": "hist", "cluster_id": 0, "cluster_label": "TAG", "model_run_id": "run_1"},
                {"player_id": "p3", "source_dataset": "hist", "cluster_id": 1, "cluster_label": "LAG", "model_run_id": "run_1"},
            ]
        )
        centroids = pd.DataFrame(
            [
                {"cluster_id": 0, "cluster_label": "TAG", "centroid_json": {"vpip_rate": 0.19, "pfr_rate": 0.15, "aggression_frequency": 0.41, "river_seen_rate": 0.27, "flop_cbet_rate": 0.54, "all_in_rate": 0.03}},
                {"cluster_id": 1, "cluster_label": "LAG", "centroid_json": {"vpip_rate": 0.3, "pfr_rate": 0.22, "aggression_frequency": 0.59, "river_seen_rate": 0.2, "flop_cbet_rate": 0.6, "all_in_rate": 0.05}},
            ]
        )
        personas = pd.DataFrame(
            [
                {"persona_name": "nit", "persona_text": "tight preflop", "vpip": 0.12, "pfr": 0.1, "aggression_frequency": 0.26, "showdown_frequency": 0.33, "cbet_rate": 0.42, "all_in_rate": 0.01},
                {"persona_name": "maniac", "persona_text": "very aggressive", "vpip": 0.57, "pfr": 0.33, "aggression_frequency": 0.8, "showdown_frequency": 0.15, "cbet_rate": 0.7, "all_in_rate": 0.12},
            ]
        )
        live_metrics = pd.DataFrame(
            [
                {"simulation_run_id": "sim_1", "agent_id": "a1", "actions_per_second": 2.1, "hands_per_second": 0.4, "vpip": 0.32, "pfr": 0.22, "aggression_frequency": 0.58, "cbet_rate": 0.6, "all_in_rate": 0.05, "bb_won": 4.2},
                {"simulation_run_id": "sim_1", "agent_id": "a2", "actions_per_second": 1.8, "hands_per_second": 0.35, "vpip": 0.28, "pfr": 0.2, "aggression_frequency": 0.52, "cbet_rate": 0.57, "all_in_rate": 0.04, "bb_won": 2.1},
            ]
        )
        actions = pd.DataFrame(
            [
                {"hand_id": "h1", "action_index": 1, "street": "preflop", "player_id": "p1", "agent_id": "", "action_type": "raise", "source_type": "historical", "persona_name": "TAG", "table_id": "t1", "event_ts": "2026-04-16T12:00:00Z"},
                {"hand_id": "h1", "action_index": 2, "street": "flop", "player_id": "p2", "agent_id": "", "action_type": "call", "source_type": "historical", "persona_name": "TAG", "table_id": "t1", "event_ts": "2026-04-16T12:00:01Z"},
                {"hand_id": "h2", "action_index": 1, "street": "preflop", "player_id": "", "agent_id": "a1", "action_type": "bet", "source_type": "simulated", "persona_name": "maniac", "table_id": "t1", "event_ts": "2026-04-16T12:00:02Z"},
            ]
        )

        summary = summarize_player_clusters(training_features, assignments, centroids)
        self.assertEqual(len(summary), 2)
        self.assertIn("avg_vpip_rate", summary.columns)

        comparison = compare_personas_to_clusters(centroids, personas)
        self.assertEqual(len(comparison), 2)
        self.assertIn("style_distance", comparison.columns)

        live_assignments = pd.DataFrame(
            [
                {"simulation_run_id": "sim_1", "agent_id": "a1", "player_id": "player_1", "nearest_cluster_id": 1, "nearest_cluster_label": "LAG", "distance_to_centroid": 0.12},
                {"simulation_run_id": "sim_1", "agent_id": "a2", "player_id": "player_2", "nearest_cluster_id": 0, "nearest_cluster_label": "TAG", "distance_to_centroid": 0.08},
            ]
        )
        live_comparison = compare_live_agents_to_clusters(
            live_metrics.assign(player_id=["player_1", "player_2"], persona_name=["maniac", "nit"]),
            live_assignments,
            centroids,
            personas,
        )
        self.assertEqual(len(live_comparison), 2)
        self.assertIn("nearest_cluster_label", live_comparison.columns)

        live_summary = summarize_live_metrics(live_metrics)
        self.assertEqual(live_summary.iloc[0]["agent_count"], 2)

        filtered = filter_actions(actions, source_type="simulated")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["hand_id"], "h2")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
