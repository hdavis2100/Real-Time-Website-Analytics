from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import unittest

from historical_ingest.handhq_backfill import canonicalize_handhq_hand, iter_handhq_documents
from jobs.stream_kafka import _summary_payloads_to_player_frame
from poker_platform.event_contracts import raw_action_row_from_event, raw_hand_row_from_summary_event, serialize_action_event
from simulator.engine import PokerSimulator
from simulator.run_simulation import default_players
from simulator.types import SimulationConfig


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "historical" / "handhq_6max_sample.phhs"


class EventContractTests(unittest.TestCase):
    def test_historical_actions_serialize_to_kafka_contract(self) -> None:
        _source_file, first_document = next(iter_handhq_documents(FIXTURE_PATH))
        canonicalized = canonicalize_handhq_hand(
            document=first_document,
            source_file=FIXTURE_PATH,
            source_dataset="fixture_set",
            source_run_id="hist_contracts",
            table_id="historical_phh_6max",
            document_index=0,
            file_content_hash="fixture_hash",
        )
        action_row = canonicalized["actions"][0]

        payload = serialize_action_event(action_row)
        raw_row = raw_action_row_from_event(payload)

        self.assertEqual(payload["event_type"], "action")
        self.assertEqual(payload["source_type"], "historical")
        self.assertIn("event_ts", payload)
        self.assertIsInstance(payload["raw_lineage"], dict)
        self.assertTrue(raw_row["board_cards_visible"].startswith("["))
        self.assertTrue(raw_row["raw_lineage"].startswith("{"))

    def test_simulator_contracts_include_required_summary_fields(self) -> None:
        config = SimulationConfig(
            num_hands=1,
            simulation_run_id="contract_sim",
            table_id="table_1",
            user_id="user_123",
            decision_backend="heuristic",
            hero_context_hash="hero_ctx_hash",
            hero_seat=1,
            run_started_at=datetime(2026, 4, 29, 18, 30, tzinfo=timezone.utc),
        )
        result = PokerSimulator(config).simulate_hand(default_players(100.0), runtime_seed=42, hand_index=0)

        action_payload = result.action_events[0].to_dict()
        summary_payload = result.hand_summary.to_dict()
        raw_hand_row = raw_hand_row_from_summary_event(summary_payload)
        player_rows = _summary_payloads_to_player_frame([summary_payload])

        self.assertEqual(action_payload["event_type"], "action")
        self.assertTrue(action_payload["hand_id"].startswith("contract_sim_hand_"))
        self.assertTrue(action_payload["event_ts"].startswith("2026-"))
        self.assertEqual(action_payload["user_id"], "user_123")
        self.assertEqual(action_payload["decision_backend"], "heuristic")
        self.assertEqual(action_payload["hero_context_hash"], "hero_ctx_hash")
        self.assertEqual(action_payload["hero_seat"], 1)
        self.assertIn("is_hero_player", action_payload)
        self.assertEqual(summary_payload["event_type"], "hand_summary")
        self.assertIn("winner_player_ids", summary_payload)
        self.assertIn("total_pot_bb", summary_payload)
        self.assertEqual(summary_payload["user_id"], "user_123")
        self.assertEqual(summary_payload["decision_backend"], "heuristic")
        self.assertEqual(summary_payload["hero_context_hash"], "hero_ctx_hash")
        self.assertEqual(summary_payload["hero_seat"], 1)
        self.assertIn("showdown_player_ids", summary_payload)
        self.assertIn("player_hole_cards", summary_payload)
        self.assertTrue(summary_payload["started_at"].startswith("2026-"))
        self.assertTrue(raw_hand_row["board_cards"].startswith("["))
        self.assertIn("made_showdown", player_rows.columns)
        self.assertIn("showdown_hand_category", player_rows.columns)
        self.assertIn("showdown_hand_score", player_rows.columns)
        self.assertIn("user_id", player_rows.columns)
        self.assertIn("decision_backend", player_rows.columns)
        self.assertIn("hero_context_hash", player_rows.columns)
        self.assertIn("is_hero_player", player_rows.columns)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
