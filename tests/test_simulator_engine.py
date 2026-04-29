from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest
from dataclasses import dataclass

from simulator.cards import compare_hands
from simulator.engine import PokerSimulator, SimulationConfig, SimulatedPlayer, SimulationResult
from simulator.personas import compile_persona
from simulator.types import ActionDecision, HandSummary


@dataclass(slots=True)
class ScriptedAgent:
    agent_id: str
    backend_type: str
    compiled_persona: object
    mode: str

    def select_action(self, state):
        if self.mode == "call":
            return ActionDecision("call") if state.to_call_bb > 0 else ActionDecision("check")
        if self.mode == "fold":
            return ActionDecision("fold") if state.to_call_bb > 0 else ActionDecision("check")
        if self.mode == "raise_first":
            if state.street == "preflop" and state.to_call_bb > 0:
                return ActionDecision("raise", state.min_raise_to_bb + 2.0)
            return ActionDecision("fold") if state.to_call_bb > 0 else ActionDecision("check")
        return ActionDecision("check")


class SimulatorEngineTest(unittest.TestCase):
    def _players(self, personas: list[str]) -> list[SimulatedPlayer]:
        players = []
        for seat, persona_name in enumerate(personas):
            compiled = compile_persona(persona_name)
            players.append(
                SimulatedPlayer(
                    seat_index=seat,
                    player_id=f"player_{seat}",
                    agent_id=f"agent_{seat}",
                    persona_name=persona_name,
                    persona_text=compiled.persona_text,
                    backend_type="heuristic_persona",
                    stack_bb=100.0,
                )
            )
        return players

    def test_deterministic_showdown_path_conserves_chips(self) -> None:
        simulator = PokerSimulator(SimulationConfig(simulation_run_id="test_showdown"))
        players = self._players(["calling_station"] * 6)

        result_a = simulator.simulate_hand(players, runtime_seed=42, hand_index=0)
        result_b = simulator.simulate_hand(players, runtime_seed=42, hand_index=0)

        self.assertEqual([event.to_dict() for event in result_a.action_events], [event.to_dict() for event in result_b.action_events])
        self.assertEqual(result_a.hand_summary.to_dict(), result_b.hand_summary.to_dict())
        self.assertTrue(result_a.hand_summary.showdown)
        self.assertEqual(len(result_a.hand_summary.board_cards), 5)
        self.assertIn("preflop", result_a.hand_summary.street_action_counts)
        self.assertIn("flop", result_a.hand_summary.street_action_counts)
        self.assertIn("turn", result_a.hand_summary.street_action_counts)
        self.assertIn("river", result_a.hand_summary.street_action_counts)

        self.assertEqual(
            [event.action_index for event in result_a.action_events],
            list(range(len(result_a.action_events))),
        )
        self.assertTrue(
            all(
                event.pot_after_bb >= event.pot_before_bb
                for event in result_a.action_events
                if event.action_type != "collect_pot"
            )
        )
        self.assertTrue(any(event.action_type == "collect_pot" for event in result_a.action_events))

        total_final_stacks = sum(result_a.hand_summary.final_stacks_bb.values())
        self.assertAlmostEqual(total_final_stacks, 600.0, places=4)

    def test_raise_then_folds_end_hand_preflop(self) -> None:
        simulator = PokerSimulator(SimulationConfig(simulation_run_id="test_raise_fold"))
        players = self._players(["nit", "nit", "nit", "TAG", "nit", "nit"])
        scripted_agents = {
            0: ScriptedAgent("agent_0", "scripted", compile_persona("nit"), "fold"),
            1: ScriptedAgent("agent_1", "scripted", compile_persona("nit"), "fold"),
            2: ScriptedAgent("agent_2", "scripted", compile_persona("nit"), "fold"),
            3: ScriptedAgent("agent_3", "scripted", compile_persona("TAG"), "raise_first"),
            4: ScriptedAgent("agent_4", "scripted", compile_persona("nit"), "fold"),
            5: ScriptedAgent("agent_5", "scripted", compile_persona("nit"), "fold"),
        }

        simulator._agent_for_player = lambda player: scripted_agents[player.seat_index]  # type: ignore[method-assign]
        result = simulator.simulate_hand(players, runtime_seed=7, hand_index=0)

        self.assertFalse(result.hand_summary.showdown)
        self.assertIn("preflop", result.hand_summary.street_action_counts)
        self.assertNotIn("flop", result.hand_summary.street_action_counts)
        self.assertEqual(result.action_events[0].action_type, "post_small_blind")
        self.assertEqual(result.action_events[1].action_type, "post_big_blind")
        self.assertTrue(any(event.action_type == "raise" for event in result.action_events))
        self.assertTrue(any(event.action_type == "fold" for event in result.action_events[2:]))
        self.assertEqual(len(result.hand_summary.winners), 1)

    def test_check_through_streets_require_every_player_action(self) -> None:
        simulator = PokerSimulator(SimulationConfig(simulation_run_id="test_check_through"))
        players = self._players(["calling_station"] * 6)
        scripted_agents = {
            seat: ScriptedAgent(f"agent_{seat}", "scripted", compile_persona("calling_station"), "call")
            for seat in range(6)
        }

        simulator._agent_for_player = lambda player: scripted_agents[player.seat_index]  # type: ignore[method-assign]
        result = simulator.simulate_hand(players, runtime_seed=11, hand_index=0)

        street_actions = {
            street: [event for event in result.action_events if event.street == street and event.action_type != "collect_pot"]
            for street in ("preflop", "flop", "turn", "river")
        }

        self.assertEqual(result.hand_summary.street_action_counts["preflop"], 8)
        self.assertEqual([event.action_type for event in street_actions["preflop"][-2:]], ["call", "check"])
        self.assertEqual(len(street_actions["flop"]), 6)
        self.assertEqual(len(street_actions["turn"]), 6)
        self.assertEqual(len(street_actions["river"]), 6)
        self.assertTrue(all(event.action_type == "check" for event in street_actions["flop"]))
        self.assertTrue(all(event.action_type == "check" for event in street_actions["turn"]))
        self.assertTrue(all(event.action_type == "check" for event in street_actions["river"]))
        self.assertTrue(result.hand_summary.showdown)

    def test_button_and_positions_rotate_between_hands(self) -> None:
        simulator = PokerSimulator(SimulationConfig(simulation_run_id="test_rotation"))
        players = self._players(["calling_station"] * 6)
        scripted_agents = {
            seat: ScriptedAgent(f"agent_{seat}", "scripted", compile_persona("calling_station"), "call")
            for seat in range(6)
        }

        simulator._agent_for_player = lambda player: scripted_agents[player.seat_index]  # type: ignore[method-assign]
        first_hand = simulator.simulate_hand(players, runtime_seed=19, hand_index=0)
        second_hand = simulator.simulate_hand(players, runtime_seed=19, hand_index=1)

        def _first_positions(result):
            positions = {}
            for event in result.action_events:
                if event.street != "preflop":
                    continue
                if event.action_type in {"post_small_blind", "post_big_blind"}:
                    continue
                positions.setdefault(event.player_id, event.position)
            return positions

        first_positions = _first_positions(first_hand)
        second_positions = _first_positions(second_hand)

        self.assertEqual(first_hand.hand_summary.button_seat, 1)
        self.assertEqual(second_hand.hand_summary.button_seat, 2)
        self.assertNotEqual(first_positions["player_0"], second_positions["player_0"])
        self.assertNotEqual(first_positions["player_5"], second_positions["player_5"])

    def test_simulated_timestamps_use_run_start_base(self) -> None:
        run_started_at = datetime(2026, 4, 29, 18, 30, tzinfo=timezone.utc)
        simulator = PokerSimulator(
            SimulationConfig(
                simulation_run_id="test_time_base",
                run_started_at=run_started_at,
            )
        )
        players = self._players(["calling_station"] * 6)

        first_hand = simulator.simulate_hand(players, runtime_seed=19, hand_index=0)
        second_hand = simulator.simulate_hand(players, runtime_seed=19, hand_index=1)
        first_event_ts = datetime.fromtimestamp(
            first_hand.action_events[0].timestamp_ms / 1000,
            tz=timezone.utc,
        )

        self.assertEqual(first_hand.hand_summary.started_at, run_started_at)
        self.assertGreaterEqual(first_event_ts, run_started_at)
        self.assertLess(first_event_ts, run_started_at + timedelta(seconds=60))
        self.assertEqual(
            second_hand.hand_summary.started_at,
            run_started_at + timedelta(seconds=60),
        )

    def test_compare_hands_helper(self) -> None:
        self.assertEqual(compare_hands(["As", "Ah", "Ac", "Ad", "Kc", "Qs", "Jd"], ["2s", "3h", "4c", "5d", "7s", "9h", "Td"]), 1)

    def test_simulate_session_tracks_stack_resets(self) -> None:
        simulator = PokerSimulator(
            SimulationConfig(
                num_hands=2,
                simulation_run_id="test_resets",
                table_id="table_1",
                starting_stack_bb=100.0,
                rebuy_below_bb=1.0,
            )
        )
        players = self._players(["calling_station"] * 6)

        def fake_simulate_hand(current_players, *, runtime_seed, hand_index):
            starting = {player.player_id: player.stack_bb for player in current_players}
            final = dict(starting)
            if hand_index == 0:
                final["player_0"] = 0.0
            summary = HandSummary(
                hand_id=f"test_resets_hand_{hand_index}",
                simulation_run_id="test_resets",
                table_id="table_1",
                source_type="simulated",
                source_dataset="persona_simulation",
                source_run_id="test_resets",
                button_seat=1,
                board_cards=tuple(),
                started_at=datetime(2026, 4, 19, 10, 0, 0),
                finished_at=datetime(2026, 4, 19, 10, 0, 1),
                pot_bb=0.0,
                small_blind_bb=0.5,
                big_blind_bb=1.0,
                action_count=0,
                starting_stacks_bb=starting,
                collections_bb={},
                street_action_counts={"preflop": 0},
                showdown=False,
                winners=tuple(),
                winning_seat_indices=tuple(),
                final_stacks_bb=final,
                player_personas={player.player_id: player.persona_name for player in current_players},
                player_agent_ids={player.player_id: player.agent_id for player in current_players},
                player_seats={player.player_id: player.seat_index + 1 for player in current_players},
                backend_types={player.player_id: player.backend_type for player in current_players},
                payload_version="1",
                runtime_seed=runtime_seed,
            )
            return SimulationResult(hand_summary=summary, action_events=[])

        simulator.simulate_hand = fake_simulate_hand  # type: ignore[method-assign]
        results, reset_counts = simulator.simulate_session_with_reset_counts(players, runtime_seed=42)

        self.assertEqual(len(results), 2)
        self.assertEqual(reset_counts["player_0"], 1)
        self.assertEqual(reset_counts["player_1"], 0)


if __name__ == "__main__":
    unittest.main()
