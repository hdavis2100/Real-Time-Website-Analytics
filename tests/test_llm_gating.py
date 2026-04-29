from __future__ import annotations

import unittest
from dataclasses import dataclass
import json

from simulator.agents import GatedLlmAgent
from simulator.engine import PokerSimulator, SimulationConfig, SimulatedPlayer
from simulator.gating import gate_decision, gating_config
from simulator.llm_policy import _user_payload, compile_heuristic_persona_for_run
from simulator.personas import compile_persona
from simulator.run_simulation import build_context_matchup_players
from simulator.types import ActionDecision, CompiledPersona, DecisionState, PersonaParameters


@dataclass(slots=True)
class _FakeRuntime:
    decisions: list[ActionDecision]
    calls_used: int = 0

    def can_call(self) -> bool:
        return True

    def decide_action(self, **_kwargs) -> ActionDecision | None:
        self.calls_used += 1
        if not self.decisions:
            return None
        return self.decisions.pop(0)


@dataclass(slots=True)
class _FakeAgent:
    agent_id: str
    backend_type: str
    compiled_persona: object
    _decision: ActionDecision

    def select_action(self, _state):
        return self._decision


@dataclass(slots=True)
class _CapturingAgent:
    agent_id: str
    backend_type: str
    compiled_persona: object
    _decision: ActionDecision
    seen_states: list[DecisionState]

    def select_action(self, state):
        self.seen_states.append(state)
        return self._decision


class LlmGatingTests(unittest.TestCase):
    def test_compile_heuristic_persona_for_run_applies_hard_constraint_guardrails(self) -> None:
        class _FakeResponses:
            def create(self, **_kwargs):
                return type(
                    "Response",
                    (),
                    {
                        "output_text": json.dumps(
                            {
                                "persona_name": "pair_hunter",
                                "heuristic_summary": "very tight pair-only style",
                                "parameters": {
                                    "preflop_open_bias": 0.88,
                                    "cold_call_bias": 0.77,
                                    "fold_threshold": 0.12,
                                    "raise_bias": 0.64,
                                    "cbet_bias": 0.52,
                                    "bluff_turn_bias": 0.73,
                                    "bluff_river_bias": 0.81,
                                    "jam_bias": 0.22,
                                    "trap_bias": 0.14,
                                    "showdown_tendency": 0.31,
                                },
                            }
                        )
                    },
                )()

        class _FakeClient:
            responses = _FakeResponses()

        compiled = compile_heuristic_persona_for_run(
            "Only play pocket pairs and avoid bluffing",
            client=_FakeClient(),
        )

        self.assertIsNotNone(compiled)
        assert compiled is not None
        self.assertLessEqual(compiled.parameters.preflop_open_bias, 0.18)
        self.assertLessEqual(compiled.parameters.cold_call_bias, 0.10)
        self.assertGreaterEqual(compiled.parameters.fold_threshold, 0.82)
        self.assertLessEqual(compiled.parameters.bluff_turn_bias, 0.04)
        self.assertLessEqual(compiled.parameters.bluff_river_bias, 0.04)

    def test_pocket_pairs_trigger_the_model_preflop(self) -> None:
        state = _decision_state(hole_cards=("Ah", "Ad"), position="UTG", to_call_bb=1.0)
        should_call_model, reason = gate_decision(
            state,
            config=gating_config("conservative"),
            hand_model_active=False,
        )

        self.assertTrue(should_call_model)
        self.assertEqual(reason, "pocket_pair")

    def test_gated_agent_uses_model_and_keeps_control_for_rest_of_hand(self) -> None:
        runtime = _FakeRuntime(
            decisions=[
                ActionDecision("raise", 6.0, metadata={"decision_source": "llm", "trigger_reason": "broadway"}),
                ActionDecision("bet", 8.0, metadata={"decision_source": "llm", "trigger_reason": "hand_already_model_controlled"}),
            ]
        )
        agent = GatedLlmAgent(
            compiled_persona=compile_persona("aggressive in position"),
            agent_id="hero",
            runtime=runtime,
            model_name="gpt-5-nano",
            gating_profile="conservative",
            max_model_calls_per_hand=4,
        )

        preflop = _decision_state(hand_id="hand_1", hole_cards=("As", "Kh"), position="BTN", to_call_bb=1.0)
        flop = _decision_state(
            hand_id="hand_1",
            street="flop",
            hole_cards=("As", "Kh"),
            board_cards=("2c", "7d", "Tc"),
            position="BTN",
            to_call_bb=0.0,
            legal_actions=("check", "bet", "all_in"),
        )

        preflop_decision = agent.select_action(preflop)
        flop_decision = agent.select_action(flop)

        self.assertEqual(preflop_decision.metadata["decision_source"], "llm")
        self.assertEqual(flop_decision.metadata["decision_source"], "llm")
        self.assertEqual(runtime.calls_used, 2)

    def test_gated_agent_falls_back_when_model_is_unavailable(self) -> None:
        runtime = _FakeRuntime(decisions=[])
        agent = GatedLlmAgent(
            compiled_persona=compile_persona("aggressive and loose"),
            agent_id="hero",
            runtime=runtime,
            model_name="gpt-5-nano",
            gating_profile="conservative",
            max_model_calls_per_hand=2,
        )
        state = _decision_state(hole_cards=("Ks", "Qh"), position="CO", to_call_bb=1.0)

        decision = agent.select_action(state)

        self.assertIn(decision.action_type, state.legal_actions)
        self.assertEqual(decision.metadata["decision_source"], "heuristic_fallback")
        self.assertEqual(decision.metadata["fallback_reason"], "llm_unavailable_or_invalid")

    def test_context_matchup_players_applies_llm_settings_to_all_seats(self) -> None:
        players = build_context_matchup_players(
            "solver-like and disciplined",
            starting_stack_bb=100.0,
            hero_seat=2,
            backend_type="llm_gated_nano",
            model_name="gpt-5-nano",
            reasoning_effort="low",
            gating_profile="balanced",
            always_model_postflop=True,
            max_model_calls_per_hand=6,
        )

        hero = players[1]
        self.assertEqual(hero.backend_type, "llm_gated_nano")
        self.assertEqual(hero.model_name, "gpt-5-nano")
        self.assertEqual(hero.reasoning_effort, "low")
        self.assertEqual(hero.gating_profile, "balanced")
        self.assertTrue(hero.always_model_postflop)
        self.assertEqual(hero.max_model_calls_per_hand, 6)
        baseline_players = [player for index, player in enumerate(players) if index != 1]
        self.assertTrue(all(player.backend_type == "llm_gated_nano" for player in baseline_players))
        self.assertTrue(all(player.model_name == "gpt-5-nano" for player in baseline_players))
        self.assertTrue(all(player.reasoning_effort == "low" for player in baseline_players))
        self.assertTrue(all(player.gating_profile == "balanced" for player in baseline_players))
        self.assertTrue(all(player.always_model_postflop is True for player in baseline_players))
        self.assertTrue(all(player.max_model_calls_per_hand == 6 for player in baseline_players))

    def test_context_matchup_players_can_use_heuristic_backend_for_all_seats(self) -> None:
        players = build_context_matchup_players(
            "play very tight",
            starting_stack_bb=100.0,
            hero_seat=3,
            backend_type="heuristic_persona",
            use_context_model_for_heuristics=False,
        )

        self.assertTrue(all(player.backend_type == "heuristic_persona" for player in players))
        self.assertTrue(all(player.model_name is None for player in players))
        self.assertTrue(all(player.reasoning_effort is None for player in players))
        self.assertTrue(all(player.always_model_postflop is False for player in players))

    def test_context_matchup_players_can_compile_heuristics_with_nano_once(self) -> None:
        compiled = CompiledPersona(
            name="pair_hunter",
            persona_text="Only play pocket pairs",
            parameters=PersonaParameters(
                preflop_open_bias=0.08,
                cold_call_bias=0.04,
                fold_threshold=0.92,
                raise_bias=0.22,
                cbet_bias=0.12,
                bluff_turn_bias=0.01,
                bluff_river_bias=0.01,
                jam_bias=0.03,
                trap_bias=0.18,
                showdown_tendency=0.22,
            ),
            source="llm_compiled",
            preset_name=None,
        )

        from unittest.mock import patch

        with patch("simulator.run_simulation.compile_heuristic_persona_for_run", return_value=compiled) as mocked:
            players = build_context_matchup_players(
                "Only play pocket pairs",
                starting_stack_bb=100.0,
                hero_seat=2,
                backend_type="heuristic_persona",
                heuristic_compiler_timeout_seconds=3.0,
            )

        hero = players[1]
        self.assertEqual(hero.backend_type, "heuristic_persona")
        self.assertEqual(hero.persona_name, "pair_hunter")
        self.assertEqual(hero.compiled_persona, compiled)
        mocked.assert_called_once_with(
            "Only play pocket pairs",
            model_name="gpt-5.4-nano",
            timeout_seconds=3.0,
        )

    def test_engine_raw_lineage_carries_decision_metadata(self) -> None:
        players = [
            SimulatedPlayer(
                seat_index=seat,
                player_id=f"player_{seat}",
                agent_id=f"agent_{seat}",
                persona_name="tag",
                persona_text="tight aggressive",
                backend_type="heuristic_persona",
                stack_bb=100.0,
            )
            for seat in range(6)
        ]
        simulator = PokerSimulator(SimulationConfig(simulation_run_id="llm_lineage"))
        scripted_agents = {
            0: _FakeAgent(
                "agent_0",
                "llm_gated_nano",
                compile_persona("tag"),
                ActionDecision("fold", metadata={"decision_source": "llm", "trigger_reason": "river_decision"}),
            ),
            1: _FakeAgent("agent_1", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            2: _FakeAgent("agent_2", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            3: _FakeAgent("agent_3", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            4: _FakeAgent("agent_4", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            5: _FakeAgent("agent_5", "heuristic_persona", compile_persona("nit"), ActionDecision("check")),
        }
        simulator._agent_for_player = lambda player: scripted_agents[player.seat_index]  # type: ignore[method-assign]

        result = simulator.simulate_hand(players, runtime_seed=11, hand_index=0)
        model_event = next(
            event for event in result.action_events if event.player_id == "player_0" and event.action_type == "fold"
        )

        self.assertEqual(model_event.raw_lineage["decision_source"], "llm")
        self.assertEqual(model_event.raw_lineage["trigger_reason"], "river_decision")

    def test_engine_marks_illegal_llm_actions_as_fallbacks(self) -> None:
        players = [
            SimulatedPlayer(
                seat_index=seat,
                player_id=f"player_{seat}",
                agent_id=f"agent_{seat}",
                persona_name="tag",
                persona_text="tight aggressive",
                backend_type="heuristic_persona",
                stack_bb=100.0,
            )
            for seat in range(6)
        ]
        simulator = PokerSimulator(SimulationConfig(simulation_run_id="llm_illegal_lineage"))
        scripted_agents = {
            0: _FakeAgent(
                "agent_0",
                "llm_gated_nano",
                compile_persona("tag"),
                ActionDecision("bet", 2.0, metadata={"decision_source": "llm", "trigger_reason": "river_decision"}),
            ),
            1: _FakeAgent("agent_1", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            2: _FakeAgent("agent_2", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            3: _FakeAgent("agent_3", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            4: _FakeAgent("agent_4", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            5: _FakeAgent("agent_5", "heuristic_persona", compile_persona("nit"), ActionDecision("check")),
        }
        simulator._agent_for_player = lambda player: scripted_agents[player.seat_index]  # type: ignore[method-assign]

        result = simulator.simulate_hand(players, runtime_seed=11, hand_index=0)
        model_event = next(
            event
            for event in result.action_events
            if event.player_id == "player_0"
            and event.raw_lineage.get("fallback_reason") == "illegal_action_from_agent"
        )

        self.assertEqual(model_event.raw_lineage["decision_source"], "heuristic_fallback")
        self.assertEqual(model_event.raw_lineage["original_decision_source"], "llm")
        self.assertEqual(model_event.raw_lineage["original_action_type"], "bet")
        self.assertEqual(model_event.raw_lineage["fallback_reason"], "illegal_action_from_agent")
        self.assertEqual(model_event.action_type, "call")

    def test_engine_decision_state_includes_public_hand_action_history(self) -> None:
        players = [
            SimulatedPlayer(
                seat_index=seat,
                player_id=f"player_{seat}",
                agent_id=f"agent_{seat}",
                persona_name="tag",
                persona_text="tight aggressive",
                backend_type="heuristic_persona",
                stack_bb=100.0,
            )
            for seat in range(6)
        ]
        captured_states: list[DecisionState] = []
        simulator = PokerSimulator(SimulationConfig(simulation_run_id="llm_history_state"))
        scripted_agents = {
            0: _CapturingAgent(
                "agent_0",
                "llm_gated_nano",
                compile_persona("tag"),
                ActionDecision("fold", metadata={"decision_source": "llm", "trigger_reason": "broadway"}),
                captured_states,
            ),
            1: _FakeAgent("agent_1", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            2: _FakeAgent("agent_2", "heuristic_persona", compile_persona("nit"), ActionDecision("check")),
            3: _FakeAgent("agent_3", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            4: _FakeAgent("agent_4", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
            5: _FakeAgent("agent_5", "heuristic_persona", compile_persona("nit"), ActionDecision("fold")),
        }
        simulator._agent_for_player = lambda player: scripted_agents[player.seat_index]  # type: ignore[method-assign]

        simulator.simulate_hand(players, runtime_seed=17, hand_index=0)

        self.assertEqual(len(captured_states), 1)
        state = captured_states[0]
        self.assertEqual(
            [event["action_type"] for event in state.hand_action_history],
            ["post_small_blind", "post_big_blind", "fold", "fold", "fold"],
        )
        self.assertEqual(state.hand_action_history[0]["street"], "preflop")
        self.assertNotIn("hole_cards_visible", state.hand_action_history[0])

    def test_llm_user_payload_is_json_serializable(self) -> None:
        payload = _user_payload(
            state=_decision_state(
                hand_action_history=(
                    {
                        "action_index": 0,
                        "street": "preflop",
                        "player_id": "villain_1",
                        "seat": 2,
                        "position": "SB",
                        "action_type": "post_small_blind",
                        "amount_bb": 0.5,
                        "pot_before_bb": 0.0,
                        "pot_after_bb": 0.5,
                        "to_call_bb": 0.0,
                        "players_remaining": 6,
                        "is_all_in": False,
                    },
                    {
                        "action_index": 1,
                        "street": "preflop",
                        "player_id": "villain_2",
                        "seat": 3,
                        "position": "BB",
                        "action_type": "post_big_blind",
                        "amount_bb": 1.0,
                        "pot_before_bb": 0.5,
                        "pot_after_bb": 1.5,
                        "to_call_bb": 0.0,
                        "players_remaining": 6,
                        "is_all_in": False,
                    },
                ),
            ),
            compiled_persona=compile_persona("balanced aggressive value bettor"),
            trigger_reason="broadway",
            context_text="balanced aggressive value bettor",
        )

        parsed = json.loads(payload)
        self.assertEqual(parsed["trigger_reason"], "broadway")
        self.assertEqual(len(parsed["hand_action_history"]), 2)
        self.assertEqual(parsed["hand_action_history"][1]["action_type"], "post_big_blind")
        self.assertEqual(parsed["state"]["legal_actions"], ["fold", "call", "raise", "all_in"])
        self.assertEqual(
            parsed["output_rules"],
            [
                "If the action is bet or raise, size_bb must be the target total wager in big blinds.",
                "If the action is fold, check, call, or all_in, size_bb should be null.",
                "Never return an illegal action.",
            ],
        )


def _decision_state(
    *,
    hand_id: str = "hand_0",
    street: str = "preflop",
    hole_cards: tuple[str, str] = ("As", "Kh"),
    board_cards: tuple[str, ...] = (),
    position: str = "BTN",
    to_call_bb: float = 1.0,
    legal_actions: tuple[str, ...] = ("fold", "call", "raise", "all_in"),
    hand_action_history: tuple[dict[str, object], ...] = (),
) -> DecisionState:
    return DecisionState(
        hand_id=hand_id,
        simulation_run_id="sim_1",
        table_id="table_1",
        street=street,
        seat_index=0,
        player_id="hero",
        agent_id="hero_agent",
        position=position,
        stack_bb=100.0,
        committed_bb=0.0,
        pot_bb=3.0,
        to_call_bb=to_call_bb,
        min_raise_to_bb=3.0,
        current_bet_bb=1.0,
        players_remaining=6,
        active_players=("hero", "villain"),
        board_cards=board_cards,
        hole_cards=hole_cards,
        legal_actions=legal_actions,
        street_action_count=0,
        action_index=0,
        button_seat=1,
        big_blind_bb=1.0,
        small_blind_bb=0.5,
        backend_type="llm_gated_nano",
        source_type="simulated",
        source_dataset="persona_simulation",
        payload_version="1",
        persona_name="hero_style",
        persona_text="play good hands aggressively",
        seed=42,
        hand_action_history=hand_action_history,
    )


if __name__ == "__main__":
    unittest.main()
