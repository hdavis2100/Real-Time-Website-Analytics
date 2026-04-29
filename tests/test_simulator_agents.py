from __future__ import annotations

import itertools

import simulator.agents as agent_module
from simulator.agents import build_agent
from simulator.types import DecisionState


def _state(
    *,
    hand_id: str,
    hole_cards: tuple[str, str],
    action_index: int,
    stack_bb: float = 100.0,
    to_call_bb: float = 1.0,
) -> DecisionState:
    return DecisionState(
        hand_id=hand_id,
        simulation_run_id="sim_agent_test",
        table_id="table_1",
        street="preflop",
        seat_index=0,
        player_id="hero",
        agent_id="hero_agent",
        position="BTN",
        stack_bb=stack_bb,
        committed_bb=0.0,
        pot_bb=1.5,
        to_call_bb=to_call_bb,
        min_raise_to_bb=3.0,
        current_bet_bb=1.0,
        players_remaining=6,
        active_players=("hero", "villain_1", "villain_2", "villain_3", "villain_4", "villain_5"),
        board_cards=tuple(),
        hole_cards=hole_cards,
        legal_actions=("fold", "call", "raise", "all_in"),
        street_action_count=0,
        action_index=action_index,
        button_seat=5,
        big_blind_bb=1.0,
        small_blind_bb=0.5,
        backend_type="heuristic_persona",
        source_type="simulated",
        source_dataset="persona_simulation",
        payload_version="v1",
        persona_name="hero",
        persona_text="persona",
        seed=42,
    )


def test_deep_stack_personas_do_not_randomly_jam_weak_hands(monkeypatch) -> None:
    monkeypatch.setattr(agent_module, "_state_noise", lambda state: 0.0 if state.hand_id == "weak" else 0.5)
    lag = build_agent("heuristic_persona", "loose aggressive", agent_id="lag")

    decision = lag.select_action(
        _state(hand_id="weak", hole_cards=("2c", "7d"), action_index=0, stack_bb=100.0, to_call_bb=1.0)
    )

    assert decision.action_type != "all_in"


def test_short_stack_personas_can_still_jam_strong_hands(monkeypatch) -> None:
    monkeypatch.setattr(agent_module, "_state_noise", lambda _state: 0.0)
    lag = build_agent("heuristic_persona", "loose aggressive", agent_id="lag")

    decision = lag.select_action(
        _state(hand_id="short_stack", hole_cards=("As", "Kh"), action_index=0, stack_bb=8.0, to_call_bb=1.0)
    )

    assert decision.action_type == "all_in"


def test_lag_profile_is_more_aggressive_than_nit_without_deep_stack_shove_spam(monkeypatch) -> None:
    monkeypatch.setattr(agent_module, "_state_noise", lambda state: (state.action_index % 10) / 10.0)
    nit = build_agent("heuristic_persona", "nit", agent_id="nit")
    lag = build_agent("heuristic_persona", "lag", agent_id="lag")

    hole_cards = [
        ("As", "Ah"),
        ("Ks", "Kh"),
        ("Ad", "Kd"),
        ("Qs", "Qd"),
        ("Jh", "Th"),
        ("9c", "9d"),
        ("Ac", "Qc"),
        ("Kd", "Jd"),
        ("Tc", "9c"),
        ("8h", "7h"),
        ("As", "5s"),
        ("Kh", "8h"),
        ("Qd", "8c"),
        ("Jc", "7d"),
        ("Ts", "6s"),
        ("9d", "5d"),
        ("8c", "4h"),
        ("7s", "3s"),
        ("6d", "4c"),
        ("2c", "7d"),
    ]

    nit_actions = []
    lag_actions = []
    for action_index, cards in enumerate(itertools.islice(itertools.cycle(hole_cards), 40)):
        nit_actions.append(nit.select_action(_state(hand_id=f"nit_{action_index}", hole_cards=cards, action_index=action_index)).action_type)
        lag_actions.append(lag.select_action(_state(hand_id=f"lag_{action_index}", hole_cards=cards, action_index=action_index)).action_type)

    nit_aggressive = sum(action in {"raise", "all_in"} for action in nit_actions)
    lag_aggressive = sum(action in {"raise", "all_in"} for action in lag_actions)
    nit_all_ins = nit_actions.count("all_in")
    lag_all_ins = lag_actions.count("all_in")

    assert lag_aggressive > nit_aggressive
    assert nit_all_ins == 0
    assert lag_all_ins <= 2


def test_pocket_pairs_only_context_folds_non_pairs_preflop() -> None:
    agent = build_agent("heuristic_persona", "Only play pocket pairs", agent_id="hero")

    decision = agent.select_action(
        _state(hand_id="no_pair", hole_cards=("As", "Kd"), action_index=0, to_call_bb=1.0)
    )

    assert decision.action_type == "fold"
    assert decision.note == "pocket_pairs_only"


def test_pocket_pairs_only_context_can_continue_with_pairs_preflop(monkeypatch) -> None:
    monkeypatch.setattr(agent_module, "_state_noise", lambda _state: 0.0)
    agent = build_agent("heuristic_persona", "Only play pocket pairs", agent_id="hero")

    decision = agent.select_action(
        _state(hand_id="pair", hole_cards=("9s", "9d"), action_index=0, to_call_bb=1.0)
    )

    assert decision.action_type in {"call", "raise", "all_in"}
