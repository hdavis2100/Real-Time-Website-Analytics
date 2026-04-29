from __future__ import annotations

from datetime import datetime, timezone

import simulator.run_simulation as run_simulation
from simulator.engine import SimulationResult
from simulator.types import ActionEvent, HandSummary, SimulatedPlayer


class _FakeProducer:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _players() -> list[SimulatedPlayer]:
    return [
        SimulatedPlayer(
            seat_index=0,
            player_id="p1",
            agent_id="a1",
            persona_name="lag",
            persona_text="loose aggressive",
            backend_type="heuristic_persona",
            stack_bb=100.0,
        ),
        SimulatedPlayer(
            seat_index=1,
            player_id="p2",
            agent_id="a2",
            persona_name="nit",
            persona_text="tight passive",
            backend_type="heuristic_persona",
            stack_bb=100.0,
        ),
    ]


def _action_event(*, hand_id: str, action_index: int, player_id: str, agent_id: str, seat: int, decision_source: str) -> ActionEvent:
    return ActionEvent(
        hand_id=hand_id,
        simulation_run_id="sim_live",
        table_id="table_1",
        source_type="simulated",
        source_dataset="persona_simulation",
        source_run_id="sim_live",
        street="preflop",
        action_index=action_index,
        action_order=action_index,
        player_id=player_id,
        agent_id=agent_id,
        seat=seat,
        position="BTN" if seat == 1 else "SB",
        action_type="raise" if action_index == 0 else "call",
        amount_bb=2.0,
        pot_before_bb=1.5,
        pot_after_bb=3.5,
        to_call_bb=1.0,
        effective_stack_bb=100.0,
        players_remaining=2,
        board_cards_visible=tuple(),
        hole_cards_visible=tuple(),
        is_all_in=False,
        timestamp_ms=1_710_000_000_000 + action_index,
        backend_type="heuristic_persona",
        persona_name="lag" if player_id == "p1" else "nit",
        persona_text="persona",
        payload_version="1",
        user_id="user_1",
        decision_backend="heuristic",
        hero_context_hash="ctx",
        hero_seat=1,
        is_hero_player=seat == 1,
        raw_lineage={"decision_source": decision_source},
    )


def _hand_summary(*, hand_id: str, p1_final: float, p2_final: float) -> HandSummary:
    return HandSummary(
        hand_id=hand_id,
        simulation_run_id="sim_live",
        table_id="table_1",
        source_type="simulated",
        source_dataset="persona_simulation",
        source_run_id="sim_live",
        button_seat=1,
        board_cards=tuple(),
        started_at=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 21, 12, 0, 1, tzinfo=timezone.utc),
        pot_bb=4.0,
        small_blind_bb=0.5,
        big_blind_bb=1.0,
        action_count=2,
        starting_stacks_bb={"p1": 100.0, "p2": 100.0},
        collections_bb={"p1": max(0.0, p1_final - 100.0)},
        street_action_counts={"preflop": 2},
        showdown=False,
        winners=("p1",) if p1_final >= p2_final else ("p2",),
        winning_seat_indices=(0,) if p1_final >= p2_final else (1,),
        final_stacks_bb={"p1": p1_final, "p2": p2_final},
        player_personas={"p1": "lag", "p2": "nit"},
        player_agent_ids={"p1": "a1", "p2": "a2"},
        player_seats={"p1": 1, "p2": 2},
        backend_types={"p1": "heuristic_persona", "p2": "heuristic_persona"},
        payload_version="1",
        runtime_seed=42,
        user_id="user_1",
        decision_backend="heuristic",
        hero_context_hash="ctx",
        hero_seat=1,
    )


def test_simulate_and_record_streams_kafka_per_hand(monkeypatch) -> None:
    players = _players()
    producer = _FakeProducer()
    publish_calls: list[tuple[str, list[str], bool]] = []
    results = [
        SimulationResult(
            hand_summary=_hand_summary(hand_id="sim_live_hand_00000", p1_final=101.5, p2_final=98.5),
            action_events=[
                _action_event(hand_id="sim_live_hand_00000", action_index=0, player_id="p1", agent_id="a1", seat=1, decision_source="heuristic"),
                _action_event(hand_id="sim_live_hand_00000", action_index=1, player_id="p2", agent_id="a2", seat=2, decision_source="heuristic_fallback"),
            ],
        ),
        SimulationResult(
            hand_summary=_hand_summary(hand_id="sim_live_hand_00001", p1_final=103.0, p2_final=97.0),
            action_events=[
                _action_event(hand_id="sim_live_hand_00001", action_index=0, player_id="p1", agent_id="a1", seat=1, decision_source="llm"),
                _action_event(hand_id="sim_live_hand_00001", action_index=1, player_id="p2", agent_id="a2", seat=2, decision_source="heuristic"),
            ],
        ),
    ]

    class _FakeSimulator:
        def __init__(self, _config) -> None:
            pass

        def iter_session_with_reset_counts(self, _players, *, runtime_seed):
            assert runtime_seed == 42
            yield results[0], {"p1": 0, "p2": 0}
            yield results[1], {"p1": 0, "p2": 0}

    def _fake_publish_records(*, producer, topic, records, key_builder, flush=True):
        rows = list(records)
        publish_calls.append((topic, [str(key_builder(row)) for row in rows], bool(flush)))
        return len(rows)

    monkeypatch.setattr(run_simulation, "PokerSimulator", _FakeSimulator)
    monkeypatch.setattr(run_simulation, "build_producer", lambda _brokers: producer)
    monkeypatch.setattr(run_simulation, "publish_records", _fake_publish_records)

    result = run_simulation.simulate_and_record(
        hand_count=2,
        seed=42,
        table_id="table_1",
        simulation_run_id="sim_live",
        starting_stack_bb=100.0,
        small_blind_bb=0.5,
        big_blind_bb=1.0,
        players=players,
        publish_kafka=True,
        kafka_brokers="kafka:29092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        write_backend=False,
        user_id="user_1",
        decision_backend="heuristic",
        hero_context_hash="ctx",
        hero_seat=1,
    )

    assert publish_calls == [
        (
            "poker.actions",
            ["sim_live|table_1|sim_live_hand_00000", "sim_live|table_1|sim_live_hand_00000"],
            False,
        ),
        (
            "poker.hand_summaries",
            ["sim_live|table_1|sim_live_hand_00000"],
            True,
        ),
        (
            "poker.actions",
            ["sim_live|table_1|sim_live_hand_00001", "sim_live|table_1|sim_live_hand_00001"],
            False,
        ),
        (
            "poker.hand_summaries",
            ["sim_live|table_1|sim_live_hand_00001"],
            True,
        ),
    ]
    assert result["hands"] == 2
    assert result["actions"] == 4
    assert result["published_actions"] == 4
    assert result["published_hand_summaries"] == 2
    assert result["player_summaries"][0]["player_id"] == "p1"
    assert result["player_summaries"][0]["llm_decision_count"] == 1
    assert result["player_summaries"][0]["heuristic_decision_count"] == 1
    assert result["player_summaries"][0]["vpip"] == 1.0
    assert result["player_summaries"][0]["pfr"] == 1.0
    assert result["player_summaries"][0]["aggression_frequency"] == 1.0
    assert result["player_summaries"][1]["llm_fallback_count"] == 1
    assert result["player_summaries"][1]["vpip"] == 1.0
    assert result["player_summaries"][1]["pfr"] == 0.0
    assert result["player_summaries"][1]["aggression_frequency"] == 0.0
    assert len(result["profile_session_features"]) == 2
    assert {
        row["player_id"] for row in result["profile_session_features"]
    } == {"p1", "p2"}
    assert all(
        int(row["hands_observed"]) == 2 for row in result["profile_session_features"]
    )
    assert producer.closed is True


def test_simulate_and_record_emits_progress_snapshots(monkeypatch) -> None:
    players = _players()
    progress_updates = []
    results = [
        SimulationResult(
            hand_summary=_hand_summary(hand_id="sim_live_hand_00000", p1_final=101.5, p2_final=98.5),
            action_events=[
                _action_event(hand_id="sim_live_hand_00000", action_index=0, player_id="p1", agent_id="a1", seat=1, decision_source="heuristic"),
                _action_event(hand_id="sim_live_hand_00000", action_index=1, player_id="p2", agent_id="a2", seat=2, decision_source="heuristic_fallback"),
            ],
        ),
        SimulationResult(
            hand_summary=_hand_summary(hand_id="sim_live_hand_00001", p1_final=103.0, p2_final=97.0),
            action_events=[
                _action_event(hand_id="sim_live_hand_00001", action_index=0, player_id="p1", agent_id="a1", seat=1, decision_source="llm"),
                _action_event(hand_id="sim_live_hand_00001", action_index=1, player_id="p2", agent_id="a2", seat=2, decision_source="heuristic"),
            ],
        ),
    ]

    class _FakeSimulator:
        def __init__(self, _config) -> None:
            pass

        def iter_session_with_reset_counts(self, _players, *, runtime_seed):
            assert runtime_seed == 42
            yield results[0], {"p1": 0, "p2": 0}
            yield results[1], {"p1": 0, "p2": 0}

    monkeypatch.setattr(run_simulation, "PokerSimulator", _FakeSimulator)
    monkeypatch.setattr(run_simulation, "build_producer", lambda _brokers: None)

    run_simulation.simulate_and_record(
        hand_count=2,
        seed=42,
        table_id="table_1",
        simulation_run_id="sim_live",
        starting_stack_bb=100.0,
        small_blind_bb=0.5,
        big_blind_bb=1.0,
        players=players,
        publish_kafka=False,
        kafka_brokers="kafka:29092",
        actions_topic="poker.actions",
        summaries_topic="poker.hand_summaries",
        write_backend=False,
        user_id="user_1",
        decision_backend="heuristic",
        hero_context_hash="ctx",
        hero_seat=1,
        progress_callback=progress_updates.append,
        progress_interval_hands=1,
    )

    assert len(progress_updates) == 2
    assert progress_updates[0]["hands"] == 1
    assert progress_updates[0]["published_hand_summaries"] == 0
    assert progress_updates[0]["player_summaries"][0]["player_id"] == "p1"
    assert progress_updates[0]["player_summaries"][0]["vpip"] == 1.0
    assert progress_updates[0]["player_summaries"][0]["pfr"] == 1.0
    assert progress_updates[0]["player_summaries"][0]["aggression_frequency"] == 1.0
    assert progress_updates[1]["hands"] == 2
    assert progress_updates[1]["actions"] == 4
    assert progress_updates[1]["player_summaries"][0]["llm_decision_count"] == 1
    assert progress_updates[1]["player_summaries"][1]["llm_fallback_count"] == 1
    assert progress_updates[1]["player_summaries"][1]["vpip"] == 1.0
    assert progress_updates[1]["player_summaries"][1]["pfr"] == 0.0
    assert progress_updates[1]["player_summaries"][1]["aggression_frequency"] == 0.0
