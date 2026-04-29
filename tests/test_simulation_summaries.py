from __future__ import annotations

from datetime import datetime

from simulator.engine import SimulationResult
from simulator.run_simulation import _player_summary_rows
from simulator.types import ActionEvent, HandSummary, SimulatedPlayer


def _players() -> list[SimulatedPlayer]:
    return [
        SimulatedPlayer(
            seat_index=0,
            player_id="user_agent",
            agent_id="user_agent",
            persona_name="hero_style",
            persona_text="balanced aggressive",
            backend_type="llm_gated_nano",
            stack_bb=100.0,
        ),
        SimulatedPlayer(
            seat_index=1,
            player_id="baseline_2",
            agent_id="baseline_agent_2",
            persona_name="tag",
            persona_text="tight aggressive",
            backend_type="llm_gated_nano",
            stack_bb=100.0,
        ),
    ]


def _result(
    hand_id: str,
    *,
    hero_delta: float,
    villain_delta: float,
    hero_source: str,
    villain_source: str,
) -> SimulationResult:
    started = datetime(2026, 4, 19, 10, 0, 0)
    finished = datetime(2026, 4, 19, 10, 0, 1)
    summary = HandSummary(
        hand_id=hand_id,
        simulation_run_id="sim_summary_1",
        table_id="table_1",
        source_type="simulated",
        source_dataset="persona_simulation",
        source_run_id="sim_summary_1",
        button_seat=1,
        board_cards=tuple(),
        started_at=started,
        finished_at=finished,
        pot_bb=0.0,
        small_blind_bb=0.5,
        big_blind_bb=1.0,
        action_count=2,
        starting_stacks_bb={"user_agent": 100.0, "baseline_2": 100.0},
        collections_bb={},
        street_action_counts={"preflop": 2},
        showdown=False,
        winners=tuple(),
        winning_seat_indices=tuple(),
        final_stacks_bb={"user_agent": 100.0 + hero_delta, "baseline_2": 100.0 + villain_delta},
        player_personas={"user_agent": "hero_style", "baseline_2": "tag"},
        player_agent_ids={"user_agent": "user_agent", "baseline_2": "baseline_agent_2"},
        player_seats={"user_agent": 1, "baseline_2": 2},
        backend_types={"user_agent": "llm_gated_nano", "baseline_2": "llm_gated_nano"},
        payload_version="1",
        runtime_seed=42,
    )
    action_events = [
        ActionEvent(
            hand_id=hand_id,
            simulation_run_id="sim_summary_1",
            table_id="table_1",
            source_type="simulated",
            source_dataset="persona_simulation",
            source_run_id="sim_summary_1",
            street="preflop",
            action_index=0,
            action_order=0,
            player_id="user_agent",
            agent_id="user_agent",
            seat=1,
            position="BTN",
            action_type="raise",
            amount_bb=2.0,
            pot_before_bb=0.0,
            pot_after_bb=2.0,
            to_call_bb=1.0,
            effective_stack_bb=100.0,
            players_remaining=2,
            board_cards_visible=tuple(),
            hole_cards_visible=tuple(),
            is_all_in=False,
            timestamp_ms=1,
            backend_type="llm_gated_nano",
            persona_name="hero_style",
            persona_text="balanced aggressive",
            payload_version="1",
            raw_lineage={"decision_source": hero_source},
        ),
        ActionEvent(
            hand_id=hand_id,
            simulation_run_id="sim_summary_1",
            table_id="table_1",
            source_type="simulated",
            source_dataset="persona_simulation",
            source_run_id="sim_summary_1",
            street="preflop",
            action_index=1,
            action_order=1,
            player_id="baseline_2",
            agent_id="baseline_agent_2",
            seat=2,
            position="SB",
            action_type="call",
            amount_bb=1.0,
            pot_before_bb=2.0,
            pot_after_bb=3.0,
            to_call_bb=1.0,
            effective_stack_bb=100.0,
            players_remaining=2,
            board_cards_visible=tuple(),
            hole_cards_visible=tuple(),
            is_all_in=False,
            timestamp_ms=2,
            backend_type="llm_gated_nano",
            persona_name="tag",
            persona_text="tight aggressive",
            payload_version="1",
            raw_lineage={"decision_source": villain_source},
        ),
    ]
    return SimulationResult(hand_summary=summary, action_events=action_events)


def test_player_summary_rows_compute_rankings_and_decision_counts() -> None:
    summaries = _player_summary_rows(
        [
            _result("hand_1", hero_delta=5.0, villain_delta=-5.0, hero_source="llm", villain_source="heuristic"),
            _result("hand_2", hero_delta=-1.0, villain_delta=1.0, hero_source="heuristic_fallback", villain_source="llm"),
        ],
        _players(),
        {"user_agent": 2, "baseline_2": 0},
    )

    assert summaries[0]["player_id"] == "user_agent"
    assert summaries[0]["final_rank"] == 1
    assert summaries[0]["stack_reset_count"] == 2
    assert summaries[0]["llm_decision_count"] == 1
    assert summaries[0]["llm_fallback_count"] == 1
    assert summaries[1]["player_id"] == "baseline_2"
    assert summaries[1]["heuristic_decision_count"] == 1
    assert summaries[1]["llm_decision_count"] == 1
