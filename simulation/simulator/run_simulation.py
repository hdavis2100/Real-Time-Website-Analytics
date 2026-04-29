from __future__ import annotations

"""CLI entrypoint for persona-backed 6-max NLH simulation runs."""

import argparse
from datetime import datetime, timezone
import json
import uuid
from typing import Any

import pandas as pd

from profiles.constants import FEATURE_VERSION as PROFILE_FEATURE_VERSION
from profiles.reference_features import (
    aggregate_session_hand_features,
    compute_player_hand_features,
)
from poker_platform.kafka_utils import build_producer, publish_records
from simulator.cards import best_five_of_seven
from simulator.engine import PokerSimulator
from simulator.llm_policy import (
    DEFAULT_MODEL,
    DEFAULT_REASONING_EFFORT,
    compile_heuristic_persona_for_run,
)
from simulator.personas import BUILTIN_PERSONAS, compile_persona
from simulator.session_metadata import hero_context_hash as compute_hero_context_hash
from simulator.session_metadata import normalize_decision_backend
from simulator.types import SimulationConfig, SimulatedPlayer


DEFAULT_PERSONAS = [
    ("agent_1", "player_1", "nit"),
    ("agent_2", "player_2", "tag"),
    ("agent_3", "player_3", "lag"),
    ("agent_4", "player_4", "calling_station"),
    ("agent_5", "player_5", "maniac"),
    ("agent_6", "player_6", "shove_bot"),
]

SHOWDOWN_HAND_CATEGORIES = {
    8: "straight_flush",
    7: "four_of_a_kind",
    6: "full_house",
    5: "flush",
    4: "straight",
    3: "three_of_a_kind",
    2: "two_pair",
    1: "one_pair",
    0: "high_card",
}


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _normalized_limit(value: int | None) -> int | None:
    if value is None:
        return None
    normalized = int(value)
    return normalized if normalized > 0 else None


def default_players(starting_stack_bb: float) -> list[SimulatedPlayer]:
    players: list[SimulatedPlayer] = []
    for seat_index, (agent_id, player_id, persona_name) in enumerate(DEFAULT_PERSONAS):
        preset = BUILTIN_PERSONAS[persona_name]
        players.append(
            SimulatedPlayer(
                seat_index=seat_index,
                player_id=player_id,
                agent_id=agent_id,
                persona_name=persona_name,
                persona_text=preset.persona_text,
                backend_type="llm_gated_nano",
                stack_bb=starting_stack_bb,
                model_name=DEFAULT_MODEL,
                reasoning_effort=DEFAULT_REASONING_EFFORT,
                gating_profile="conservative",
                always_model_postflop=True,
                max_model_calls_per_hand=None,
            )
        )
    return players


def build_context_matchup_players(
    context: str,
    *,
    starting_stack_bb: float,
    hero_seat: int = 1,
    backend_type: str = "llm_gated_nano",
    baseline_backend_type: str | None = None,
    model_name: str | None = None,
    reasoning_effort: str | None = None,
    gating_profile: str | None = "conservative",
    always_model_postflop: bool = True,
    max_model_calls_per_hand: int | None = None,
    heuristic_compiler_timeout_seconds: float = 5.0,
    use_context_model_for_heuristics: bool = True,
) -> list[SimulatedPlayer]:
    """Build a six-agent table where one user-supplied context plays baselines."""

    if not 1 <= hero_seat <= 6:
        raise ValueError("hero_seat must be between 1 and 6")
    baselines = ["tag", "lag", "calling_station", "nit", "maniac"]
    resolved_backend_type = str(backend_type).strip() or "heuristic_persona"
    use_llm_backend = resolved_backend_type.lower() == "llm_gated_nano"
    compiled = compile_persona(context)
    if not use_llm_backend and use_context_model_for_heuristics:
        compiled = compile_heuristic_persona_for_run(
            context,
            model_name=model_name or DEFAULT_MODEL,
            timeout_seconds=heuristic_compiler_timeout_seconds,
        ) or compiled
    resolved_baseline_backend_type = str(
        baseline_backend_type
        or resolved_backend_type
    ).strip() or "heuristic_persona"
    baseline_uses_llm_backend = resolved_baseline_backend_type.lower() == "llm_gated_nano"
    players: list[SimulatedPlayer] = []
    baseline_index = 0
    for seat in range(1, 7):
        if seat == hero_seat:
            players.append(
                SimulatedPlayer(
                    seat_index=seat - 1,
                    player_id="user_agent",
                    agent_id="user_agent",
                    persona_name=compiled.name,
                    persona_text=context,
                    backend_type=resolved_backend_type,
                    stack_bb=starting_stack_bb,
                    compiled_persona=compiled,
                    model_name=(model_name or DEFAULT_MODEL) if use_llm_backend else None,
                    reasoning_effort=(reasoning_effort or DEFAULT_REASONING_EFFORT) if use_llm_backend else None,
                    gating_profile=gating_profile if use_llm_backend else None,
                    always_model_postflop=always_model_postflop if use_llm_backend else False,
                    max_model_calls_per_hand=_normalized_limit(max_model_calls_per_hand) if use_llm_backend else None,
                )
            )
            continue
        persona_name = baselines[baseline_index % len(baselines)]
        baseline_index += 1
        preset = BUILTIN_PERSONAS[persona_name]
        players.append(
            SimulatedPlayer(
                seat_index=seat - 1,
                player_id=f"baseline_{seat}",
                agent_id=f"baseline_agent_{seat}",
                persona_name=persona_name,
                persona_text=preset.persona_text,
                backend_type=resolved_baseline_backend_type,
                stack_bb=starting_stack_bb,
                model_name=(model_name or DEFAULT_MODEL) if baseline_uses_llm_backend else None,
                reasoning_effort=(reasoning_effort or DEFAULT_REASONING_EFFORT) if baseline_uses_llm_backend else None,
                gating_profile=gating_profile if baseline_uses_llm_backend else None,
                always_model_postflop=always_model_postflop if baseline_uses_llm_backend else False,
                max_model_calls_per_hand=(
                    _normalized_limit(max_model_calls_per_hand)
                    if baseline_uses_llm_backend
                    else None
                ),
            )
        )
    return players


def simulate_and_record(
    *,
    hand_count: int,
    seed: int,
    table_id: str,
    simulation_run_id: str,
    starting_stack_bb: float,
    small_blind_bb: float,
    big_blind_bb: float,
    players: list[SimulatedPlayer] | None,
    publish_kafka: bool,
    kafka_brokers: str,
    actions_topic: str,
    summaries_topic: str,
    write_backend: bool,
    carry_stacks_between_hands: bool = True,
    max_model_calls_per_run: int = 0,
    llm_timeout_seconds: float = 15.0,
    user_id: str | None = None,
    decision_backend: str | None = None,
    hero_context_hash: str | None = None,
    hero_seat: int | None = None,
    run_started_at: datetime | None = None,
    progress_callback=None,
    progress_interval_hands: int = 50,
) -> dict[str, Any]:
    players = players or default_players(starting_stack_bb)
    config = SimulationConfig(
        num_hands=hand_count,
        starting_stack_bb=starting_stack_bb,
        small_blind_bb=small_blind_bb,
        big_blind_bb=big_blind_bb,
        table_id=table_id,
        simulation_run_id=simulation_run_id,
        source_type="simulated",
        source_dataset="persona_simulation",
        payload_version="1",
        user_id=user_id,
        decision_backend=decision_backend or _decision_backend_for_players(players),
        hero_context_hash=hero_context_hash,
        hero_seat=hero_seat,
        run_started_at=run_started_at,
        carry_stacks_between_hands=carry_stacks_between_hands,
        max_model_calls_per_run=int(max_model_calls_per_run),
        llm_timeout_seconds=llm_timeout_seconds,
    )
    simulator = PokerSimulator(config)
    player_totals = _empty_player_totals(players)
    first_summary = None
    reset_counts = {player.player_id: 0 for player in players}
    profile_hand_feature_rows: list[dict[str, Any]] = []
    hands_simulated = 0
    actions_simulated = 0
    published_actions = 0
    published_summaries = 0
    producer = build_producer(kafka_brokers) if publish_kafka else None
    normalized_progress_interval = max(1, int(progress_interval_hands or 1))
    try:
        for result, reset_snapshot in simulator.iter_session_with_reset_counts(players, runtime_seed=seed):
            first_summary = first_summary or result.hand_summary
            reset_counts = reset_snapshot
            hands_simulated += 1
            actions_simulated += len(result.action_events)
            _accumulate_player_totals(player_totals, result)
            hand_feature_rows = _single_result_profile_hand_feature_rows(
                result,
                config,
                players,
            )
            profile_hand_feature_rows.extend(hand_feature_rows)
            _accumulate_live_profile_metrics(player_totals, hand_feature_rows)
            if producer is not None:
                action_records = [event.to_dict() for event in result.action_events]
                summary_record = result.hand_summary.to_dict()
                published_actions += publish_records(
                    producer=producer,
                    topic=actions_topic,
                    records=action_records,
                    key_builder=lambda event: f"{event['simulation_run_id']}|{event['table_id']}|{event['hand_id']}",
                    flush=False,
                )
                published_summaries += publish_records(
                    producer=producer,
                    topic=summaries_topic,
                    records=[summary_record],
                    key_builder=lambda summary: f"{summary['simulation_run_id']}|{summary['table_id']}|{summary['hand_id']}",
                    flush=True,
                )

            if progress_callback and (
                hands_simulated % normalized_progress_interval == 0 or hands_simulated == hand_count
            ):
                progress_callback(
                    {
                        "hands": hands_simulated,
                        "actions": actions_simulated,
                        "published_actions": published_actions,
                        "published_hand_summaries": published_summaries,
                        "player_summaries": _player_summary_rows_from_totals(
                            players,
                            player_totals,
                            reset_counts,
                            session_summary=result.hand_summary,
                        ),
                    }
                )
    finally:
        if producer is not None:
            producer.close()

    player_summaries = _player_summary_rows_from_totals(
        players,
        player_totals,
        reset_counts,
        session_summary=first_summary,
    )
    profile_session_features = _session_feature_rows_from_hand_features(
        profile_hand_feature_rows
    )

    return {
        "hands": hands_simulated,
        "actions": actions_simulated,
        "published_actions": published_actions,
        "published_hand_summaries": published_summaries,
        "performance": _performance_summary(player_summaries),
        "player_summaries": player_summaries,
        "profile_session_features": profile_session_features,
        "backend_write_skipped": True,
    }


def _new_player_totals() -> dict[str, float | int]:
    return {
        "hands_played": 0,
        "total_bb_won": 0.0,
        "llm_decision_count": 0,
        "heuristic_decision_count": 0,
        "llm_fallback_count": 0,
        "observed_hands": 0,
        "vpip_hands": 0,
        "pfr_hands": 0,
        "aggressive_actions": 0,
        "passive_actions": 0,
        "showdown_hand_score": None,
        "showdown_hand_category": None,
    }


def _empty_player_totals(players: list[SimulatedPlayer]) -> dict[str, dict[str, float | int]]:
    return {player.player_id: _new_player_totals() for player in players}


def _accumulate_player_totals(
    player_totals: dict[str, dict[str, float | int]],
    result,
) -> None:
    summary = result.hand_summary
    for player_id, start_stack in summary.starting_stacks_bb.items():
        totals = player_totals.setdefault(player_id, _new_player_totals())
        final_stack = float(summary.final_stacks_bb.get(player_id, start_stack))
        totals["hands_played"] = int(totals["hands_played"]) + 1
        totals["total_bb_won"] = float(totals["total_bb_won"]) + (final_stack - float(start_stack))
        showdown_score = _showdown_hand_score(summary, player_id)
        current_showdown_score = totals.get("showdown_hand_score")
        if showdown_score is not None and (
            current_showdown_score is None or float(showdown_score) > float(current_showdown_score)
        ):
            totals["showdown_hand_score"] = float(showdown_score)
            totals["showdown_hand_category"] = _showdown_hand_category(summary, player_id)

    for event in result.action_events:
        totals = player_totals.setdefault(event.player_id, _new_player_totals())
        source = str(event.raw_lineage.get("decision_source") or "").strip().lower()
        if source == "llm":
            totals["llm_decision_count"] = int(totals["llm_decision_count"]) + 1
        elif source == "heuristic":
            totals["heuristic_decision_count"] = int(totals["heuristic_decision_count"]) + 1
        elif source == "heuristic_fallback":
            totals["llm_fallback_count"] = int(totals["llm_fallback_count"]) + 1


def _accumulate_live_profile_metrics(
    player_totals: dict[str, dict[str, float | int]],
    hand_feature_rows: list[dict[str, Any]],
) -> None:
    for row in hand_feature_rows:
        player_id = str(row.get("player_id") or "").strip()
        if not player_id:
            continue
        totals = player_totals.setdefault(player_id, _new_player_totals())
        totals["observed_hands"] = int(totals["observed_hands"]) + 1
        if bool(row.get("preflop_vpip")):
            totals["vpip_hands"] = int(totals["vpip_hands"]) + 1
        if bool(row.get("preflop_pfr")):
            totals["pfr_hands"] = int(totals["pfr_hands"]) + 1
        totals["aggressive_actions"] = int(totals["aggressive_actions"]) + int(
            row.get("aggressive_actions") or 0
        )
        totals["passive_actions"] = int(totals["passive_actions"]) + int(
            row.get("passive_actions") or 0
        )


def _player_summary_rows_from_totals(
    players: list[SimulatedPlayer],
    player_totals: dict[str, dict[str, float | int]],
    reset_counts: dict[str, int],
    *,
    session_summary,
) -> list[dict[str, Any]]:
    if session_summary is None:
        return []

    summary_rows: list[dict[str, Any]] = []
    for player in players:
        totals = player_totals.get(player.player_id, _new_player_totals())
        hands_played = int(totals["hands_played"])
        observed_hands = int(totals["observed_hands"])
        vpip_hands = int(totals["vpip_hands"])
        pfr_hands = int(totals["pfr_hands"])
        aggressive_actions = int(totals["aggressive_actions"])
        passive_actions = int(totals["passive_actions"])
        total_bb_won = round(float(totals["total_bb_won"]), 4)
        avg_bb_per_hand = round(total_bb_won / max(1, hands_played), 4) if hands_played else 0.0
        bb_per_100 = round(100.0 * total_bb_won / max(1, hands_played), 4) if hands_played else 0.0
        vpip = round(vpip_hands / max(1, observed_hands), 4) if observed_hands else 0.0
        pfr = round(pfr_hands / max(1, observed_hands), 4) if observed_hands else 0.0
        aggression_frequency = (
            round(aggressive_actions / max(1, aggressive_actions + passive_actions), 4)
            if aggressive_actions or passive_actions
            else 0.0
        )
        summary_rows.append(
            {
                "simulation_run_id": session_summary.simulation_run_id,
                "user_id": session_summary.user_id,
                "decision_backend": session_summary.decision_backend,
                "hero_context_hash": session_summary.hero_context_hash,
                "is_hero_player": player.seat_index + 1 == int(session_summary.hero_seat or 0),
                "seat": player.seat_index + 1,
                "player_id": player.player_id,
                "agent_id": player.agent_id,
                "persona_name": player.persona_name,
                "backend_type": player.backend_type,
                "hands_played": hands_played,
                "observed_hands": observed_hands,
                "observed_actions": aggressive_actions + passive_actions,
                "total_bb_won": total_bb_won,
                "avg_bb_per_hand": avg_bb_per_hand,
                "bb_per_100": bb_per_100,
                "vpip": vpip,
                "pfr": pfr,
                "aggression_frequency": aggression_frequency,
                "stack_reset_count": int(reset_counts.get(player.player_id, 0)),
                "llm_decision_count": int(totals["llm_decision_count"]),
                "heuristic_decision_count": int(totals["heuristic_decision_count"]),
                "llm_fallback_count": int(totals["llm_fallback_count"]),
                "showdown_hand_category": totals.get("showdown_hand_category"),
                "showdown_hand_score": totals.get("showdown_hand_score"),
            }
        )

    ordered = sorted(summary_rows, key=lambda item: (-float(item["total_bb_won"]), int(item["seat"])))
    for index, item in enumerate(ordered, start=1):
        item["final_rank"] = index
        item["updated_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    return ordered


def _single_result_profile_hand_feature_rows(
    result,
    config: SimulationConfig,
    players: list[SimulatedPlayer],
) -> list[dict[str, Any]]:
    summary = result.hand_summary
    player_lookup = {player.player_id: player for player in players}
    player_rows: list[dict[str, Any]] = []
    for player_id, start_stack in summary.starting_stacks_bb.items():
        player_rows.append(
            {
                "hand_id": summary.hand_id,
                "source_run_id": config.simulation_run_id,
                "source_type": "simulated",
                "source_dataset": config.source_dataset,
                "simulation_run_id": config.simulation_run_id,
                "player_id": player_id,
                "stack_start_bb": start_stack,
            }
        )

    action_rows: list[dict[str, Any]] = []
    for event in result.action_events:
        player = player_lookup.get(event.player_id)
        action_rows.append(
            {
                "hand_id": event.hand_id,
                "source_run_id": config.simulation_run_id,
                "source_type": "simulated",
                "source_dataset": config.source_dataset,
                "simulation_run_id": config.simulation_run_id,
                "table_id": config.table_id,
                "player_id": event.player_id,
                "agent_id": event.agent_id or (player.agent_id if player else None),
                "position": event.position,
                "street": event.street,
                "action_type": event.action_type,
                "amount_bb": event.amount_bb,
                "pot_before_bb": event.pot_before_bb,
                "effective_stack_bb": event.effective_stack_bb,
                "is_all_in": event.is_all_in,
                "action_index": event.action_index,
            }
        )

    if not action_rows:
        return []

    hand_features = compute_player_hand_features(
        pd.DataFrame(action_rows),
        pd.DataFrame(player_rows),
        training_dataset="session_scoring",
        feature_version=PROFILE_FEATURE_VERSION,
    )
    if hand_features.empty:
        return []
    return hand_features.to_dict(orient="records")


def _session_feature_rows_from_hand_features(
    hand_feature_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not hand_feature_rows:
        return []
    session_features = aggregate_session_hand_features(
        pd.DataFrame(hand_feature_rows),
        feature_version=PROFILE_FEATURE_VERSION,
    )
    if session_features.empty:
        return []
    return session_features.to_dict(orient="records")


def _results_to_frames(
    results,
    config: SimulationConfig,
    players: list[SimulatedPlayer],
    seed: int,
) -> dict[str, pd.DataFrame]:
    action_rows = []
    hand_rows = []
    player_rows = []
    for result in results:
        summary = result.hand_summary
        hand_rows.append(
            {
                "hand_id": summary.hand_id,
                "source_run_id": config.simulation_run_id,
                "source_type": "simulated",
                "source_dataset": config.source_dataset,
                "simulation_run_id": config.simulation_run_id,
                "user_id": config.user_id,
                "decision_backend": config.decision_backend,
                "hero_context_hash": config.hero_context_hash,
                "hero_seat": config.hero_seat,
                "table_id": config.table_id,
                "button_seat": summary.button_seat,
                "big_blind_bb": config.big_blind_bb,
                "small_blind_bb": config.small_blind_bb,
                "total_pot_bb": summary.pot_bb,
                "board_cards": json.dumps(list(summary.board_cards)),
                "started_at": summary.started_at.replace(tzinfo=None),
                "finished_at": summary.finished_at.replace(tzinfo=None),
                "payload_version": config.payload_version,
                "raw_lineage": json.dumps({"runtime_seed": summary.runtime_seed}),
            }
        )
        final_stack_lookup = summary.final_stacks_bb
        starting_stack_lookup = summary.starting_stacks_bb
        player_lookup = {player.player_id: player for player in players}
        for player_id, start_stack in starting_stack_lookup.items():
            player = player_lookup[player_id]
            final_stack = float(final_stack_lookup[player_id])
            player_rows.append(
                {
                    "source_run_id": config.simulation_run_id,
                    "source_type": "simulated",
                    "source_dataset": config.source_dataset,
                    "simulation_run_id": config.simulation_run_id,
                    "user_id": config.user_id,
                    "decision_backend": config.decision_backend,
                    "hero_context_hash": config.hero_context_hash,
                    "hero_seat": config.hero_seat,
                    "is_hero_player": player.seat_index + 1 == int(config.hero_seat or 0),
                    "table_id": config.table_id,
                    "hand_id": summary.hand_id,
                    "player_id": player.player_id,
                    "agent_id": player.agent_id,
                    "seat": player.seat_index + 1,
                    "player_name": player.player_id,
                    "stack_start_bb": start_stack,
                    "stack_end_bb": final_stack,
                    "hole_cards": json.dumps(list(summary.player_hole_cards.get(player.player_id, ()))),
                    "result_bb": round(final_stack - float(start_stack), 4),
                    "backend_type": player.backend_type,
                    "persona_name": player.persona_name,
                    "persona_text": player.persona_text,
                    "made_showdown": player.player_id in set(summary.showdown_player_ids),
                    "showdown_hand_category": _showdown_hand_category(summary, player.player_id),
                    "showdown_hand_score": _showdown_hand_score(summary, player.player_id),
                    "payload_version": config.payload_version,
                    "raw_lineage": json.dumps({"runtime_seed": seed}),
                }
            )
        for event in result.action_events:
            action_rows.append(
                {
                    "hand_id": event.hand_id,
                    "action_index": event.action_index,
                    "source_run_id": config.simulation_run_id,
                    "source_type": "simulated",
                    "source_dataset": config.source_dataset,
                    "simulation_run_id": config.simulation_run_id,
                    "user_id": event.user_id,
                    "decision_backend": event.decision_backend,
                    "hero_context_hash": event.hero_context_hash,
                    "hero_seat": event.hero_seat,
                    "is_hero_player": event.is_hero_player,
                    "table_id": event.table_id,
                    "street": event.street,
                    "player_id": event.player_id,
                    "agent_id": event.agent_id,
                    "seat": event.seat,
                    "position": event.position,
                    "action_type": event.action_type,
                    "amount_bb": event.amount_bb,
                    "pot_before_bb": event.pot_before_bb,
                    "pot_after_bb": event.pot_after_bb,
                    "to_call_bb": event.to_call_bb,
                    "effective_stack_bb": event.effective_stack_bb,
                    "players_remaining": event.players_remaining,
                    "board_cards_visible": json.dumps(list(event.board_cards_visible)),
                    "hole_cards_visible": json.dumps(list(event.hole_cards_visible)),
                    "is_all_in": event.is_all_in,
                    "event_ts": datetime.fromtimestamp(event.timestamp_ms / 1000, tz=timezone.utc).replace(tzinfo=None),
                    "backend_type": event.backend_type,
                    "persona_name": event.persona_name,
                    "persona_text": event.persona_text,
                    "payload_version": event.payload_version,
                    "raw_lineage": json.dumps(event.raw_lineage),
                }
            )

    simulation_runs = pd.DataFrame(
        [
            {
                "simulation_run_id": config.simulation_run_id,
                "user_id": config.user_id,
                "decision_backend": config.decision_backend,
                "hero_context_hash": config.hero_context_hash,
                "table_id": config.table_id,
                "backend_type": _session_backend_type(players),
                "started_at": min(result.hand_summary.started_at for result in results).replace(tzinfo=None),
                "finished_at": max(result.hand_summary.finished_at for result in results).replace(tzinfo=None),
                "hand_count": config.num_hands,
                "seed": seed,
                "hero_seat": config.hero_seat,
                "config_json": json.dumps(
                    {
                        "user_id": config.user_id,
                        "decision_backend": config.decision_backend,
                        "hero_context_hash": config.hero_context_hash,
                        "small_blind_bb": config.small_blind_bb,
                        "big_blind_bb": config.big_blind_bb,
                        "starting_stack_bb": config.starting_stack_bb,
                        "personas": [player.persona_name for player in players],
                        "backend_types": [player.backend_type for player in players],
                        "models": [player.model_name for player in players],
                        "max_model_calls_per_run": config.max_model_calls_per_run,
                    }
                ),
            }
        ]
    )

    raw_hands = pd.DataFrame(hand_rows)
    raw_players = pd.DataFrame(player_rows)
    raw_actions = pd.DataFrame(action_rows)
    return {
        "RAW_SIMULATION_RUNS": simulation_runs,
        "RAW_HANDS": raw_hands,
        "RAW_PLAYERS": raw_players,
        "RAW_ACTIONS": raw_actions,
        "CURATED_HANDS": raw_hands.copy(),
        "CURATED_PLAYERS": raw_players.copy(),
        "CURATED_ACTIONS": raw_actions.copy(),
    }


def _player_summary_rows(
    results,
    players: list[SimulatedPlayer],
    reset_counts: dict[str, int],
) -> list[dict[str, Any]]:
    if not results:
        return []
    player_totals = _empty_player_totals(players)
    for result in results:
        _accumulate_player_totals(player_totals, result)
    return _player_summary_rows_from_totals(
        players,
        player_totals,
        reset_counts,
        session_summary=results[0].hand_summary,
    )


def _performance_summary(player_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "agent_id": item["agent_id"],
            "player_id": item["player_id"],
            "persona_name": item["persona_name"],
            "hands_played": item["hands_played"],
            "total_bb_won": item["total_bb_won"],
            "avg_bb_per_hand": item["avg_bb_per_hand"],
            "bb_per_100": item["bb_per_100"],
            "final_rank": item["final_rank"],
        }
        for item in player_summaries
    ]


def _session_backend_type(players: list[SimulatedPlayer]) -> str:
    backend_types = sorted({player.backend_type for player in players})
    if len(backend_types) == 1:
        return backend_types[0]
    return "mixed"


def _decision_backend_for_players(players: list[SimulatedPlayer]) -> str | None:
    if not players:
        return None
    backend_types = {str(player.backend_type).strip().lower() for player in players}
    if backend_types == {"heuristic_persona"}:
        return "heuristic"
    if backend_types == {"llm_gated_nano"}:
        return "llm"
    return "mixed"


def _showdown_strength(summary, player_id: str) -> tuple[int, tuple[int, ...]] | None:
    if player_id not in set(summary.showdown_player_ids):
        return None
    hole_cards = tuple(summary.player_hole_cards.get(player_id, ()))
    if len(hole_cards) != 2:
        return None
    return best_five_of_seven(tuple(summary.board_cards) + hole_cards)


def _showdown_hand_category(summary, player_id: str) -> str | None:
    strength = _showdown_strength(summary, player_id)
    if strength is None:
        return None
    return SHOWDOWN_HAND_CATEGORIES.get(int(strength[0]), "unknown")


def _showdown_hand_score(summary, player_id: str) -> float | None:
    strength = _showdown_strength(summary, player_id)
    if strength is None:
        return None
    rank, tiebreak = strength
    components = [int(rank), *[int(value) for value in tiebreak], 0, 0, 0, 0, 0]
    score = 0
    for value in components[:6]:
        score = (score * 15) + value
    return float(score)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a deterministic persona-backed 6-max simulation.")
    parser.add_argument("--hands", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--table-id", default="table_1")
    parser.add_argument("--simulation-run-id", default=f"sim_{uuid.uuid4().hex[:8]}")
    parser.add_argument("--starting-stack-bb", type=float, default=100.0)
    parser.add_argument("--small-blind-bb", type=float, default=0.5)
    parser.add_argument("--big-blind-bb", type=float, default=1.0)
    parser.add_argument("--publish-kafka", action="store_true")
    parser.add_argument("--kafka-brokers", default="localhost:9092")
    parser.add_argument("--actions-topic", default="poker.actions")
    parser.add_argument("--summaries-topic", default="poker.hand_summaries")
    parser.add_argument("--no-backend-write", action="store_true")
    parser.add_argument("--context", default=None, help="Short user context string for one user-controlled agent")
    parser.add_argument("--hero-seat", type=int, default=1, help="Seat for --context agent, 1 through 6")
    parser.add_argument("--user-id", default=None)
    parser.add_argument("--reset-stacks-each-hand", action="store_true", help="Disable session stack carry-over")
    parser.add_argument("--backend-type", default="llm_gated_nano")
    parser.add_argument("--model-name", default=DEFAULT_MODEL)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    parser.add_argument("--gating-profile", default="conservative")
    parser.add_argument("--always-model-postflop", action="store_true")
    parser.add_argument("--max-model-calls-per-hand", type=int, default=0)
    parser.add_argument("--max-model-calls-per-run", type=int, default=0)
    parser.add_argument("--llm-timeout-seconds", type=float, default=15.0)
    parser.add_argument(
        "--personas-json",
        default=None,
        help="Optional JSON array of six persona configs from the API contract",
    )
    args = parser.parse_args(argv)

    players = None
    if args.personas_json:
        payload = json.loads(args.personas_json)
        players = [
            SimulatedPlayer(
                seat_index=int(persona["seat"]) - 1,
                player_id=str(persona["player_id"]),
                agent_id=str(persona.get("agent_id") or f"agent_{persona['seat']}"),
                persona_name=str(persona["persona_name"]),
                persona_text=str(persona["persona_text"]),
                backend_type=str(persona.get("backend_type", "heuristic_persona")),
                stack_bb=float(args.starting_stack_bb),
                centroid=persona.get("centroid"),
                model_name=persona.get("model_name"),
                reasoning_effort=persona.get("reasoning_effort"),
                gating_profile=persona.get("gating_profile"),
                always_model_postflop=bool(persona.get("always_model_postflop", False)),
                max_model_calls_per_hand=_normalized_limit(persona.get("max_model_calls_per_hand")),
            )
            for persona in payload
        ]
    elif args.context:
        backend_type = str(args.backend_type)
        players = build_context_matchup_players(
            args.context,
            starting_stack_bb=float(args.starting_stack_bb),
            hero_seat=int(args.hero_seat),
            backend_type=backend_type,
            model_name=args.model_name,
            reasoning_effort=args.reasoning_effort,
            gating_profile=args.gating_profile,
            always_model_postflop=True if not args.always_model_postflop else True,
            max_model_calls_per_hand=_normalized_limit(args.max_model_calls_per_hand),
            heuristic_compiler_timeout_seconds=float(args.llm_timeout_seconds),
        )

    result = simulate_and_record(
        hand_count=args.hands,
        seed=args.seed,
        table_id=args.table_id,
        simulation_run_id=args.simulation_run_id,
        starting_stack_bb=args.starting_stack_bb,
        small_blind_bb=args.small_blind_bb,
        big_blind_bb=args.big_blind_bb,
        players=players,
        publish_kafka=args.publish_kafka,
        kafka_brokers=args.kafka_brokers,
        actions_topic=args.actions_topic,
        summaries_topic=args.summaries_topic,
        write_backend=not args.no_backend_write,
        carry_stacks_between_hands=not args.reset_stacks_each_hand,
        max_model_calls_per_run=int(args.max_model_calls_per_run),
        llm_timeout_seconds=float(args.llm_timeout_seconds),
        user_id=args.user_id,
        decision_backend=normalize_decision_backend(args.backend_type),
        hero_context_hash=compute_hero_context_hash(args.context),
        hero_seat=int(args.hero_seat) if args.context else None,
    )
    print(
        json.dumps(
            {"simulation_run_id": args.simulation_run_id, **result},
            indent=2,
            default=_json_default,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
