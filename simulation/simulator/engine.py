from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Iterator

from .agents import PokerAgent, build_agent, preset_agent
from .cards import best_five_of_seven, build_deck
from .llm_policy import LlmDecisionRuntime
from .types import ActionDecision, ActionEvent, DecisionState, HandSummary, SimulatedPlayer, SimulationConfig


STREET_SEQUENCE = ("preflop", "flop", "turn", "river")
HAND_TIME_SPACING_SECONDS = 60


@dataclass(slots=True)
class PlayerRuntime:
    setup: SimulatedPlayer
    stack_bb: float
    street_committed_bb: float = 0.0
    total_committed_bb: float = 0.0
    hole_cards: list[str] = field(default_factory=list)
    folded: bool = False
    all_in: bool = False


@dataclass(slots=True)
class SimulationResult:
    hand_summary: HandSummary
    action_events: list[ActionEvent]


class PokerSimulator:
    def __init__(self, config: SimulationConfig | None = None):
        self.config = config or SimulationConfig()
        self._time_base = self._normalize_time_base(self.config.run_started_at)
        self._llm_runtime = LlmDecisionRuntime(
            max_calls_per_run=self.config.max_model_calls_per_run,
            timeout_seconds=self.config.llm_timeout_seconds,
        )

    def simulate_hand(
        self,
        players: list[SimulatedPlayer],
        *,
        runtime_seed: int,
        hand_index: int = 0,
    ) -> SimulationResult:
        if len(players) != 6:
            raise ValueError("The simulator expects exactly 6 players for 6-max NLH.")

        seed = self._mix_seed(runtime_seed, hand_index)
        deck = build_deck()
        import random

        rng = random.Random(seed)
        rng.shuffle(deck)

        players_runtime = [PlayerRuntime(setup=p, stack_bb=p.stack_bb) for p in players]
        agents = {player.seat_index: self._agent_for_player(player) for player in players}

        self._deal_hole_cards(deck, players_runtime)
        board_runout = [deck.pop(0) for _ in range(5)]
        button_seat = hand_index % 6
        hand_id = f"{self.config.simulation_run_id}_hand_{hand_index:05d}"
        started_at = self._synthetic_datetime(hand_index, 0)
        starting_stacks = {player.setup.player_id: round(player.stack_bb, 4) for player in players_runtime}

        action_events: list[ActionEvent] = []
        street_action_counts = defaultdict(int)
        action_index = 0
        for event in self._post_blinds(
            players_runtime,
            button_seat,
            hand_id,
            seed,
            hand_index,
            action_index,
        ):
            action_events.append(event)
            street_action_counts["preflop"] += 1
            action_index += 1

        pot_bb = sum(player.total_committed_bb for player in players_runtime)
        showdown_required = True
        revealed_board: list[str] = []
        for street in STREET_SEQUENCE:
            if street == "flop":
                revealed_board.extend(board_runout[:3])
            elif street == "turn":
                revealed_board.append(board_runout[3])
            elif street == "river":
                revealed_board.append(board_runout[4])
            if street != "preflop":
                for player in players_runtime:
                    player.street_committed_bb = 0.0

            if sum(1 for player in players_runtime if not player.folded) <= 1:
                showdown_required = False
                break

            if self._all_remaining_all_in(players_runtime):
                continue

            current_bet = max((player.street_committed_bb for player in players_runtime), default=0.0)
            last_raise_size = self.config.big_blind_bb
            start_seat = self._street_start_seat(button_seat, street)
            queue = self._street_queue(players_runtime, start_seat)
            acted_seats: set[int] = set()

            while queue:
                seat_index = queue.pop(0)
                player = players_runtime[seat_index]
                if player.folded or player.all_in or player.stack_bb <= 0.0:
                    continue

                to_call = max(0.0, current_bet - player.street_committed_bb)
                legal_actions = self._legal_actions(player, to_call, current_bet, last_raise_size)
                state = self._decision_state(
                    hand_id=hand_id,
                    seed=seed,
                    street=street,
                    seat_index=seat_index,
                    player=player,
                    board_cards=tuple(revealed_board),
                    pot_bb=pot_bb,
                    to_call_bb=to_call,
                    current_bet_bb=current_bet,
                    last_raise_size=last_raise_size,
                    players_runtime=players_runtime,
                    button_seat=button_seat,
                    action_index=action_index,
                    legal_actions=legal_actions,
                    agent=agents[seat_index],
                    action_events=action_events,
                )
                decision = self._sanitize_decision(agents[seat_index], state)
                event, raised = self._apply_decision(
                    state,
                    decision,
                    player,
                    players_runtime,
                    revealed_board,
                    hand_index,
                    action_index,
                )
                action_events.append(event)
                street_action_counts[street] += 1
                action_index += 1
                pot_bb = sum(p.total_committed_bb for p in players_runtime)
                current_bet = max((p.street_committed_bb for p in players_runtime), default=0.0)
                acted_seats.add(seat_index)
                if raised:
                    last_raise_size = max(self.config.big_blind_bb, current_bet - state.current_bet_bb)
                    queue = self._street_queue_after_raise(players_runtime, seat_index)
                    acted_seats = {seat_index}
                if sum(1 for p in players_runtime if not p.folded) <= 1:
                    showdown_required = False
                    queue = []
                    break
                if self._street_complete(players_runtime, current_bet, acted_seats):
                    queue = []
                    break

        if showdown_required and len(revealed_board) < 5:
            revealed_board = board_runout[:5]

        total_pot_before_settlement = round(sum(player.total_committed_bb for player in players_runtime), 4)
        winners, winning_seats, showdown, collections = self._settle_hand(players_runtime, revealed_board)
        pot_remaining = total_pot_before_settlement
        for winner_player_id, amount_bb in collections.items():
            winner = next(player for player in players_runtime if player.setup.player_id == winner_player_id)
            action_events.append(
                self._collect_pot_event(
                    player=winner,
                    hand_id=hand_id,
                    seed=seed,
                    hand_index=hand_index,
                    button_seat=button_seat,
                    action_index=action_index,
                    amount_bb=amount_bb,
                    pot_before_bb=pot_remaining,
                    pot_after_bb=max(0.0, pot_remaining - amount_bb),
                    board_cards=tuple(revealed_board),
                    players_runtime=players_runtime,
                )
            )
            street_action_counts["showdown"] += 1
            action_index += 1
            pot_remaining = max(0.0, pot_remaining - amount_bb)

        final_stacks = {player.setup.player_id: round(player.stack_bb, 4) for player in players_runtime}
        summary = HandSummary(
            hand_id=hand_id,
            simulation_run_id=self.config.simulation_run_id,
            table_id=self.config.table_id,
            source_type=self.config.source_type,
            source_dataset=self.config.source_dataset,
            source_run_id=self.config.simulation_run_id,
            user_id=self.config.user_id,
            decision_backend=self.config.decision_backend,
            hero_context_hash=self.config.hero_context_hash,
            hero_seat=self.config.hero_seat,
            button_seat=button_seat + 1,
            board_cards=tuple(revealed_board),
            started_at=started_at,
            finished_at=self._synthetic_datetime(hand_index, max(len(action_events), 1) + 1),
            pot_bb=total_pot_before_settlement,
            small_blind_bb=self.config.small_blind_bb,
            big_blind_bb=self.config.big_blind_bb,
            action_count=len(action_events),
            starting_stacks_bb=starting_stacks,
            collections_bb={player_id: round(amount, 4) for player_id, amount in collections.items()},
            street_action_counts=dict(street_action_counts),
            showdown=showdown,
            winners=tuple(winners),
            winning_seat_indices=tuple(winning_seats),
            final_stacks_bb=final_stacks,
            player_personas={p.setup.player_id: p.setup.persona_name for p in players_runtime},
            player_agent_ids={p.setup.player_id: p.setup.agent_id for p in players_runtime},
            player_seats={p.setup.player_id: p.setup.seat_index + 1 for p in players_runtime},
            showdown_player_ids=tuple(p.setup.player_id for p in players_runtime if showdown and not p.folded),
            player_hole_cards={p.setup.player_id: tuple(p.hole_cards) for p in players_runtime},
            backend_types={p.setup.player_id: p.setup.backend_type for p in players_runtime},
            payload_version=self.config.payload_version,
            runtime_seed=seed,
        )
        return SimulationResult(hand_summary=summary, action_events=action_events)

    def simulate_session(self, players: list[SimulatedPlayer], *, runtime_seed: int) -> list[SimulationResult]:
        results, _ = self.simulate_session_with_reset_counts(players, runtime_seed=runtime_seed)
        return results

    def iter_session_with_reset_counts(
        self,
        players: list[SimulatedPlayer],
        *,
        runtime_seed: int,
    ) -> Iterator[tuple[SimulationResult, dict[str, int]]]:
        current_players = list(players)
        reset_counts = {player.player_id: 0 for player in players}
        for hand_index in range(self.config.num_hands):
            result = self.simulate_hand(current_players, runtime_seed=runtime_seed, hand_index=hand_index)
            if self.config.carry_stacks_between_hands:
                next_players: list[SimulatedPlayer] = []
                for player in current_players:
                    next_stack = float(result.hand_summary.final_stacks_bb.get(player.player_id, player.stack_bb))
                    if next_stack < self.config.rebuy_below_bb:
                        next_stack = self.config.starting_stack_bb
                        reset_counts[player.player_id] = reset_counts.get(player.player_id, 0) + 1
                    next_players.append(replace(player, stack_bb=round(next_stack, 4)))
                current_players = next_players
            yield result, dict(reset_counts)

    def simulate_session_with_reset_counts(
        self,
        players: list[SimulatedPlayer],
        *,
        runtime_seed: int,
    ) -> tuple[list[SimulationResult], dict[str, int]]:
        results: list[SimulationResult] = []
        reset_counts = {player.player_id: 0 for player in players}
        for result, reset_snapshot in self.iter_session_with_reset_counts(players, runtime_seed=runtime_seed):
            results.append(result)
            reset_counts = reset_snapshot
        return results, reset_counts

    def _agent_for_player(self, player: SimulatedPlayer) -> PokerAgent:
        if player.backend_type == "cluster_clone":
            return build_agent(
                "cluster_clone",
                player.persona_text,
                agent_id=player.agent_id,
                compiled_persona=player.compiled_persona,
                centroid=dict(player.centroid) if player.centroid is not None else None,
            )
        if player.backend_type == "llm_gated_nano":
            return build_agent(
                "llm_gated_nano",
                player.persona_text,
                agent_id=player.agent_id,
                compiled_persona=player.compiled_persona,
                runtime=self._llm_runtime,
                model_name=player.model_name,
                reasoning_effort=player.reasoning_effort,
                gating_profile=player.gating_profile,
                always_model_postflop=player.always_model_postflop,
                max_model_calls_per_hand=player.max_model_calls_per_hand,
            )
        if player.persona_name in {"nit", "tag", "lag", "calling_station", "maniac", "shove_bot"}:
            return preset_agent(player.persona_name, agent_id=player.agent_id)
        return build_agent(
            "heuristic_persona",
            player.persona_text,
            agent_id=player.agent_id,
            compiled_persona=player.compiled_persona,
        )

    def _deal_hole_cards(self, deck: list[str], players_runtime: list[PlayerRuntime]) -> None:
        for _ in range(2):
            for player in players_runtime:
                player.hole_cards.append(deck.pop(0))

    def _post_blinds(
        self,
        players_runtime: list[PlayerRuntime],
        button_seat: int,
        hand_id: str,
        seed: int,
        hand_index: int,
        action_index: int,
    ) -> list[ActionEvent]:
        sb_seat = (button_seat + 1) % 6
        bb_seat = (button_seat + 2) % 6
        events = [
            self._forced_bet_event(
                players_runtime=players_runtime,
                player=players_runtime[sb_seat],
                button_seat=button_seat,
                hand_id=hand_id,
                seed=seed,
                hand_index=hand_index,
                action_index=action_index,
                action_type="post_small_blind",
                amount_bb=self.config.small_blind_bb,
            )
        ]
        events.append(
            self._forced_bet_event(
                players_runtime=players_runtime,
                player=players_runtime[bb_seat],
                button_seat=button_seat,
                hand_id=hand_id,
                seed=seed,
                hand_index=hand_index,
                action_index=action_index + 1,
                action_type="post_big_blind",
                amount_bb=self.config.big_blind_bb,
            )
        )
        return events

    def _forced_bet_event(
        self,
        *,
        players_runtime: list[PlayerRuntime],
        player: PlayerRuntime,
        button_seat: int,
        hand_id: str,
        seed: int,
        hand_index: int,
        action_index: int,
        action_type: str,
        amount_bb: float,
    ) -> ActionEvent:
        pot_before = sum(p.total_committed_bb for p in players_runtime)
        amount = self._commit(player, amount_bb)
        pot_after = sum(p.total_committed_bb for p in players_runtime)
        return ActionEvent(
            hand_id=hand_id,
            simulation_run_id=self.config.simulation_run_id,
            table_id=self.config.table_id,
            source_type=self.config.source_type,
            source_dataset=self.config.source_dataset,
            source_run_id=self.config.simulation_run_id,
            user_id=self.config.user_id,
            decision_backend=self.config.decision_backend,
            hero_context_hash=self.config.hero_context_hash,
            hero_seat=self.config.hero_seat,
            is_hero_player=self._is_hero_player(player.setup.seat_index),
            street="preflop",
            action_index=action_index,
            action_order=action_index,
            player_id=player.setup.player_id,
            agent_id=player.setup.agent_id,
            seat=player.setup.seat_index + 1,
            position=self._position_name(button_seat, player.setup.seat_index),
            action_type=action_type,
            amount_bb=round(amount, 4),
            pot_before_bb=round(pot_before, 4),
            pot_after_bb=round(pot_after, 4),
            to_call_bb=0.0,
            effective_stack_bb=round(player.stack_bb + amount, 4),
            players_remaining=len([p for p in players_runtime if not p.folded]),
            board_cards_visible=tuple(),
            hole_cards_visible=tuple(player.hole_cards),
            is_all_in=player.stack_bb <= 0.0,
            timestamp_ms=int(self._synthetic_datetime(hand_index, action_index + 1).timestamp() * 1000),
            backend_type=player.setup.backend_type,
            persona_name=player.setup.persona_name,
            persona_text=player.setup.persona_text,
            payload_version=self.config.payload_version,
            raw_lineage={"button_seat": button_seat + 1, "seed": seed, "forced_action": True},
        )

    def _commit(self, player: PlayerRuntime, amount: float) -> float:
        actual = min(player.stack_bb, amount)
        player.stack_bb -= actual
        player.street_committed_bb += actual
        player.total_committed_bb += actual
        if player.stack_bb <= 0.0:
            player.all_in = True
        return actual

    def _street_start_seat(self, button_seat: int, street: str) -> int:
        return (button_seat + 3) % 6 if street == "preflop" else (button_seat + 1) % 6

    def _street_queue(self, players_runtime: list[PlayerRuntime], start_seat: int) -> list[int]:
        active = [player.setup.seat_index for player in players_runtime if not player.folded and not player.all_in and player.stack_bb > 0.0]
        return self._ordered_from_start(active, start_seat)

    def _street_queue_after_raise(self, players_runtime: list[PlayerRuntime], raiser_seat: int) -> list[int]:
        active = [
            player.setup.seat_index
            for player in players_runtime
            if not player.folded and not player.all_in and player.stack_bb > 0.0 and player.setup.seat_index != raiser_seat
        ]
        return self._ordered_from_start(active, (raiser_seat + 1) % 6)

    def _ordered_from_start(self, active: list[int], start_seat: int) -> list[int]:
        ordered: list[int] = []
        for offset in range(6):
            seat = (start_seat + offset) % 6
            if seat in active:
                ordered.append(seat)
        return ordered

    def _legal_actions(self, player: PlayerRuntime, to_call_bb: float, current_bet_bb: float, last_raise_size: float) -> tuple[str, ...]:
        if to_call_bb <= 0.0:
            return ("check", "bet", "all_in")
        actions = ["fold", "call", "all_in"]
        min_total = current_bet_bb + max(last_raise_size, self.config.big_blind_bb)
        if player.stack_bb + player.street_committed_bb >= min_total:
            actions.append("raise")
        return tuple(actions)

    def _decision_state(
        self,
        *,
        hand_id: str,
        seed: int,
        street: str,
        seat_index: int,
        player: PlayerRuntime,
        board_cards: tuple[str, ...],
        pot_bb: float,
        to_call_bb: float,
        current_bet_bb: float,
        last_raise_size: float,
        players_runtime: list[PlayerRuntime],
        button_seat: int,
        action_index: int,
        legal_actions: tuple[str, ...],
        agent: PokerAgent,
        action_events: list[ActionEvent],
    ) -> DecisionState:
        active_players = tuple(player.setup.player_id for player in players_runtime if not player.folded)
        position = self._position_name(button_seat, seat_index)
        return DecisionState(
            hand_id=hand_id,
            simulation_run_id=self.config.simulation_run_id,
            table_id=self.config.table_id,
            street=street,
            seat_index=seat_index,
            player_id=player.setup.player_id,
            agent_id=agent.agent_id,
            position=position,
            stack_bb=player.stack_bb,
            committed_bb=player.street_committed_bb,
            pot_bb=pot_bb,
            to_call_bb=to_call_bb,
            min_raise_to_bb=current_bet_bb + max(last_raise_size, self.config.big_blind_bb),
            current_bet_bb=current_bet_bb,
            players_remaining=len(active_players),
            active_players=active_players,
            board_cards=board_cards,
            hole_cards=tuple(player.hole_cards),
            legal_actions=legal_actions,
            street_action_count=action_index,
            action_index=action_index,
            button_seat=button_seat + 1,
            big_blind_bb=self.config.big_blind_bb,
            small_blind_bb=self.config.small_blind_bb,
            backend_type=agent.backend_type,
            source_type=self.config.source_type,
            source_dataset=self.config.source_dataset,
            payload_version=self.config.payload_version,
            persona_name=player.setup.persona_name,
            persona_text=player.setup.persona_text,
            seed=seed,
            hand_action_history=self._public_hand_action_history(action_events),
            hand_model_active=bool(getattr(agent, "_hand_model_active", False)),
        )

    def _is_hero_player(self, seat_index: int) -> bool:
        if self.config.hero_seat is None:
            return False
        return int(self.config.hero_seat) == int(seat_index + 1)

    def _public_hand_action_history(self, action_events: list[ActionEvent]) -> tuple[dict[str, object], ...]:
        history: list[dict[str, object]] = []
        for event in action_events:
            history.append(
                {
                    "action_index": int(event.action_index),
                    "street": event.street,
                    "player_id": event.player_id,
                    "seat": int(event.seat),
                    "position": event.position,
                    "action_type": event.action_type,
                    "amount_bb": round(float(event.amount_bb), 4),
                    "pot_before_bb": round(float(event.pot_before_bb), 4),
                    "pot_after_bb": round(float(event.pot_after_bb), 4),
                    "to_call_bb": round(float(event.to_call_bb), 4),
                    "players_remaining": int(event.players_remaining),
                    "is_all_in": bool(event.is_all_in),
                }
            )
        return tuple(history)

    def _sanitize_decision(self, agent: PokerAgent, state: DecisionState) -> ActionDecision:
        decision = agent.select_action(state)
        metadata = dict(decision.metadata)
        if decision.action_type not in state.legal_actions:
            original_source = str(metadata.get("decision_source") or "").strip().lower() or "unknown"
            metadata["original_decision_source"] = original_source
            metadata["original_action_type"] = decision.action_type
            metadata["decision_source"] = "heuristic_fallback"
            metadata.setdefault("fallback_reason", "illegal_action_from_agent")
            if "check" in state.legal_actions:
                return ActionDecision("check", note=decision.note, metadata=metadata)
            if "call" in state.legal_actions:
                return ActionDecision("call", note=decision.note, metadata=metadata)
            if "fold" in state.legal_actions:
                return ActionDecision("fold", note=decision.note, metadata=metadata)
            return ActionDecision("all_in", note=decision.note, metadata=metadata)
        if decision.action_type in {"raise", "bet"}:
            minimum = state.min_raise_to_bb if decision.action_type == "raise" else state.big_blind_bb
            target = decision.amount_bb if decision.amount_bb is not None else minimum
            target = max(minimum, min(state.stack_bb + state.committed_bb, target))
            return ActionDecision(decision.action_type, target, decision.note, metadata=metadata)
        return decision

    def _apply_decision(
        self,
        state: DecisionState,
        decision: ActionDecision,
        player: PlayerRuntime,
        players_runtime: list[PlayerRuntime],
        board_cards: list[str],
        hand_index: int,
        action_index: int,
    ) -> tuple[ActionEvent, bool]:
        pot_before = sum(p.total_committed_bb for p in players_runtime)
        current_bet_before = state.current_bet_bb
        raised = False
        amount = 0.0
        is_all_in = False
        if decision.action_type == "fold":
            player.folded = True
        elif decision.action_type == "check":
            pass
        elif decision.action_type == "call":
            amount = self._commit(player, state.to_call_bb)
            is_all_in = player.stack_bb <= 0.0
        elif decision.action_type == "bet":
            target = decision.amount_bb or state.min_raise_to_bb
            amount = self._commit(player, max(0.0, target - player.street_committed_bb))
            raised = player.street_committed_bb > current_bet_before
            is_all_in = player.stack_bb <= 0.0
        elif decision.action_type == "raise":
            target = decision.amount_bb or state.min_raise_to_bb
            amount = self._commit(player, max(0.0, target - player.street_committed_bb))
            raised = player.street_committed_bb > current_bet_before
            is_all_in = player.stack_bb <= 0.0
        elif decision.action_type == "all_in":
            amount = self._commit(player, player.stack_bb)
            raised = player.street_committed_bb > current_bet_before
            is_all_in = True
        else:
            raise ValueError(f"Unsupported action: {decision.action_type}")

        if is_all_in:
            player.all_in = True
        event = ActionEvent(
            hand_id=state.hand_id,
            simulation_run_id=state.simulation_run_id,
            table_id=state.table_id,
            source_type=state.source_type,
            source_dataset=state.source_dataset,
            source_run_id=state.simulation_run_id,
            user_id=self.config.user_id,
            decision_backend=self.config.decision_backend,
            hero_context_hash=self.config.hero_context_hash,
            hero_seat=self.config.hero_seat,
            is_hero_player=self._is_hero_player(state.seat_index),
            street=state.street,
            action_index=state.action_index,
            action_order=state.street_action_count,
            player_id=player.setup.player_id,
            agent_id=player.setup.agent_id,
            seat=state.seat_index + 1,
            position=state.position,
            action_type=decision.action_type,
            amount_bb=round(amount, 4),
            pot_before_bb=round(pot_before, 4),
            pot_after_bb=round(sum(p.total_committed_bb for p in players_runtime), 4),
            to_call_bb=round(state.to_call_bb, 4),
            effective_stack_bb=round(state.stack_bb + state.committed_bb, 4),
            players_remaining=state.players_remaining,
            board_cards_visible=board_cards_to_tuple(board_cards),
            hole_cards_visible=tuple(state.hole_cards),
            is_all_in=is_all_in,
            timestamp_ms=int(self._synthetic_datetime(hand_index, action_index + 1).timestamp() * 1000),
            backend_type=state.backend_type,
            persona_name=state.persona_name,
            persona_text=state.persona_text,
            payload_version=state.payload_version,
            raw_lineage={
                "button_seat": state.button_seat,
                "seed": state.seed,
                "legal_actions": list(state.legal_actions),
                "street": state.street,
                "decision_source": decision.metadata.get("decision_source", "heuristic"),
                **dict(decision.metadata),
            },
        )
        return event, raised

    def _collect_pot_event(
        self,
        *,
        player: PlayerRuntime,
        hand_id: str,
        seed: int,
        hand_index: int,
        button_seat: int,
        action_index: int,
        amount_bb: float,
        pot_before_bb: float,
        pot_after_bb: float,
        board_cards: tuple[str, ...],
        players_runtime: list[PlayerRuntime],
    ) -> ActionEvent:
        return ActionEvent(
            hand_id=hand_id,
            simulation_run_id=self.config.simulation_run_id,
            table_id=self.config.table_id,
            source_type=self.config.source_type,
            source_dataset=self.config.source_dataset,
            source_run_id=self.config.simulation_run_id,
            user_id=self.config.user_id,
            decision_backend=self.config.decision_backend,
            hero_context_hash=self.config.hero_context_hash,
            hero_seat=self.config.hero_seat,
            is_hero_player=self._is_hero_player(player.setup.seat_index),
            street="showdown",
            action_index=action_index,
            action_order=action_index,
            player_id=player.setup.player_id,
            agent_id=player.setup.agent_id,
            seat=player.setup.seat_index + 1,
            position=self._position_name(button_seat, player.setup.seat_index),
            action_type="collect_pot",
            amount_bb=round(amount_bb, 4),
            pot_before_bb=round(pot_before_bb, 4),
            pot_after_bb=round(pot_after_bb, 4),
            to_call_bb=0.0,
            effective_stack_bb=round(player.stack_bb, 4),
            players_remaining=len([p for p in players_runtime if not p.folded]),
            board_cards_visible=board_cards,
            hole_cards_visible=tuple(player.hole_cards),
            is_all_in=False,
            timestamp_ms=int(self._synthetic_datetime(hand_index, action_index + 1).timestamp() * 1000),
            backend_type=player.setup.backend_type,
            persona_name=player.setup.persona_name,
            persona_text=player.setup.persona_text,
            payload_version=self.config.payload_version,
            raw_lineage={"button_seat": button_seat + 1, "seed": seed, "settlement": True},
        )

    def _street_complete(
        self,
        players_runtime: list[PlayerRuntime],
        current_bet_bb: float,
        acted_seats: set[int],
    ) -> bool:
        active = [player for player in players_runtime if not player.folded and not player.all_in]
        if len([player for player in players_runtime if not player.folded]) <= 1:
            return True
        if not active:
            return True
        if any(player.setup.seat_index not in acted_seats for player in active):
            return False
        return all(player.street_committed_bb >= current_bet_bb or player.all_in for player in active)

    def _all_remaining_all_in(self, players_runtime: list[PlayerRuntime]) -> bool:
        active = [player for player in players_runtime if not player.folded]
        return len(active) > 1 and all(player.all_in for player in active)

    def _settle_hand(
        self,
        players_runtime: list[PlayerRuntime],
        board_cards: list[str],
    ) -> tuple[list[str], list[int], bool, dict[str, float]]:
        live = [player for player in players_runtime if not player.folded]
        total_pot = sum(player.total_committed_bb for player in players_runtime)
        if len(live) == 1:
            live[0].stack_bb += total_pot
            return [live[0].setup.player_id], [live[0].setup.seat_index], False, {live[0].setup.player_id: round(total_pot, 4)}

        showdown = True
        if len(board_cards) < 5:
            board_cards = board_cards[:]
            while len(board_cards) < 5:
                board_cards.append("As")

        commitments = sorted({player.total_committed_bb for player in players_runtime if player.total_committed_bb > 0.0})
        previous_level = 0.0
        winner_ids: list[str] = []
        winning_seats: list[int] = []
        collections: defaultdict[str, float] = defaultdict(float)
        for level in commitments:
            eligible = [player for player in live if player.total_committed_bb >= level]
            if not eligible:
                previous_level = level
                continue
            pot_slice = (level - previous_level) * len([player for player in players_runtime if player.total_committed_bb >= level])
            ranked: list[PlayerRuntime] = []
            best_score = None
            for player in eligible:
                score = best_five_of_seven(tuple(player.hole_cards) + tuple(board_cards))
                if best_score is None or score > best_score:
                    best_score = score
                    ranked = [player]
                elif score == best_score:
                    ranked.append(player)
            share = pot_slice / max(1, len(ranked))
            for winner in ranked:
                winner.stack_bb += share
                collections[winner.setup.player_id] += share
            for winner in ranked:
                if winner.setup.player_id not in winner_ids:
                    winner_ids.append(winner.setup.player_id)
                if winner.setup.seat_index not in winning_seats:
                    winning_seats.append(winner.setup.seat_index)
            previous_level = level

        return winner_ids, winning_seats, showdown, {player_id: round(amount, 4) for player_id, amount in collections.items()}

    def _position_name(self, button_seat: int, seat_index: int) -> str:
        labels = ["BTN", "SB", "BB", "UTG", "HJ", "CO"]
        order = [(button_seat + offset) % 6 for offset in range(6)]
        return labels[order.index(seat_index)]

    def _mix_seed(self, runtime_seed: int, hand_index: int) -> int:
        return (runtime_seed * 1_000_003 + hand_index * 97_513 + 17) % (2**31 - 1)

    @staticmethod
    def _normalize_time_base(value: datetime | None) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _synthetic_datetime(self, hand_index: int, step: int) -> datetime:
        return self._time_base + timedelta(
            seconds=hand_index * HAND_TIME_SPACING_SECONDS,
            milliseconds=step * 250,
        )


def board_cards_to_tuple(board_cards: list[str]) -> tuple[str, ...]:
    return tuple(board_cards)
