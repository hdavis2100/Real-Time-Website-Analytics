from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .gating import gate_decision, gating_config
from .llm_policy import DEFAULT_MODEL, LlmDecisionRuntime
from .cards import preflop_strength
from .personas import compile_persona, preset_persona
from .types import ActionDecision, CompiledPersona, DecisionState, PersonaParameters


def _state_noise(state: DecisionState) -> float:
    raw = "|".join(
        [
            state.hand_id,
            state.simulation_run_id,
            state.table_id,
            state.street,
            str(state.seat_index),
            state.player_id,
            str(state.action_index),
            ",".join(state.hole_cards),
            ",".join(state.board_cards),
        ]
    ).encode("utf-8")
    digest = hashlib.blake2b(raw, digest_size=8).digest()
    return int.from_bytes(digest, "big") / float(2**64 - 1)


def _normalized_persona_text(compiled_persona: CompiledPersona) -> str:
    return str(compiled_persona.persona_text or "").strip().lower()


def _requires_pocket_pairs_only(compiled_persona: CompiledPersona) -> bool:
    text = _normalized_persona_text(compiled_persona)
    if "pocket pair" not in text and "pairs only" not in text:
        return False
    return "only" in text or "pocket pairs only" in text or "play pocket pairs" in text


def _is_pocket_pair_hand(state: DecisionState) -> bool:
    if len(state.hole_cards) != 2:
        return False
    return str(state.hole_cards[0])[0] == str(state.hole_cards[1])[0]


@dataclass(slots=True)
class PokerAgent:
    compiled_persona: CompiledPersona
    agent_id: str
    backend_type: str = "heuristic_persona"

    def select_action(self, state: DecisionState) -> ActionDecision:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass(slots=True)
class HeuristicPersonaAgent(PokerAgent):
    def select_action(self, state: DecisionState) -> ActionDecision:
        if (
            state.street == "preflop"
            and _requires_pocket_pairs_only(self.compiled_persona)
            and not _is_pocket_pair_hand(state)
        ):
            if state.to_call_bb <= 0.0 and "check" in state.legal_actions:
                return ActionDecision("check", None, "pocket_pairs_only")
            return ActionDecision("fold", None, "pocket_pairs_only")

        p = self.compiled_persona.parameters
        noise = _state_noise(state)
        strength = self._strength(state)
        street_index = {"preflop": 0, "flop": 1, "turn": 2, "river": 3}[state.street]
        aggression = self._street_aggression(p, street_index)
        effective_stack_bb = max(state.big_blind_bb, state.stack_bb + state.committed_bb)
        stack_pressure = state.to_call_bb / effective_stack_bb if effective_stack_bb > 0 else 1.0
        short_stack = effective_stack_bb <= max(12.0, state.big_blind_bb * 18.0)
        pot_committed = stack_pressure >= 0.45 or state.stack_bb <= state.to_call_bb * 1.35

        if state.to_call_bb <= 0.0:
            if strength >= max(0.74, 0.84 - p.raise_bias * 0.35) or noise < (p.cbet_bias * aggression):
                return ActionDecision("bet", self._bet_size(state, strength, aggression))
            if noise < p.trap_bias * 0.5 and strength >= 0.55:
                return ActionDecision("check", None, "trap")
            return ActionDecision("check")

        fold_cutoff = p.fold_threshold - strength * 0.18 - state.to_call_bb / max(1.0, state.stack_bb * 1.5)
        call_cutoff = fold_cutoff + p.cold_call_bias * 0.40 + p.showdown_tendency * 0.12
        raise_cutoff = call_cutoff + p.raise_bias * 0.26 + strength * 0.22
        jam_cutoff = 0.94 - p.jam_bias * 0.18 - (0.14 if short_stack else 0.0) - min(0.16, stack_pressure * 0.35)
        jam_cutoff = max(0.58, min(0.95, jam_cutoff))

        if strength < fold_cutoff and noise > p.showdown_tendency:
            return ActionDecision("fold")
        if strength < call_cutoff:
            call_noise = p.cold_call_bias * (0.55 + 0.20 * (1.0 - min(1.0, strength)))
            return ActionDecision("call") if noise < call_noise else ActionDecision("fold")
        if pot_committed and (strength >= max(call_cutoff, jam_cutoff - 0.10) or noise < p.jam_bias * 0.22):
            return ActionDecision("all_in")
        if short_stack and (strength >= jam_cutoff or (strength >= call_cutoff and noise < p.jam_bias * 0.18)):
            return ActionDecision("all_in")
        raise_noise = p.raise_bias * (0.22 + 0.32 * strength + 0.12 * aggression)
        semibluff_strength = max(call_cutoff + 0.03, 0.46 - p.fold_threshold * 0.10)
        if strength >= raise_cutoff or (strength >= semibluff_strength and noise < raise_noise):
            return ActionDecision("raise", self._raise_size(state, strength, aggression))
        return ActionDecision("call")

    def _strength(self, state: DecisionState) -> float:
        if state.street == "preflop":
            return preflop_strength(state.hole_cards)
        from .cards import best_five_of_seven

        rank, tiebreak = best_five_of_seven(tuple(state.hole_cards) + tuple(state.board_cards))
        base = rank / 8.0
        kicker = (tiebreak[0] / 14.0) if tiebreak else 0.0
        return max(0.0, min(1.0, 0.72 * base + 0.28 * kicker))

    def _street_aggression(self, p: PersonaParameters, street_index: int) -> float:
        bonuses = [0.0, p.cbet_bias * 0.35, p.bluff_turn_bias * 0.35, p.bluff_river_bias * 0.35]
        return max(0.2, min(1.0, p.preflop_open_bias + p.raise_bias * 0.5 + bonuses[street_index]))

    def _bet_size(self, state: DecisionState, strength: float, aggression: float) -> float:
        base = max(state.big_blind_bb, state.pot_bb * min(0.90, 0.32 + aggression * 0.28 + strength * 0.18))
        if self.compiled_persona.preset_name == "shove_bot":
            return max(base, state.stack_bb)
        if state.stack_bb <= max(18.0, state.pot_bb * 1.75) and (strength >= 0.90 or aggression > 0.90):
            return max(base, state.stack_bb)
        if strength >= 0.88:
            return max(base, min(state.stack_bb, state.pot_bb * 0.85))
        return max(base, min(state.stack_bb, state.pot_bb * (0.38 + aggression * 0.18)))

    def _raise_size(self, state: DecisionState, strength: float, aggression: float) -> float:
        min_total = state.min_raise_to_bb
        effective_stack_bb = max(min_total, state.stack_bb + state.committed_bb)
        if self.compiled_persona.preset_name == "shove_bot":
            return effective_stack_bb
        if state.stack_bb <= max(18.0, state.pot_bb * 1.60) and (strength > 0.90 or aggression > 0.90):
            return effective_stack_bb
        growth = 0.45 + aggression * 0.35 + strength * 0.25
        target = max(min_total, state.current_bet_bb + max(state.big_blind_bb * 2.0, state.pot_bb * growth))
        ceiling = effective_stack_bb
        if state.stack_bb > 30.0:
            ceiling = min(ceiling, state.current_bet_bb + max(state.big_blind_bb * 8.0, state.pot_bb * 1.4))
        return min(ceiling, target)


@dataclass(slots=True)
class GatedLlmAgent(HeuristicPersonaAgent):
    runtime: LlmDecisionRuntime | None = None
    model_name: str | None = None
    reasoning_effort: str | None = None
    gating_profile: str | None = None
    always_model_postflop: bool = False
    max_model_calls_per_hand: int | None = None
    _hand_id: str | None = None
    _hand_model_active: bool = False
    _hand_model_calls: int = 0

    def __post_init__(self) -> None:
        self.backend_type = "llm_gated_nano"
        if self.max_model_calls_per_hand is not None:
            self.max_model_calls_per_hand = max(1, int(self.max_model_calls_per_hand))

    def select_action(self, state: DecisionState) -> ActionDecision:
        self._reset_for_hand(state.hand_id)
        gate_config_value = gating_config(
            self.gating_profile,
            always_model_postflop=self.always_model_postflop,
        )
        should_call_model, trigger_reason = gate_decision(
            state,
            config=gate_config_value,
            hand_model_active=self._hand_model_active,
        )
        if not should_call_model:
            return self._heuristic_with_metadata(
                state,
                decision_source="heuristic",
                fallback_reason="gate_not_triggered",
            )
        if self.max_model_calls_per_hand is not None and self._hand_model_calls >= self.max_model_calls_per_hand:
            return self._heuristic_with_metadata(
                state,
                decision_source="heuristic",
                fallback_reason="per_hand_budget_exhausted",
                trigger_reason=trigger_reason,
            )
        if self.runtime is None or not self.runtime.can_call():
            return self._heuristic_with_metadata(
                state,
                decision_source="heuristic",
                fallback_reason="run_budget_exhausted",
                trigger_reason=trigger_reason,
            )

        decision = self.runtime.decide_action(
            state=state,
            compiled_persona=self.compiled_persona,
            trigger_reason=trigger_reason or "gated_model_decision",
            model_name=self.model_name or DEFAULT_MODEL,
            reasoning_effort=self.reasoning_effort,
            context_text=state.persona_text,
        )
        if decision is None:
            return self._heuristic_with_metadata(
                state,
                decision_source="heuristic_fallback",
                fallback_reason="llm_unavailable_or_invalid",
                trigger_reason=trigger_reason,
            )

        self._hand_model_active = True
        self._hand_model_calls += 1
        metadata = dict(decision.metadata)
        metadata.setdefault("decision_source", "llm")
        metadata.setdefault("trigger_reason", trigger_reason or "gated_model_decision")
        return ActionDecision(
            action_type=decision.action_type,
            amount_bb=decision.amount_bb,
            note=decision.note,
            metadata=metadata,
        )

    def _reset_for_hand(self, hand_id: str) -> None:
        if hand_id == self._hand_id:
            return
        self._hand_id = hand_id
        self._hand_model_active = False
        self._hand_model_calls = 0

    def _heuristic_with_metadata(
        self,
        state: DecisionState,
        *,
        decision_source: str,
        fallback_reason: str,
        trigger_reason: str | None = None,
    ) -> ActionDecision:
        decision = HeuristicPersonaAgent.select_action(self, state)
        metadata = {
            "decision_source": decision_source,
            "fallback_reason": fallback_reason,
        }
        if trigger_reason:
            metadata["trigger_reason"] = trigger_reason
        return ActionDecision(
            action_type=decision.action_type,
            amount_bb=decision.amount_bb,
            note=decision.note,
            metadata=metadata,
        )


@dataclass(slots=True)
class ClusterCloneAgent(HeuristicPersonaAgent):
    centroid: dict[str, float] | None = None

    def __post_init__(self) -> None:
        if self.centroid:
            self.compiled_persona = self._from_centroid(self.centroid)
            self.backend_type = "cluster_clone"
        else:
            self.backend_type = "cluster_clone_placeholder"

    def _from_centroid(self, centroid: dict[str, float]) -> CompiledPersona:
        def pick(*names: str, default: float = 0.5) -> float:
            for name in names:
                if name in centroid:
                    return float(centroid[name])
            return default

        params = PersonaParameters(
            preflop_open_bias=pick("vpip", "vpip_rate", "preflop_open_bias", default=0.34),
            cold_call_bias=pick("cold_call_bias", "vpip", "vpip_rate", default=0.25),
            fold_threshold=pick("fold_threshold", default=0.5),
            raise_bias=pick("pfr", "pfr_rate", "raise_bias", default=0.35),
            cbet_bias=pick("cbet_bias", "cbet_rate", default=0.33),
            bluff_turn_bias=pick("bluff_turn_bias", default=0.22),
            bluff_river_bias=pick("bluff_river_bias", default=0.18),
            jam_bias=pick("jam_bias", default=0.18),
            trap_bias=pick("trap_bias", default=0.20),
            showdown_tendency=pick("showdown_tendency", "river_seen_rate", "showdown_rate", default=0.35),
        ).clamp()
        label = centroid.get("label") or centroid.get("cluster_name") or "cluster_clone"
        return CompiledPersona(
            name=str(label),
            persona_text="cluster-derived style clone",
            parameters=params,
            source="cluster_clone",
            preset_name=None,
        )


def build_agent(
    backend_type: str,
    persona_text: str | None = None,
    *,
    agent_id: str,
    compiled_persona: CompiledPersona | None = None,
    centroid: dict[str, float] | None = None,
    runtime: LlmDecisionRuntime | None = None,
    model_name: str | None = None,
    reasoning_effort: str | None = None,
    gating_profile: str | None = None,
    always_model_postflop: bool = True,
    max_model_calls_per_hand: int | None = None,
) -> PokerAgent:
    compiled = compiled_persona or compile_persona(persona_text or "TAG")
    backend = backend_type.lower()
    if backend == "cluster_clone":
        return ClusterCloneAgent(compiled_persona=compiled, agent_id=agent_id, centroid=centroid)
    if backend == "llm_gated_nano":
        return GatedLlmAgent(
            compiled_persona=compiled,
            agent_id=agent_id,
            runtime=runtime,
            model_name=model_name or DEFAULT_MODEL,
            reasoning_effort=reasoning_effort,
            gating_profile=gating_profile,
            always_model_postflop=always_model_postflop,
            max_model_calls_per_hand=max_model_calls_per_hand,
        )
    return HeuristicPersonaAgent(compiled_persona=compiled, agent_id=agent_id)


def preset_agent(preset_name: str, *, agent_id: str) -> HeuristicPersonaAgent:
    return HeuristicPersonaAgent(compiled_persona=preset_persona(preset_name), agent_id=agent_id)
