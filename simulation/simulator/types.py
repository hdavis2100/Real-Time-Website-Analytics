from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Mapping

from poker_platform.event_contracts import serialize_action_event, serialize_hand_summary_event


@dataclass(frozen=True, slots=True)
class PersonaParameters:
    preflop_open_bias: float
    cold_call_bias: float
    fold_threshold: float
    raise_bias: float
    cbet_bias: float
    bluff_turn_bias: float
    bluff_river_bias: float
    jam_bias: float
    trap_bias: float
    showdown_tendency: float

    def clamp(self) -> "PersonaParameters":
        values = {}
        for key, value in asdict(self).items():
            values[key] = max(0.0, min(1.0, float(value)))
        return PersonaParameters(**values)


@dataclass(frozen=True, slots=True)
class CompiledPersona:
    name: str
    persona_text: str
    parameters: PersonaParameters
    source: str = "compiled"
    preset_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["parameters"] = asdict(self.parameters)
        return data


@dataclass(frozen=True, slots=True)
class ActionDecision:
    action_type: str
    amount_bb: float | None = None
    note: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["metadata"] = dict(self.metadata)
        return data


@dataclass(frozen=True, slots=True)
class DecisionState:
    hand_id: str
    simulation_run_id: str
    table_id: str
    street: str
    seat_index: int
    player_id: str
    agent_id: str
    position: str
    stack_bb: float
    committed_bb: float
    pot_bb: float
    to_call_bb: float
    min_raise_to_bb: float
    current_bet_bb: float
    players_remaining: int
    active_players: tuple[str, ...]
    board_cards: tuple[str, ...]
    hole_cards: tuple[str, ...]
    legal_actions: tuple[str, ...]
    street_action_count: int
    action_index: int
    button_seat: int
    big_blind_bb: float
    small_blind_bb: float
    backend_type: str
    source_type: str
    source_dataset: str
    payload_version: str
    persona_name: str
    persona_text: str
    seed: int
    hand_action_history: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    last_aggressor_seat: int | None = None
    hand_model_active: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ActionEvent:
    hand_id: str
    simulation_run_id: str
    table_id: str
    source_type: str
    source_dataset: str
    source_run_id: str
    street: str
    action_index: int
    action_order: int
    player_id: str
    agent_id: str
    seat: int
    position: str
    action_type: str
    amount_bb: float
    pot_before_bb: float
    pot_after_bb: float
    to_call_bb: float
    effective_stack_bb: float
    players_remaining: int
    board_cards_visible: tuple[str, ...]
    hole_cards_visible: tuple[str, ...]
    is_all_in: bool
    timestamp_ms: int
    backend_type: str
    persona_name: str
    persona_text: str
    payload_version: str
    user_id: str | None = None
    decision_backend: str | None = None
    hero_context_hash: str | None = None
    hero_seat: int | None = None
    is_hero_player: bool = False
    raw_lineage: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return serialize_action_event(asdict(self))


@dataclass(frozen=True, slots=True)
class HandSummary:
    hand_id: str
    simulation_run_id: str
    table_id: str
    source_type: str
    source_dataset: str
    source_run_id: str
    button_seat: int
    board_cards: tuple[str, ...]
    started_at: datetime
    finished_at: datetime
    pot_bb: float
    small_blind_bb: float
    big_blind_bb: float
    action_count: int
    starting_stacks_bb: Mapping[str, float]
    collections_bb: Mapping[str, float]
    street_action_counts: Mapping[str, int]
    showdown: bool
    winners: tuple[str, ...]
    winning_seat_indices: tuple[int, ...]
    final_stacks_bb: Mapping[str, float]
    player_personas: Mapping[str, str]
    player_agent_ids: Mapping[str, str]
    player_seats: Mapping[str, int]
    backend_types: Mapping[str, str]
    payload_version: str
    runtime_seed: int
    user_id: str | None = None
    decision_backend: str | None = None
    hero_context_hash: str | None = None
    hero_seat: int | None = None
    showdown_player_ids: tuple[str, ...] = field(default_factory=tuple)
    player_hole_cards: Mapping[str, tuple[str, str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["board_cards"] = list(self.board_cards)
        data["winners"] = list(self.winners)
        data["winning_seat_indices"] = list(self.winning_seat_indices)
        data["starting_stacks_bb"] = dict(self.starting_stacks_bb)
        data["collections_bb"] = dict(self.collections_bb)
        data["final_stacks_bb"] = dict(self.final_stacks_bb)
        data["player_personas"] = dict(self.player_personas)
        data["player_agent_ids"] = dict(self.player_agent_ids)
        data["player_seats"] = dict(self.player_seats)
        data["showdown_player_ids"] = list(self.showdown_player_ids)
        data["player_hole_cards"] = {player_id: list(cards) for player_id, cards in self.player_hole_cards.items()}
        data["backend_types"] = dict(self.backend_types)
        data["street_action_counts"] = dict(self.street_action_counts)
        return serialize_hand_summary_event(data)


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    num_hands: int = 1
    starting_stack_bb: float = 100.0
    small_blind_bb: float = 0.5
    big_blind_bb: float = 1.0
    payload_version: str = "v1"
    source_type: str = "simulated"
    source_dataset: str = "simulator"
    table_id: str = "table_1"
    simulation_run_id: str = "sim_run"
    user_id: str | None = None
    decision_backend: str | None = None
    hero_context_hash: str | None = None
    hero_seat: int | None = None
    run_started_at: datetime | None = None
    carry_stacks_between_hands: bool = True
    rebuy_below_bb: float = 1.0
    max_model_calls_per_run: int = 0
    llm_timeout_seconds: float = 15.0


@dataclass(frozen=True, slots=True)
class SimulatedPlayer:
    seat_index: int
    player_id: str
    agent_id: str
    persona_name: str
    persona_text: str
    backend_type: str
    stack_bb: float
    compiled_persona: CompiledPersona | None = None
    centroid: Mapping[str, float] | None = None
    model_name: str | None = None
    reasoning_effort: str | None = None
    gating_profile: str | None = None
    always_model_postflop: bool = False
    max_model_calls_per_hand: int | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if self.compiled_persona is not None:
            data["compiled_persona"] = self.compiled_persona.to_dict()
        if self.centroid is not None:
            data["centroid"] = dict(self.centroid)
        return data
