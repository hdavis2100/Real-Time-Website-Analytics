from __future__ import annotations

"""Canonical event schemas shared across historical ingest and simulation."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


PAYLOAD_VERSION = "1"


class SourceType(str, Enum):
    HISTORICAL = "historical"
    SIMULATED = "simulated"


class Street(str, Enum):
    PREFLOP = "preflop"
    FLOP = "flop"
    TURN = "turn"
    RIVER = "river"
    SHOWDOWN = "showdown"


class ActionType(str, Enum):
    POST_ANTE = "post_ante"
    POST_SMALL_BLIND = "post_small_blind"
    POST_BIG_BLIND = "post_big_blind"
    FOLD = "fold"
    CHECK = "check"
    CALL = "call"
    BET = "bet"
    RAISE = "raise"
    ALL_IN = "all_in"
    COLLECT_POT = "collect_pot"
    SHOW = "show"
    MUCK = "muck"


class ActionEvent(BaseModel):
    payload_version: str = PAYLOAD_VERSION
    event_type: str = "action"
    source_type: SourceType
    source_dataset: str | None = None
    source_run_id: str | None = None
    simulation_run_id: str | None = None
    user_id: str | None = None
    decision_backend: str | None = None
    hero_context_hash: str | None = None
    hero_seat: int | None = Field(default=None, ge=1, le=6)
    is_hero_player: bool = False
    table_id: str
    hand_id: str
    action_index: int = Field(ge=0)
    street: Street
    player_id: str
    agent_id: str | None = None
    seat: int = Field(ge=1, le=6)
    position: str
    action_type: ActionType
    amount_bb: float | None = None
    pot_before_bb: float = Field(ge=0)
    pot_after_bb: float = Field(ge=0)
    to_call_bb: float = Field(ge=0)
    effective_stack_bb: float = Field(ge=0)
    players_remaining: int = Field(ge=1, le=6)
    board_cards_visible: list[str] = Field(default_factory=list)
    hole_cards_visible: list[str] = Field(default_factory=list)
    is_all_in: bool = False
    event_ts: datetime
    backend_type: str = "heuristic_persona"
    persona_name: str | None = None
    persona_text: str | None = None
    raw_lineage: dict[str, Any] = Field(default_factory=dict)


class HandSummaryEvent(BaseModel):
    payload_version: str = PAYLOAD_VERSION
    event_type: str = "hand_summary"
    source_type: SourceType
    source_dataset: str | None = None
    source_run_id: str | None = None
    simulation_run_id: str | None = None
    user_id: str | None = None
    decision_backend: str | None = None
    hero_context_hash: str | None = None
    hero_seat: int | None = Field(default=None, ge=1, le=6)
    table_id: str
    hand_id: str
    button_seat: int = Field(ge=1, le=6)
    small_blind_bb: float | None = Field(default=None, gt=0)
    big_blind_bb: float | None = Field(default=None, gt=0)
    board_cards: list[str] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime
    winner_player_ids: list[str] = Field(default_factory=list)
    total_pot_bb: float = Field(ge=0)
    rake_bb: float = Field(ge=0, default=0.0)
    starting_stacks_bb: dict[str, float] = Field(default_factory=dict)
    final_stacks_bb: dict[str, float] = Field(default_factory=dict)
    collections_bb: dict[str, float] = Field(default_factory=dict)
    player_personas: dict[str, str] = Field(default_factory=dict)
    agent_ids: dict[str, str] = Field(default_factory=dict)
    player_agent_ids: dict[str, str] = Field(default_factory=dict)
    seats: dict[str, int] = Field(default_factory=dict)
    player_seats: dict[str, int] = Field(default_factory=dict)
    showdown_player_ids: list[str] = Field(default_factory=list)
    player_hole_cards: dict[str, list[str]] = Field(default_factory=dict)
    backend_types: dict[str, str] = Field(default_factory=dict)
    backend_type: str = "heuristic_persona"
    raw_lineage: dict[str, Any] = Field(default_factory=dict)


class PersonaConfig(BaseModel):
    seat: int = Field(ge=1, le=6)
    player_id: str
    agent_id: str | None = None
    persona_name: str
    persona_text: str
    backend_type: str = "heuristic_persona"
    centroid: dict[str, float] | None = None


class SimulationRequest(BaseModel):
    payload_version: str = PAYLOAD_VERSION
    simulation_run_id: str
    table_id: str = "table_1"
    hand_count: int = Field(gt=0, le=5000)
    seed: int = 42
    small_blind_bb: float = Field(gt=0)
    big_blind_bb: float = Field(gt=0)
    starting_stack_bb: float = Field(gt=0)
    context: str
    hero_seat: int = Field(default=1, ge=1, le=6)
    user_id: str | None = None
    decision_backend: str = "llm"
    hero_context_hash: str | None = None
