from __future__ import annotations

from dataclasses import dataclass

from .cards import preflop_strength
from .types import DecisionState


RANK_ORDER = "23456789TJQKA"
LATE_POSITIONS = {"CO", "BTN"}
BLIND_POSITIONS = {"SB", "BB"}


@dataclass(frozen=True, slots=True)
class GatingConfig:
    strength_threshold_late: float
    strength_threshold_early: float
    blind_defense_threshold: float
    short_stack_threshold: float
    postflop_pot_threshold_bb: float
    postflop_to_call_threshold_bb: float
    river_to_call_threshold_bb: float
    always_model_postflop: bool = False


GATING_PROFILES: dict[str, GatingConfig] = {
    "conservative": GatingConfig(
        strength_threshold_late=0.58,
        strength_threshold_early=0.66,
        blind_defense_threshold=0.53,
        short_stack_threshold=0.45,
        postflop_pot_threshold_bb=20.0,
        postflop_to_call_threshold_bb=6.0,
        river_to_call_threshold_bb=3.0,
    ),
    "balanced": GatingConfig(
        strength_threshold_late=0.54,
        strength_threshold_early=0.62,
        blind_defense_threshold=0.50,
        short_stack_threshold=0.42,
        postflop_pot_threshold_bb=16.0,
        postflop_to_call_threshold_bb=5.0,
        river_to_call_threshold_bb=2.5,
    ),
    "aggressive": GatingConfig(
        strength_threshold_late=0.50,
        strength_threshold_early=0.58,
        blind_defense_threshold=0.46,
        short_stack_threshold=0.38,
        postflop_pot_threshold_bb=12.0,
        postflop_to_call_threshold_bb=4.0,
        river_to_call_threshold_bb=2.0,
    ),
}


def gating_config(profile: str | None, *, always_model_postflop: bool = False) -> GatingConfig:
    key = (profile or "conservative").strip().lower()
    base = GATING_PROFILES.get(key, GATING_PROFILES["conservative"])
    return GatingConfig(
        strength_threshold_late=base.strength_threshold_late,
        strength_threshold_early=base.strength_threshold_early,
        blind_defense_threshold=base.blind_defense_threshold,
        short_stack_threshold=base.short_stack_threshold,
        postflop_pot_threshold_bb=base.postflop_pot_threshold_bb,
        postflop_to_call_threshold_bb=base.postflop_to_call_threshold_bb,
        river_to_call_threshold_bb=base.river_to_call_threshold_bb,
        always_model_postflop=always_model_postflop,
    )


def gate_decision(
    state: DecisionState,
    *,
    config: GatingConfig,
    hand_model_active: bool,
) -> tuple[bool, str | None]:
    if hand_model_active:
        return True, "hand_already_model_controlled"

    if state.street == "preflop":
        return _gate_preflop(state, config)
    return _gate_postflop(state, config)


def _gate_preflop(state: DecisionState, config: GatingConfig) -> tuple[bool, str | None]:
    hole_cards = tuple(card.upper() for card in state.hole_cards if isinstance(card, str))
    if len(hole_cards) != 2:
        return False, None

    if _is_pocket_pair(hole_cards):
        return True, "pocket_pair"
    if _is_broadway_combo(hole_cards):
        return True, "broadway"
    if _is_suited_connector(hole_cards, max_gap=1):
        return True, "suited_connector"
    if _is_suited_ace(hole_cards):
        return True, "suited_ace"

    strength = preflop_strength(hole_cards)
    if state.position in LATE_POSITIONS and strength >= config.strength_threshold_late:
        return True, "late_position_strength"
    if state.position in BLIND_POSITIONS and state.to_call_bb > 0 and strength >= config.blind_defense_threshold:
        return True, "blind_defense_candidate"
    if state.stack_bb <= 20.0 and strength >= config.short_stack_threshold:
        return True, "short_stack_decision"
    if strength >= config.strength_threshold_early:
        return True, "early_position_strength"
    if state.to_call_bb >= max(6.0, state.pot_bb * 0.40):
        return True, "expensive_preflop_decision"
    return False, None


def _gate_postflop(state: DecisionState, config: GatingConfig) -> tuple[bool, str | None]:
    if config.always_model_postflop:
        return True, "always_model_postflop"
    if state.street == "river" and state.to_call_bb >= config.river_to_call_threshold_bb:
        return True, "river_decision"
    if state.to_call_bb >= config.postflop_to_call_threshold_bb:
        return True, "large_postflop_call"
    if state.pot_bb >= config.postflop_pot_threshold_bb:
        return True, "large_postflop_pot"
    if state.to_call_bb > 0 and state.stack_bb <= state.to_call_bb * 1.35:
        return True, "facing_all_in_equivalent"
    return False, None


def _is_pocket_pair(hole_cards: tuple[str, str]) -> bool:
    return _rank(hole_cards[0]) == _rank(hole_cards[1])


def _is_broadway_combo(hole_cards: tuple[str, str]) -> bool:
    return _rank_value(_rank(hole_cards[0])) >= _rank_value("T") and _rank_value(_rank(hole_cards[1])) >= _rank_value("T")


def _is_suited_connector(hole_cards: tuple[str, str], *, max_gap: int) -> bool:
    if not _is_suited(hole_cards):
        return False
    first = _rank_value(_rank(hole_cards[0]))
    second = _rank_value(_rank(hole_cards[1]))
    gap = abs(first - second)
    return 1 <= gap <= max_gap + 1 and min(first, second) >= _rank_value("5")


def _is_suited_ace(hole_cards: tuple[str, str]) -> bool:
    return _is_suited(hole_cards) and "A" in {_rank(hole_cards[0]), _rank(hole_cards[1])}


def _is_suited(hole_cards: tuple[str, str]) -> bool:
    return len(hole_cards[0]) >= 2 and len(hole_cards[1]) >= 2 and hole_cards[0][1] == hole_cards[1][1]


def _rank(card: str) -> str:
    return card[0]


def _rank_value(rank: str) -> int:
    return RANK_ORDER.index(rank)
