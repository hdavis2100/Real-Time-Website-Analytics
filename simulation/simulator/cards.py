from __future__ import annotations

from collections import Counter
from itertools import combinations

RANKS = "23456789TJQKA"
SUITS = "cdhs"
RANK_TO_VALUE = {rank: index + 2 for index, rank in enumerate(RANKS)}
VALUE_TO_RANK = {value: rank for rank, value in RANK_TO_VALUE.items()}


def build_deck() -> list[str]:
    return [rank + suit for rank in RANKS for suit in SUITS]


def card_value(card: str) -> int:
    return RANK_TO_VALUE[card[0]]


def card_suit(card: str) -> str:
    return card[1]


def preflop_strength(hole_cards: list[str] | tuple[str, str]) -> float:
    a, b = sorted((card_value(hole_cards[0]), card_value(hole_cards[1])), reverse=True)
    suited = card_suit(hole_cards[0]) == card_suit(hole_cards[1])
    pair = a == b
    gap = abs(card_value(hole_cards[0]) - card_value(hole_cards[1]))
    high = (a - 2) / 12.0
    low = (b - 2) / 12.0
    score = 0.35 * high + 0.25 * low
    if pair:
        score += 0.35 + (a - 2) / 20.0
    if suited:
        score += 0.06
    if gap == 1:
        score += 0.05
    elif gap == 0:
        score += 0.12
    elif gap == 2:
        score += 0.02
    if a >= 11:
        score += 0.04
    if b >= 10:
        score += 0.02
    return max(0.0, min(1.0, score))


def _straight_high(values: list[int]) -> int | None:
    unique = sorted(set(values), reverse=True)
    if 14 in unique:
        unique.append(1)
    streak = 1
    best = None
    for i in range(1, len(unique)):
        if unique[i - 1] - 1 == unique[i]:
            streak += 1
            if streak >= 5:
                best = unique[i - 4]
        else:
            streak = 1
    return best


def evaluate_five(cards: list[str] | tuple[str, ...]) -> tuple[int, tuple[int, ...]]:
    values = sorted((card_value(card) for card in cards), reverse=True)
    counts = Counter(values)
    by_count = sorted(counts.items(), key=lambda item: (-item[1], -item[0]))
    suits = Counter(card_suit(card) for card in cards)
    flush_suit = next((suit for suit, count in suits.items() if count == 5), None)
    is_flush = flush_suit is not None
    straight_high = _straight_high(values)

    if is_flush and straight_high is not None:
        return 8, (straight_high,)
    if by_count[0][1] == 4:
        quad = by_count[0][0]
        kicker = max(value for value in values if value != quad)
        return 7, (quad, kicker)
    if by_count[0][1] == 3 and by_count[1][1] >= 2:
        return 6, (by_count[0][0], by_count[1][0])
    if is_flush:
        return 5, tuple(values)
    if straight_high is not None:
        return 4, (straight_high,)
    if by_count[0][1] == 3:
        trips = by_count[0][0]
        kickers = sorted((value for value in values if value != trips), reverse=True)[:2]
        return 3, (trips, *kickers)
    if by_count[0][1] == 2 and by_count[1][1] == 2:
        pair_high, pair_low = sorted((by_count[0][0], by_count[1][0]), reverse=True)
        kicker = max(value for value in values if value not in (pair_high, pair_low))
        return 2, (pair_high, pair_low, kicker)
    if by_count[0][1] == 2:
        pair = by_count[0][0]
        kickers = sorted((value for value in values if value != pair), reverse=True)[:3]
        return 1, (pair, *kickers)
    return 0, tuple(values)


def best_five_of_seven(cards: list[str] | tuple[str, ...]) -> tuple[int, tuple[int, ...]]:
    best: tuple[int, tuple[int, ...]] | None = None
    for combo in combinations(cards, 5):
        score = evaluate_five(combo)
        if best is None or score > best:
            best = score
    assert best is not None
    return best


def compare_hands(a_cards: list[str] | tuple[str, ...], b_cards: list[str] | tuple[str, ...]) -> int:
    a_rank = best_five_of_seven(a_cards)
    b_rank = best_five_of_seven(b_cards)
    if a_rank > b_rank:
        return 1
    if b_rank > a_rank:
        return -1
    return 0

