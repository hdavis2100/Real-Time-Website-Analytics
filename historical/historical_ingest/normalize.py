from __future__ import annotations

"""Shared HandHQ seat-position helpers."""

POSITION_LABELS_6MAX = ["BTN", "SB", "BB", "UTG", "HJ", "CO"]


def position_for_seat(seat: int | None, button_seat: int = 1) -> str:
    if seat is None:
        return "UNKNOWN"
    try:
        normalized_seat = int(seat)
    except (TypeError, ValueError):
        return "UNKNOWN"
    if not 1 <= normalized_seat <= 6:
        return "UNKNOWN"
    button = button_seat if 1 <= button_seat <= 6 else 1
    order = [((button - 1 + offset) % 6) + 1 for offset in range(6)]
    return POSITION_LABELS_6MAX[order.index(normalized_seat)]
