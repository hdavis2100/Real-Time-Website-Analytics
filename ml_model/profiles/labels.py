from __future__ import annotations

from typing import Any


def _feature(centroid: dict[str, Any], name: str, *, default: float = 0.0) -> float:
    if name in centroid:
        return float(centroid[name])
    if name == "river_seen_rate" and "showdown_rate" in centroid:
        return float(centroid["showdown_rate"])
    return default


def label_centroid(centroid: dict[str, Any]) -> str:
    vpip = _feature(centroid, "vpip_rate")
    pfr = _feature(centroid, "pfr_rate")
    aggression = _feature(centroid, "aggression_frequency")
    calls = _feature(centroid, "call_preference")
    cbet = _feature(centroid, "flop_cbet_rate")
    turn_barrel = _feature(centroid, "turn_barrel_rate")
    river_seen = _feature(centroid, "river_seen_rate")
    all_in = _feature(centroid, "all_in_rate")
    blind_defend = _feature(centroid, "blind_defend_rate")
    short_stack = _feature(centroid, "short_stack_aggression_rate")
    deep_stack = _feature(centroid, "deep_stack_looseness")
    avg_preflop_raise = _feature(centroid, "avg_preflop_raise_bb")

    if short_stack >= 0.68 or (short_stack >= 0.58 and all_in >= 0.04):
        return "Short-stack jammer"
    if vpip < 0.18 and pfr < 0.12 and aggression < 0.42:
        return "Nit"
    if blind_defend >= 0.46 and calls >= 0.5 and vpip >= 0.22:
        return "Blind defender"
    if vpip >= 0.34 and pfr >= 0.2 and aggression >= 0.6 and turn_barrel >= 0.44:
        return "LAG barreler"
    if deep_stack >= 0.42 and aggression >= 0.56 and cbet >= 0.54:
        return "Deep-stack pressure"
    if vpip < 0.24 and aggression >= 0.68 and calls <= 0.34:
        return "Tight pressure"
    if vpip < 0.26 and aggression >= 0.52 and turn_barrel >= 0.46:
        return "Turn-barrel regular"
    if pfr >= 0.17 and aggression >= 0.62 and calls <= 0.38:
        return "Aggressive regular"
    if vpip >= 0.34 and pfr < 0.09 and calls >= 0.62 and avg_preflop_raise >= 10.0:
        return "Polarized caller"
    if vpip >= 0.3 and pfr < 0.1 and calls >= 0.6 and aggression < 0.42:
        return "Loose caller"
    if vpip >= 0.37 and pfr >= 0.12 and blind_defend >= 0.35 and aggression < 0.58:
        return "Loose regular"
    if vpip < 0.3 and pfr >= 0.16 and aggression >= 0.58 and cbet >= 0.54:
        return "TAG pressure"
    if vpip >= 0.38 and calls >= 0.55 and aggression < 0.45:
        return "Calling station"
    if river_seen >= 0.4 and calls >= 0.5:
        return "River chaser"
    if 0.22 <= vpip <= 0.32 and calls >= 0.54 and aggression < 0.5:
        return "Passive regular"
    if 0.24 <= vpip <= 0.32 and pfr >= 0.14 and aggression >= 0.5:
        return "Adaptive regular"
    if vpip >= 0.32 and aggression >= 0.54:
        return "Splashy aggressor"
    return "Balanced regular"


def summarize_centroid(centroid: dict[str, Any]) -> str:
    label = label_centroid(centroid)
    traits: list[str] = []
    avg_preflop_raise = _feature(centroid, "avg_preflop_raise_bb")
    vpip = _feature(centroid, "vpip_rate")
    aggression = _feature(centroid, "aggression_frequency")
    calls = _feature(centroid, "call_preference")
    pfr = _feature(centroid, "pfr_rate")
    blind_defend = _feature(centroid, "blind_defend_rate")

    if vpip >= 0.35:
        traits.append("enters many pots")
    elif vpip <= 0.18:
        traits.append("plays selectively")

    if aggression >= 0.58:
        traits.append("applies pressure often")
    elif calls >= 0.55:
        traits.append("leans on calls over raises")

    if avg_preflop_raise >= 10.0:
        traits.append("uses unusually large preflop raises")

    if pfr < 0.1 and calls >= 0.6 and vpip >= 0.3:
        traits.append("prefers calling to opening preflop")
    elif pfr >= 0.17 and aggression >= 0.62:
        traits.append("wins pots with raises more than calls")

    if _feature(centroid, "flop_cbet_rate") >= 0.58:
        traits.append("follows through on the flop")
    if _feature(centroid, "turn_barrel_rate") >= 0.45:
        traits.append("keeps barreling turns")
    if _feature(centroid, "river_seen_rate") >= 0.4:
        traits.append("sticks around to later streets")
    if blind_defend >= 0.46:
        traits.append("defends the blinds actively")
    elif blind_defend >= 0.35 and vpip >= 0.35:
        traits.append("continues widely from the blinds")

    if not traits:
        traits.append("sits near the middle of the historical population")

    return f"{label}: " + "; ".join(traits[:3])
