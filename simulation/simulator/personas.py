from __future__ import annotations

import re
from dataclasses import replace

from .types import CompiledPersona, PersonaParameters


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return slug or "persona"


def _params(
    preflop_open_bias: float,
    cold_call_bias: float,
    fold_threshold: float,
    raise_bias: float,
    cbet_bias: float,
    bluff_turn_bias: float,
    bluff_river_bias: float,
    jam_bias: float,
    trap_bias: float,
    showdown_tendency: float,
) -> PersonaParameters:
    return PersonaParameters(
        preflop_open_bias=preflop_open_bias,
        cold_call_bias=cold_call_bias,
        fold_threshold=fold_threshold,
        raise_bias=raise_bias,
        cbet_bias=cbet_bias,
        bluff_turn_bias=bluff_turn_bias,
        bluff_river_bias=bluff_river_bias,
        jam_bias=jam_bias,
        trap_bias=trap_bias,
        showdown_tendency=showdown_tendency,
    ).clamp()


BUILTIN_PERSONAS: dict[str, CompiledPersona] = {
    "nit": CompiledPersona(
        name="nit",
        persona_text="tight preflop, low bluff frequency, avoids marginal spots",
        parameters=_params(0.18, 0.12, 0.72, 0.16, 0.18, 0.06, 0.05, 0.08, 0.32, 0.28),
        source="preset",
        preset_name="nit",
    ),
    "tag": CompiledPersona(
        name="tag",
        persona_text="tight aggressive, disciplined value-heavy style",
        parameters=_params(0.36, 0.18, 0.58, 0.42, 0.40, 0.20, 0.14, 0.24, 0.34, 0.40),
        source="preset",
        preset_name="TAG",
    ),
    "lag": CompiledPersona(
        name="lag",
        persona_text="loose aggressive, attacks weakness and applies pressure",
        parameters=_params(0.62, 0.30, 0.34, 0.72, 0.58, 0.44, 0.36, 0.42, 0.18, 0.30),
        source="preset",
        preset_name="LAG",
    ),
    "calling_station": CompiledPersona(
        name="calling_station",
        persona_text="calls too much, hates folding, low bluff frequency",
        parameters=_params(0.20, 0.78, 0.82, 0.10, 0.12, 0.04, 0.03, 0.02, 0.16, 0.66),
        source="preset",
        preset_name="calling_station",
    ),
    "maniac": CompiledPersona(
        name="maniac",
        persona_text="hyper aggressive, bluffs often, takes many thin raises",
        parameters=_params(0.78, 0.42, 0.22, 0.88, 0.74, 0.64, 0.58, 0.56, 0.08, 0.20),
        source="preset",
        preset_name="maniac",
    ),
    "shove_bot": CompiledPersona(
        name="shove_bot",
        persona_text="short-stack jam-heavy, overuses all-in pressure",
        parameters=_params(0.56, 0.20, 0.28, 0.64, 0.30, 0.16, 0.12, 0.96, 0.10, 0.18),
        source="preset",
        preset_name="shove_bot",
    ),
}

_NAME_ALIASES = {
    "tight aggressive": "tag",
    "tight-aggressive": "tag",
    "loose aggressive": "lag",
    "loose-aggressive": "lag",
    "call station": "calling_station",
    "calling station": "calling_station",
    "shovebot": "shove_bot",
}


def _merge(base: PersonaParameters, **changes: float) -> PersonaParameters:
    return replace(base, **changes).clamp()


def compile_persona(persona_text: str) -> CompiledPersona:
    text = (persona_text or "").strip()
    normalized = text.lower()
    if not normalized:
        preset = BUILTIN_PERSONAS["tag"]
        return CompiledPersona(
            name=preset.name,
            persona_text=preset.persona_text,
            parameters=preset.parameters,
            source="preset",
            preset_name=preset.preset_name,
        )

    preset_key = normalized if normalized in BUILTIN_PERSONAS else _NAME_ALIASES.get(normalized)
    if preset_key and preset_key in BUILTIN_PERSONAS:
        preset = BUILTIN_PERSONAS[preset_key]
        return CompiledPersona(
            name=preset.name,
            persona_text=text,
            parameters=preset.parameters,
            source="preset",
            preset_name=preset.preset_name,
        )

    base = _params(0.34, 0.24, 0.50, 0.34, 0.34, 0.18, 0.12, 0.18, 0.22, 0.34)
    scores = {
        "tight": normalized.count("tight") + normalized.count("nit") * 2,
        "loose": normalized.count("loose"),
        "aggressive": normalized.count("aggressive") + normalized.count("attack"),
        "passive": normalized.count("passive") + normalized.count("cautious"),
        "bluff": normalized.count("bluff"),
        "call": normalized.count("call"),
        "jam": normalized.count("jam") + normalized.count("shove") + normalized.count("all-in"),
        "trap": normalized.count("trap") + normalized.count("slowplay"),
        "fold": normalized.count("fold"),
        "value": normalized.count("value"),
    }

    if scores["tight"]:
        base = _merge(
            base,
            preflop_open_bias=max(0.08, base.preflop_open_bias - 0.12 * scores["tight"]),
            cold_call_bias=max(0.05, base.cold_call_bias - 0.08 * scores["tight"]),
            fold_threshold=min(0.92, base.fold_threshold + 0.12 * scores["tight"]),
            cbet_bias=max(0.08, base.cbet_bias - 0.05 * scores["tight"]),
        )
    if scores["loose"]:
        base = _merge(
            base,
            preflop_open_bias=min(0.92, base.preflop_open_bias + 0.12 * scores["loose"]),
            cold_call_bias=min(0.92, base.cold_call_bias + 0.08 * scores["loose"]),
            fold_threshold=max(0.05, base.fold_threshold - 0.10 * scores["loose"]),
        )
    if scores["aggressive"]:
        base = _merge(
            base,
            raise_bias=min(0.96, base.raise_bias + 0.14 * scores["aggressive"]),
            cbet_bias=min(0.96, base.cbet_bias + 0.12 * scores["aggressive"]),
            bluff_turn_bias=min(0.96, base.bluff_turn_bias + 0.12 * scores["aggressive"]),
            bluff_river_bias=min(0.96, base.bluff_river_bias + 0.12 * scores["aggressive"]),
        )
    if scores["passive"]:
        base = _merge(
            base,
            raise_bias=max(0.04, base.raise_bias - 0.14 * scores["passive"]),
            cbet_bias=max(0.04, base.cbet_bias - 0.08 * scores["passive"]),
            cold_call_bias=min(0.96, base.cold_call_bias + 0.10 * scores["passive"]),
        )
    if scores["bluff"]:
        base = _merge(
            base,
            bluff_turn_bias=min(0.98, base.bluff_turn_bias + 0.10 * scores["bluff"]),
            bluff_river_bias=min(0.98, base.bluff_river_bias + 0.12 * scores["bluff"]),
        )
    if scores["call"]:
        base = _merge(
            base,
            cold_call_bias=min(0.98, base.cold_call_bias + 0.12 * scores["call"]),
            showdown_tendency=min(0.98, base.showdown_tendency + 0.10 * scores["call"]),
            fold_threshold=min(0.98, base.fold_threshold + 0.08 * scores["call"]),
        )
    if "calls too much" in normalized or "hates folding" in normalized or "sticky" in normalized:
        base = _merge(
            base,
            cold_call_bias=min(0.98, base.cold_call_bias + 0.28),
            fold_threshold=min(0.98, base.fold_threshold + 0.18),
            showdown_tendency=min(0.98, base.showdown_tendency + 0.18),
        )
    if scores["jam"]:
        base = _merge(
            base,
            jam_bias=min(0.98, base.jam_bias + 0.18 * scores["jam"]),
            raise_bias=min(0.98, base.raise_bias + 0.08 * scores["jam"]),
            fold_threshold=max(0.02, base.fold_threshold - 0.10 * scores["jam"]),
        )
    if scores["trap"]:
        base = _merge(
            base,
            trap_bias=min(0.98, base.trap_bias + 0.16 * scores["trap"]),
            cbet_bias=max(0.02, base.cbet_bias - 0.08 * scores["trap"]),
        )
    if scores["fold"]:
        base = _merge(base, fold_threshold=min(0.98, base.fold_threshold + 0.08 * scores["fold"]))
    if scores["value"]:
        base = _merge(
            base,
            raise_bias=min(0.98, base.raise_bias + 0.06 * scores["value"]),
            trap_bias=min(0.98, base.trap_bias + 0.05 * scores["value"]),
        )

    persona_name = _slugify(text)
    if any(alias in normalized for alias in ("nit", "tight")) and not scores["loose"]:
        persona_name = "nit_like_" + persona_name
    elif scores["aggressive"] and scores["loose"]:
        persona_name = "lag_like_" + persona_name
    elif scores["call"] and scores["passive"]:
        persona_name = "station_like_" + persona_name

    return CompiledPersona(
        name=persona_name,
        persona_text=text,
        parameters=base,
        source="compiled",
        preset_name=None,
    )


def preset_persona(name: str) -> CompiledPersona:
    key = name.strip().lower()
    if key not in BUILTIN_PERSONAS:
        raise KeyError(f"Unknown preset persona: {name}")
    preset = BUILTIN_PERSONAS[key]
    return CompiledPersona(
        name=preset.name,
        persona_text=preset.persona_text,
        parameters=preset.parameters,
        source="preset",
        preset_name=preset.preset_name,
    )
