from __future__ import annotations

import json
import time
from dataclasses import replace
from typing import Any, Mapping

from poker_platform.env import getenv_stripped

from .types import ActionDecision, CompiledPersona, DecisionState, PersonaParameters


ALLOWED_ACTIONS = {"fold", "check", "call", "bet", "raise", "all_in"}
DEFAULT_MODEL = "gpt-5.4-nano"
DEFAULT_REASONING_EFFORT = "none"
DEFAULT_TEXT_VERBOSITY = "low"
DEFAULT_BASE_URL = "https://api.openai.com/v1"
RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "action": {
            "type": "string",
            "enum": sorted(ALLOWED_ACTIONS),
        },
        "size_bb": {
            "anyOf": [
                {"type": "number"},
                {"type": "null"},
            ]
        },
        "confidence": {"type": "number"},
        "rationale_tag": {
            "type": "string",
            "enum": ["value", "bluff", "pressure", "price", "trap", "draw", "default"],
        },
    },
    "required": ["action", "size_bb", "confidence", "rationale_tag"],
}

HEURISTIC_COMPILER_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "persona_name": {"type": "string"},
        "heuristic_summary": {"type": "string"},
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "preflop_open_bias": {"type": "number"},
                "cold_call_bias": {"type": "number"},
                "fold_threshold": {"type": "number"},
                "raise_bias": {"type": "number"},
                "cbet_bias": {"type": "number"},
                "bluff_turn_bias": {"type": "number"},
                "bluff_river_bias": {"type": "number"},
                "jam_bias": {"type": "number"},
                "trap_bias": {"type": "number"},
                "showdown_tendency": {"type": "number"},
            },
            "required": [
                "preflop_open_bias",
                "cold_call_bias",
                "fold_threshold",
                "raise_bias",
                "cbet_bias",
                "bluff_turn_bias",
                "bluff_river_bias",
                "jam_bias",
                "trap_bias",
                "showdown_tendency",
            ],
        },
    },
    "required": ["persona_name", "heuristic_summary", "parameters"],
}


def _responses_client():
    try:
        from openai import OpenAI
    except ImportError:
        return None

    api_key = getenv_stripped("OPENAI_API_KEY")
    if not api_key:
        return None

    base_url = getenv_stripped("OPENAI_BASE_URL", DEFAULT_BASE_URL) or DEFAULT_BASE_URL
    return OpenAI(api_key=api_key, base_url=base_url)


def compile_heuristic_persona_for_run(
    context_text: str,
    *,
    model_name: str | None = None,
    timeout_seconds: float = 5.0,
    client: Any | None = None,
) -> CompiledPersona | None:
    normalized_context = str(context_text or "").strip()
    if not normalized_context:
        return None
    effective_client = client or _responses_client()
    if effective_client is None:
        return None
    try:
        response = effective_client.responses.create(
            model=model_name or DEFAULT_MODEL,
            instructions=_heuristic_compiler_system_prompt(),
            input=[
                {
                    "role": "user",
                    "content": _heuristic_compiler_user_payload(normalized_context),
                }
            ],
            text={
                "verbosity": DEFAULT_TEXT_VERBOSITY,
                "format": {
                    "type": "json_schema",
                    "name": "heuristic_persona",
                    "strict": True,
                    "schema": HEURISTIC_COMPILER_SCHEMA,
                },
            },
            reasoning=_reasoning_payload(DEFAULT_REASONING_EFFORT),
            max_output_tokens=220,
            timeout=max(1.0, float(timeout_seconds)),
        )
    except Exception:
        return None

    raw_text = getattr(response, "output_text", "") or _extract_output_text(response)
    if not raw_text:
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None

    try:
        parameters = PersonaParameters(
            preflop_open_bias=float(parsed["parameters"]["preflop_open_bias"]),
            cold_call_bias=float(parsed["parameters"]["cold_call_bias"]),
            fold_threshold=float(parsed["parameters"]["fold_threshold"]),
            raise_bias=float(parsed["parameters"]["raise_bias"]),
            cbet_bias=float(parsed["parameters"]["cbet_bias"]),
            bluff_turn_bias=float(parsed["parameters"]["bluff_turn_bias"]),
            bluff_river_bias=float(parsed["parameters"]["bluff_river_bias"]),
            jam_bias=float(parsed["parameters"]["jam_bias"]),
            trap_bias=float(parsed["parameters"]["trap_bias"]),
            showdown_tendency=float(parsed["parameters"]["showdown_tendency"]),
        ).clamp()
    except (KeyError, TypeError, ValueError):
        return None
    parameters = _apply_heuristic_context_guardrails(normalized_context, parameters)

    persona_name = _slugify_persona_name(parsed.get("persona_name"))
    heuristic_summary = str(parsed.get("heuristic_summary") or normalized_context).strip()
    return CompiledPersona(
        name=persona_name,
        persona_text=normalized_context,
        parameters=parameters,
        source="llm_compiled",
        preset_name=None,
    )


class LlmDecisionRuntime:
    def __init__(
        self,
        *,
        max_calls_per_run: int,
        timeout_seconds: float,
        client: Any | None = None,
    ) -> None:
        self.max_calls_per_run = max(0, int(max_calls_per_run))
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self._client = client
        self.calls_used = 0

    def can_call(self) -> bool:
        return self.max_calls_per_run <= 0 or self.calls_used < self.max_calls_per_run

    def decide_action(
        self,
        *,
        state: DecisionState,
        compiled_persona: CompiledPersona,
        trigger_reason: str,
        model_name: str | None,
        reasoning_effort: str | None,
        context_text: str,
    ) -> ActionDecision | None:
        if not self.can_call():
            return None
        client = self._client or _responses_client()
        if client is None:
            return None

        started = time.perf_counter()
        self.calls_used += 1
        try:
            effective_reasoning_effort = reasoning_effort or DEFAULT_REASONING_EFFORT
            response = client.responses.create(
                model=model_name or DEFAULT_MODEL,
                instructions=_system_prompt(),
                input=[
                    {
                        "role": "user",
                        "content": _user_payload(
                            state=state,
                            compiled_persona=compiled_persona,
                            trigger_reason=trigger_reason,
                            context_text=context_text,
                        ),
                    }
                ],
                text={
                    "verbosity": DEFAULT_TEXT_VERBOSITY,
                    "format": {
                        "type": "json_schema",
                        "name": "poker_action",
                        "strict": True,
                        "schema": RESPONSE_SCHEMA,
                    }
                },
                reasoning=_reasoning_payload(effective_reasoning_effort),
                max_output_tokens=80,
                timeout=self.timeout_seconds,
            )
        except Exception:
            return None

        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        raw_text = getattr(response, "output_text", "") or _extract_output_text(response)
        if not raw_text:
            return None
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            return None

        action = str(parsed.get("action", "")).strip().lower()
        if action not in ALLOWED_ACTIONS:
            return None
        size_bb = parsed.get("size_bb")
        if size_bb is not None:
            try:
                size_bb = float(size_bb)
            except (TypeError, ValueError):
                size_bb = None
        confidence = _coerce_float(parsed.get("confidence"), default=0.0)
        rationale_tag = str(parsed.get("rationale_tag", "default"))
        usage = _extract_usage(response)
        metadata = {
            "decision_source": "llm",
            "trigger_reason": trigger_reason,
            "model_name": model_name or DEFAULT_MODEL,
            "reasoning_effort": effective_reasoning_effort,
            "latency_ms": latency_ms,
            "confidence": confidence,
            "rationale_tag": rationale_tag,
            "response_id": getattr(response, "id", None),
            "usage": usage,
        }
        return ActionDecision(action_type=action, amount_bb=size_bb, note=rationale_tag, metadata=metadata)


def _extract_output_text(response: Any) -> str:
    outputs = getattr(response, "output", None)
    if outputs is None and isinstance(response, Mapping):
        outputs = response.get("output")
    if not outputs:
        return ""

    chunks: list[str] = []
    for message in outputs:
        content = getattr(message, "content", None)
        if content is None and isinstance(message, Mapping):
            content = message.get("content")
        if not content:
            continue
        for part in content:
            text = getattr(part, "text", None)
            if text is None and isinstance(part, Mapping):
                text = part.get("text")
            if text:
                chunks.append(str(text))
    return "".join(chunks).strip()


def _extract_usage(response: Any) -> dict[str, int] | None:
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, Mapping):
        usage = response.get("usage")
    if usage is None:
        return None
    if isinstance(usage, Mapping):
        return {
            "input_tokens": int(usage.get("input_tokens", 0) or 0),
            "output_tokens": int(usage.get("output_tokens", 0) or 0),
            "total_tokens": int(usage.get("total_tokens", 0) or 0),
        }
    return {
        "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
        "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
    }


def _reasoning_payload(reasoning_effort: str | None) -> dict[str, str] | None:
    return {"effort": str(reasoning_effort or DEFAULT_REASONING_EFFORT)}


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _system_prompt() -> str:
    return (
        "You are a fast six-max no-limit hold'em decision subroutine. "
        "Choose exactly one legal poker action. "
        "Use the full current-hand betting history when it is provided. "
        "Use the player context naturally, but stay grounded in the actual game state. "
        "Return only JSON matching the schema."
    )


def _heuristic_compiler_system_prompt() -> str:
    return (
        "You convert a user's poker style instructions into normalized heuristic knobs for a fast simulator. "
        "Infer a stable strategy profile for the whole run, not a single hand. "
        "Honor explicit hard constraints like pocket-pairs-only, very tight folding, or jam-heavy play. "
        "Return only JSON matching the schema."
    )


def _heuristic_compiler_user_payload(context_text: str) -> str:
    payload = {
        "task": "Translate the user's desired style into one heuristic persona for a six-max no-limit hold'em simulator.",
        "context_text": context_text,
        "parameter_guide": {
            "preflop_open_bias": "0=very tight opens, 1=very loose opens",
            "cold_call_bias": "0=rarely flats, 1=flats often",
            "fold_threshold": "0=continues wide, 1=folds easily",
            "raise_bias": "0=passive, 1=raise-heavy",
            "cbet_bias": "0=rarely bets when checked to, 1=frequent continuation betting",
            "bluff_turn_bias": "0=rarely barrels turn, 1=bluffs turn often",
            "bluff_river_bias": "0=rarely bluffs river, 1=bluffs river often",
            "jam_bias": "0=rarely jams, 1=jam-heavy",
            "trap_bias": "0=rarely slowplays, 1=slowplays often",
            "showdown_tendency": "0=avoids bluff-catch/calls down less, 1=goes to showdown often",
        },
        "output_rules": [
            "Use a short lowercase snake_case persona_name.",
            "Set each parameter between 0 and 1.",
            "Make heuristic_summary a concise explanation of the resulting style.",
        ],
    }
    return json.dumps(payload, separators=(",", ":"))


def _user_payload(
    *,
    state: DecisionState,
    compiled_persona: CompiledPersona,
    trigger_reason: str,
    context_text: str,
) -> str:
    payload = {
        "task": "Select one legal poker action.",
        "trigger_reason": trigger_reason,
        "persona_name": compiled_persona.name,
        "persona_text": context_text or compiled_persona.persona_text,
        "hand_action_history": list(state.hand_action_history),
        "state": {
            "street": state.street,
            "position": state.position,
            "hole_cards": list(state.hole_cards),
            "board_cards": list(state.board_cards),
            "stack_bb": round(float(state.stack_bb), 4),
            "committed_bb": round(float(state.committed_bb), 4),
            "pot_bb": round(float(state.pot_bb), 4),
            "to_call_bb": round(float(state.to_call_bb), 4),
            "current_bet_bb": round(float(state.current_bet_bb), 4),
            "min_raise_to_bb": round(float(state.min_raise_to_bb), 4),
            "players_remaining": int(state.players_remaining),
            "legal_actions": list(state.legal_actions),
            "button_seat": int(state.button_seat),
            "big_blind_bb": round(float(state.big_blind_bb), 4),
            "small_blind_bb": round(float(state.small_blind_bb), 4),
        },
        "output_rules": [
            "If the action is bet or raise, size_bb must be the target total wager in big blinds.",
            "If the action is fold, check, call, or all_in, size_bb should be null.",
            "Never return an illegal action.",
        ],
    }
    return json.dumps(payload, separators=(",", ":"))


def _slugify_persona_name(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    pieces: list[str] = []
    last_was_underscore = False
    for char in raw:
        if char.isalnum():
            pieces.append(char)
            last_was_underscore = False
            continue
        if char == "_" and not last_was_underscore:
            pieces.append("_")
            last_was_underscore = True
    normalized = "".join(pieces).strip("_")
    return normalized or "hero_style"


def _apply_heuristic_context_guardrails(context_text: str, parameters: PersonaParameters) -> PersonaParameters:
    normalized = " ".join(str(context_text or "").strip().lower().split())
    adjusted = parameters
    if _requires_pocket_pairs_only(normalized):
        adjusted = replace(
            adjusted,
            preflop_open_bias=min(adjusted.preflop_open_bias, 0.18),
            cold_call_bias=min(adjusted.cold_call_bias, 0.10),
            fold_threshold=max(adjusted.fold_threshold, 0.82),
        ).clamp()
    if any(phrase in normalized for phrase in ("avoid bluff", "avoid bluffing", "no bluff", "never bluff")):
        adjusted = replace(
            adjusted,
            bluff_turn_bias=min(adjusted.bluff_turn_bias, 0.04),
            bluff_river_bias=min(adjusted.bluff_river_bias, 0.04),
        ).clamp()
    return adjusted


def _requires_pocket_pairs_only(normalized_context: str) -> bool:
    if "pocket pair" not in normalized_context and "pairs only" not in normalized_context:
        return False
    return (
        "only" in normalized_context
        or "pocket pairs only" in normalized_context
        or "play pocket pairs" in normalized_context
    )
