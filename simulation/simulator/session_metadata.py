from __future__ import annotations

from hashlib import sha256


def normalize_decision_backend(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"heuristic", "heuristic_persona"}:
        return "heuristic"
    return "llm"


def hero_context_hash(context: str | None) -> str | None:
    normalized = str(context or "").strip()
    if not normalized:
        return None
    return sha256(normalized.encode("utf-8")).hexdigest()


def hero_context_preview(context: str | None, *, limit: int = 80) -> str:
    normalized = " ".join(str(context or "").strip().split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 3)].rstrip() + "..."
