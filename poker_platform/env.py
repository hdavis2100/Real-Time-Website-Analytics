from __future__ import annotations

"""Environment helpers that tolerate whitespace and CRLF-heavy .env files."""

import os
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parent.parent
_LOADED_DOTENV_PATH: Path | None = None


def _dotenv_candidates() -> Iterable[Path]:
    configured = os.getenv("POKER_PLATFORM_DOTENV_PATH")
    if configured:
        yield Path(configured).expanduser()
    yield Path.cwd() / ".env"
    yield PROJECT_ROOT / ".env"


def _parse_dotenv_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped[len("export ") :].strip()
    if "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None

    if value and value[0] in {'"', "'"} and value[-1:] == value[0]:
        value = value[1:-1]
    elif " #" in value:
        value = value.split(" #", 1)[0].rstrip()

    return key, value.strip()


def load_dotenv_file(path: str | os.PathLike[str] | None = None, *, override: bool = False) -> Path | None:
    global _LOADED_DOTENV_PATH

    candidate_paths = [Path(path).expanduser()] if path is not None else list(_dotenv_candidates())
    for candidate in candidate_paths:
        resolved = resolve_repo_path(candidate)
        if resolved is None or not resolved.exists():
            continue

        for raw_line in resolved.read_text(encoding="utf-8").splitlines():
            parsed = _parse_dotenv_line(raw_line)
            if parsed is None:
                continue
            key, value = parsed
            if override or key not in os.environ:
                os.environ[key] = value

        _LOADED_DOTENV_PATH = resolved
        return resolved

    return _LOADED_DOTENV_PATH


def getenv_stripped(name: str, default: str | os.PathLike[str] | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        if default is None:
            return None
        value = os.fspath(default)
    return str(value).strip()


def getenv_path(name: str, default: str | os.PathLike[str] | None = None) -> Path | None:
    value = getenv_stripped(name, default)
    if value is None or value == "":
        return None
    return Path(value).expanduser()


def resolve_repo_path(path_value: str | os.PathLike[str] | None) -> Path | None:
    if path_value is None:
        return None

    stripped = str(path_value).strip()
    if not stripped:
        return None

    candidate = Path(stripped).expanduser()
    if candidate.exists():
        return candidate.resolve()

    if candidate.is_absolute():
        return candidate

    for base in (Path.cwd(), PROJECT_ROOT):
        resolved = (base / candidate).resolve()
        if resolved.exists():
            return resolved

    return candidate


# Mirror the Node service behavior by loading the repo .env file when present so
# local Python and Make-based runs can use the same configuration surface.
load_dotenv_file()
