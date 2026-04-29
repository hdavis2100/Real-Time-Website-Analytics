from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import math
from typing import Any, Iterable

from poker_platform.config import PlatformConfig, get_config


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    return value


def _json_member(payload: dict[str, Any]) -> str:
    return json.dumps(_json_safe_value(payload), sort_keys=True, separators=(",", ":"), default=str)


TERMINAL_RUN_STATUSES = {"completed", "failed"}
RUN_STATUS_RANK = {
    "queued": 0,
    "running": 1,
    "finalizing": 2,
    "completed": 3,
    "failed": 3,
}


def _status_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _status_rank(value: Any) -> int:
    return int(RUN_STATUS_RANK.get(_status_text(value), -1))


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _is_stale_update(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    existing_updated_at = _parse_timestamp(existing.get("updated_at"))
    incoming_updated_at = _parse_timestamp(incoming.get("updated_at"))
    if existing_updated_at is None or incoming_updated_at is None:
        return False
    return incoming_updated_at < existing_updated_at


def _is_new_execution(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    if not existing:
        return False

    incoming_status = _status_text(incoming.get("status"))
    existing_requested_at = _parse_timestamp(existing.get("requested_at"))
    incoming_requested_at = _parse_timestamp(incoming.get("requested_at"))
    if (
        incoming_status == "queued"
        and existing_requested_at is not None
        and incoming_requested_at is not None
        and incoming_requested_at.timestamp() > existing_requested_at.timestamp() + 1
    ):
        return True

    incoming_started_at = _parse_timestamp(incoming.get("started_at"))
    existing_finished_at = _parse_timestamp(existing.get("finished_at"))
    existing_updated_at = _parse_timestamp(existing.get("updated_at"))
    existing_end = existing_finished_at or existing_updated_at
    if (
        incoming_started_at is not None
        and existing_end is not None
        and incoming_started_at.timestamp() > existing_end.timestamp() + 1
    ):
        return True

    for field in ["user_id", "decision_backend", "hero_context_hash"]:
        existing_value = str(existing.get(field) or "").strip()
        incoming_value = str(incoming.get(field) or "").strip()
        if existing_value and incoming_value and existing_value != incoming_value:
            return True

    return False


def _earliest_timestamp_text(left: Any, right: Any) -> str | None:
    left_ts = _parse_timestamp(left)
    right_ts = _parse_timestamp(right)
    if left_ts and right_ts:
        return str(left) if left_ts <= right_ts else str(right)
    if left_ts:
        return str(left) if not isinstance(left, datetime) else left_ts.isoformat()
    if right_ts:
        return str(right) if not isinstance(right, datetime) else right_ts.isoformat()
    return None


def _latest_timestamp_text(left: Any, right: Any) -> str | None:
    left_ts = _parse_timestamp(left)
    right_ts = _parse_timestamp(right)
    if left_ts and right_ts:
        return str(left) if left_ts >= right_ts else str(right)
    if left_ts:
        return str(left) if not isinstance(left, datetime) else left_ts.isoformat()
    if right_ts:
        return str(right) if not isinstance(right, datetime) else right_ts.isoformat()
    return None


def _max_count_text(left: Any, right: Any) -> str | None:
    values: list[int] = []
    for candidate in (left, right):
        try:
            parsed = int(candidate)
        except (TypeError, ValueError):
            continue
        values.append(parsed)
    if not values:
        return None
    return str(max(values))


@dataclass(slots=True)
class RedisLiveStore:
    config: PlatformConfig
    client: Any | None = None

    def _client(self):
        if self.client is not None:
            return self.client
        try:
            from redis import Redis
        except ImportError as exc:  # pragma: no cover - optional dependency in tests
            raise RuntimeError("redis package is required for live dashboard storage") from exc
        self.client = Redis.from_url(self.config.redis_url, decode_responses=True)
        return self.client

    @property
    def prefix(self) -> str:
        return str(self.config.redis_key_prefix or "live").strip(":")

    def _key(self, suffix: str) -> str:
        return f"{self.prefix}:{suffix}"

    def ping(self) -> bool:
        return bool(self._client().ping())

    def list_active_runs(self, *, user_id: str | None = None) -> list[str]:
        key = self._key("runs:active") if user_id is None else self._key(f"user:{user_id}:runs:active")
        return sorted(str(item) for item in self._client().smembers(key))

    def list_run_meta(self, run_ids: Iterable[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for run_id in run_ids:
            meta = self.get_run_meta(run_id)
            if meta:
                rows.append(meta)
        return rows

    def get_run_meta(self, run_id: str) -> dict[str, Any]:
        return dict(self._client().hgetall(self._key(f"run:{run_id}:meta")))

    def get_run_agents(self, run_id: str) -> list[dict[str, Any]]:
        payloads = self._client().hgetall(self._key(f"run:{run_id}:agents"))
        return [json.loads(value) for _, value in sorted(payloads.items())]

    def get_run_leaderboard(self, run_id: str, kind: str, *, topn: int = 25) -> list[dict[str, Any]]:
        return self._read_sorted_payloads(self._key(f"run:{run_id}:leaderboard:{kind}"), topn=topn)

    def get_global_leaderboard(self, kind: str, *, topn: int = 25) -> list[dict[str, Any]]:
        return self._read_sorted_payloads(self._key(f"global:leaderboard:{kind}"), topn=topn)

    def list_json_hash(self, suffix: str) -> list[dict[str, Any]]:
        payloads = self._client().hgetall(self._key(suffix))
        return [json.loads(value) for _, value in sorted(payloads.items())]

    def get_json_hash(self, suffix: str) -> dict[str, dict[str, Any]]:
        payloads = self._client().hgetall(self._key(suffix))
        return {str(key): json.loads(value) for key, value in payloads.items()}

    def _read_sorted_payloads(self, key: str, *, topn: int) -> list[dict[str, Any]]:
        members = self._client().zrevrange(key, 0, max(0, int(topn) - 1))
        return [json.loads(member) for member in members]

    def set_run_meta(self, meta: dict[str, Any]) -> None:
        run_id = str(meta["simulation_run_id"])
        existing = self.get_run_meta(run_id)
        incoming = {str(key): value for key, value in meta.items()}
        if _is_new_execution(existing, incoming):
            self.clear_run(run_id, user_id=str(existing.get("user_id") or "") or None)
            existing = {}
        merged = dict(existing)
        merged.update(incoming)

        existing_status = _status_text(existing.get("status"))
        incoming_status = _status_text(incoming.get("status"))
        existing_status_rank = _status_rank(existing_status)
        incoming_status_rank = _status_rank(incoming_status)
        update_is_stale = _is_stale_update(existing, incoming)
        final_status = _status_text(merged.get("status"))

        if (
            existing_status in TERMINAL_RUN_STATUSES
            and incoming_status not in TERMINAL_RUN_STATUSES
        ):
            final_status = existing_status
        elif incoming_status and existing_status_rank > incoming_status_rank:
            final_status = existing_status
        elif update_is_stale:
            final_status = existing_status or final_status

        if final_status:
            merged["status"] = final_status

        if update_is_stale or (
            existing_status in TERMINAL_RUN_STATUSES
            and incoming_status not in TERMINAL_RUN_STATUSES
        ):
            for field in ["started_at", "finished_at", "updated_at", "error_message"]:
                if field in existing:
                    merged[field] = existing[field]

        merged_requested_at = _earliest_timestamp_text(
            existing.get("requested_at"), incoming.get("requested_at")
        )
        if merged_requested_at:
            merged["requested_at"] = merged_requested_at

        merged_started_at = _earliest_timestamp_text(
            existing.get("started_at"), incoming.get("started_at")
        )
        if merged_started_at:
            merged["started_at"] = merged_started_at

        merged_finished_at = _latest_timestamp_text(
            existing.get("finished_at"), incoming.get("finished_at")
        )
        if merged_finished_at:
            merged["finished_at"] = merged_finished_at

        merged_updated_at = _latest_timestamp_text(
            existing.get("updated_at"), incoming.get("updated_at")
        )
        if merged_updated_at:
            merged["updated_at"] = merged_updated_at

        published_actions = _max_count_text(
            existing.get("published_actions"), incoming.get("published_actions")
        )
        if published_actions is not None:
            merged["published_actions"] = published_actions

        published_hand_summaries = _max_count_text(
            existing.get("published_hand_summaries"),
            incoming.get("published_hand_summaries"),
        )
        if published_hand_summaries is not None:
            merged["published_hand_summaries"] = published_hand_summaries

        if existing_status_rank > incoming_status_rank:
            for field in ["error_message", "finished_at", "updated_at"]:
                if field in existing and existing.get(field) not in {None, ""}:
                    merged[field] = existing[field]

        clean = {str(key): "" if value is None else str(value) for key, value in merged.items()}
        pipe = self._client().pipeline()
        pipe.hset(self._key(f"run:{run_id}:meta"), mapping=clean)
        user_id = merged.get("user_id")
        if final_status in TERMINAL_RUN_STATUSES:
            pipe.srem(self._key("runs:active"), run_id)
            if user_id:
                pipe.srem(self._key(f"user:{user_id}:runs:active"), run_id)
        else:
            pipe.sadd(self._key("runs:active"), run_id)
            if user_id:
                pipe.sadd(self._key(f"user:{user_id}:runs:active"), run_id)
        pipe.execute()

    def remove_active_run(self, run_id: str, *, user_id: str | None = None) -> None:
        pipe = self._client().pipeline()
        pipe.srem(self._key("runs:active"), run_id)
        if user_id:
            pipe.srem(self._key(f"user:{user_id}:runs:active"), run_id)
        pipe.execute()

    def clear_run(self, run_id: str, *, user_id: str | None = None) -> None:
        pipe = self._client().pipeline()
        for suffix in [
            f"run:{run_id}:meta",
            f"run:{run_id}:leaderboard:profit",
            f"run:{run_id}:leaderboard:bb_per_100",
            f"run:{run_id}:leaderboard:high_hand",
            f"run:{run_id}:agents",
            f"run:{run_id}:state:player_hands",
            f"run:{run_id}:state:player_aggregates",
            f"run:{run_id}:state:agents",
        ]:
            pipe.delete(self._key(suffix))
        pipe.srem(self._key("runs:active"), run_id)
        if user_id:
            pipe.srem(self._key(f"user:{user_id}:runs:active"), run_id)
        pipe.execute()

    def expire_run(self, run_id: str, *, user_id: str | None = None) -> None:
        ttl = int(self.config.redis_live_ttl_seconds)
        pipe = self._client().pipeline()
        for suffix in [
            f"run:{run_id}:meta",
            f"run:{run_id}:leaderboard:profit",
            f"run:{run_id}:leaderboard:bb_per_100",
            f"run:{run_id}:leaderboard:high_hand",
            f"run:{run_id}:agents",
            f"run:{run_id}:state:player_hands",
            f"run:{run_id}:state:player_aggregates",
            f"run:{run_id}:state:agents",
        ]:
            pipe.expire(self._key(suffix), ttl)
        pipe.srem(self._key("runs:active"), run_id)
        if user_id:
            pipe.srem(self._key(f"user:{user_id}:runs:active"), run_id)
        pipe.execute()

    def replace_run_leaderboard(self, run_id: str, kind: str, rows: Iterable[dict[str, Any]], *, score_field: str) -> None:
        self._replace_sorted_set(self._key(f"run:{run_id}:leaderboard:{kind}"), rows, score_field=score_field)

    def replace_global_leaderboard(self, kind: str, rows: Iterable[dict[str, Any]], *, score_field: str) -> None:
        self._replace_sorted_set(self._key(f"global:leaderboard:{kind}"), rows, score_field=score_field)

    def _replace_sorted_set(self, key: str, rows: Iterable[dict[str, Any]], *, score_field: str) -> None:
        pipe = self._client().pipeline()
        pipe.delete(key)
        mapping: dict[str, float] = {}
        for row in rows:
            try:
                score = float(row.get(score_field))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(score):
                continue
            mapping[_json_member(dict(row))] = score
        if mapping:
            pipe.zadd(key, mapping)
        pipe.execute()

    def replace_run_agents(self, run_id: str, rows: Iterable[dict[str, Any]]) -> None:
        key = self._key(f"run:{run_id}:agents")
        pipe = self._client().pipeline()
        pipe.delete(key)
        mapping = {}
        for row in rows:
            agent_id = str(row.get("agent_id") or row.get("player_id") or len(mapping))
            mapping[agent_id] = _json_member(dict(row))
        if mapping:
            pipe.hset(key, mapping=mapping)
        pipe.execute()

    def replace_json_hash(self, suffix: str, rows: Iterable[dict[str, Any]], *, key_field: str) -> None:
        key = self._key(suffix)
        pipe = self._client().pipeline()
        pipe.delete(key)
        mapping = {}
        for row in rows:
            field = str(row.get(key_field) or len(mapping))
            mapping[field] = _json_member(dict(row))
        if mapping:
            pipe.hset(key, mapping=mapping)
        pipe.execute()

    def upsert_json_hash(self, suffix: str, rows: Iterable[dict[str, Any]], *, key_builder) -> None:
        key = self._key(suffix)
        mapping = {}
        for row in rows:
            mapping[str(key_builder(row))] = _json_member(dict(row))
        if mapping:
            self._client().hset(key, mapping=mapping)

    def insert_json_hash_if_absent(self, suffix: str, field: str, row: dict[str, Any]) -> bool:
        return bool(self._client().hsetnx(self._key(suffix), str(field), _json_member(dict(row))))

    def delete(self, suffix: str) -> None:
        self._client().delete(self._key(suffix))


def get_live_store(config: PlatformConfig | None = None, *, client: Any | None = None) -> RedisLiveStore:
    return RedisLiveStore(config or get_config(), client=client)
