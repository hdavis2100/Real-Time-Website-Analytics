from __future__ import annotations

"""Configuration helpers for the containerized poker analytics platform."""

from dataclasses import dataclass
from pathlib import Path

from poker_platform.env import getenv_path, getenv_stripped


def _env_bool(name: str, default: bool = False) -> bool:
    value = getenv_stripped(name)
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


@dataclass(frozen=True)
class PlatformConfig:
    runtime_dir: Path
    sql_dir: Path
    schema_version: str
    kafka_brokers: str
    kafka_simulation_requests_topic: str
    kafka_actions_topic: str
    kafka_hand_summaries_topic: str
    kafka_simulation_events_topic: str
    historical_fixture_path: Path
    stream_window_duration: str
    stream_slide_duration: str
    stream_watermark: str
    stream_checkpoint_path: Path
    redis_url: str = "redis://localhost:6379/0"
    redis_live_ttl_seconds: int = 86400
    redis_key_prefix: str = "live"
    simulation_worker_legacy_registry_enabled: bool = False
    simulation_worker_concurrency: int = 1
    simulation_worker_max_poll_interval_ms: int = 1800000


def get_config() -> PlatformConfig:
    runtime_dir = getenv_path("POKER_PLATFORM_RUNTIME_DIR", "runtime") or Path("runtime")
    sql_dir = getenv_path("POKER_PLATFORM_SQL_DIR", "sql") or Path("sql")
    historical_fixture_path = getenv_path(
        "HISTORICAL_FIXTURE_PATH",
        "sample_data/historical/handhq_6max_sample.phhs",
    ) or Path("sample_data/historical/handhq_6max_sample.phhs")

    return PlatformConfig(
        runtime_dir=runtime_dir,
        sql_dir=sql_dir,
        schema_version=getenv_stripped("POKER_PLATFORM_SCHEMA_VERSION", "1") or "1",
        kafka_brokers=getenv_stripped("KAFKA_BROKERS", "localhost:9092") or "localhost:9092",
        kafka_simulation_requests_topic=(
            getenv_stripped("KAFKA_TOPIC_SIMULATION_REQUESTS", "poker.simulation_requests")
            or "poker.simulation_requests"
        ),
        kafka_actions_topic=getenv_stripped("KAFKA_TOPIC_ACTIONS", "poker.actions") or "poker.actions",
        kafka_hand_summaries_topic=(
            getenv_stripped("KAFKA_TOPIC_HAND_SUMMARIES", "poker.hand_summaries")
            or "poker.hand_summaries"
        ),
        kafka_simulation_events_topic=(
            getenv_stripped("KAFKA_TOPIC_SIMULATION_EVENTS", "poker.simulation_events")
            or "poker.simulation_events"
        ),
        historical_fixture_path=historical_fixture_path,
        stream_window_duration=getenv_stripped("STREAM_WINDOW_DURATION", "1 minute") or "1 minute",
        stream_slide_duration=getenv_stripped("STREAM_SLIDE_DURATION", "30 seconds") or "30 seconds",
        stream_watermark=getenv_stripped("STREAM_WATERMARK", "5 minutes") or "5 minutes",
        stream_checkpoint_path=getenv_path(
            "STREAM_CHECKPOINT_PATH",
            runtime_dir / "stream_checkpoints" / "poker_actions",
        ) or (runtime_dir / "stream_checkpoints" / "poker_actions"),
        redis_url=getenv_stripped("REDIS_URL", "redis://localhost:6379/0") or "redis://localhost:6379/0",
        redis_live_ttl_seconds=int(getenv_stripped("REDIS_LIVE_TTL_SECONDS", "86400") or "86400"),
        redis_key_prefix=getenv_stripped("REDIS_KEY_PREFIX", "live") or "live",
        simulation_worker_legacy_registry_enabled=_env_bool(
            "SIMULATION_WORKER_LEGACY_REGISTRY_ENABLED",
            False,
        ),
        simulation_worker_concurrency=max(
            1,
            int(getenv_stripped("SIMULATION_WORKER_CONCURRENCY", "1") or "1"),
        ),
        simulation_worker_max_poll_interval_ms=max(
            300000,
            int(
                getenv_stripped(
                    "SIMULATION_WORKER_MAX_POLL_INTERVAL_MS",
                    "1800000",
                )
                or "1800000"
            ),
        ),
    )
