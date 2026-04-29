from __future__ import annotations

import importlib
from pathlib import Path

import poker_platform.config as config_module
import poker_platform.env as env_module
from poker_platform.config import get_config
from poker_platform.env import getenv_stripped, load_dotenv_file, resolve_repo_path


def test_getenv_stripped_removes_crlf(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_BROKERS", "localhost:9092\r\n")
    assert getenv_stripped("KAFKA_BROKERS") == "localhost:9092"


def test_get_config_strips_whitespace_from_env(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_BROKERS", "localhost:9092\r\n")
    monkeypatch.setenv("KAFKA_TOPIC_ACTIONS", "poker.actions\r\n")
    monkeypatch.setenv("KAFKA_TOPIC_SIMULATION_REQUESTS", "poker.simulation_requests\r\n")

    config = get_config()

    assert config.kafka_brokers == "localhost:9092"
    assert config.kafka_actions_topic == "poker.actions"
    assert config.kafka_simulation_requests_topic == "poker.simulation_requests"


def test_get_config_parses_worker_legacy_registry_flag(monkeypatch) -> None:
    monkeypatch.setenv("SIMULATION_WORKER_LEGACY_REGISTRY_ENABLED", "true\r\n")

    config = get_config()

    assert config.simulation_worker_legacy_registry_enabled is True


def test_get_config_parses_worker_concurrency(monkeypatch) -> None:
    monkeypatch.setenv("SIMULATION_WORKER_CONCURRENCY", "4\r\n")

    config = get_config()

    assert config.simulation_worker_concurrency == 4


def test_get_config_parses_worker_max_poll_interval(monkeypatch) -> None:
    monkeypatch.setenv("SIMULATION_WORKER_MAX_POLL_INTERVAL_MS", "1800000\r\n")

    config = get_config()

    assert config.simulation_worker_max_poll_interval_ms == 1800000


def test_resolve_repo_path_handles_relative_path_with_crlf() -> None:
    resolved = resolve_repo_path("secrets/snowflake/.gitkeep\r\n")

    assert resolved is not None
    assert resolved.exists()
    assert resolved.name == ".gitkeep"
    assert resolved == Path("secrets/snowflake/.gitkeep").resolve()


def test_load_dotenv_file_sets_missing_values_from_repo_style_env(tmp_path: Path, monkeypatch) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("KAFKA_BROKERS=test-broker:9092\r\nKAFKA_TOPIC_ACTIONS=test.actions\r\n", encoding="utf-8")

    monkeypatch.delenv("KAFKA_BROKERS", raising=False)
    monkeypatch.delenv("KAFKA_TOPIC_ACTIONS", raising=False)
    load_dotenv_file(dotenv_path, override=True)

    config = get_config()

    assert getenv_stripped("KAFKA_BROKERS") == "test-broker:9092"
    assert getenv_stripped("KAFKA_TOPIC_ACTIONS") == "test.actions"
    assert config.kafka_brokers == "test-broker:9092"
    assert config.kafka_actions_topic == "test.actions"


def test_env_module_autoloads_configured_dotenv_path(tmp_path: Path, monkeypatch) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("KAFKA_HAND_SUMMARIES_TOPIC=unused\r\nKAFKA_TOPIC_HAND_SUMMARIES=test.hand_summaries\r\n", encoding="utf-8")

    monkeypatch.setenv("POKER_PLATFORM_DOTENV_PATH", str(dotenv_path))
    monkeypatch.delenv("KAFKA_TOPIC_HAND_SUMMARIES", raising=False)

    importlib.reload(env_module)
    importlib.reload(config_module)

    try:
        assert env_module.getenv_stripped("KAFKA_TOPIC_HAND_SUMMARIES") == "test.hand_summaries"
        assert config_module.get_config().kafka_hand_summaries_topic == "test.hand_summaries"
    finally:
        monkeypatch.delenv("POKER_PLATFORM_DOTENV_PATH", raising=False)
        importlib.reload(env_module)
        importlib.reload(config_module)


def test_compose_spark_stream_subscribes_to_simulation_events() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")

    assert "--events-topic poker.simulation_events" in compose_text


def test_compose_spark_stream_persists_runtime_checkpoint_directory() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")
    spark_stream_block = compose_text.split("spark-stream:", 1)[1]

    assert "poker-platform-runtime:/tmp/poker_platform_runtime" in spark_stream_block
    assert "STREAM_CHECKPOINT_PATH: /tmp/poker_platform_runtime/stream_checkpoints/poker" in spark_stream_block
    assert "KAFKA_STREAM_STARTING_OFFSETS: latest" in spark_stream_block
    assert 'POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP: "1"' in spark_stream_block


def test_compose_airflow_sets_redis_url_for_catchup_jobs() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")
    airflow_block = compose_text.split("airflow:", 1)[1].split("\n  spark-stream:", 1)[0]

    assert "REDIS_URL: redis://redis:6379/0" in airflow_block


def test_compose_simulation_worker_does_not_require_app_service() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")
    worker_block = compose_text.split("simulation-worker:", 1)[1].split("\n  dashboard:", 1)[0]

    assert "app:" not in worker_block
    assert 'SIMULATION_WORKER_LEGACY_REGISTRY_ENABLED: "false"' in worker_block
    assert 'SIMULATION_WORKER_CONCURRENCY: "6"' in worker_block
    assert 'SIMULATION_WORKER_MAX_POLL_INTERVAL_MS: "1800000"' in worker_block
    assert "--concurrency $${SIMULATION_WORKER_CONCURRENCY:-6}" in worker_block
    assert "--max-poll-interval-ms $${SIMULATION_WORKER_MAX_POLL_INTERVAL_MS:-1800000}" in worker_block


def test_compose_app_service_bootstraps_postgres_app_database() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")
    app_block = compose_text.split("app:", 1)[1].split("\n  simulation-worker:", 1)[0]

    assert "postgres:" in compose_text
    assert "APP_DATABASE_URL: postgresql://poker:poker@postgres:5432/poker_app" in app_block
    assert "postgres:\n        condition: service_healthy" in app_block


def test_compose_simulation_event_projector_uses_kafka_and_postgres() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")
    projector_block = compose_text.split("simulation-event-projector:", 1)[1].split(
        "\n  dashboard:",
        1,
    )[0]

    assert "postgres:" in projector_block
    assert "kafka:" in projector_block
    assert "node simulation/scripts/project-simulation-events.js" in projector_block
    assert "APP_DATABASE_URL: postgresql://poker:poker@postgres:5432/poker_app" in projector_block
    assert "KAFKA_TOPIC_SIMULATION_EVENTS: poker.simulation_events" in projector_block


def test_docker_app_image_installs_openai_extra_for_llm_simulation_runs() -> None:
    dockerfile_text = Path("docker/app/Dockerfile").read_text(encoding="utf-8")

    assert ".[kafka,snowflake,openai]" in dockerfile_text


def test_makefile_stream_catchup_subscribes_to_simulation_events() -> None:
    makefile_text = Path("Makefile").read_text(encoding="utf-8")

    assert "--events-topic poker.simulation_events" in makefile_text
    assert "/tmp/poker_platform_runtime/stream_checkpoints/poker" in makefile_text


def test_makefile_exposes_simulation_event_projector_target() -> None:
    makefile_text = Path("Makefile").read_text(encoding="utf-8")

    assert "project-events:" in makefile_text
    assert "docker compose up -d simulation-event-projector" in makefile_text


def test_makefile_historical_ingest_uses_handhq_backfill_only() -> None:
    makefile_text = Path("Makefile").read_text(encoding="utf-8")

    assert "historical_ingest.handhq_backfill" in makefile_text
    assert "historical_ingest.load" not in makefile_text
    assert "replay-sample-historical" not in makefile_text


def test_historical_airflow_dag_uses_handhq_backfill_only() -> None:
    dag_text = Path("airflow/dags/poker_batch_pipeline.py").read_text(encoding="utf-8")

    assert "historical_ingest.handhq_backfill" in dag_text
    assert "historical_ingest.load" not in dag_text
    assert "only supports HandHQ .phhs sources" in dag_text


def test_public_api_exposes_profiles_and_not_historical_ingest_routes() -> None:
    router_text = Path("simulation/src/routes/events.js").read_text(encoding="utf-8")

    assert 'router.get("/simulations/:simulationRunId/profiles"' in router_text
    assert 'router.post("/historical/ingest"' not in router_text
    assert 'router.post("/historical/replay"' not in router_text


def test_retired_generic_historical_paths_are_removed() -> None:
    retired_paths = [
        "historical/historical_ingest/load.py",
        "historical/historical_ingest/models.py",
        "historical/historical_ingest/parser.py",
        "historical/historical_ingest/replay_to_kafka.py",
        "sample_data/historical/nlh_6max_sample_01.phh",
        "tests/fixtures/historical/nlh_6max_sample_01.phh",
        "simulation/scripts/__init__.py",
        "simulation/scripts/run-python-module.mjs",
    ]

    for retired_path in retired_paths:
        assert not Path(retired_path).exists()


def test_env_example_uses_handhq_fixture_and_app_database_defaults() -> None:
    env_text = Path(".env.example").read_text(encoding="utf-8")

    assert "APP_DATABASE_URL=postgresql://poker:poker@postgres:5432/poker_app" in env_text
    assert "APP_SESSION_TTL_DAYS=14" in env_text
    assert "REDIS_URL=redis://redis:6379/0" in env_text
    assert "HISTORICAL_FIXTURE_PATH=sample_data/historical/handhq_6max_sample.phhs" in env_text
    assert "PHH_DATASET_SOURCE=sample_data/historical/handhq_6max_sample.phhs" in env_text
    assert "SIMULATION_WORKER_LEGACY_REGISTRY_ENABLED=false" in env_text
    assert "SIMULATION_WORKER_CONCURRENCY=6" in env_text
    assert "SIMULATION_WORKER_MAX_POLL_INTERVAL_MS=1800000" in env_text


def test_postgres_schema_file_exists_for_app_bootstrap() -> None:
    schema_path = Path("sql/ddl/postgres/create_tables.sql")

    assert schema_path.exists()
    schema_text = schema_path.read_text(encoding="utf-8")
    assert "create table if not exists users" in schema_text
    assert "create table if not exists app_sessions" in schema_text
    assert "create table if not exists simulation_runs" in schema_text
