# Architecture

The platform is organized around one shared action-level poker schema, with a
deliberate split between app ownership and analytics ownership.

## Runtime Ownership

- PostgreSQL owns app-facing data:
  - users
  - saved agents and immutable agent versions
  - simulation run ownership and status
  - saved explorer queries
  - profile job audit history
- Snowflake owns warehouse and durable analytics data:
  - historical ingest tables
  - curated actions, hands, and players
  - model artifacts and profile outputs
  - completed run summaries and explorer query sources
- Redis owns live serving state only:
  - active runs
  - live leaderboards
  - rolling agent metrics
- Kafka owns async transport only:
  - simulation requests
  - simulation lifecycle events
  - action and hand-summary events
- Spark is the ingestion and materialization engine between Kafka, Snowflake,
  and Redis.

## Core Flow

1. Historical ingest resolves HandHQ `.phhs` sources, canonicalizes them into
   the shared row model, and writes raw plus curated Snowflake rows.
2. Snowflake-backed feature builds materialize per-hand and per-player style
   features from curated historical actions.
3. `MiniBatchKMeans` archetype training runs offline and stores centroids, model
   metadata, player assignments, prototypes, and scoring metadata.
4. The browser app or API creates a Postgres-backed simulation run in `queued`
   state and publishes a Kafka `simulation_request`.
5. The Python worker consumes the request and runs six 6-max NLH agents. The
   hero can come from a raw context string or a saved agent version. The run can
   use either an all-heuristic lane or an LLM-gated lane.
6. The worker publishes actions, hand summaries, and lifecycle events to Kafka.
7. `simulation/scripts/project-simulation-events.js` consumes
   `poker.simulation_events` and projects run lifecycle changes into Postgres.
8. `jobs/stream_kafka.py` consumes `poker.actions`,
   `poker.hand_summaries`, and `poker.simulation_events` with Spark Structured
   Streaming, writes raw and curated Snowflake rows, refreshes
   `SIMULATION_RUN_PLAYER_SUMMARIES`, materializes Redis live dashboard state,
   and updates live metrics plus nearest historical profile assignments.
9. The Express app serves:
   - Postgres-backed run ownership, agent, and saved-query metadata
   - Redis-backed live run views
   - Snowflake-backed completed results and explorer queries
10. Streamlit reads Snowflake tables for historical profiles and action
    exploration, and Redis for live simulation metrics and comparisons.

## Product Surfaces

### Browser app

`simulation/src/public/` provides a lightweight operator shell at `/app`. It is
the app-facing MVP surface for:

- dev-session sign-in
- saved agent creation and versioning
- simulation launch
- run list and run detail
- explicit profile requests and job history
- structured performance exploration

### API

`simulation/src/routes/events.js` is the main HTTP surface. It exposes:

- session and status endpoints
- agent CRUD-lite and versioning endpoints
- simulation create/list/detail/live/results endpoints
- explicit profile request plus cached profile reads
- performance summary and structured query endpoints
- action and hand-summary ingestion endpoints

### Dashboard

`dashboard/app.py` is a separate Streamlit UI for historical analysis and live
monitoring. The Node app also calls `dashboard/api_bridge.py` for structured
Snowflake-backed result and explorer queries.

## Streaming Ingestion

Spark streaming is the canonical Kafka-to-Snowflake ingestion path. The
`spark-stream` service continuously consumes action, hand-summary, and lifecycle
topics, writes raw and curated warehouse rows, refreshes completed-run summaries,
and materializes Redis-backed live views. The Airflow
`kafka_stream_snowflake_catchup` DAG runs the same job in bounded
`availableNow` mode for catch-up workflows.

## Main Components

### Historical and modeling

- `historical/historical_ingest/fetch_phh.py` resolves local and remote HandHQ
  sources and safely extracts archives.
- `historical/historical_ingest/handhq_backfill.py` is the historical backfill
  entrypoint and bulk-loads canonical rows into Snowflake.
- `ml_model/profiles/build_features.py` materializes Snowflake-backed hand and
  training feature tables.
- `ml_model/profiles/train_model.py` trains the historical archetype model and
  writes centroids, prototypes, assignments, and model metadata.
- `ml_model/profiles/score_session.py` scores completed runs against the active
  model artifact and caches `PROFILE_SESSION_RESULTS`.

### Simulation and live metrics

- `simulation/simulator/engine.py` runs deterministic 6-max NLH hands with
  blind posts, stack carry-over, settlement, and collect-pot events.
- `simulation/simulator/run_simulation.py` creates context-driven or explicit
  persona matchups, returns performance summaries, and publishes events to
  Kafka.
- `simulation/simulator/worker.py` consumes simulation requests and executes
  runs.
- `simulation/src/lib/simulation-event-projector.js` normalizes lifecycle events
  and writes Postgres status transitions.
- `jobs/stream_kafka.py` ingests Kafka events into Snowflake, updates live
  agent metrics, refreshes durable run player summaries, and materializes
  Redis-backed live leaderboards.

### Orchestration and serving

- `simulation/src/server.js` boots the Express app after app DB and Kafka
  initialization.
- `simulation/src/lib/app-db.js` initializes the Postgres pool and schema.
- `simulation/src/lib/app-store.js` is the Postgres repository layer for agents,
  runs, profile jobs, and saved queries.
- `simulation/src/routes/events.js` exposes session, simulation, profile,
  explorer, and event publishing routes.
- `dashboard/api_bridge.py` exposes structured Snowflake-backed summaries and
  query helpers for the Node app.
- `airflow/dags/*.py` wraps Snowflake bootstrap, HandHQ backfill, profile
  feature builds, model training, simulation, and optional Kafka catch-up.
- `compose.yaml` defines the local container stack.

## Runtime Assumptions

- PostgreSQL is required for the app-facing API and browser shell.
- Snowflake is the warehouse and completed-results source.
- Redis is the live dashboard and live-run serving source.
- Kafka is required for the API, worker, projector, and stream path.
- Spark is the live metrics and Kafka ingestion engine.
- Airflow is the orchestrator for bootstrap, ingest, training, and batch flows.
- Docker Compose is the supported local runtime.
