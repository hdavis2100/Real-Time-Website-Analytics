# Codebase Guide

This document is the practical onboarding guide for the repository. It is meant
to answer four questions quickly:

1. What does this system do?
2. Which service owns which responsibility?
3. What does each main folder and file do?
4. How do I run and use the application safely?

This guide focuses on source code and operational files that matter during
development. Generated caches such as `node_modules/`, `__pycache__/`,
`.pytest_cache/`, `.git/`, and runtime scratch directories are intentionally not
documented file by file.

## What The System Is

The repository is a real-time poker analytics platform with four connected
lanes:

- Historical ingest: parse HandHQ `.phhs` data into action-level warehouse rows.
- Profile modeling: cluster historical player styles into reusable archetypes.
- Simulation: run async 6-max NLH simulations for a saved or ad hoc hero agent.
- Serving: expose live and completed results through an API, a small web app,
  and a Streamlit dashboard.

The current system-of-record split is:

- PostgreSQL: app data such as users, saved agents, agent versions, run
  ownership, saved queries, and profile jobs.
- Snowflake: analytics warehouse, historical data, completed run results, and
  profile outputs.
- Redis: live run state and leaderboards.
- Kafka: async event transport.
- Spark: Kafka ingestion plus live/durable materialization.

## End-To-End Flow

### Historical lane

1. HandHQ `.phhs` files are fetched or resolved locally.
2. They are canonicalized into shared hand, player, and action rows.
3. Raw and curated Snowflake tables are populated.
4. Historical feature tables are built from curated action-level data.
5. A clustering model is trained and activated.

### Simulation lane

1. The app creates a Postgres-backed simulation run in `queued` state.
2. The API publishes a `simulation_request` event to Kafka.
3. The Python worker consumes the request and runs a 6-max simulation.
4. The worker publishes actions, hand summaries, and lifecycle events.
5. The Node lifecycle projector consumes lifecycle events and updates Postgres
   run status.
6. Spark consumes the Kafka topics and writes warehouse/live outputs.
7. The app reads:
   - Postgres for run ownership and status
   - Redis for live state
   - Snowflake for completed results and explorer queries

## Code Quality Overview

The repository is organized so each runtime responsibility has a clear owner:
Postgres app state, Snowflake analytics state, Redis live state, Kafka transport,
Spark materialization, Airflow orchestration, and separate app/dashboard product
surfaces. The automated test suites cover the app API, lifecycle projection,
worker behavior, stream metrics, configuration, historical ingest, dashboard
bridges, profile scoring, and simulator behavior. The README's grading section
contains the supported local run path for reproducing the application workflow
from the submitted codebase.

## Top-Level Layout

### Root files

- `README.md`: top-level project overview and quickstart.
- `compose.yaml`: the full local development stack.
- `Makefile`: shortcuts for bootstrap, ingest, training, simulation, streaming,
  and tests.
- `.env.example`: documented environment variables.
- `package.json`: Node runtime, scripts, and dependencies.
- `pyproject.toml`: Python package metadata and dependencies.
- `package-lock.json`: pinned Node dependency tree.
- `.nvmrc`: Node version hint.
- `.gitignore`, `.dockerignore`: ignore and container build rules.

### Source folders

- `airflow/`: orchestration DAGs and Airflow plugins.
- `dashboard/`: Streamlit UI and data-access bridge code.
- `docs/`: architecture, dataset, and onboarding docs.
- `historical/`: HandHQ historical ingest code.
- `jobs/`: long-running Spark and batch-style data jobs.
- `ml_model/`: feature engineering, clustering, and profile scoring.
- `poker_platform/`: shared platform libraries for config, storage, schemas,
  Redis, and Kafka contracts.
- `sample_data/`: bundled sample historical dataset.
- `secrets/`: local secret mount points, especially Snowflake keys.
- `simulation/`: Express API, browser app shell, scripts, schemas, tests, and
  the Python poker simulator.
- `sql/`: Postgres and Snowflake DDL.
- `tests/`: Python test suite.

## Folder And File Guide

### `airflow/`

This folder contains orchestration, not business logic.

- `dags/poker_pipeline_bootstrap.py`: one-time Snowflake bootstrap DAG.
- `dags/poker_batch_pipeline.py`: historical HandHQ ingest DAG.
- `dags/poker_modeling_pipeline.py`: feature-build plus profile-training DAG.
- `dags/simulation_batch_pipeline.py`: simulation plus Spark catch-up DAG.
- `dags/kafka_stream_snowflake_catchup.py`: bounded Spark catch-up DAG for
  deployments that do not run the stream continuously.
- `plugins/poker_pipeline_common.py`: shared Airflow constants and subprocess
  helpers for Python and Spark invocations.
- `plugins/snowflake_sql_operator.py`: thin operator for running one or more
  Snowflake SQL statements.

### `dashboard/`

This folder is the Streamlit-facing read layer.

- `app.py`: the Streamlit application with pages for historical profiles, live
  simulation views, compare, and explorer.
- `data_access.py`: read helpers for Snowflake and Redis-backed dashboard data.
- `analytics.py`: dashboard-only transformations and comparisons.
- `api_bridge.py`: structured JSON bridge used by the Node app for performance
  summary, simulation results, and explorer queries.
- `config.py`: small constants for dashboard display behavior.
- `README.md`: dashboard-specific runtime notes.
- `__init__.py`: package marker.

### `docs/`

This folder is the human-facing documentation set.

- `ARCHITECTURE.md`: high-level system architecture.
- `DATASETS.md`: supported historical dataset formats and commands.
- `DATA_MODEL.md`: warehouse table purpose and required fields.
- `CODEBASE_GUIDE.md`: this onboarding guide.

### `historical/historical_ingest/`

This folder is the historical parsing and load path.

- `fetch_phh.py`: resolves local or remote `.phhs` datasets and extracts
  archives.
- `normalize.py`: seat-to-position helper logic.
- `handhq_backfill.py`: canonical historical ingest entrypoint; parses,
  validates, chunks, and loads HandHQ data into Snowflake.
- `__init__.py`: package marker.

### `jobs/`

This folder contains shared jobs that are not part of the app server.

- `stream_kafka.py`: the canonical Kafka ingestion and live-materialization job.
  It writes raw and curated Snowflake tables, computes live metrics, writes
  Redis views, updates dashboard summaries, and refreshes durable run player
  summaries.
- `__init__.py`: package marker.

### `ml_model/profiles/`

This folder owns the historical-style model and session scoring flow.

- `constants.py`: feature names, dataset names, version constants, and
  thresholds.
- `sql.py`: SQL builders for Snowflake feature materialization.
- `reference_features.py`: pure feature-engineering logic from actions/players.
- `build_features.py`: materializes historical feature tables in Snowflake.
- `train_model.py`: trains and stores the clustering model.
- `labels.py`: maps centroids to human-readable style labels and summaries.
- `export_active_model.py`: exports the active model as a scoring artifact.
- `active_model_status.py`: reports the active model metadata.
- `scoring.py`: nearest-centroid scoring logic and feature delta explanations.
- `score_session.py`: scores completed sessions, loads caches, and writes
  `PROFILE_SESSION_RESULTS`.
- `__init__.py`: package marker.

### `poker_platform/`

This folder is the shared infrastructure layer used by Python services.

- `env.py`: robust `.env` loading and path helpers.
- `config.py`: central platform configuration object.
- `schemas.py`: canonical event/request schemas shared across historical and
  simulated data.
- `event_contracts.py`: serializers/deserializers for Kafka event contracts.
- `kafka_utils.py`: Python producer helper utilities.
- `redis_live.py`: Redis live store abstraction for run state and leaderboards.
- `storage.py`: Snowflake warehouse abstraction, schema map, row loading, and
  write helpers.
- `__init__.py`: package marker.

### `sample_data/`

This folder holds repository-managed sample datasets.

- `historical/handhq_6max_sample.phhs`: small HandHQ sample used for smoke
  tests, demos, and deterministic fixtures.

### `secrets/`

This folder is a local mount point for sensitive files.

- `snowflake/rsa_key.p8`: Snowflake private key used by services.
- `snowflake/rsa_key.pub`: matching public key.
- `snowflake/.gitkeep`: keeps the directory structure in git.

### `simulation/`

This folder contains both the Node app and the Python poker simulator.

#### `simulation/schemas/`

- `action_event.schema.json`: JSON schema for action events.
- `hand_summary_event.schema.json`: JSON schema for hand summary events.
- `simulation_request.schema.json`: JSON schema for simulation requests.

#### `simulation/scripts/`

- `migrate-app-db.js`: bootstraps the Postgres app schema.
- `project-simulation-events.js`: consumes Kafka lifecycle events and projects
  them into Postgres.
- `simulate.js`: CLI helper that submits a simulation request to the API and
  polls completion.

#### `simulation/simulator/`

- `types.py`: dataclasses for decisions, state, events, players, and config.
- `cards.py`: deck creation, card utilities, and hand evaluation.
- `personas.py`: built-in persona presets and natural-language persona
  compilation.
- `gating.py`: rules for escalating from heuristic to LLM decisions.
- `llm_policy.py`: OpenAI-backed decision runtime and response shaping.
- `agents.py`: heuristic, gated-LLM, and other agent implementations.
- `engine.py`: core 6-max NLH hand/session simulation engine.
- `run_simulation.py`: high-level simulation runner plus Kafka publishing.
- `session_metadata.py`: backend normalization and context hashing helpers.
- `run_registry.py`: Snowflake-backed run record helper.
- `worker.py`: Kafka consumer that executes simulation requests.
- `__init__.py`: package marker.

#### `simulation/src/`

- `app.js`: creates the Express app.
- `server.js`: boots Postgres, Kafka, and HTTP.
- `routes/events.js`: main API routes for sessions, dashboard/live data, saved
  queries, agents, runs, results, profiles, and event ingestion.

#### `simulation/src/lib/`

- `app-db.js`: Postgres connection pool and schema bootstrap.
- `app-store.js`: Postgres repository layer for users, agents, runs, saved
  queries, and profile jobs.
- `kafka.js`: Node Kafka producer and topic management.
- `python.js`: helper to run Python modules from Node.
- `request-user.js`: resolves request-scoped user identity from headers, query,
  body, or dev defaults.
- `simulation-event-projector.js`: normalizes lifecycle events and updates
  app-owned run state in Postgres.
- `simulation-runs.js`: bridge for Snowflake-backed run records.

#### `simulation/src/public/`

- `index.html`: the operator app shell markup.
- `app.js`: frontend logic for dev session, agents, versions, run launch,
  results, profile requests, and explorer queries.
- `app.css`: browser app shell styling.

#### `simulation/test/`

- `api.test.js`: Node API regression suite.
- `simulation-event-projector.test.js`: lifecycle projector regression suite.

### `sql/`

This folder contains database schema definitions.

- `ddl/postgres/create_tables.sql`: Postgres app schema for users, agents,
  versions, runs, queries, jobs, and preferences.
- `ddl/snowflake/create_tables.sql`: Snowflake raw, curated, feature, model,
  live, profile, and summary tables.

### `tests/`

This folder contains the Python regression suite.

- `test_config_env.py`: env and configuration parsing.
- `test_dashboard_api_bridge.py`: dashboard bridge payloads.
- `test_dashboard_data.py`: dashboard analytics helpers.
- `test_event_contracts.py`: event serialization and shared contract integrity.
- `test_handhq_backfill.py`: HandHQ historical ingest behavior.
- `test_live_dashboard_redis.py`: Redis live-store behavior.
- `test_llm_gating.py`: LLM gating and escalation logic.
- `test_personas.py`: persona presets and NLP compilation.
- `test_profiles.py`: profile feature and scoring behavior.
- `test_simulation_registry.py`: Snowflake-backed run record behavior.
- `test_simulation_summaries.py`: per-run player summary logic.
- `test_simulation_worker.py`: simulation worker behavior.
- `test_simulator_engine.py`: core engine behavior.
- `test_storage.py`: Snowflake storage abstraction.
- `test_streaming_metrics.py`: stream job materialization behavior.

## Runtime Services

The main services defined in `compose.yaml` are:

- `postgres`: app database
- `redis`: live state
- `kafka`: async event bus
- `app`: Express API and browser shell
- `simulation-worker`: Kafka-driven simulation executor
- `simulation-event-projector`: Kafka-driven Postgres lifecycle updater
- `dashboard`: Streamlit dashboard
- `spark-master`, `spark-worker`: Spark infrastructure
- `spark-stream`: continuous Spark Kafka consumer
- `airflow`: orchestration UI and scheduler

## How To Run The Stack

### Initial setup

1. Copy `.env.example` to `.env`.
2. Fill in Snowflake credentials and key path values.
3. Add `OPENAI_API_KEY` if you want the `llm` decision backend.

### Start services

```bash
make up
```

Useful service URLs:

- App API and app shell: `http://localhost:3000`
- App shell page: `http://localhost:3000/app`
- Dashboard: `http://localhost:8501`
- Airflow: `http://localhost:8080`
- Spark master UI: `http://localhost:8088`
- Spark worker UI: `http://localhost:8081`

### Start continuous stream processing

```bash
make stream-live
```

### Load historical sample data

```bash
make ingest-sample-historical
```

### Train the profile model

```bash
make train-profiles
```

## How To Use The Application

### Browser app shell

Open `http://localhost:3000/app`.

Typical workflow:

1. Enter a dev user identity.
2. Create a saved agent with a name, description, and initial context.
3. Add new immutable versions as you refine the context.
4. Launch a simulation from:
   - a raw context string, or
   - a saved agent version
5. Watch run status and live views.
6. Open completed results.
7. Explicitly request profile scoring for a completed run.
8. Browse profile-job history.
9. Use the performance explorer to run structured warehouse-backed queries.

### Streamlit dashboard

Open `http://localhost:8501`.

Pages:

- `Historical Profiles`: active profile model and historical assignments
- `Live Simulation`: live Redis-backed run views
- `Compare`: compare live agents and clusters
- `Explorer`: Snowflake-backed table exploration and filtering

### Direct API usage

Useful endpoints:

- `GET /healthz`
- `GET /api/status`
- `GET /api/session`
- `POST /api/session/dev`
- `POST /api/agents`
- `GET /api/agents`
- `GET /api/agents/:agentDefinitionId`
- `POST /api/agents/:agentDefinitionId/versions`
- `GET /api/agents/:agentDefinitionId/performance`
- `POST /api/simulations`
- `GET /api/simulations`
- `GET /api/simulations/:simulationRunId`
- `GET /api/simulations/:simulationRunId/live`
- `GET /api/simulations/:simulationRunId/results`
- `GET /api/simulations/:simulationRunId/profiles`
- `POST /api/simulations/:simulationRunId/profiles/request`
- `GET /api/simulations/:simulationRunId/profile-jobs`
- `GET /api/performance/summary`
- `GET /api/performance/queries`
- `POST /api/performance/query`
- `GET /api/performance/query/:savedQueryId`

## Common Commands

- `make up`: build and start the full stack
- `make down`: stop the stack
- `make grade-local`: build and start all services, then print grading URLs
- `make ingest-sample-historical`: ingest the bundled sample dataset
- `make ingest-phh`: ingest the configured HandHQ dataset source
- `make train-profiles`: build features and train the profile model
- `make simulate-context CONTEXT="..."`: submit a simulation via the live API
- `make stream-live`: start the continuous Spark streaming service
- `make stream-catchup`: run Spark catch-up in bounded mode
- `make dashboard`: start the dashboard service
- `make test`: run both Python and Node tests
- `npm run migrate:app-db`: bootstrap the Postgres app schema
- `npm run project:simulation-events`: run the lifecycle projector manually
- `npm run simulate`: submit and poll a simulation from the command line
- `npm run consume -- --topic=poker.actions`: inspect Kafka messages manually

## Where To Look First When Debugging

- App/API issue: `simulation/src/routes/events.js`
- Postgres app-data issue: `simulation/src/lib/app-store.js`
- Kafka publishing issue: `simulation/src/lib/kafka.js` or
  `poker_platform/kafka_utils.py`
- Worker issue: `simulation/simulator/worker.py`
- Poker behavior issue: `simulation/simulator/engine.py`
- Persona or LLM escalation issue: `simulation/simulator/personas.py`,
  `simulation/simulator/gating.py`, `simulation/simulator/llm_policy.py`
- Warehouse write issue: `jobs/stream_kafka.py` or `poker_platform/storage.py`
- Dashboard query/result issue: `dashboard/api_bridge.py`
- Historical ingest issue: `historical/historical_ingest/handhq_backfill.py`
- Profile-model issue: `ml_model/profiles/train_model.py` or
  `ml_model/profiles/score_session.py`
