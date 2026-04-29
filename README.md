# Real-Time Action-Level Poker Analytics

This repo is a containerized poker analytics platform built around one shared
action-level schema and two primary product surfaces:

- An Express API plus lightweight browser app shell for simulation operations
- A Streamlit dashboard for historical and live analytics

The active runtime is split by responsibility:

- PostgreSQL: app data such as users, saved agents, agent versions, run
  ownership, saved queries, and profile jobs
- Snowflake: historical warehouse, completed run results, profile outputs, and
  explorer queries
- Redis: live run state and leaderboards
- Kafka: async transport between API, worker, and stream jobs
- Spark: Kafka ingestion, live materialization, and durable summary refreshes
- Airflow: orchestration for bootstrap, ingest, training, and batch workflows

## Architecture

### Historical lane

1. Historical HandHQ `.phhs` data is resolved and canonicalized into shared
   hand, player, and action rows.
2. Raw and curated warehouse tables are written into Snowflake.
3. Player feature tables are built from curated action-level data.
4. A clustering model is trained and activated for profile scoring.

### Simulation lane

1. `POST /api/simulations` creates a Postgres-backed simulation run in
   `queued` state and publishes a Kafka request.
2. The Python worker consumes `poker.simulation_requests` and runs either
   heuristic or LLM-gated 6-max NLH agents.
3. The worker publishes actions, hand summaries, and lifecycle events.
4. The Node lifecycle projector consumes `poker.simulation_events` and updates
   Postgres run status.
5. Spark consumes the Kafka topics, writes Snowflake rows, refreshes
   `SIMULATION_RUN_PLAYER_SUMMARIES`, and updates Redis live state.

### Serving lane

- The Express app serves `/api` and the browser shell at `/app`.
- The browser shell reads Postgres-backed metadata through the API, Redis for
  live state, and Snowflake-backed summaries through dashboard bridge calls.
- The Streamlit dashboard reads Snowflake for historical/explorer pages and
  Redis for live simulation pages.

## Main Services

- `app`: Express API and browser shell on `http://localhost:3000`
- `postgres`: app database
- `redis`: live state store
- `kafka`: event transport
- `simulation-worker`: Python Kafka consumer that executes simulations
- `simulation-event-projector`: Node Kafka consumer that projects lifecycle
  events into Postgres
- `spark-stream`: continuous Spark job consuming `poker.actions`,
  `poker.hand_summaries`, and `poker.simulation_events`
- `airflow`: Airflow UI on `http://localhost:8080`
- `dashboard`: Streamlit UI on `http://localhost:8501`
- `spark-master`, `spark-worker`: Spark infrastructure

## Documentation

- [docs/CODEBASE_GUIDE.md](docs/CODEBASE_GUIDE.md): practical onboarding guide,
  folder/file map, verified paths, and how to use the app
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): high-level system design
- [docs/DATA_MODEL.md](docs/DATA_MODEL.md): app, warehouse, and live data model
- [docs/DATASETS.md](docs/DATASETS.md): supported historical dataset sources

## Repository Layout

- `airflow/`: orchestration DAGs and Airflow helpers
- `dashboard/`: Streamlit UI and Snowflake/Redis read bridge
- `docs/`: architecture, data model, dataset, and onboarding docs
- `historical/`: HandHQ source resolution and historical backfill
- `jobs/`: Spark streaming and materialization jobs
- `ml_model/`: profile feature engineering, clustering, and scoring
- `poker_platform/`: shared Python config, storage, Redis, and schema helpers
- `simulation/`: Express API, browser app shell, scripts, tests, and simulator
- `sql/`: Postgres and Snowflake DDL
- `sample_data/`, `secrets/`, `tests/`: sample inputs, secret mount points, and
  Python tests

## Quickstart

1. `cp .env.example .env`
2. Fill in Snowflake credentials and key path values in `.env`
3. Add `OPENAI_API_KEY` if you want the `llm` decision backend
4. `make up`
5. If needed, bootstrap Snowflake objects through Airflow
6. `make ingest-sample-historical` or `make ingest-phh`
7. `make train-profiles`
8. Open `http://localhost:3000/app` for the operator shell
9. Open `http://localhost:8501` for the Streamlit dashboard

## Grading The Project

This submitted folder is intended to run locally with Docker Compose. It includes
the `.env` file, an `OPENAI_API_KEY`, and Snowflake key material under
`secrets/snowflake/`. No local Python or Node dependency install is required when
using Docker.

Prerequisites:

- Docker Desktop with Docker Compose
- Internet access for Docker images, Python packages, Node packages, and Spark
  packages
- Available local ports: `3000`, `5432`, `6379`, `7077`, `8080`, `8081`,
  `8088`, `8501`, `9092`
- Included credential files:
  - `.env`
  - `secrets/snowflake/rsa_key.p8`

Start the grading stack:

```bash
make grade-local
```

If `make` is not installed, run the equivalent Docker Compose command instead:

```bash
docker compose up -d --build --wait --wait-timeout 600
```

This command builds and starts the Docker Compose stack, then prints the UI URLs.
It does not load data, retrain profiles, or submit simulations by itself.

After startup, open:

- App shell: `http://localhost:3000/app`
- Streamlit dashboard: `http://localhost:8501`
- Airflow: `http://localhost:8080` with username `airflow` and password
  `airflow`
- Spark master UI: `http://localhost:8088`
- Spark worker UI: `http://localhost:8081`

Historical dataset configuration (Optional, historical data already loaded in Snowflake):

- `HISTORICAL_FIXTURE_PATH` is only the small smoke-test fixture. It is not the
  full historical corpus.
- Full historical ingestion uses `PHH_DATASET_SOURCE`. Run `make ingest-phh` to
  load that source through `historical_ingest.handhq_backfill`.
- The historical corpus used for this project is the HandHQ no-limit hold'em
  portion of [A Dataset of Poker Hand Histories](https://zenodo.org/records/13997158),
  also mirrored in the [uoftcprg/phh-dataset](https://github.com/uoftcprg/phh-dataset)
  repository. The HandHQ portion contains 21,605,687 uncorrupted anonymized
  online no-limit hold'em hands from July 1-23, 2009, across Absolute Poker,
  Full Tilt Poker, iPoker Network, Ongame Network, PokerStars, and PartyPoker.
- The submitted `.env` expects the extracted HandHQ dataset at:

  ```bash
  PHH_DATASET_SOURCE=sample_data/historical/phh-dataset/data/handhq
  ```

- If the extracted dataset is present at that path, no historical dataset
  environment changes are needed.
- If the extracted dataset is not present, download and extract it using the
  commands below before running `make ingest-phh`.
- Do not use a host-specific absolute path such as
  `/tmp/phh-dataset-scout/data/handhq` unless the Docker Compose file is also
  updated to mount that path into the containers.
- `PHH_CACHE_DIR=runtime/phh_cache` can stay unchanged. It is a generated local
  cache path.

To run the historical ingestion with the public HandHQ dataset:

1. Download and extract the PHH dataset archive. The compact v2 Zenodo archive is
   the closest match for this project's HandHQ-only historical workflow:

   ```bash
   mkdir -p sample_data/historical/phh-dataset
   curl -L "https://zenodo.org/records/13997158/files/poker-hand-histories.zip?download=1" \
     -o sample_data/historical/poker-hand-histories.zip
   unzip -q sample_data/historical/poker-hand-histories.zip \
     -d sample_data/historical/phh-dataset
   ```

2. Confirm `.env` points at the extracted HandHQ directory. The extracted
   directory should contain a `data/handhq` folder; if the archive layout
   differs, use the actual extracted HandHQ folder path.

   ```bash
   PHH_DATASET_SOURCE=sample_data/historical/phh-dataset/data/handhq
   ```

3. Start the stack and run ingestion:

   ```bash
   make grade-local
   make ingest-phh
   ```

The newer Zenodo v3 record at `https://zenodo.org/records/17136841` is the
latest full archive and still includes the same HandHQ corpus, but it is much
larger because it also includes ACPC data. For grading this project, use the
extracted `data/handhq` subdirectory rather than asking the app to ingest the
entire archive.

Submission contents:

- `.env` and `secrets/snowflake/rsa_key.p8` are required for the provided
  Snowflake and OpenAI-backed grading setup.

## Commands

- `make up`
  Builds and starts the full local stack.
- `make down`
  Stops the stack.
- `make grade-local`
  Builds and starts all Docker Compose services, then prints the grading URLs.
- `make fetch-phh`
  Resolves the configured HandHQ `.phhs` source into the runtime cache.
- `make ingest-sample-historical`
  Runs the sample HandHQ backfill inside the Airflow container.
- `make ingest-phh`
  Loads `PHH_DATASET_SOURCE`, which must resolve to HandHQ `.phhs` data.
- `make train-profiles`
  Runs feature building and profile training inside the Airflow container.
- `make simulate-demo`
  Runs a demo simulation workflow and publishes events to Kafka.
- `make simulate-context`
  Runs one context-controlled hero against baseline opponents.
- `make stream-live`
  Starts the continuous Spark streaming service.
- `make stream-catchup`
  Runs a bounded Spark `availableNow` catch-up load from Kafka into Snowflake.
- `make dashboard`
  Starts the Streamlit dashboard service.
- `make test`
  Runs the Python and Node test suites locally.

## Browser App

The browser app shell lives at `http://localhost:3000/app` and currently
supports:

- dev-session sign-in for a local user identity
- saved agent creation and immutable versioning
- simulation launch from raw context or a saved version
- run list and run detail views
- live and completed results
- explicit profile requests plus profile job history
- structured performance explorer queries

## API

The main API routes live under `/api`:

- Session and health
  - `GET /api/status`
  - `GET /api/session`
  - `POST /api/session/dev`
- Dashboard and performance
  - `GET /api/dashboard/live`
  - `GET /api/performance/summary`
  - `GET /api/performance/queries`
  - `POST /api/performance/query`
  - `GET /api/performance/query/:savedQueryId`
- Agents
  - `POST /api/agents`
  - `GET /api/agents`
  - `GET /api/agents/:agentDefinitionId`
  - `POST /api/agents/:agentDefinitionId/versions`
  - `GET /api/agents/:agentDefinitionId/performance`
- Simulations
  - `POST /api/simulations`
  - `GET /api/simulations`
  - `GET /api/simulations/:simulationRunId`
  - `GET /api/simulations/:simulationRunId/live`
  - `GET /api/simulations/:simulationRunId/results`
  - `GET /api/simulations/:simulationRunId/profiles`
  - `POST /api/simulations/:simulationRunId/profiles/request`
  - `GET /api/simulations/:simulationRunId/profile-jobs`
- Event ingestion
  - `POST /api/actions`
  - `POST /api/actions/batch`
  - `POST /api/hand-summaries`

`POST /api/simulations` is async. The request can launch from raw context or a
saved agent version. A typical raw-context request body is:

```json
{
  "simulation_run_id": "sim_123",
  "context": "balanced aggressive value bettor",
  "decision_backend": "llm",
  "hand_count": 500,
  "seed": 42,
  "hero_seat": 1,
  "small_blind_bb": 0.5,
  "big_blind_bb": 1.0,
  "starting_stack_bb": 100
}
```

`decision_backend` defaults to `llm`. Set it to `heuristic` for a much faster,
model-free run that still uses the same hero-context flow and baseline villain
archetypes.

`GET /api/simulations/:simulationRunId/profiles` is a cached read of scored
profile results. To generate or refresh those results, use
`POST /api/simulations/:simulationRunId/profiles/request`.

When the app database is configured, simulation list/detail ownership and
status are Postgres-backed. Completed results remain Snowflake-backed and live
views remain Redis-backed.

Health checks:

- `GET /`
- `GET /healthz`

## Data Model

The active runtime uses multiple stores on purpose.

Postgres app tables include:

- `users`, `auth_identities`
- `agent_definitions`, `agent_versions`
- `simulation_runs`, `simulation_run_agents`
- `profile_jobs`
- `saved_queries`
- `dashboard_preferences`

Snowflake tables include:

- Raw and curated data:
  `RAW_HANDS`, `RAW_ACTIONS`, `RAW_PLAYERS`, `RAW_SIMULATION_RUNS`,
  `CURATED_HANDS`, `CURATED_ACTIONS`, `CURATED_PLAYERS`
- Profile/model outputs:
  `PROFILE_PLAYER_HAND_FEATURES`, `PROFILE_PLAYER_TRAINING_FEATURES`,
  `PROFILE_MODEL_RUNS`, `PROFILE_MODEL_FEATURES`,
  `PROFILE_CLUSTER_CENTROIDS`, `PROFILE_CLUSTER_PROTOTYPES`,
  `PROFILE_HISTORICAL_ASSIGNMENTS`, `PROFILE_SESSION_FEATURES`,
  `PROFILE_SESSION_RESULTS`
- Serving summaries:
  `SIMULATION_RUN_PLAYER_SUMMARIES`, `LIVE_AGENT_METRICS`,
  `LIVE_PROFILE_ASSIGNMENTS`, `DASHBOARD_SUMMARIES`

Redis keys serve live run and leaderboard views. See
[`dashboard/README.md`](dashboard/README.md) for the live-key map.

## Runtime Notes

- Kafka is required for the API and worker flow.
- Postgres is the app system of record; Snowflake is the analytics system of
  record.
- Historical dashboard pages read Snowflake-populated tables.
- Live Simulation and Compare read Redis-backed live state.
- Spark streaming is the canonical Kafka-to-Snowflake ingestion path for raw
  warehouse rows, live metrics, and completed simulation summaries.
