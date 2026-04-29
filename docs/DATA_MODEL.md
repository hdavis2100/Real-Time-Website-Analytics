# Data Model

The project uses a split storage model on purpose. There is no single database
that owns every concern.

## Ownership Split

- PostgreSQL owns app-facing metadata and permissions-adjacent state.
- Snowflake owns historical analytics, completed simulation outputs, and
  profile/model data.
- Redis owns live state only.

## PostgreSQL App Tables

These tables are created from `sql/ddl/postgres/create_tables.sql`.

### Users and identity

- `users`
- `auth_identities`

### Saved agents

- `agent_definitions`
- `agent_versions`

`agent_definitions` store the logical saved agent. `agent_versions` store the
immutable versioned context text and metadata used to launch simulations.

### Simulation ownership and status

- `simulation_runs`
- `simulation_run_agents`

`simulation_runs` is the app system of record for:

- run ownership
- requested backend
- app-facing status
- hero agent/version links
- request payload preview
- requested, started, completed, and updated timestamps

`simulation_run_agents` stores seat-level run participants and optional links to
saved agents or versions.

### Profiles and explorer state

- `profile_jobs`
- `saved_queries`
- `dashboard_preferences`

`profile_jobs` audit explicit profile requests and cached-result refreshes.
`saved_queries` store structured explorer filters. `dashboard_preferences`
stores per-user defaults.

## Snowflake Warehouse Tables

These tables are created from `sql/ddl/snowflake/create_tables.sql`.

### Raw tables

- `RAW_HANDS`
- `RAW_ACTIONS`
- `RAW_PLAYERS`
- `RAW_SIMULATION_RUNS`

These preserve direct ingest or simulation-lineage rows close to source shape.

### Curated tables

- `CURATED_HANDS`
- `CURATED_ACTIONS`
- `CURATED_PLAYERS`

These are the normalized action-level analytics tables used by feature
engineering, dashboard queries, and durable run summaries.

### Feature and model tables

- `PROFILE_PLAYER_HAND_FEATURES`
- `PROFILE_PLAYER_TRAINING_FEATURES`
- `PROFILE_MODEL_RUNS`
- `PROFILE_MODEL_FEATURES`
- `PROFILE_CLUSTER_CENTROIDS`
- `PROFILE_CLUSTER_PROTOTYPES`
- `PROFILE_HISTORICAL_ASSIGNMENTS`
- `PROFILE_SESSION_FEATURES`
- `PROFILE_SESSION_RESULTS`

These support offline archetype training and post-run session scoring.

### Serving and summary tables

- `SIMULATION_RUN_PLAYER_SUMMARIES`
- `LIVE_AGENT_METRICS`
- `LIVE_PROFILE_ASSIGNMENTS`
- `DASHBOARD_SUMMARIES`

`SIMULATION_RUN_PLAYER_SUMMARIES` is the durable completed-run summary source
for the app. It is refreshed from curated simulation actions and players by
`jobs/stream_kafka.py`.

`LIVE_AGENT_METRICS` and `LIVE_PROFILE_ASSIGNMENTS` are warehouse-visible live
or near-live serving tables derived from the stream.

`DASHBOARD_SUMMARIES` stores additional dashboard-oriented aggregates.

## Redis Live State

Redis keys are materialized by `jobs/stream_kafka.py` and power the live UI.
Examples include:

- `live:runs:active`
- `live:run:{run_id}:meta`
- `live:run:{run_id}:leaderboard:profit`
- `live:run:{run_id}:leaderboard:bb_per_100`
- `live:run:{run_id}:leaderboard:high_hand`
- `live:global:leaderboard:profit`
- `live:global:leaderboard:high_hand`
- `live:global:leaderboard:hero_context`
- `live:run:{run_id}:agents`

## Required `CURATED_ACTIONS` Fields

- `hand_id`
- `source_run_id`
- `simulation_run_id`
- `source_type`
- `source_dataset`
- `table_id`
- `action_index`
- `street`
- `player_id`
- `agent_id`
- `seat`
- `position`
- `action_type`
- `amount_bb`
- `pot_before_bb`
- `pot_after_bb`
- `to_call_bb`
- `effective_stack_bb`
- `players_remaining`
- `board_cards_visible`
- `hole_cards_visible`
- `is_all_in`
- `event_ts`
- `backend_type`
- `persona_name`
- `persona_text`
- `payload_version`
- `raw_lineage`

## Warehouse Row Semantics

### `RAW_HANDS` / `CURATED_HANDS`

These hold one row per hand with run lineage, button seat, blinds, board cards,
and total pot.

### `RAW_PLAYERS` / `CURATED_PLAYERS`

These hold one row per player-seat per hand with starting stack, ending stack,
result in big blinds, and persona metadata for simulated runs.

### `PROFILE_PLAYER_HAND_FEATURES`

Per player-hand rollups used to build historical archetype features. Examples:

- VPIP flag
- PFR flag
- aggression action count
- passive action count
- saw flop
- saw river
- all-in flag
- c-bet flag

### `PROFILE_PLAYER_TRAINING_FEATURES`

Player-level aggregates used for offline clustering:

- `vpip_rate`
- `pfr_rate`
- `aggression_frequency`
- `call_preference`
- `flop_cbet_rate`
- `turn_barrel_rate`
- `river_seen_rate`
- `all_in_rate`
- `avg_preflop_raise_bb`
- `avg_postflop_bet_pot_ratio`
- `late_position_vpip_rate`
- `blind_defend_rate`
- `short_stack_aggression_rate`
- `deep_stack_looseness`

### `LIVE_AGENT_METRICS`

Rolling per-agent metrics:

- `metric_window_start`
- `metric_window_end`
- `simulation_run_id`
- `agent_id`
- `player_id`
- `persona_name`
- `actions_per_second`
- `hands_per_second`
- `vpip`
- `pfr`
- `aggression_frequency`
- `cbet_rate`
- `all_in_rate`
- `bb_won`
- `observed_hands`
- `observed_actions`
- `updated_at`

## Serialization Notes

- Card arrays and lineage payloads are serialized as JSON strings for warehouse
  portability.
- Monetary quantities are normalized to big blinds where practical.
- Full agent context text stays in Postgres; warehouse rows carry stable IDs,
  hashes, and previews where useful.
- Simulation hole cards are retained only for simulation/debug use, not as
  leaked historical training features.
