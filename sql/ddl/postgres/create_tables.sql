create table if not exists users (
  id text primary key,
  email text unique,
  display_name text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists auth_identities (
  id text primary key,
  user_id text not null references users(id) on delete cascade,
  provider text not null,
  provider_subject text not null,
  created_at timestamptz not null default now(),
  unique (provider, provider_subject)
);

alter table auth_identities
  add column if not exists password_hash text;

create table if not exists app_sessions (
  id text primary key,
  user_id text not null references users(id) on delete cascade,
  session_token_hash text not null unique,
  ip_address text,
  user_agent text,
  created_at timestamptz not null default now(),
  expires_at timestamptz not null
);

create table if not exists agent_definitions (
  id text primary key,
  user_id text not null references users(id) on delete cascade,
  name text not null,
  description text,
  is_archived boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists agent_versions (
  id text primary key,
  agent_definition_id text not null references agent_definitions(id) on delete cascade,
  version_number integer not null,
  context_text text not null,
  context_hash text,
  decision_backend_default text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (agent_definition_id, version_number)
);

create table if not exists simulation_runs (
  id text primary key,
  user_id text references users(id) on delete set null,
  hero_agent_definition_id text references agent_definitions(id) on delete set null,
  hero_agent_version_id text references agent_versions(id) on delete set null,
  status text not null,
  decision_backend text not null,
  backend_type text,
  model_name text,
  request_mode text not null,
  hand_count integer not null,
  seed integer,
  hero_seat integer,
  hero_context_hash text,
  hero_context_preview text,
  request_payload jsonb not null default '{}'::jsonb,
  error_message text,
  requested_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  updated_at timestamptz not null default now()
);

alter table simulation_runs
  add column if not exists published_actions integer not null default 0;

alter table simulation_runs
  add column if not exists published_hand_summaries integer not null default 0;

alter table simulation_runs
  add column if not exists player_summaries jsonb not null default '[]'::jsonb;

alter table simulation_runs
  add column if not exists profile_session_features jsonb not null default '[]'::jsonb;

alter table simulation_runs
  add column if not exists hero_profit_timeseries jsonb not null default '[]'::jsonb;

create table if not exists simulation_run_agents (
  id text primary key,
  simulation_run_id text not null references simulation_runs(id) on delete cascade,
  seat integer not null,
  player_id text,
  agent_id text,
  persona_name text,
  agent_definition_id text references agent_definitions(id) on delete set null,
  agent_version_id text references agent_versions(id) on delete set null,
  is_hero_player boolean not null default false,
  unique (simulation_run_id, seat)
);

create table if not exists profile_jobs (
  id text primary key,
  simulation_run_id text not null references simulation_runs(id) on delete cascade,
  user_id text references users(id) on delete set null,
  agent_definition_id text references agent_definitions(id) on delete set null,
  agent_version_id text references agent_versions(id) on delete set null,
  player_id text,
  agent_id text,
  status text not null,
  refresh_requested boolean not null default false,
  requested_at timestamptz not null default now(),
  completed_at timestamptz,
  error_message text
);

create table if not exists saved_queries (
  id text primary key,
  user_id text not null references users(id) on delete cascade,
  name text not null,
  query_type text not null,
  filters jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists dashboard_preferences (
  id text primary key,
  user_id text not null references users(id) on delete cascade,
  default_decision_backend text,
  default_run_filter jsonb not null default '{}'::jsonb,
  default_dashboard_view text,
  updated_at timestamptz not null default now()
);

create index if not exists idx_agent_definitions_user_id
  on agent_definitions (user_id, updated_at desc);

create index if not exists idx_agent_versions_agent_definition_id
  on agent_versions (agent_definition_id, version_number desc);

create index if not exists idx_simulation_runs_user_id
  on simulation_runs (user_id, requested_at desc);

create index if not exists idx_simulation_runs_status
  on simulation_runs (status, requested_at desc);

create index if not exists idx_simulation_runs_decision_backend
  on simulation_runs (decision_backend, requested_at desc);

create index if not exists idx_profile_jobs_simulation_run_id
  on profile_jobs (simulation_run_id, requested_at desc);

create index if not exists idx_app_sessions_user_id
  on app_sessions (user_id, created_at desc);

create index if not exists idx_app_sessions_expires_at
  on app_sessions (expires_at);
