import express from "express";
import { createHash } from "node:crypto";
import { z } from "zod";

import {
  isKafkaConnected as defaultIsKafkaConnected,
  publishActionEvents as defaultPublishActionEvents,
  publishHandSummaries as defaultPublishHandSummaries,
  publishSimulationEvents as defaultPublishSimulationEvents,
  publishSimulationRequests as defaultPublishSimulationRequests,
  topics as defaultTopics
} from "../lib/kafka.js";
import {
  getSimulationRun as defaultGetSimulationRun,
  markSimulationRunFailed as defaultMarkSimulationRunFailed,
  queueSimulationRun as defaultQueueSimulationRun
} from "../lib/simulation-runs.js";
import {
  parsePythonJson as defaultParsePythonJson,
  runPythonModule as defaultRunPythonModule
} from "../lib/python.js";
import {
  AppStoreConflictError,
  getAppStore as defaultGetAppStore
} from "../lib/app-store.js";
import {
  buildSessionExpiry,
  clearSessionCookie,
  createSessionToken,
  getSessionTokenFromRequest,
  hashPassword,
  setSessionCookie,
  validatePasswordStrength,
  verifyPassword
} from "../lib/auth.js";
import { resolveRequestUser } from "../lib/request-user.js";

const ACTION_TYPES = [
  "post_ante",
  "post_small_blind",
  "post_big_blind",
  "fold",
  "check",
  "call",
  "bet",
  "raise",
  "all_in",
  "collect_pot",
  "show",
  "muck"
];

const DECISION_BACKEND_OPTIONS = {
  llm: {
    backendType: "llm_gated_nano",
    modelName: "gpt-5.4-nano",
    reasoningEffort: "none"
  },
  heuristic: {
    backendType: "heuristic_persona",
    modelName: null,
    reasoningEffort: null
  }
};

const PERFORMANCE_QUERY_DIMENSIONS = [
  "simulation_run_id",
  "decision_backend",
  "hero_context_hash",
  "player_id",
  "agent_id",
  "persona_name",
  "backend_type",
  "final_rank",
  "profile_status",
  "cluster_label"
];

const PERFORMANCE_QUERY_SORT_FIELDS = [
  ...PERFORMANCE_QUERY_DIMENSIONS,
  "updated_at",
  "hands_played",
  "total_bb_won",
  "bb_per_100",
  "confidence_score",
  "run_count",
  "total_hands",
  "avg_bb_per_100",
  "avg_finish_rank",
  "first_place_rate",
  "ready_profiles",
  "avg_confidence_score"
];

const MIN_PROFILE_SESSION_HANDS = 50;

const actionEventSchema = z.object({
  event_type: z.literal("action").default("action"),
  payload_version: z.string().default("1"),
  source_type: z.enum(["historical", "simulated"]),
  source_dataset: z.string().optional().nullable(),
  source_run_id: z.string().optional().nullable(),
  simulation_run_id: z.string().optional().nullable(),
  user_id: z.string().optional().nullable(),
  decision_backend: z.string().optional().nullable(),
  hero_context_hash: z.string().optional().nullable(),
  hero_seat: z.number().int().min(1).max(6).optional().nullable(),
  is_hero_player: z.boolean().default(false),
  table_id: z.string(),
  hand_id: z.string(),
  action_index: z.number().int().nonnegative(),
  street: z.enum(["preflop", "flop", "turn", "river", "showdown"]),
  player_id: z.string(),
  agent_id: z.string().optional().nullable(),
  seat: z.number().int().min(1).max(6),
  position: z.string(),
  action_type: z.enum(ACTION_TYPES),
  amount_bb: z.number().optional().nullable(),
  pot_before_bb: z.number().nonnegative(),
  pot_after_bb: z.number().nonnegative(),
  to_call_bb: z.number().nonnegative(),
  effective_stack_bb: z.number().nonnegative(),
  players_remaining: z.number().int().min(1).max(6),
  board_cards_visible: z.array(z.string()).default([]),
  hole_cards_visible: z.array(z.string()).default([]),
  is_all_in: z.boolean().default(false),
  event_ts: z.string().datetime().optional(),
  backend_type: z.string().default("unknown"),
  persona_name: z.string().optional().nullable(),
  persona_text: z.string().optional().nullable(),
  raw_lineage: z.record(z.any()).default({})
});

const actionBatchSchema = z.object({
  events: z.array(actionEventSchema).min(1)
});

const handSummarySchema = z.object({
  event_type: z.literal("hand_summary").default("hand_summary"),
  payload_version: z.string().default("1"),
  source_type: z.enum(["historical", "simulated"]),
  source_dataset: z.string().optional().nullable(),
  source_run_id: z.string().optional().nullable(),
  simulation_run_id: z.string().optional().nullable(),
  user_id: z.string().optional().nullable(),
  decision_backend: z.string().optional().nullable(),
  hero_context_hash: z.string().optional().nullable(),
  hero_seat: z.number().int().min(1).max(6).optional().nullable(),
  table_id: z.string(),
  hand_id: z.string(),
  button_seat: z.number().int().min(1).max(6),
  board_cards: z.array(z.string()).default([]),
  started_at: z.string().datetime(),
  finished_at: z.string().datetime(),
  winner_player_ids: z.array(z.string()).default([]),
  total_pot_bb: z.number().nonnegative(),
  rake_bb: z.number().nonnegative().default(0),
  backend_type: z.string().default("unknown"),
  showdown_player_ids: z.array(z.string()).default([]),
  player_hole_cards: z.record(z.array(z.string())).default({}),
  raw_lineage: z.record(z.any()).default({})
});

const simulationRequestSchema = z
  .object({
    simulation_run_id: z.string(),
    agent_definition_id: z.string().optional().nullable(),
    agent_version_id: z.string().optional().nullable(),
    hand_count: z.number().int().min(1).max(5000).default(500),
    seed: z.number().int().default(42),
    small_blind_bb: z.number().positive().default(0.5),
    big_blind_bb: z.number().positive().default(1.0),
    starting_stack_bb: z.number().positive().default(100),
    reset_stacks_each_hand: z.boolean().default(true),
    context: z.string().trim().min(1).optional().nullable(),
    hero_seat: z.number().int().min(1).max(6).default(1),
    user_id: z.string().nullable().optional(),
    decision_backend: z.enum(["llm", "heuristic"]).nullable().optional()
  })
  .strict()
  .superRefine((value, ctx) => {
    if (!value.context && !value.agent_version_id) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["context"],
        message: "Either context or agent_version_id is required"
      });
    }
  });

const createAgentSchema = z
  .object({
    name: z.string().trim().min(1),
    description: z.string().trim().optional().nullable(),
    context: z.string().trim().min(1),
    decision_backend_default: z.enum(["llm", "heuristic"]).optional().nullable(),
    metadata: z.record(z.any()).optional().default({}),
    user_id: z.string().optional().nullable(),
    user_email: z.string().email().optional().nullable(),
    user_display_name: z.string().trim().optional().nullable()
  })
  .strict();

const createAgentVersionSchema = z
  .object({
    context: z.string().trim().min(1),
    decision_backend_default: z.enum(["llm", "heuristic"]).optional().nullable(),
    metadata: z.record(z.any()).optional().default({}),
    user_id: z.string().optional().nullable(),
    user_email: z.string().email().optional().nullable(),
    user_display_name: z.string().trim().optional().nullable()
  })
  .strict();

const profileRequestSchema = z
  .object({
    player_id: z.string().trim().optional().nullable(),
    agent_id: z.string().trim().optional().nullable(),
    refresh: z.boolean().optional().default(false)
  })
  .strict();

const devSessionSchema = z
  .object({
    user_id: z.string().trim().min(1),
    user_email: z.string().email().optional().nullable(),
    user_display_name: z.string().trim().optional().nullable()
  })
  .strict();

const registerSessionSchema = z
  .object({
    email: z.string().trim().email(),
    display_name: z.string().trim().min(1).max(120),
    password: z.string().min(8).max(200)
  })
  .strict();

const loginSessionSchema = z
  .object({
    email: z.string().trim().email(),
    password: z.string().min(1).max(200)
  })
  .strict();

const performanceQueryFiltersSchema = z
  .object({
    simulation_run_ids: z.array(z.string().trim().min(1)).max(200).optional().default([]),
    hero_context_hashes: z
      .array(z.string().trim().min(1))
      .max(200)
      .optional()
      .default([]),
    player_ids: z.array(z.string().trim().min(1)).max(200).optional().default([]),
    agent_ids: z.array(z.string().trim().min(1)).max(200).optional().default([]),
    persona_names: z.array(z.string().trim().min(1)).max(200).optional().default([]),
    profile_statuses: z
      .array(z.string().trim().min(1))
      .max(20)
      .optional()
      .default([]),
    cluster_labels: z
      .array(z.string().trim().min(1))
      .max(50)
      .optional()
      .default([]),
    decision_backend: z.enum(["llm", "heuristic"]).optional().nullable(),
    status: z.enum(["queued", "running", "completed", "failed"]).optional().nullable(),
    hero_agent_definition_id: z.string().trim().optional().nullable(),
    hero_agent_version_id: z.string().trim().optional().nullable(),
    date_from: z.string().trim().optional().nullable(),
    date_to: z.string().trim().optional().nullable(),
    min_hands_played: z.number().int().nonnegative().optional().nullable(),
    max_final_rank: z.number().int().positive().optional().nullable(),
    min_total_bb_won: z.number().optional().nullable(),
    max_total_bb_won: z.number().optional().nullable(),
    min_bb_per_100: z.number().optional().nullable(),
    max_bb_per_100: z.number().optional().nullable()
  })
  .strict();

const performanceQuerySortSchema = z
  .object({
    field: z.enum(PERFORMANCE_QUERY_SORT_FIELDS),
    direction: z.enum(["asc", "desc"]).default("desc")
  })
  .strict();

const performanceQueryRequestSchema = z
  .object({
    name: z.string().trim().min(1).optional().nullable(),
    query_type: z.literal("hero_performance").default("hero_performance"),
    filters: performanceQueryFiltersSchema.default({}),
    group_by: z.array(z.enum(PERFORMANCE_QUERY_DIMENSIONS)).max(3).default([]),
    sort: performanceQuerySortSchema.optional().nullable(),
    save: z.boolean().default(false),
    topn: z.number().int().positive().max(100).default(25)
  })
  .strict();

function heroContextHash(context) {
  const normalized = String(context || "").trim();
  if (!normalized) {
    return null;
  }
  return createHash("sha256").update(normalized).digest("hex");
}

function validationError(res, error) {
  return res.status(400).json({
    message: "Validation failed",
    issues: error.issues
  });
}

function parseRefreshFlag(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "t", "yes", "y"].includes(normalized);
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function parseNonNegativeInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

function asFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizePlayerSummaries(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }
  return rows
    .filter((row) => row && typeof row === "object")
    .map((row) => ({ ...row }));
}

function sortPlayerSummaries(rows, sortColumns = []) {
  const normalized = normalizePlayerSummaries(rows);
  return normalized.sort((left, right) => {
    for (const column of sortColumns) {
      const direction = column.desc ? -1 : 1;
      const leftValue = left[column.key];
      const rightValue = right[column.key];
      const leftNumber = asFiniteNumber(leftValue);
      const rightNumber = asFiniteNumber(rightValue);
      if (leftNumber != null || rightNumber != null) {
        if (leftNumber == null) {
          return 1;
        }
        if (rightNumber == null) {
          return -1;
        }
        if (leftNumber !== rightNumber) {
          return leftNumber > rightNumber ? direction : -direction;
        }
        continue;
      }
      const leftText = String(leftValue ?? "");
      const rightText = String(rightValue ?? "");
      if (leftText === rightText) {
        continue;
      }
      return leftText > rightText ? direction : -direction;
    }
    return 0;
  });
}

function buildPlayerSummaryLeaderboard(rows, metric, topn = 10) {
  const normalizedTopn = Math.max(1, parsePositiveInt(topn, 10));
  if (!metric) {
    return [];
  }
  return sortPlayerSummaries(rows, [
    { key: metric, desc: true },
    { key: "total_bb_won", desc: true },
    { key: "final_rank", desc: false },
    { key: "seat", desc: false }
  ]).slice(0, normalizedTopn);
}

function normalizeDashboardResultRows(rows, selectedRunMeta = null) {
  const selectedRunId = String(selectedRunMeta?.simulation_run_id || "").trim();
  const userId = selectedRunMeta?.user_id || null;
  const decisionBackend = selectedRunMeta?.decision_backend || null;
  const status = normalizedRunStatus(selectedRunMeta?.status);
  return normalizePlayerSummaries(rows).map((row) => {
    const normalized = {
      ...row,
      simulation_run_id: row.simulation_run_id || selectedRunId || null,
      user_id: row.user_id || userId,
      decision_backend: row.decision_backend || decisionBackend
    };
    if (!normalized.status && status !== "unknown") {
      normalized.status = status;
    }
    if (normalized.bb_won == null && normalized.total_bb_won != null) {
      normalized.bb_won = normalized.total_bb_won;
    }
    if (normalized.total_bb_won == null && normalized.bb_won != null) {
      normalized.total_bb_won = normalized.bb_won;
    }
    return normalized;
  });
}

function buildResultSummaryFromPlayerSummaries(rows) {
  const normalized = normalizePlayerSummaries(rows);
  if (!normalized.length) {
    return null;
  }
  const ordered = sortPlayerSummaries(normalized, [
    { key: "final_rank", desc: false },
    { key: "total_bb_won", desc: true },
    { key: "seat", desc: false }
  ]);
  const hero =
    normalized.find((row) => Boolean(row.is_hero_player)) || null;
  return {
    winner: ordered[0] || null,
    hero
  };
}

function buildResultPayload(rows) {
  const playerSummaries = normalizePlayerSummaries(rows);
  return {
    performance: playerSummaries,
    summary: buildResultSummaryFromPlayerSummaries(playerSummaries)
  };
}

function parseDateMaybe(value) {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return null;
  }
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function hasExecutionFinished(run) {
  if (!run || typeof run !== "object") {
    return false;
  }
  if (parseDateMaybe(run.completed_at) || parseDateMaybe(run.finished_at)) {
    return true;
  }
  const handCount = parsePositiveInt(run.hand_count, 0);
  const publishedHandSummaries = parseNonNegativeInt(run.published_hand_summaries, 0);
  return handCount > 0 && publishedHandSummaries >= handCount;
}

function isMaterializingStatus(value) {
  return String(value || "").trim().toLowerCase() === "materializing";
}

function normalizedDecisionBackend(value) {
  return value === "heuristic" ? "heuristic" : "llm";
}

function resolveDashboardUrl(req) {
  const configured = String(process.env.APP_DASHBOARD_URL || "").trim();
  if (configured) {
    return configured;
  }

  const hostHeader = String(
    req.get("x-forwarded-host") || req.get("host") || "localhost:3000"
  ).trim();
  const hostname = hostHeader.includes(":")
    ? hostHeader.slice(0, hostHeader.lastIndexOf(":"))
    : hostHeader;
  const protocol = String(req.get("x-forwarded-proto") || req.protocol || "http");
  const port = String(process.env.DASHBOARD_PORT || "8501").trim() || "8501";
  return `${protocol}://${hostname}:${port}`;
}

function requestIpAddress(req) {
  return String(
    req.get("x-forwarded-for") ||
      req.socket?.remoteAddress ||
      req.ip ||
      ""
  )
    .split(",")[0]
    .trim() || null;
}

function serializeSessionUser(requestUser, req) {
  if (!requestUser?.userId) {
    return null;
  }
  return {
    user_id: requestUser.userId,
    email: requestUser.email || null,
    display_name: requestUser.displayName || null,
    auth_mode: requestUser.authMode || "dev",
    dashboard_url: resolveDashboardUrl(req)
  };
}

function appDatabaseUnavailable(res) {
  return res.status(503).json({
    message: "App database is not configured"
  });
}

function missingUserError(res) {
  return res.status(400).json({
    message: "A user_id is required for this endpoint"
  });
}

async function ensureUserRecord(appStore, requestUser) {
  if (!appStore || !requestUser?.userId) {
    return null;
  }
  return appStore.ensureUser(requestUser);
}

async function createLoginSession(appStore, req, res, user) {
  if (!appStore?.createSession || !user?.user_id) {
    return null;
  }

  const sessionToken = createSessionToken();
  const expiresAt = buildSessionExpiry();
  const session = await appStore.createSession({
    userId: user.user_id,
    sessionToken,
    expiresAt,
    ipAddress: requestIpAddress(req),
    userAgent: req.get("user-agent") || null
  });
  setSessionCookie(res, sessionToken, expiresAt, req);
  return session;
}

async function createProfileJobRecord(
  appStore,
  {
    simulationRunId,
    requestUser = null,
    run = null,
    playerId = null,
    agentId = null,
    refreshRequested = false
  } = {}
) {
  if (!appStore?.createProfileJob) {
    return null;
  }

  try {
    return await appStore.createProfileJob({
      simulationRunId,
      userId: requestUser?.userId || run?.user_id || null,
      agentDefinitionId: run?.hero_agent_definition_id || null,
      agentVersionId: run?.hero_agent_version_id || null,
      playerId,
      agentId,
      status: "running",
      refreshRequested
    });
  } catch (_error) {
    return null;
  }
}

async function updateProfileJobRecord(
  appStore,
  profileJob,
  {
    status = null,
    completedAt = new Date().toISOString(),
    errorMessage = null
  } = {}
) {
  const profileJobId = profileJob?.profile_job_id || profileJob?.id || null;
  if (!appStore?.updateProfileJob || !profileJobId) {
    return profileJob;
  }

  try {
    const updated = await appStore.updateProfileJob(profileJobId, {
      status,
      completedAt,
      errorMessage
    });
    return updated || profileJob;
  } catch (_error) {
    return profileJob;
  }
}

async function loadLivePayload({
  runPythonModule,
  parsePythonJson,
  simulationRunId = null,
  requestUser = null,
  decisionBackend = null,
  topn = 25
}) {
  const args = ["live", "--topn", String(parsePositiveInt(topn, 25))];
  if (simulationRunId) {
    args.push("--simulation-run-id", String(simulationRunId));
  }
  if (requestUser?.userId) {
    args.push("--user-id", requestUser.userId);
  }
  if (decisionBackend) {
    args.push("--decision-backend", normalizedDecisionBackend(decisionBackend));
  }

  const rawResult = await runPythonModule("dashboard.api_bridge", args);
  const payload = parsePythonJson(rawResult);
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid live dashboard payload");
  }
  return payload;
}

async function loadPerformancePayload({
  runPythonModule,
  parsePythonJson,
  simulationRunIds = [],
  requestUser = null,
  decisionBackend = null,
  topn = 10
}) {
  const args = ["performance-summary", "--topn", String(parsePositiveInt(topn, 10))];
  for (const simulationRunId of simulationRunIds) {
    args.push("--simulation-run-id", String(simulationRunId));
  }
  if (requestUser?.userId) {
    args.push("--user-id", requestUser.userId);
  }
  if (decisionBackend) {
    args.push("--decision-backend", normalizedDecisionBackend(decisionBackend));
  }

  const rawResult = await runPythonModule("dashboard.api_bridge", args);
  const payload = parsePythonJson(rawResult);
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid performance summary payload");
  }
  return payload;
}

async function loadSimulationResultsPayload({
  runPythonModule,
  parsePythonJson,
  simulationRunId,
  topn = 25
}) {
  const args = [
    "simulation-results",
    "--simulation-run-id",
    String(simulationRunId),
    "--topn",
    String(parsePositiveInt(topn, 25))
  ];

  const rawResult = await runPythonModule("dashboard.api_bridge", args);
  const payload = parsePythonJson(rawResult);
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid simulation results payload");
  }
  return payload;
}

async function loadSimulationResultsStatusPayload({
  runPythonModule,
  parsePythonJson,
  simulationRunId
}) {
  const args = [
    "simulation-results-status",
    "--simulation-run-id",
    String(simulationRunId)
  ];

  const rawResult = await runPythonModule("dashboard.api_bridge", args);
  const payload = parsePythonJson(rawResult);
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid simulation results status payload");
  }
  return payload;
}

async function loadProfileSourceStatusPayload({
  runPythonModule,
  parsePythonJson,
  simulationRunId
}) {
  const args = [
    "profile-source-status",
    "--simulation-run-id",
    String(simulationRunId)
  ];

  const rawResult = await runPythonModule("dashboard.api_bridge", args);
  const payload = parsePythonJson(rawResult);
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid profile source status payload");
  }
  return payload;
}

async function loadPerformanceQueryPayload({
  runPythonModule,
  parsePythonJson,
  simulationRunIds = [],
  requestUser = null,
  filters = {},
  groupBy = [],
  sort = null,
  topn = 25
}) {
  const args = ["performance-query", "--topn", String(parsePositiveInt(topn, 25))];
  const queryPayload = {
    simulation_run_ids: simulationRunIds,
    user_id: requestUser?.userId || null,
    topn: parsePositiveInt(topn, 25),
    group_by: Array.isArray(groupBy) ? groupBy : [],
    sort: sort || null,
    filters: {
      ...filters,
      decision_backend: filters?.decision_backend || null
    }
  };
  args.push("--query-json", JSON.stringify(queryPayload));
  for (const simulationRunId of simulationRunIds) {
    args.push("--simulation-run-id", String(simulationRunId));
  }
  if (requestUser?.userId) {
    args.push("--user-id", requestUser.userId);
  }
  if (filters?.decision_backend) {
    args.push("--decision-backend", normalizedDecisionBackend(filters.decision_backend));
  }
  if (filters?.min_hands_played != null) {
    args.push("--min-hands-played", String(filters.min_hands_played));
  }
  if (filters?.max_final_rank != null) {
    args.push("--max-final-rank", String(filters.max_final_rank));
  }
  for (const dimension of Array.isArray(groupBy) ? groupBy : []) {
    args.push("--group-by", String(dimension));
  }
  if (sort?.field) {
    args.push("--sort-field", String(sort.field));
    args.push("--sort-direction", String(sort.direction || "desc"));
  }

  const rawResult = await runPythonModule("dashboard.api_bridge", args);
  const payload = parsePythonJson(rawResult);
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid performance query payload");
  }
  return payload;
}

function emptyPerformancePayload(filters = {}, options = {}) {
  return {
    status: "ready",
    summary: null,
    rows: [],
    comparison_rows: [],
    recent_runs: [],
    matched_run_ids: [],
    matched_run_count: 0,
    profile_overview: {
      ready_profiles: 0,
      insufficient_evidence: 0,
      not_found: 0,
      other: 0
    },
    breakdowns: {
      by_backend: [],
      by_finish_rank: [],
      by_profile_status: [],
      by_cluster_label: []
    },
    query: {
      user_id: filters.user_id || null,
      decision_backend: filters.decision_backend || null,
      simulation_run_ids: filters.simulation_run_ids || [],
      group_by: options.groupBy || [],
      sort: options.sort || null,
      topn: parsePositiveInt(options.topn, 25),
      filters
    },
    filters
  };
}

function emptyProfileOverview() {
  return {
    ready_profiles: 0,
    insufficient_evidence: 0,
    not_found: 0,
    other: 0
  };
}

function parseSavedQueryDefinition(savedQuery, fallbackTopn = 25) {
  const stored = savedQuery?.filters && typeof savedQuery.filters === "object"
    ? { ...savedQuery.filters }
    : {};
  const topn = parsePositiveInt(stored.topn, fallbackTopn);
  const groupBy = Array.isArray(stored.group_by)
    ? stored.group_by.map((value) => String(value).trim()).filter(Boolean)
    : [];
  const sort =
    stored.sort && typeof stored.sort === "object" && stored.sort.field
      ? {
          field: String(stored.sort.field).trim(),
          direction:
            String(stored.sort.direction || "desc").trim().toLowerCase() === "asc"
              ? "asc"
              : "desc"
        }
      : null;
  delete stored.topn;
  delete stored.group_by;
  delete stored.sort;
  delete stored.query_version;
  return {
    filters: stored,
    topn,
    groupBy,
    sort
  };
}

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(object, key);
}

function normalizedRunStatus(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (["queued", "running", "finalizing", "completed", "failed"].includes(normalized)) {
    return normalized;
  }
  return "unknown";
}

const RUN_STATUS_RANK = {
  unknown: -1,
  queued: 0,
  running: 1,
  finalizing: 2,
  completed: 3,
  failed: 3
};

function runStatusRank(value) {
  return RUN_STATUS_RANK[normalizedRunStatus(value)] ?? -1;
}

function preferredRunStatus(...values) {
  let winner = "unknown";
  let winnerRank = -1;
  for (const value of values) {
    const normalized = normalizedRunStatus(value);
    const rank = RUN_STATUS_RANK[normalized] ?? -1;
    if (rank > winnerRank) {
      winner = normalized;
      winnerRank = rank;
    }
  }
  return winner;
}

function parseBooleanQuery(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "t", "yes", "y", "on"].includes(normalized);
}

function preferEarlierTimestamp(...values) {
  let winner = null;
  for (const value of values) {
    const candidate = parseDateMaybe(value);
    if (!candidate) {
      continue;
    }
    if (!winner || candidate < winner) {
      winner = candidate;
    }
  }
  return winner ? winner.toISOString() : null;
}

function preferLaterTimestamp(...values) {
  let winner = null;
  for (const value of values) {
    const candidate = parseDateMaybe(value);
    if (!candidate) {
      continue;
    }
    if (!winner || candidate > winner) {
      winner = candidate;
    }
  }
  return winner ? winner.toISOString() : null;
}

function preferNumericMax(...values) {
  let winner = null;
  for (const value of values) {
    const candidate = Number(value);
    if (!Number.isFinite(candidate)) {
      continue;
    }
    winner = winner == null ? candidate : Math.max(winner, candidate);
  }
  return winner;
}

function summarizeRunStatuses(runs = []) {
  const counts = {
    queued: 0,
    running: 0,
    finalizing: 0,
    completed: 0,
    failed: 0,
    unknown: 0
  };
  for (const run of runs) {
    counts[normalizedRunStatus(run?.status)] += 1;
  }
  return counts;
}

function sortRunsByRequestedAt(runs = []) {
  return [...runs].sort((left, right) => {
    const leftDate = parseDateMaybe(
      left?.requested_at || left?.updated_at || left?.completed_at || left?.finished_at
    );
    const rightDate = parseDateMaybe(
      right?.requested_at || right?.updated_at || right?.completed_at || right?.finished_at
    );
    const leftTime = leftDate ? leftDate.getTime() : 0;
    const rightTime = rightDate ? rightDate.getTime() : 0;
    return rightTime - leftTime;
  });
}

function hasLiveRunProgress(run) {
  if (!run || typeof run !== "object") {
    return false;
  }
  if (parseNonNegativeInt(run.published_actions, 0) > 0) {
    return true;
  }
  if (parseNonNegativeInt(run.published_hand_summaries, 0) > 0) {
    return true;
  }
  return Array.isArray(run.player_summaries) && run.player_summaries.length > 0;
}

function hasKnownZeroLiveRunProgress(run) {
  if (!run || typeof run !== "object") {
    return false;
  }
  const publishedActions = parseNonNegativeInt(run.published_actions, null);
  const publishedHandSummaries = parseNonNegativeInt(run.published_hand_summaries, null);
  return (
    publishedActions === 0 &&
    publishedHandSummaries === 0 &&
    Array.isArray(run.player_summaries) &&
    run.player_summaries.length === 0
  );
}

function hasProvisionalProfileFeatures(run) {
  return (
    Array.isArray(run?.profile_session_features) &&
    run.profile_session_features.length > 0
  );
}

function expectedProfileHands(run = null, ...payloads) {
  const candidates = [
    run?.hand_count,
    run?.published_hand_summaries,
    ...payloads.flatMap((payload) => [
      payload?.run_summary?.hand_count,
      payload?.run_summary?.published_hand_summaries,
      payload?.hand_count,
      payload?.published_hand_summaries
    ])
  ];
  let expected = 0;
  for (const value of candidates) {
    const parsed = parseNonNegativeInt(value, null);
    if (parsed != null) {
      expected = Math.max(expected, parsed);
    }
  }
  return expected;
}

function profileResultRows(result = null) {
  return Array.isArray(result?.results) ? result.results : [];
}

function profilePayloadLooksStale(result = null, run = null, ...payloads) {
  const rows = profileResultRows(result);
  if (!rows.length || normalizedRunStatus(run?.status) !== "completed") {
    return false;
  }
  const appRunStarted = runExecutionStart(run);
  if (appRunStarted) {
    const scoredTimes = rows
      .map((row) => row?.scored_at)
      .filter(Boolean);
    if (
      scoredTimes.length > 0 &&
      scoredTimes.some((value) => timestampBefore(value, appRunStarted))
    ) {
      return true;
    }
  }
  const expectedHands = expectedProfileHands(run, ...payloads);
  if (expectedHands < MIN_PROFILE_SESSION_HANDS) {
    return false;
  }
  const observedHands = rows
    .map((row) => parseNonNegativeInt(row?.hands_observed, null))
    .filter((value) => value != null);
  if (!observedHands.length) {
    return false;
  }
  return Math.max(...observedHands) < Math.floor(expectedHands * 0.9);
}

async function loadAppLiveRuns({
  appStore,
  requestUser,
  decisionBackend = null,
  limit = 25
}) {
  if (!appStore?.listLiveSimulationRuns) {
    return [];
  }
  const runs = await appStore.listLiveSimulationRuns({
    userId: requestUser?.userId || null,
    decisionBackend: decisionBackend || null,
    limit
  });
  return sortRunsByRequestedAt(
    (Array.isArray(runs) ? runs : []).filter(
      (run) => !["completed", "failed"].includes(normalizedRunStatus(run?.status))
    )
  );
}

function buildLivePayloadFromAppRuns({
  runs = [],
  selectedRunId = null,
  topn = 25
}) {
  const activeRuns = sortRunsByRequestedAt(runs)
    .map((run) => mergeLiveMetaWithAppRun({ simulation_run_id: run?.simulation_run_id }, run))
    .filter((run) => !["completed", "failed"].includes(normalizedRunStatus(run?.status)));
  const activeRunIds = activeRuns
    .map((run) => String(run?.simulation_run_id || "").trim())
    .filter(Boolean);
  const normalizedSelectedRunId = activeRunIds.includes(String(selectedRunId || "").trim())
    ? String(selectedRunId || "").trim()
    : activeRunIds[0] || null;
  const selectedRun = normalizedSelectedRunId
    ? runs.find((run) => String(run?.simulation_run_id || "").trim() === normalizedSelectedRunId) ||
      null
    : null;
  const selectedRunMeta = normalizedSelectedRunId
    ? activeRuns.find((run) => String(run?.simulation_run_id || "").trim() === normalizedSelectedRunId) ||
      null
    : null;
  const selectedRows = withDashboardRunFields(selectedRun?.player_summaries, selectedRun);
  const allRows = runs.flatMap((run) => withDashboardRunFields(run?.player_summaries, run));
  const selectedHeroProfitTimeseries = normalizeHeroProfitTimeseriesRows(
    selectedRun?.hero_profit_timeseries,
    selectedRunMeta
  );
  const coarseHeroProfitTimeseries = buildCoarseHeroProfitTimeseries(
    selectedRunMeta,
    selectedRows
  );
  const heroProfitTimeseries = chooseHeroProfitTimeseries({
    appRows: selectedHeroProfitTimeseries,
    coarseRows: coarseHeroProfitTimeseries,
    selectedRunMeta,
    sourceRows: selectedRows
  });

  return {
    available: Boolean(activeRunIds.length),
    source: "app",
    message: activeRunIds.length
      ? ""
      : "No active simulation runs are available yet.",
    active_run_ids: activeRunIds,
    active_runs: activeRuns,
    selected_run_id: normalizedSelectedRunId,
    selected_run_meta: selectedRunMeta,
    run_profit_leaderboard: buildPlayerSummaryLeaderboard(selectedRows, "total_bb_won", topn),
    run_bb_per_100_leaderboard: buildPlayerSummaryLeaderboard(selectedRows, "bb_per_100", topn),
    run_high_hand_leaderboard: buildPlayerSummaryLeaderboard(
      selectedRows,
      "showdown_hand_score",
      topn
    ),
    global_profit_leaderboard: buildPlayerSummaryLeaderboard(allRows, "total_bb_won", topn),
    global_high_hand_leaderboard: buildPlayerSummaryLeaderboard(
      allRows,
      "showdown_hand_score",
      topn
    ),
    global_hero_context_leaderboard: [],
    hero_profit_timeseries: heroProfitTimeseries,
    run_agents: sortPlayerSummaries(selectedRows, [
      { key: "total_bb_won", desc: true },
      { key: "bb_per_100", desc: true },
      { key: "seat", desc: false }
    ])
  };
}

const DASHBOARD_ACCOUNT_ROW_FIELDS = [
  "active_runs",
  "run_profit_leaderboard",
  "run_bb_per_100_leaderboard",
  "run_high_hand_leaderboard",
  "global_profit_leaderboard",
  "global_high_hand_leaderboard",
  "global_hero_context_leaderboard",
  "run_agents"
];

function dashboardUserIds(payload = {}) {
  const ids = new Set();
  const collect = (row) => {
    const userId = String(row?.user_id || "").trim();
    if (userId) {
      ids.add(userId);
    }
  };
  collect(payload?.selected_run_meta);
  for (const field of DASHBOARD_ACCOUNT_ROW_FIELDS) {
    for (const row of Array.isArray(payload?.[field]) ? payload[field] : []) {
      collect(row);
    }
  }
  return [...ids];
}

function dashboardAccountName(user = {}) {
  return (
    String(user?.display_name || "").trim() ||
    String(user?.email || "").trim() ||
    String(user?.user_id || "").trim()
  );
}

async function hydrateDashboardAccountNames({ payload, appStore }) {
  if (!payload || !appStore?.listUsersByIds) {
    return payload;
  }

  const users = await appStore.listUsersByIds(dashboardUserIds(payload));
  if (!Array.isArray(users) || !users.length) {
    return payload;
  }

  const usersById = new Map(
    users
      .filter((user) => String(user?.user_id || "").trim())
      .map((user) => [String(user.user_id).trim(), user])
  );
  if (!usersById.size) {
    return payload;
  }

  const hydrateRow = (row) => {
    if (!row || typeof row !== "object" || Array.isArray(row)) {
      return row;
    }
    const userId = String(row.user_id || "").trim();
    const user = usersById.get(userId);
    if (!user) {
      return row;
    }
    return {
      ...row,
      account_name: dashboardAccountName(user),
      user_display_name: user.display_name || row.user_display_name || null,
      user_email: user.email || row.user_email || null
    };
  };

  const hydrated = {
    ...payload,
    selected_run_meta: hydrateRow(payload.selected_run_meta)
  };
  for (const field of DASHBOARD_ACCOUNT_ROW_FIELDS) {
    hydrated[field] = Array.isArray(payload[field])
      ? payload[field].map(hydrateRow)
      : payload[field];
  }
  return hydrated;
}

function sameNonEmptyValue(left, right) {
  const normalizedLeft = String(left || "").trim();
  const normalizedRight = String(right || "").trim();
  return !normalizedLeft || !normalizedRight || normalizedLeft === normalizedRight;
}

function timestampsNear(left, right, toleranceMs = 60_000) {
  const leftDate = parseDateMaybe(left);
  const rightDate = parseDateMaybe(right);
  if (!leftDate || !rightDate) {
    return true;
  }
  return Math.abs(leftDate.getTime() - rightDate.getTime()) <= toleranceMs;
}

function runExecutionStart(run = null) {
  return preferEarlierTimestamp(run?.requested_at, run?.started_at);
}

function timestampBefore(left, right, toleranceMs = 1000) {
  const leftDate = parseDateMaybe(left);
  const rightDate = parseDateMaybe(right);
  if (!leftDate || !rightDate) {
    return false;
  }
  return leftDate.getTime() < rightDate.getTime() - toleranceMs;
}

function runSummaryMatchesAppRun(summary = {}, appRun = null) {
  if (!appRun) {
    return true;
  }
  const normalizedSummary = summary && typeof summary === "object" ? summary : {};
  if (!sameNonEmptyValue(normalizedSummary.simulation_run_id, appRun.simulation_run_id)) {
    return false;
  }
  if (!sameNonEmptyValue(normalizedSummary.user_id, appRun.user_id)) {
    return false;
  }
  if (!sameNonEmptyValue(normalizedSummary.decision_backend, appRun.decision_backend)) {
    return false;
  }
  if (!sameNonEmptyValue(normalizedSummary.hero_context_hash, appRun.hero_context_hash)) {
    return false;
  }

  const appRunStarted = runExecutionStart(appRun);
  const summaryStarted =
    normalizedSummary.requested_at ||
    normalizedSummary.started_at ||
    null;
  if (appRunStarted && summaryStarted && !timestampsNear(summaryStarted, appRunStarted)) {
    return false;
  }

  const summaryFinished =
    normalizedSummary.finished_at ||
    normalizedSummary.completed_at ||
    normalizedSummary.updated_at ||
    null;
  if (appRunStarted && timestampBefore(summaryFinished, appRunStarted)) {
    return false;
  }

  return true;
}

function payloadMatchesAppRun(payload = null, appRun = null) {
  if (!payload || typeof payload !== "object" || !appRun) {
    return true;
  }
  const runSummary = payload.run_summary || {};
  if (payload.ready === true && !Object.keys(runSummary).length) {
    return false;
  }
  return runSummaryMatchesAppRun(runSummary, appRun);
}

function markStaleWarehousePayload(
  payload = null,
  appRun = null,
  message = "Ignoring stale warehouse rows for a reused simulation_run_id"
) {
  if (!payload || typeof payload !== "object" || payloadMatchesAppRun(payload, appRun)) {
    return payload;
  }
  return {
    ...payload,
    status: "pending",
    ready: false,
    stale: true,
    message,
    run_summary: payload.run_summary || {}
  };
}

function appRunMatchesLiveExecution(meta = {}, appRun = null) {
  if (!appRun) {
    return true;
  }
  const normalizedMeta = meta && typeof meta === "object" ? meta : {};
  if (!sameNonEmptyValue(normalizedMeta.user_id, appRun.user_id)) {
    return false;
  }
  if (!sameNonEmptyValue(normalizedMeta.decision_backend, appRun.decision_backend)) {
    return false;
  }
  if (!sameNonEmptyValue(normalizedMeta.hero_context_hash, appRun.hero_context_hash)) {
    return false;
  }
  const appRunStarted = runExecutionStart(appRun);
  const metaStarted = runExecutionStart(normalizedMeta);
  if (appRunStarted && !metaStarted) {
    return false;
  }
  if (!timestampsNear(normalizedMeta.started_at, appRun.started_at)) {
    return false;
  }
  if (!timestampsNear(normalizedMeta.requested_at, appRun.requested_at)) {
    return false;
  }
  return true;
}

function liveRowsMatchAppExecution(meta = {}, appRun = null) {
  if (!appRun) {
    return true;
  }
  const appRunStarted = runExecutionStart(appRun);
  if (!meta || typeof meta !== "object") {
    return !appRunStarted;
  }
  if (!sameNonEmptyValue(meta.simulation_run_id || meta.run_id, appRun.simulation_run_id)) {
    return false;
  }
  if (!sameNonEmptyValue(meta.user_id, appRun.user_id)) {
    return false;
  }
  if (!sameNonEmptyValue(meta.decision_backend, appRun.decision_backend)) {
    return false;
  }
  if (!sameNonEmptyValue(meta.hero_context_hash, appRun.hero_context_hash)) {
    return false;
  }

  const metaStarted = runExecutionStart(meta);
  if (!appRunStarted) {
    return true;
  }
  if (appRunStarted && !metaStarted) {
    return false;
  }
  if (appRunStarted && metaStarted && !timestampsNear(metaStarted, appRunStarted)) {
    return false;
  }

  const metaFinished =
    meta.finished_at ||
    meta.completed_at ||
    meta.updated_at ||
    null;
  if (appRunStarted && timestampBefore(metaFinished, appRunStarted)) {
    return false;
  }

  return true;
}

function mergeLiveMetaWithAppRun(meta = {}, appRun = null) {
  const metaRunId = String(meta?.simulation_run_id || meta?.run_id || "").trim();
  if (!appRun) {
    const normalizedStatus = normalizedRunStatus(meta.status);
    const publishedActions = preferNumericMax(meta.published_actions);
    const publishedHandSummaries = preferNumericMax(meta.published_hand_summaries);
    const hasProgress =
      Number.isFinite(publishedActions) && publishedActions > 0 ||
      Number.isFinite(publishedHandSummaries) && publishedHandSummaries > 0 ||
      (Array.isArray(meta.player_summaries) && meta.player_summaries.length > 0);
    return {
      ...meta,
      status: normalizedStatus === "queued" && hasProgress ? "running" : normalizedStatus,
      requested_at: preferEarlierTimestamp(meta.requested_at),
      started_at: preferEarlierTimestamp(meta.started_at),
      completed_at: preferLaterTimestamp(meta.completed_at, meta.finished_at),
      finished_at: preferLaterTimestamp(meta.finished_at, meta.completed_at),
      updated_at: preferLaterTimestamp(meta.updated_at),
      published_actions: publishedActions,
      published_hand_summaries: publishedHandSummaries
    };
  }
  if (!appRunMatchesLiveExecution(meta, appRun)) {
    meta = { simulation_run_id: appRun.simulation_run_id };
  }
  const appRunId = String(appRun?.simulation_run_id || "").trim();
  const compatibleMeta =
    metaRunId && appRunId && metaRunId !== appRunId
      ? { simulation_run_id: appRunId }
      : meta;
  const mergedPublishedActions = preferNumericMax(
    appRun.published_actions,
    compatibleMeta.published_actions
  );
  const mergedPublishedHandSummaries = preferNumericMax(
    appRun.published_hand_summaries,
    compatibleMeta.published_hand_summaries
  );
  const mergedHasProgress =
    (Number.isFinite(mergedPublishedActions) && mergedPublishedActions > 0) ||
    (Number.isFinite(mergedPublishedHandSummaries) && mergedPublishedHandSummaries > 0) ||
    (Array.isArray(appRun.player_summaries) && appRun.player_summaries.length > 0) ||
    (Array.isArray(compatibleMeta.player_summaries) && compatibleMeta.player_summaries.length > 0);
  const mergedStatus = preferredRunStatus(meta.status, appRun.status);
  return {
    ...compatibleMeta,
    simulation_run_id:
      appRun.simulation_run_id ||
      compatibleMeta.simulation_run_id ||
      compatibleMeta.run_id ||
      null,
    status: mergedStatus === "queued" && mergedHasProgress ? "running" : mergedStatus,
    requested_at: preferEarlierTimestamp(appRun.requested_at, compatibleMeta.requested_at),
    started_at: preferEarlierTimestamp(appRun.started_at, compatibleMeta.started_at),
    completed_at: preferLaterTimestamp(
      appRun.completed_at,
      appRun.finished_at,
      compatibleMeta.completed_at,
      compatibleMeta.finished_at
    ),
    finished_at: preferLaterTimestamp(
      appRun.completed_at,
      appRun.finished_at,
      compatibleMeta.finished_at,
      compatibleMeta.completed_at
    ),
    updated_at: preferLaterTimestamp(appRun.updated_at, compatibleMeta.updated_at),
    user_id: appRun.user_id || compatibleMeta.user_id || null,
    decision_backend: appRun.decision_backend || compatibleMeta.decision_backend || null,
    backend_type: appRun.backend_type || compatibleMeta.backend_type || null,
    model_name: appRun.model_name || compatibleMeta.model_name || null,
    hand_count: appRun.hand_count || compatibleMeta.hand_count || null,
    published_actions: mergedPublishedActions,
    published_hand_summaries: mergedPublishedHandSummaries,
    hero_context_hash:
      appRun.hero_context_hash || compatibleMeta.hero_context_hash || null,
    hero_context_preview:
      appRun.hero_context_preview || compatibleMeta.hero_context_preview || null
  };
}

function filterLeaderboardRowsByRunIds(rows = [], allowedRunIds = []) {
  const allowed = new Set(
    (Array.isArray(allowedRunIds) ? allowedRunIds : [])
      .map((runId) => String(runId || "").trim())
      .filter(Boolean)
  );
  if (!allowed.size) {
    return [];
  }
  const normalizedRows = Array.isArray(rows) ? rows : [];
  const hasRunScopedRows = normalizedRows.some((row) =>
    String(row?.simulation_run_id || "").trim()
  );
  if (!hasRunScopedRows) {
    return normalizedRows;
  }
  return normalizedRows.filter((row) =>
    allowed.has(String(row?.simulation_run_id || "").trim())
  );
}

function payloadHasSelectedRunRows(payload = {}, simulationRunId = null, { implicitRunScope = false } = {}) {
  const targetRunId = String(simulationRunId || "").trim();
  if (!targetRunId) {
    return false;
  }
  const candidateSets = [
    payload.run_profit_leaderboard,
    payload.run_bb_per_100_leaderboard,
    payload.run_agents
  ];
  return candidateSets.some((rows) => {
    const normalizedRows = Array.isArray(rows) ? rows : [];
    if (!normalizedRows.length) {
      return false;
    }
    const scopedRunIds = normalizedRows
      .map((row) => String(row?.simulation_run_id || "").trim())
      .filter(Boolean);
    if (!scopedRunIds.length) {
      return implicitRunScope;
    }
    return scopedRunIds.every((runId) => runId === targetRunId);
  });
}

function payloadShowsRunActivity(
  payload = {},
  simulationRunId = null,
  { implicitRunScope = false } = {}
) {
  const targetRunId = String(simulationRunId || "").trim();
  if (!targetRunId) {
    return false;
  }

  const selectedRunMetaId = String(
    payload?.selected_run_meta?.simulation_run_id || payload?.selected_run_meta?.run_id || ""
  ).trim();
  if (selectedRunMetaId === targetRunId && hasLiveRunProgress(payload?.selected_run_meta)) {
    return true;
  }

  const activeRun = (Array.isArray(payload?.active_runs) ? payload.active_runs : []).find(
    (run) => String(run?.simulation_run_id || run?.run_id || "").trim() === targetRunId
  );
  if (hasLiveRunProgress(activeRun)) {
    return true;
  }

  const candidateSets = [
    payload.run_profit_leaderboard,
    payload.run_bb_per_100_leaderboard,
    payload.run_high_hand_leaderboard,
    payload.run_agents
  ];
  for (const rows of candidateSets) {
    const normalizedRows = Array.isArray(rows) ? rows : [];
    if (!normalizedRows.length) {
      continue;
    }
    const scopedRunIds = normalizedRows
      .map((row) => String(row?.simulation_run_id || "").trim())
      .filter(Boolean);
    if (!scopedRunIds.length && implicitRunScope) {
      return true;
    }
    if (scopedRunIds.includes(targetRunId)) {
      return true;
    }
  }

  if (
    Array.isArray(payload.hero_profit_timeseries) &&
    payload.hero_profit_timeseries.length > 0
  ) {
    const payloadSelectedRunId = String(
      payload.selected_run_id || payload.selected_run_meta?.simulation_run_id || ""
    ).trim();
    if (payloadSelectedRunId === targetRunId || implicitRunScope) {
      return true;
    }
  }

  return false;
}

function selectedRunPayloadHasCompleteVisuals(
  payload = {},
  simulationRunId = null,
  { implicitRunScope = false } = {}
) {
  if (
    Array.isArray(payload.hero_profit_timeseries) &&
    payload.hero_profit_timeseries.length > 0
  ) {
    return heroProfitTimeseriesLooksCurrent(
      payload.hero_profit_timeseries,
      payload.selected_run_meta,
      payload.run_agents
    );
  }
  return !payloadShowsRunActivity(payload, simulationRunId, { implicitRunScope });
}

function rowHasAggressionMetrics(row = {}) {
  return ["vpip", "pfr", "aggression_frequency"].some((key) => {
    const value = Number(row?.[key]);
    return Number.isFinite(value);
  });
}

function findHeroRow(rows = []) {
  const normalizedRows = Array.isArray(rows) ? rows : [];
  for (const row of normalizedRows) {
    if (row?.is_hero_player) {
      return row;
    }
    const playerId = String(row?.player_id || "").trim();
    const agentId = String(row?.agent_id || "").trim();
    if (playerId === "user_agent" || agentId === "user_agent") {
      return row;
    }
  }
  return null;
}

function maxHeroProfitHandNumber(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce((winner, row, index) => {
    const handNumber = parseNonNegativeInt(row?.hand_number, index + 1);
    return Math.max(winner, handNumber || 0);
  }, 0);
}

function normalizeHeroProfitTimeseriesRows(rows = [], selectedRunMeta = null) {
  const selectedRunId = String(selectedRunMeta?.simulation_run_id || "").trim() || null;
  const byHandNumber = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || typeof row !== "object") {
      continue;
    }
    const handNumber = Number(row.hand_number);
    const cumulativeBbWon = Number(row.cumulative_bb_won);
    if (
      !Number.isFinite(handNumber) ||
      handNumber < 0 ||
      !Number.isFinite(cumulativeBbWon)
    ) {
      continue;
    }
    const normalizedHandNumber = Math.floor(handNumber);
    byHandNumber.set(String(normalizedHandNumber), {
      simulation_run_id: row.simulation_run_id || selectedRunId || null,
      hand_number: normalizedHandNumber,
      cumulative_bb_won: cumulativeBbWon,
      event_ts: row.event_ts || null
    });
  }
  return [...byHandNumber.values()].sort(
    (left, right) => left.hand_number - right.hand_number
  );
}

function expectedHeroProfitHands(selectedRunMeta = null, rows = []) {
  const heroRow = findHeroRow(rows);
  const terminalExpectedHands = ["completed", "finalizing"].includes(
    normalizedRunStatus(selectedRunMeta?.status)
  )
    ? parseNonNegativeInt(selectedRunMeta?.hand_count, 0)
    : 0;
  return Math.max(
    parseNonNegativeInt(selectedRunMeta?.published_hand_summaries, 0),
    terminalExpectedHands,
    parseNonNegativeInt(heroRow?.observed_hands, 0),
    parseNonNegativeInt(heroRow?.hands_played, 0)
  );
}

function maxObservedHands(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce((winner, row) => {
    return Math.max(
      winner,
      parseNonNegativeInt(row?.observed_hands, 0),
      parseNonNegativeInt(row?.hands_played, 0)
    );
  }, 0);
}

function playerRowsFitRun(rows = [], selectedRunMeta = null) {
  const expectedHands = Math.max(
    parseNonNegativeInt(selectedRunMeta?.published_hand_summaries, 0),
    ["completed", "finalizing"].includes(normalizedRunStatus(selectedRunMeta?.status))
      ? parseNonNegativeInt(selectedRunMeta?.hand_count, 0)
      : 0
  );
  if (expectedHands <= 0) {
    return true;
  }
  return maxObservedHands(rows) <= Math.ceil(expectedHands * 1.1) + 2;
}

function heroProfitTimeseriesLooksCurrent(
  rows = [],
  selectedRunMeta = null,
  sourceRows = []
) {
  if (!Array.isArray(rows) || !rows.length) {
    return false;
  }
  const expectedHands = expectedHeroProfitHands(selectedRunMeta, sourceRows);
  const maxHandNumber = maxHeroProfitHandNumber(rows);
  if (expectedHands >= 10 && maxHandNumber > Math.ceil(expectedHands * 1.1) + 2) {
    return false;
  }
  if (expectedHands < 10) {
    return true;
  }
  return maxHandNumber >= Math.max(5, Math.floor(expectedHands * 0.75));
}

function chooseHeroProfitTimeseries({
  existingRows = [],
  appRows = [],
  coarseRows = [],
  selectedRunMeta = null,
  sourceRows = []
} = {}) {
  const existing = Array.isArray(existingRows) ? existingRows : [];
  const app = Array.isArray(appRows) ? appRows : [];
  const coarse = Array.isArray(coarseRows) ? coarseRows : [];
  const existingLooksCurrent = heroProfitTimeseriesLooksCurrent(
    existing,
    selectedRunMeta,
    sourceRows
  );
  const appLooksCurrent = heroProfitTimeseriesLooksCurrent(
    app,
    selectedRunMeta,
    sourceRows
  );
  if (appLooksCurrent && app.length > existing.length) {
    return app;
  }
  if (existingLooksCurrent) {
    return existing;
  }
  if (appLooksCurrent) {
    return app;
  }
  if (app.length > existing.length) {
    return app;
  }
  if (coarse.length) {
    return coarse;
  }
  return existing.length ? existing : app;
}

function buildCoarseHeroProfitTimeseries(selectedRunMeta = null, rows = []) {
  const expectedHands = expectedHeroProfitHands(selectedRunMeta, rows);
  if (expectedHands <= 0) {
    return [];
  }
  const heroRow = findHeroRow(rows);
  if (!heroRow) {
    return [];
  }
  const totalBbWon = Number(heroRow.total_bb_won ?? heroRow.bb_won);
  if (!Number.isFinite(totalBbWon)) {
    return [];
  }
  const simulationRunId =
    heroRow.simulation_run_id || selectedRunMeta?.simulation_run_id || null;
  return [
    {
      simulation_run_id: simulationRunId,
      hand_number: 0,
      cumulative_bb_won: 0
    },
    {
      simulation_run_id: simulationRunId,
      hand_number: expectedHands,
      cumulative_bb_won: totalBbWon
    }
  ];
}

function mergeDashboardResultSource(payloadSource, resultSource) {
  const sources = new Set();
  for (const source of [payloadSource, resultSource]) {
    for (const part of String(source || "").split("+")) {
      const normalized = part.trim();
      if (normalized) {
        sources.add(normalized);
      }
    }
  }
  return [...sources].join("+") || payloadSource || resultSource || "unknown";
}

function completedResultPayloadIsReady(resultPayload = null) {
  return (
    resultPayload &&
    typeof resultPayload === "object" &&
    resultPayload.status === "ready" &&
    (
      Array.isArray(resultPayload.player_summaries) ||
      Array.isArray(resultPayload.agent_metrics) ||
      Array.isArray(resultPayload.hero_profit_timeseries)
    )
  );
}

function resultPayloadFitsSelectedRun(resultPayload = null, selectedRunMeta = null) {
  const expectedHands = ["completed", "finalizing"].includes(
    normalizedRunStatus(selectedRunMeta?.status)
  )
    ? parseNonNegativeInt(selectedRunMeta?.hand_count, 0)
    : 0;
  if (!expectedHands || !resultPayload || typeof resultPayload !== "object") {
    return true;
  }
  const resultRows = [
    ...(Array.isArray(resultPayload.player_summaries) ? resultPayload.player_summaries : []),
    ...(Array.isArray(resultPayload.agent_metrics) ? resultPayload.agent_metrics : []),
    ...(Array.isArray(resultPayload.run_profit_leaderboard) ? resultPayload.run_profit_leaderboard : [])
  ];
  const resultHands = Math.max(
    parseNonNegativeInt(resultPayload.run_summary?.published_hand_summaries, 0),
    parseNonNegativeInt(resultPayload.run_summary?.hand_count, 0),
    maxObservedHands(resultRows),
    maxHeroProfitHandNumber(resultPayload.hero_profit_timeseries)
  );
  return resultHands <= Math.ceil(expectedHands * 1.1) + 2;
}

async function hydrateCompletedSelectedRunResults({
  payload,
  runPythonModule,
  parsePythonJson,
  topn = 25
}) {
  if (!payload || typeof payload !== "object") {
    return payload;
  }

  const selectedRunId = String(
    payload.selected_run_id || payload.selected_run_meta?.simulation_run_id || ""
  ).trim();
  if (!selectedRunId || normalizedRunStatus(payload.selected_run_meta?.status) !== "completed") {
    return payload;
  }
  const existingRunAgents = filterLeaderboardRowsByRunIds(payload.run_agents, [selectedRunId]);
  const existingHeroProfitTimeseries = normalizeHeroProfitTimeseriesRows(
    payload.hero_profit_timeseries,
    payload.selected_run_meta
  );
  if (
    existingRunAgents.length > 0 &&
    playerRowsFitRun(existingRunAgents, payload.selected_run_meta) &&
    heroProfitTimeseriesLooksCurrent(
      existingHeroProfitTimeseries,
      payload.selected_run_meta,
      existingRunAgents
    )
  ) {
    return payload;
  }

  let resultPayload = null;
  try {
    resultPayload = await loadSimulationResultsPayload({
      runPythonModule,
      parsePythonJson,
      simulationRunId: selectedRunId,
      topn
    });
  } catch (_error) {
    return payload;
  }

  if (!completedResultPayloadIsReady(resultPayload)) {
    return payload;
  }
  if (!payloadMatchesAppRun(resultPayload, payload.selected_run_meta)) {
    return payload;
  }
  if (!resultPayloadFitsSelectedRun(resultPayload, payload.selected_run_meta)) {
    return payload;
  }

  const selectedRunMeta = mergeLiveMetaWithAppRun(
    {
      ...(payload.selected_run_meta || {}),
      ...(resultPayload.run_summary || {}),
      simulation_run_id: selectedRunId,
      status: "completed"
    },
    null
  );
  const playerSummaries = normalizeDashboardResultRows(
    resultPayload.player_summaries,
    selectedRunMeta
  );
  const agentMetrics = normalizeDashboardResultRows(
    resultPayload.agent_metrics,
    selectedRunMeta
  );
  const runAgents = agentMetrics.length ? agentMetrics : playerSummaries;

  return {
    ...payload,
    source: mergeDashboardResultSource(payload.source, resultPayload.source),
    available: true,
    selected_run_id: selectedRunId,
    selected_run_meta: selectedRunMeta,
    run_profit_leaderboard: normalizeDashboardResultRows(
      resultPayload.run_profit_leaderboard?.length
        ? resultPayload.run_profit_leaderboard
        : buildPlayerSummaryLeaderboard(playerSummaries, "total_bb_won", topn),
      selectedRunMeta
    ),
    run_bb_per_100_leaderboard: normalizeDashboardResultRows(
      resultPayload.run_bb_per_100_leaderboard?.length
        ? resultPayload.run_bb_per_100_leaderboard
        : buildPlayerSummaryLeaderboard(playerSummaries, "bb_per_100", topn),
      selectedRunMeta
    ),
    run_high_hand_leaderboard: normalizeDashboardResultRows(
      resultPayload.run_high_hand_leaderboard || [],
      selectedRunMeta
    ),
    run_agents: runAgents,
    hero_profit_timeseries: Array.isArray(resultPayload.hero_profit_timeseries)
      ? resultPayload.hero_profit_timeseries
      : payload.hero_profit_timeseries,
    global_hero_context_leaderboard: []
  };
}

function promoteRunMetaFromPayloadActivity(
  meta = null,
  payload = {},
  simulationRunId = null,
  { implicitRunScope = false } = {}
) {
  if (!meta || typeof meta !== "object") {
    return meta;
  }
  const normalizedStatus = normalizedRunStatus(meta.status);
  if (normalizedStatus !== "queued") {
    return meta;
  }
  if (!payloadShowsRunActivity(payload, simulationRunId, { implicitRunScope })) {
    return meta;
  }
  return {
    ...meta,
    status: "running",
    started_at:
      meta.started_at ||
      meta.updated_at ||
      meta.requested_at ||
      null
  };
}

function sortLeaderboardRows(rows = [], scoreField, topn = 25) {
  const limit = parsePositiveInt(topn, 25);
  const normalizedRows = Array.isArray(rows) ? rows.map((row) => ({ ...row })) : [];
  normalizedRows.sort((left, right) => {
    const leftScore = Number(left?.[scoreField]);
    const rightScore = Number(right?.[scoreField]);
    const normalizedLeftScore = Number.isFinite(leftScore) ? leftScore : Number.NEGATIVE_INFINITY;
    const normalizedRightScore = Number.isFinite(rightScore)
      ? rightScore
      : Number.NEGATIVE_INFINITY;
    return normalizedRightScore - normalizedLeftScore;
  });
  return normalizedRows.slice(0, limit);
}

async function hydrateSelectedLiveRunRows({
  payload,
  simulationRunId = null,
  selectedAppRun = null,
  requestUser,
  decisionBackend = null,
  topn = 25,
  runPythonModule,
  parsePythonJson
}) {
  if (!payload || typeof payload !== "object") {
    return payload;
  }

  const selectedRunId = String(
    simulationRunId || payload.selected_run_id || payload.selected_run_meta?.simulation_run_id || ""
  ).trim();
  const activeRunIds = (Array.isArray(payload.active_run_ids) ? payload.active_run_ids : [])
    .map((runId) => String(runId || "").trim())
    .filter(Boolean);
  if (!selectedRunId) {
    return {
      ...payload,
      global_hero_context_leaderboard: []
    };
  }
  if (
    payloadHasSelectedRunRows(payload, selectedRunId, {
      implicitRunScope: Boolean(simulationRunId)
    }) &&
    selectedRunPayloadHasCompleteVisuals(payload, selectedRunId, {
      implicitRunScope: Boolean(simulationRunId)
    })
  ) {
    return {
      ...payload,
      global_hero_context_leaderboard: []
    };
  }

  const focusedPayload = await loadLivePayload({
    runPythonModule,
    parsePythonJson,
    simulationRunId: selectedRunId,
    requestUser,
    decisionBackend,
    topn
  });
  const payloadSelectedMeta =
    String(payload?.selected_run_meta?.simulation_run_id || payload?.selected_run_meta?.run_id || "").trim() ===
    selectedRunId
      ? payload.selected_run_meta
      : payload.active_runs?.find(
          (run) => String(run?.simulation_run_id || run?.run_id || "").trim() === selectedRunId
        ) || null;
  const authoritativeSelectedMeta = selectedAppRun || payloadSelectedMeta;
  const focusedSelectedMeta =
    String(
      focusedPayload?.selected_run_meta?.simulation_run_id ||
        focusedPayload?.selected_run_meta?.run_id ||
        ""
    ).trim() === selectedRunId
      ? focusedPayload.selected_run_meta
      : null;
  const focusedPayloadRowsAreCurrent = liveRowsMatchAppExecution(
    focusedSelectedMeta,
    authoritativeSelectedMeta
  );
  const focusedRunAgents = Array.isArray(focusedPayload?.run_agents)
    ? filterLeaderboardRowsByRunIds(
        focusedPayloadRowsAreCurrent ? focusedPayload.run_agents : [],
        [selectedRunId]
      )
    : [];
  const payloadRunProfitLeaderboard = filterLeaderboardRowsByRunIds(
    payload.run_profit_leaderboard,
    [selectedRunId]
  );
  const payloadRunBbPer100Leaderboard = filterLeaderboardRowsByRunIds(
    payload.run_bb_per_100_leaderboard,
    [selectedRunId]
  );
  const payloadRunHighHandLeaderboard = filterLeaderboardRowsByRunIds(
    payload.run_high_hand_leaderboard,
    [selectedRunId]
  );
  const payloadRunAgents = filterLeaderboardRowsByRunIds(
    payload.run_agents,
    [selectedRunId]
  );
  const focusedHeroProfitTimeseries = focusedPayloadRowsAreCurrent && Array.isArray(focusedPayload?.hero_profit_timeseries)
    ? focusedPayload.hero_profit_timeseries
    : [];
  const payloadHeroProfitTimeseries = Array.isArray(payload?.hero_profit_timeseries)
    ? payload.hero_profit_timeseries
    : [];
  const focusedRunProfitLeaderboard = Array.isArray(focusedPayload?.run_profit_leaderboard)
    ? filterLeaderboardRowsByRunIds(
        focusedPayloadRowsAreCurrent ? focusedPayload.run_profit_leaderboard : [],
        [selectedRunId]
      )
    : [];
  const focusedRunBbPer100Leaderboard = Array.isArray(focusedPayload?.run_bb_per_100_leaderboard)
    ? filterLeaderboardRowsByRunIds(
        focusedPayloadRowsAreCurrent ? focusedPayload.run_bb_per_100_leaderboard : [],
        [selectedRunId]
      )
    : [];
  const focusedRunHighHandLeaderboard = Array.isArray(focusedPayload?.run_high_hand_leaderboard)
    ? filterLeaderboardRowsByRunIds(
        focusedPayloadRowsAreCurrent ? focusedPayload.run_high_hand_leaderboard : [],
        [selectedRunId]
      )
    : [];
  const selectedRunMeta = promoteRunMetaFromPayloadActivity(
    mergeLiveMetaWithAppRun(
      focusedPayloadRowsAreCurrent
        ? focusedSelectedMeta || { simulation_run_id: selectedRunId }
        : { simulation_run_id: selectedRunId },
      authoritativeSelectedMeta
    ) || {
      simulation_run_id: selectedRunId
    },
    focusedPayloadRowsAreCurrent ? focusedPayload || payload : payload,
    selectedRunId,
    {
      implicitRunScope: true
    }
  );
  const coarseHeroProfitTimeseries = buildCoarseHeroProfitTimeseries(
    selectedRunMeta,
    focusedRunAgents
  );
  const heroProfitTimeseries = chooseHeroProfitTimeseries({
    existingRows: focusedHeroProfitTimeseries,
    appRows: payloadHeroProfitTimeseries,
    coarseRows: coarseHeroProfitTimeseries,
    selectedRunMeta,
    sourceRows: focusedRunAgents
  });

  return {
    ...payload,
    selected_run_id: selectedRunId,
    selected_run_meta: selectedRunMeta,
    run_profit_leaderboard: focusedRunProfitLeaderboard.length
      ? focusedRunProfitLeaderboard
      : payloadRunProfitLeaderboard,
    run_bb_per_100_leaderboard: focusedRunBbPer100Leaderboard.length
      ? focusedRunBbPer100Leaderboard
      : payloadRunBbPer100Leaderboard,
    run_high_hand_leaderboard: focusedRunHighHandLeaderboard.length
      ? focusedRunHighHandLeaderboard
      : payloadRunHighHandLeaderboard,
    run_agents: focusedRunAgents.length ? focusedRunAgents : payloadRunAgents,
    hero_profit_timeseries: heroProfitTimeseries,
    global_profit_leaderboard:
      activeRunIds.length === 1 &&
      (!Array.isArray(payload.global_profit_leaderboard) ||
        payload.global_profit_leaderboard.length === 0)
        ? focusedPayloadRowsAreCurrent && Array.isArray(focusedPayload?.run_profit_leaderboard)
          ? focusedPayload.run_profit_leaderboard
          : []
        : payload.global_profit_leaderboard,
    global_high_hand_leaderboard:
      activeRunIds.length === 1 &&
      (!Array.isArray(payload.global_high_hand_leaderboard) ||
        payload.global_high_hand_leaderboard.length === 0)
        ? focusedPayloadRowsAreCurrent && Array.isArray(focusedPayload?.run_high_hand_leaderboard)
          ? focusedPayload.run_high_hand_leaderboard
          : []
        : payload.global_high_hand_leaderboard,
    global_hero_context_leaderboard: []
  };
}

function withDashboardRunFields(rows, run) {
  const simulationRunId = String(run?.simulation_run_id || "").trim();
  const userId = run?.user_id || null;
  const decisionBackend = run?.decision_backend || null;
  const status = normalizedRunStatus(run?.status);
  return normalizePlayerSummaries(rows).map((row) => ({
    ...row,
    simulation_run_id: row.simulation_run_id || simulationRunId,
    user_id: row.user_id || userId,
    decision_backend: row.decision_backend || decisionBackend,
    status: row.status || status
  }));
}

async function hydrateLiveLeaderboardsFromAppRuns({
  payload,
  appStore,
  requestUser,
  topn = 25,
  appActiveRuns = null
}) {
  if (!appStore || !payload || typeof payload !== "object") {
    return payload;
  }

  const activeRunIds = (Array.isArray(payload.active_run_ids) ? payload.active_run_ids : [])
    .map((runId) => String(runId || "").trim())
    .filter(Boolean);
  const selectedRunId = String(payload.selected_run_id || "").trim();
  if (!activeRunIds.length && !selectedRunId) {
    return {
      ...payload,
      global_hero_context_leaderboard: []
    };
  }

  const activeRunMap = new Map(
    (Array.isArray(appActiveRuns) ? appActiveRuns : [])
      .filter((run) => run?.simulation_run_id)
      .map((run) => [String(run.simulation_run_id), run])
  );
  const candidateRunIds = [...new Set([...activeRunIds, selectedRunId].filter(Boolean))];
  const missingRunIds = candidateRunIds.filter((runId) => !activeRunMap.has(runId));
  const fetchedRuns = missingRunIds.length
    ? await Promise.all(
        missingRunIds.map((runId) =>
          appStore.getSimulationRun(
            runId,
            requestUser?.userId
              ? {
                  userId: requestUser.userId
                }
              : {}
          )
        )
      )
    : [];
  for (const run of fetchedRuns) {
    if (run?.simulation_run_id) {
      activeRunMap.set(String(run.simulation_run_id), run);
    }
  }
  const activeRuns = activeRunIds.map((runId) => activeRunMap.get(runId) || null);
  const selectedRun = selectedRunId ? activeRunMap.get(selectedRunId) || null : null;
  const selectedPayloadMetaMatchesApp = selectedRun
    ? liveRowsMatchAppExecution(payload.selected_run_meta, selectedRun)
    : true;
  const selectedRunMeta = selectedRun
    ? mergeLiveMetaWithAppRun(
        selectedPayloadMetaMatchesApp
          ? payload.selected_run_meta || { simulation_run_id: selectedRunId }
          : { simulation_run_id: selectedRunId },
        selectedRun
      )
    : payload.selected_run_meta;
  const selectedAppHeroProfitTimeseries = normalizeHeroProfitTimeseriesRows(
    selectedRun?.hero_profit_timeseries,
    selectedRunMeta
  );
  const activeProvisionalRuns = activeRuns
    .map((run) => {
      const rows = withDashboardRunFields(run?.player_summaries, run);
      return rows.length ? { run, rows } : null;
    })
    .filter(Boolean);
  const selectedRows = withDashboardRunFields(selectedRun?.player_summaries, selectedRun);

  if (!activeProvisionalRuns.length && !selectedRows.length) {
    return {
      ...payload,
      hero_profit_timeseries: selectedAppHeroProfitTimeseries.length
        ? selectedAppHeroProfitTimeseries
        : Array.isArray(payload.hero_profit_timeseries)
          ? payload.hero_profit_timeseries
          : [],
      global_hero_context_leaderboard: []
    };
  }

  const allRows = activeProvisionalRuns.flatMap((entry) => entry.rows);
  const existingRunAgents = filterLeaderboardRowsByRunIds(payload.run_agents, [selectedRunId]);
  const existingRunAgentsAreCurrent = playerRowsFitRun(existingRunAgents, selectedRunMeta);
  const currentExistingRunAgents = existingRunAgentsAreCurrent ? existingRunAgents : [];
  const existingHeroRow = findHeroRow(currentExistingRunAgents);
  const heroProfitSourceRows =
    existingHeroRow && Number.isFinite(Number(existingHeroRow?.total_bb_won))
      ? currentExistingRunAgents
      : selectedRows.length
        ? selectedRows
        : currentExistingRunAgents;
  const existingHeroProfitTimeseries = Array.isArray(payload.hero_profit_timeseries)
    ? payload.hero_profit_timeseries
    : [];
  const coarseHeroProfitTimeseries = buildCoarseHeroProfitTimeseries(
    selectedRunMeta,
    heroProfitSourceRows
  );
  const heroProfitTimeseries = chooseHeroProfitTimeseries({
    existingRows: existingHeroProfitTimeseries,
    appRows: selectedAppHeroProfitTimeseries,
    coarseRows: coarseHeroProfitTimeseries,
    selectedRunMeta,
    sourceRows: heroProfitSourceRows
  });

  return {
    ...payload,
    selected_run_meta: selectedRunMeta,
    run_profit_leaderboard:
      selectedRows.length
        ? buildPlayerSummaryLeaderboard(selectedRows, "total_bb_won", topn)
        : payload.run_profit_leaderboard,
    run_bb_per_100_leaderboard:
      selectedRows.length
        ? buildPlayerSummaryLeaderboard(selectedRows, "bb_per_100", topn)
        : payload.run_bb_per_100_leaderboard,
    run_high_hand_leaderboard:
      selectedRows.length
        ? buildPlayerSummaryLeaderboard(selectedRows, "showdown_hand_score", topn)
        : payload.run_high_hand_leaderboard,
    run_agents: selectedRows.length
      ? currentExistingRunAgents.length && currentExistingRunAgents.some((row) => rowHasAggressionMetrics(row))
        ? currentExistingRunAgents
        : sortPlayerSummaries(selectedRows, [
            { key: "total_bb_won", desc: true },
            { key: "bb_per_100", desc: true },
            { key: "seat", desc: false }
          ]).slice(0, parsePositiveInt(topn, 25))
      : payload.run_agents,
    hero_profit_timeseries: heroProfitTimeseries,
    global_profit_leaderboard:
      allRows.length
        ? buildPlayerSummaryLeaderboard(allRows, "total_bb_won", topn)
        : payload.global_profit_leaderboard,
    global_high_hand_leaderboard:
      allRows.length
        ? buildPlayerSummaryLeaderboard(allRows, "showdown_hand_score", topn)
        : payload.global_high_hand_leaderboard,
    global_hero_context_leaderboard: []
  };
}

async function reconcileLivePayloadWithAppRuns({
  payload,
  appStore,
  requestUser,
  selectedRunId = null,
  appActiveRuns = null
}) {
  if (!appStore || !payload || typeof payload !== "object") {
    return payload;
  }

  const seededActiveRuns = Array.isArray(appActiveRuns) ? appActiveRuns : [];
  const candidateIds = new Set([
    ...(Array.isArray(payload.active_run_ids) ? payload.active_run_ids : []),
    ...(Array.isArray(payload.active_runs)
      ? payload.active_runs
          .map((run) => run?.simulation_run_id || run?.run_id)
          .filter(Boolean)
      : []),
    ...seededActiveRuns
      .map((run) => run?.simulation_run_id)
      .filter(Boolean),
    payload.selected_run_id || null,
    selectedRunId || null
  ]);
  candidateIds.delete(null);

  const runMap = new Map(
    seededActiveRuns
      .filter((run) => run?.simulation_run_id)
      .map((run) => [String(run.simulation_run_id), run])
  );
  const missingIds = [...candidateIds].filter((simulationRunId) => !runMap.has(String(simulationRunId)));
  const ownedRuns = await Promise.all(
    missingIds.map(async (simulationRunId) => {
      const run = await appStore.getSimulationRun(
        String(simulationRunId),
        requestUser?.userId
          ? {
              userId: requestUser.userId
            }
          : {}
      );
      return [String(simulationRunId), run];
    })
  );
  for (const [simulationRunId, run] of ownedRuns) {
    runMap.set(String(simulationRunId), run);
  }
  const missingRunIds = new Set(
    [...runMap.entries()]
      .filter(([, run]) => !run)
      .map(([simulationRunId]) => String(simulationRunId))
  );
  const payloadMetaByRunId = new Map();

  const mergedActiveRuns = [];
  const seenActiveRunIds = new Set();
  for (const run of Array.isArray(payload.active_runs) ? payload.active_runs : []) {
    const simulationRunId = String(run?.simulation_run_id || run?.run_id || "").trim();
    if (simulationRunId && missingRunIds.has(simulationRunId)) {
      continue;
    }
    const mergedRun = mergeLiveMetaWithAppRun(run, runMap.get(simulationRunId) || null);
    const mergedRunId = String(mergedRun?.simulation_run_id || "").trim();
    if (!mergedRunId || seenActiveRunIds.has(mergedRunId)) {
      continue;
    }
    payloadMetaByRunId.set(mergedRunId, run);
    seenActiveRunIds.add(mergedRunId);
    mergedActiveRuns.push(mergedRun);
  }
  for (const run of seededActiveRuns) {
    const simulationRunId = String(run?.simulation_run_id || "").trim();
    if (!simulationRunId || seenActiveRunIds.has(simulationRunId)) {
      continue;
    }
    seenActiveRunIds.add(simulationRunId);
    mergedActiveRuns.push(
      mergeLiveMetaWithAppRun({ simulation_run_id: simulationRunId }, run)
    );
  }

  const filteredActiveRuns = sortRunsByRequestedAt(mergedActiveRuns).filter(
    (run) => !["completed", "failed"].includes(normalizedRunStatus(run?.status))
  );
  const filteredActiveRunIds = filteredActiveRuns.map((run) => String(run.simulation_run_id));

  let normalizedSelectedRunId = selectedRunId || payload.selected_run_id || null;
  if (normalizedSelectedRunId && missingRunIds.has(String(normalizedSelectedRunId))) {
    normalizedSelectedRunId = null;
  }
  if (
    !selectedRunId &&
    normalizedSelectedRunId &&
    ["completed", "failed"].includes(
      normalizedRunStatus(runMap.get(String(normalizedSelectedRunId))?.status)
    )
  ) {
    normalizedSelectedRunId = null;
  }
  if (!normalizedSelectedRunId && filteredActiveRunIds.length) {
    normalizedSelectedRunId = filteredActiveRunIds[0];
  }

  let selectedRunMeta = payload.selected_run_meta || null;
  if (normalizedSelectedRunId) {
    const selectedRunMetaId = String(
      selectedRunMeta?.simulation_run_id || selectedRunMeta?.run_id || ""
    ).trim();
    if (selectedRunMetaId === String(normalizedSelectedRunId)) {
      payloadMetaByRunId.set(String(normalizedSelectedRunId), selectedRunMeta);
    }
    selectedRunMeta = promoteRunMetaFromPayloadActivity(
      mergeLiveMetaWithAppRun(
        selectedRunMetaId === String(normalizedSelectedRunId)
          ? selectedRunMeta
          : { simulation_run_id: normalizedSelectedRunId },
        runMap.get(String(normalizedSelectedRunId)) || null
      ),
      payload,
      normalizedSelectedRunId
    );
  } else {
    selectedRunMeta = null;
  }

  const filteredActiveRunsWithPromotedStatus = filteredActiveRuns.map((run) => {
    const simulationRunId = String(run?.simulation_run_id || "").trim();
    if (!simulationRunId) {
      return run;
    }
    if (simulationRunId !== String(normalizedSelectedRunId || "").trim()) {
      return run;
    }
    return promoteRunMetaFromPayloadActivity(run, payload, simulationRunId);
  });

  const payloadCurrentRunIds = new Set();
  for (const runId of filteredActiveRunIds) {
    const appRun = runMap.get(String(runId)) || null;
    const payloadMeta = payloadMetaByRunId.get(String(runId)) || null;
    if (liveRowsMatchAppExecution(payloadMeta, appRun)) {
      payloadCurrentRunIds.add(String(runId));
    }
  }

  const runScopedIds = normalizedSelectedRunId ? [normalizedSelectedRunId] : [];
  const selectedPayloadRowsAreCurrent =
    normalizedSelectedRunId && payloadCurrentRunIds.has(String(normalizedSelectedRunId));
  const runProfitLeaderboard = filterLeaderboardRowsByRunIds(
    selectedPayloadRowsAreCurrent ? payload.run_profit_leaderboard : [],
    runScopedIds
  );
  const runBbPer100Leaderboard = filterLeaderboardRowsByRunIds(
    selectedPayloadRowsAreCurrent ? payload.run_bb_per_100_leaderboard : [],
    runScopedIds
  );
  const runHighHandLeaderboard = filterLeaderboardRowsByRunIds(
    selectedPayloadRowsAreCurrent ? payload.run_high_hand_leaderboard : [],
    runScopedIds
  );
  const runAgents = filterLeaderboardRowsByRunIds(
    selectedPayloadRowsAreCurrent ? payload.run_agents : [],
    runScopedIds
  );
  const currentPayloadActiveRunIds = filteredActiveRunIds.filter((runId) =>
    payloadCurrentRunIds.has(String(runId))
  );
  const globalProfitLeaderboard = filterLeaderboardRowsByRunIds(
    payload.global_profit_leaderboard,
    currentPayloadActiveRunIds
  );
  const globalHighHandLeaderboard = filterLeaderboardRowsByRunIds(
    payload.global_high_hand_leaderboard,
    currentPayloadActiveRunIds
  );
  const globalHeroContextLeaderboard = filterLeaderboardRowsByRunIds(
    payload.global_hero_context_leaderboard,
    currentPayloadActiveRunIds
  );
  const payloadSelectedRunId = String(
    payload.selected_run_id || payload.selected_run_meta?.simulation_run_id || ""
  ).trim();
  const selectedAppRun = normalizedSelectedRunId
    ? runMap.get(String(normalizedSelectedRunId)) || null
    : null;
  const selectedRows = withDashboardRunFields(
    selectedAppRun?.player_summaries,
    selectedAppRun
  );
  const payloadHeroProfitTimeseries =
    normalizedSelectedRunId &&
    selectedPayloadRowsAreCurrent &&
    payloadSelectedRunId === String(normalizedSelectedRunId) &&
    Array.isArray(payload.hero_profit_timeseries)
      ? payload.hero_profit_timeseries
      : [];
  const appHeroProfitTimeseries = normalizeHeroProfitTimeseriesRows(
    selectedAppRun?.hero_profit_timeseries,
    selectedRunMeta
  );
  const heroProfitTimeseries = chooseHeroProfitTimeseries({
    existingRows: payloadHeroProfitTimeseries,
    appRows: appHeroProfitTimeseries,
    selectedRunMeta,
    sourceRows: selectedRows
  });

  const available = Boolean(filteredActiveRunIds.length || normalizedSelectedRunId);
  return {
    ...payload,
    available,
    message:
      available
        ? payload.message || ""
        : "No active simulation runs are available yet.",
    active_run_ids: filteredActiveRunIds,
    active_runs: filteredActiveRunsWithPromotedStatus,
    selected_run_id: normalizedSelectedRunId,
    selected_run_meta: selectedRunMeta,
    run_profit_leaderboard: runProfitLeaderboard,
    run_bb_per_100_leaderboard: runBbPer100Leaderboard,
    run_high_hand_leaderboard: runHighHandLeaderboard,
    run_agents: runAgents,
    hero_profit_timeseries: heroProfitTimeseries,
    global_profit_leaderboard: globalProfitLeaderboard,
    global_high_hand_leaderboard: globalHighHandLeaderboard,
    global_hero_context_leaderboard: globalHeroContextLeaderboard
  };
}

async function buildAgentVersionSummaries({
  agent,
  runs = [],
  requestUser,
  decisionBackend = null,
  topn = 5,
  runPythonModule,
  parsePythonJson
}) {
  const versions = Array.isArray(agent?.versions) ? agent.versions : [];
  const versionsById = new Map(
    versions
      .filter((version) => version?.agent_version_id)
      .map((version) => [version.agent_version_id, version])
  );
  const runsByVersionId = new Map();

  for (const run of runs) {
    const versionId = run?.hero_agent_version_id || "__ad_hoc__";
    if (!runsByVersionId.has(versionId)) {
      runsByVersionId.set(versionId, []);
    }
    runsByVersionId.get(versionId).push(run);
  }

  const versionEntries = versions.map((version) => ({
    key: version.agent_version_id,
    version,
    runs: runsByVersionId.get(version.agent_version_id) || []
  }));

  if (runsByVersionId.has("__ad_hoc__")) {
    versionEntries.push({
      key: "__ad_hoc__",
      version: null,
      runs: runsByVersionId.get("__ad_hoc__") || []
    });
  }

  return Promise.all(
    versionEntries.map(async (entry) => {
      const orderedRuns = sortRunsByRequestedAt(entry.runs);
      let performancePayload = emptyPerformancePayload(
        {
          user_id: requestUser?.userId || null,
          decision_backend: decisionBackend,
          hero_agent_definition_id: agent?.agent_definition_id || null,
          hero_agent_version_id: entry.version?.agent_version_id || null,
          simulation_run_ids: orderedRuns.map((run) => run.simulation_run_id)
        },
        { topn }
      );

      if (orderedRuns.length) {
        try {
          performancePayload = await loadPerformancePayload({
            runPythonModule,
            parsePythonJson,
            simulationRunIds: orderedRuns.map((run) => run.simulation_run_id),
            requestUser,
            decisionBackend,
            topn
          });
        } catch (_error) {
          performancePayload = emptyPerformancePayload(
            {
              user_id: requestUser?.userId || null,
              decision_backend: decisionBackend,
              hero_agent_definition_id: agent?.agent_definition_id || null,
              hero_agent_version_id: entry.version?.agent_version_id || null,
              simulation_run_ids: orderedRuns.map((run) => run.simulation_run_id)
            },
            { topn }
          );
        }
      }

      const statusCounts = summarizeRunStatuses(orderedRuns);
      return {
        version: entry.version,
        label: entry.version
          ? `Version ${entry.version.version_number}`
          : "Ad-hoc context",
        is_latest_version:
          Boolean(entry.version?.agent_version_id) &&
          entry.version.agent_version_id === agent?.latest_version?.agent_version_id,
        run_count: orderedRuns.length,
        status_counts: statusCounts,
        latest_requested_at: orderedRuns[0]?.requested_at || null,
        latest_simulation_run_id: orderedRuns[0]?.simulation_run_id || null,
        simulation_runs: orderedRuns.slice(0, topn).map((run) => ({
          simulation_run_id: run.simulation_run_id,
          status: run.status,
          decision_backend: run.decision_backend,
          requested_at: run.requested_at,
          hero_context_hash: run.hero_context_hash
        })),
        performance_summary: performancePayload?.summary || null,
        profile_overview:
          performancePayload?.profile_overview || emptyProfileOverview(),
        recent_runs: performancePayload?.recent_runs || []
      };
    })
  );
}

function filterRunRecordsForQuery(runs, filters = {}) {
  const requestedIds = new Set(
    (filters.simulation_run_ids || []).map((value) => String(value).trim()).filter(Boolean)
  );
  const dateFrom = parseDateMaybe(filters.date_from);
  const dateTo = parseDateMaybe(filters.date_to);
  if (filters.date_from && !dateFrom) {
    throw new Error("date_from must be a valid date or datetime");
  }
  if (filters.date_to && !dateTo) {
    throw new Error("date_to must be a valid date or datetime");
  }
  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw new Error("date_from must be earlier than date_to");
  }

  return runs.filter((run) => {
    if (requestedIds.size && !requestedIds.has(String(run.simulation_run_id || ""))) {
      return false;
    }
    if (!dateFrom && !dateTo) {
      return true;
    }
    const candidateDate = parseDateMaybe(
      run.requested_at || run.completed_at || run.updated_at || run.finished_at
    );
    if (!candidateDate) {
      return false;
    }
    if (dateFrom && candidateDate < dateFrom) {
      return false;
    }
    if (dateTo && candidateDate > dateTo) {
      return false;
    }
    return true;
  });
}

export function createEventsRouter(deps = {}) {
  const router = express.Router();
  const isKafkaConnected = deps.isKafkaConnected || defaultIsKafkaConnected;
  const publishActionEvents = deps.publishActionEvents || defaultPublishActionEvents;
  const publishHandSummaries = deps.publishHandSummaries || defaultPublishHandSummaries;
  const publishSimulationEvents =
    deps.publishSimulationEvents || defaultPublishSimulationEvents;
  const publishSimulationRequests =
    deps.publishSimulationRequests || defaultPublishSimulationRequests;
  const queueSimulationRun =
    deps.queueSimulationRun || defaultQueueSimulationRun;
  const getLegacySimulationRun =
    deps.getSimulationRun || defaultGetSimulationRun;
  const markLegacySimulationRunFailed =
    deps.markSimulationRunFailed || defaultMarkSimulationRunFailed;
  const runPythonModule = deps.runPythonModule || defaultRunPythonModule;
  const parsePythonJson = deps.parsePythonJson || defaultParsePythonJson;
  const appStore = hasOwn(deps, "appStore") ? deps.appStore : defaultGetAppStore();
  const topics = deps.topics || defaultTopics;

  router.use(async (req, res, next) => {
    try {
      const sessionToken = getSessionTokenFromRequest(req);
      if (!sessionToken || !appStore?.getSessionByToken) {
        req.authenticatedUser = null;
        return next();
      }

      const session = await appStore.getSessionByToken(sessionToken);
      if (!session?.user?.user_id) {
        req.authenticatedUser = null;
        clearSessionCookie(res, req);
        return next();
      }

      req.authenticatedUser = {
        userId: session.user.user_id,
        email: session.user.email || null,
        displayName: session.user.display_name || null,
        authMode: "password",
        sessionId: session.session?.session_id || null
      };
      return next();
    } catch (error) {
      return next(error);
    }
  });

  async function loadLegacyRun(simulationRunId) {
    try {
      return await getLegacySimulationRun(simulationRunId);
    } catch (_error) {
      return null;
    }
  }

  async function loadOwnedRun(simulationRunId, requestUser = null) {
    if (appStore) {
      const appRun = await appStore.getSimulationRun(simulationRunId, {
        userId: requestUser?.userId || null
      });
      if (requestUser?.userId && !appRun) {
        return null;
      }
      return appRun;
    }

    return loadLegacyRun(simulationRunId);
  }

  async function promoteExecutionCompletedRun(simulationRunId, run) {
    if (!run || typeof run !== "object") {
      return run;
    }
    const runStatus = String(run.status || "").trim().toLowerCase();
    if (["completed", "failed"].includes(runStatus) || !hasExecutionFinished(run)) {
      return run;
    }
    if (!appStore?.updateSimulationRunLifecycle) {
      return {
        ...run,
        status: "completed",
        completed_at: run.completed_at || run.finished_at || new Date().toISOString()
      };
    }
    return (
      (await appStore.updateSimulationRunLifecycle(simulationRunId, {
        status: "completed",
        completedAt:
          run.completed_at || run.finished_at || new Date().toISOString(),
        publishedActions: run.published_actions ?? null,
        publishedHandSummaries: run.published_hand_summaries ?? null,
        playerSummaries: normalizePlayerSummaries(run.player_summaries),
        clearErrorMessage: true
      })) || run
    );
  }

  async function syncLegacyQueuedRun(requestPayload, { required = false } = {}) {
    try {
      return await queueSimulationRun(requestPayload);
    } catch (error) {
      if (required) {
        throw error;
      }
      return null;
    }
  }

  async function bestEffortMarkLegacyRunFailed(simulationRunId, errorMessage) {
    try {
      return await markLegacySimulationRunFailed(simulationRunId, errorMessage);
    } catch (_error) {
      return null;
    }
  }

  async function executeProfileLookup({
    simulationRunId,
    requestUser = null,
    run = null,
    playerId = null,
    agentId = null,
    refreshRequested = false,
    cachedOnly = false,
    persistJob = false,
    warehouseStatusPayload = null,
    profileSourceStatusPayload = null,
    useProvisionalProfileFeatures = false
  } = {}) {
    let profileJob = null;
    if (persistJob) {
      profileJob = await createProfileJobRecord(appStore, {
        simulationRunId,
        requestUser,
        run,
        playerId,
        agentId,
        refreshRequested
      });
    }

    const buildArgs = ({
      forceRefresh = false,
      useProvisional = false
    } = {}) => {
      const args = ["--simulation-run-id", simulationRunId, "--all-subjects"];
      if (run?.user_id) {
        args.push("--user-id", String(run.user_id));
      }
      if (run?.decision_backend) {
        args.push("--decision-backend", normalizedDecisionBackend(run.decision_backend));
      }
      if (run?.hero_context_hash) {
        args.push("--hero-context-hash", String(run.hero_context_hash));
      }
      const minEventTs = runExecutionStart(run);
      if (minEventTs) {
        args.push("--min-event-ts", minEventTs);
      }
      if (playerId) {
        args.push("--player-id", playerId);
      }
      if (agentId) {
        args.push("--agent-id", agentId);
      }
      if (cachedOnly && !forceRefresh) {
        args.push("--cached-only");
      }
      if (refreshRequested || forceRefresh) {
        args.push("--refresh");
      }
      if (!cachedOnly && useProvisional && hasProvisionalProfileFeatures(run)) {
        args.push(
          "--session-features-json",
          JSON.stringify(run.profile_session_features)
        );
      }
      return args;
    };

    const invokeScorer = async ({
      forceRefresh = false,
      useProvisional = false
    } = {}) => {
      const args = buildArgs({ forceRefresh, useProvisional });
      const rawResult = await runPythonModule("profiles.score_session", args);
      return parsePythonJson(rawResult);
    };

    const invalidResponse = () => ({
      statusCode: 500,
      body: {
        message: "Failed to load simulation profiles",
        error: "Invalid scoring response",
        ...(profileJob ? { profile_job: profileJob } : {})
      }
    });

    try {
      let result = await invokeScorer({
        useProvisional: useProvisionalProfileFeatures
      });
      if (!result || typeof result !== "object") {
        profileJob = await updateProfileJobRecord(appStore, profileJob, {
          status: "failed",
          errorMessage: "Invalid scoring response"
        });
        return invalidResponse();
      }

      if (result.cached && profilePayloadLooksStale(
        result,
        run,
        warehouseStatusPayload,
        profileSourceStatusPayload
      )) {
        if (cachedOnly) {
          return {
            statusCode: 404,
            body: {
              status: "stale",
              message:
                "Cached profile results are stale for this completed run; request a refreshed profile.",
              simulation_run_id: simulationRunId,
              ...(profileJob ? { profile_job: profileJob } : {})
            }
          };
        }
        result = await invokeScorer({ forceRefresh: true });
        if (!result || typeof result !== "object") {
          profileJob = await updateProfileJobRecord(appStore, profileJob, {
            status: "failed",
            errorMessage: "Invalid scoring response"
          });
          return invalidResponse();
        }
      }

      if (result.status === "not_found") {
        profileJob = await updateProfileJobRecord(appStore, profileJob, {
          status: "not_found",
          errorMessage:
            result.message || "No completed profile results were found"
        });
        return {
          statusCode: 404,
          body: {
            message: result.message || "No completed profile results were found",
            simulation_run_id: simulationRunId,
            ...result,
            ...(profileJob ? { profile_job: profileJob } : {})
          }
        };
      }

      if (result.status === "invalid_request") {
        profileJob = await updateProfileJobRecord(appStore, profileJob, {
          status: "invalid_request",
          errorMessage: result.message || "Invalid profile request"
        });
        return {
          statusCode: 400,
          body: {
            message: result.message || "Invalid profile request",
            simulation_run_id: simulationRunId,
            ...result,
            ...(profileJob ? { profile_job: profileJob } : {})
          }
        };
      }

      profileJob = await updateProfileJobRecord(appStore, profileJob, {
        status: "completed",
        errorMessage: null
      });
      return {
        statusCode: 200,
        body: {
          ...result,
          ...(profileJob ? { profile_job: profileJob } : {})
        }
      };
    } catch (error) {
      profileJob = await updateProfileJobRecord(appStore, profileJob, {
        status: "failed",
        errorMessage: error.message
      });
      return {
        statusCode: 500,
        body: {
          message: "Failed to load simulation profiles",
          error: error.message,
          ...(profileJob ? { profile_job: profileJob } : {})
        }
      };
    }
  }

  async function resolveProfileReadyRun({
    simulationRunId,
    requestUser = null,
    run = null
  } = {}) {
    let resolvedRun = run;
    if (!resolvedRun) {
      resolvedRun = await loadOwnedRun(simulationRunId, requestUser);
    }
    const initialRunStatus = String(resolvedRun?.status || "").trim().toLowerCase();
    resolvedRun = await promoteExecutionCompletedRun(simulationRunId, resolvedRun);
    if (!resolvedRun) {
      return {
        run: null,
        ready: false,
        status: null
      };
    }

    const runStatus = String(resolvedRun.status || "").trim().toLowerCase();
    if (["completed", "failed"].includes(runStatus)) {
      if (runStatus === "failed") {
        return {
          run: resolvedRun,
          ready: true,
          status: runStatus
        };
      }
    }

    let warehouseStatusPayload = null;
    let profileSourceStatusPayload = null;
    const shouldProbeWarehouseStatus = runStatus !== "failed";
    if (shouldProbeWarehouseStatus) {
      try {
        warehouseStatusPayload = await loadSimulationResultsStatusPayload({
          runPythonModule,
          parsePythonJson,
          simulationRunId
        });
        warehouseStatusPayload = markStaleWarehousePayload(
          warehouseStatusPayload,
          resolvedRun,
          "Final results belong to an older run with the same simulation run name"
        );
      } catch (_error) {
        warehouseStatusPayload = null;
      }
    }

    if (runStatus === "completed") {
      try {
        profileSourceStatusPayload = await loadProfileSourceStatusPayload({
          runPythonModule,
          parsePythonJson,
          simulationRunId
        });
        profileSourceStatusPayload = markStaleWarehousePayload(
          profileSourceStatusPayload,
          resolvedRun,
          "Profile inputs belong to an older run with the same simulation run name"
        );
      } catch (_error) {
        profileSourceStatusPayload = null;
      }

      if (profileSourceStatusPayload?.ready === true) {
        return {
          run: resolvedRun,
          ready: true,
          status: "completed",
          warehouseStatusPayload,
          profileSourceStatusPayload
        };
      }

      if (profileSourceStatusPayload) {
        if (hasProvisionalProfileFeatures(resolvedRun)) {
          return {
            run: resolvedRun,
            ready: true,
            status: "completed",
            materializationStatus: "provisional_ready",
            warehouseStatusPayload,
            profileSourceStatusPayload
          };
        }
        return {
          run: resolvedRun,
          ready: false,
          status: "completed",
          materializationStatus: "materializing",
          warehouseStatusPayload,
          profileSourceStatusPayload
        };
      }

      if (!warehouseStatusPayload) {
        if (hasProvisionalProfileFeatures(resolvedRun)) {
          return {
            run: resolvedRun,
            ready: true,
            status: "completed",
            materializationStatus: "provisional_ready",
            warehouseStatusPayload,
            profileSourceStatusPayload
          };
        }
        return {
          run: resolvedRun,
          ready: true,
          status: "completed"
        };
      }
    }

    if (warehouseStatusPayload?.ready !== true) {
      if (runStatus === "completed") {
        if (hasProvisionalProfileFeatures(resolvedRun)) {
          return {
            run: resolvedRun,
            ready: true,
            status: "completed",
            materializationStatus: "provisional_ready",
            warehouseStatusPayload,
            profileSourceStatusPayload
          };
        }
        return {
          run: resolvedRun,
          ready: false,
          status: "completed",
          materializationStatus: "materializing",
          warehouseStatusPayload,
          profileSourceStatusPayload
        };
      }
      return {
        run: resolvedRun,
        ready: false,
        status: runStatus,
        warehouseStatusPayload,
        profileSourceStatusPayload
      };
    }

    const completedAt =
      warehouseStatusPayload?.run_summary?.finished_at ||
      warehouseStatusPayload?.run_summary?.completed_at ||
      resolvedRun.completed_at ||
      new Date().toISOString();
    const normalizedCompletedAt =
      parseDateMaybe(completedAt)?.toISOString() || completedAt;
    const currentCompletedAt =
      parseDateMaybe(resolvedRun.completed_at)?.toISOString() ||
      resolvedRun.completed_at ||
      null;

    if (
      appStore?.updateSimulationRunLifecycle &&
      (runStatus !== "completed" || currentCompletedAt !== normalizedCompletedAt)
    ) {
      resolvedRun =
        (await appStore.updateSimulationRunLifecycle(simulationRunId, {
          status: "completed",
          completedAt: normalizedCompletedAt,
          clearErrorMessage: true
        })) ||
        {
          ...resolvedRun,
          status: "completed",
          completed_at: normalizedCompletedAt
        };
    } else {
      resolvedRun = {
        ...resolvedRun,
        status: "completed",
        completed_at: normalizedCompletedAt
      };
    }

    return {
      run: resolvedRun,
      ready: true,
      status: "completed",
      warehouseStatusPayload,
      profileSourceStatusPayload
    };
  }

  router.get("/status", (req, res) => {
    res.json({
      ok: true,
      topics,
      kafkaConnected: isKafkaConnected(),
      appDatabaseConfigured: Boolean(appStore)
    });
  });

  router.get("/session", async (req, res) => {
    const requestUser = resolveRequestUser(req);
    return res.json({
      user: serializeSessionUser(requestUser, req),
      app_database_configured: Boolean(appStore),
      dashboard_url: resolveDashboardUrl(req)
    });
  });

  router.post("/session/register", async (req, res) => {
    if (!appStore?.registerPasswordUser) {
      return appDatabaseUnavailable(res);
    }

    const parsed = registerSessionSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const passwordMessage = validatePasswordStrength(parsed.data.password);
    if (passwordMessage) {
      return res.status(400).json({
        message: passwordMessage
      });
    }

    try {
      const passwordHash = await hashPassword(parsed.data.password);
      const sessionToken = createSessionToken();
      const expiresAt = buildSessionExpiry();
      const registration = await appStore.registerPasswordUser({
        email: parsed.data.email,
        displayName: parsed.data.display_name,
        passwordHash,
        sessionToken,
        expiresAt,
        ipAddress: requestIpAddress(req),
        userAgent: req.get("user-agent") || null
      });
      const user = registration?.user || null;
      if (!user?.user_id) {
        throw new Error("Registration did not return a user");
      }
      setSessionCookie(res, sessionToken, expiresAt, req);
      return res.status(201).json({
        user: serializeSessionUser(
          {
            userId: user.user_id,
            email: user.email,
            displayName: user.display_name,
            authMode: "password"
          },
          req
        ),
        app_database_configured: Boolean(appStore),
        dashboard_url: resolveDashboardUrl(req)
      });
    } catch (error) {
      if (error instanceof AppStoreConflictError) {
        return res.status(409).json({
          message: error.message
        });
      }
      return res.status(500).json({
        message: "Failed to create account",
        error: error.message
      });
    }
  });

  router.post("/session/login", async (req, res) => {
    if (!appStore?.getPasswordAuthIdentityByEmail || !appStore?.createSession) {
      return appDatabaseUnavailable(res);
    }

    const parsed = loginSessionSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    try {
      const identity = await appStore.getPasswordAuthIdentityByEmail(parsed.data.email);
      const passwordOk =
        identity?.password_hash &&
        (await verifyPassword(parsed.data.password, identity.password_hash));
      if (!identity?.user || !passwordOk) {
        return res.status(401).json({
          message: "Invalid email or password"
        });
      }

      await createLoginSession(appStore, req, res, identity.user);
      return res.json({
        user: serializeSessionUser(
          {
            userId: identity.user.user_id,
            email: identity.user.email,
            displayName: identity.user.display_name,
            authMode: "password"
          },
          req
        ),
        app_database_configured: Boolean(appStore),
        dashboard_url: resolveDashboardUrl(req)
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to sign in",
        error: error.message
      });
    }
  });

  router.post("/session/logout", async (req, res) => {
    try {
      const sessionToken = getSessionTokenFromRequest(req);
      if (sessionToken && appStore?.deleteSessionByToken) {
        await appStore.deleteSessionByToken(sessionToken);
      }
      clearSessionCookie(res, req);
      return res.json({
        ok: true
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to sign out",
        error: error.message
      });
    }
  });

  router.post("/session/dev", async (req, res) => {
    const parsed = devSessionSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const requestUser = {
      userId: parsed.data.user_id,
      email: parsed.data.user_email || null,
      displayName: parsed.data.user_display_name || null
    };

    try {
      await ensureUserRecord(appStore, requestUser);
      return res.json({
        user: serializeSessionUser(requestUser, req),
        app_database_configured: Boolean(appStore),
        dashboard_url: resolveDashboardUrl(req)
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to initialize dev session",
        error: error.message
      });
    }
  });

  router.get("/dashboard/live", async (req, res) => {
    try {
      const requestUser = resolveRequestUser(req);
      const topn = req.query.topn;
      const appActiveRuns = await loadAppLiveRuns({
        appStore,
        requestUser,
        decisionBackend: req.query.decision_backend,
        limit: parsePositiveInt(topn, 25)
      });
      if (appActiveRuns.length && appActiveRuns.every((run) => hasKnownZeroLiveRunProgress(run))) {
        const payload = await hydrateDashboardAccountNames({
          appStore,
          payload: buildLivePayloadFromAppRuns({
            runs: appActiveRuns,
            selectedRunId: req.query.simulation_run_id || null,
            topn
          })
        });
        return res.json(payload);
      }
      const rawPayload = await loadLivePayload({
        runPythonModule,
        parsePythonJson,
        simulationRunId: req.query.simulation_run_id,
        requestUser,
        decisionBackend: req.query.decision_backend,
        topn
      });
      let payload = await reconcileLivePayloadWithAppRuns({
        payload: rawPayload,
        appStore,
        requestUser,
        selectedRunId: req.query.simulation_run_id || null,
        appActiveRuns
      });
      payload = await hydrateSelectedLiveRunRows({
        payload,
        simulationRunId: req.query.simulation_run_id || null,
        requestUser,
        decisionBackend: req.query.decision_backend,
        topn,
        runPythonModule,
        parsePythonJson
      });
      payload = await hydrateLiveLeaderboardsFromAppRuns({
        payload,
        appStore,
        requestUser,
        topn,
        appActiveRuns
      });
      payload = await hydrateCompletedSelectedRunResults({
        payload,
        runPythonModule,
        parsePythonJson,
        topn
      });
      payload = await hydrateDashboardAccountNames({
        payload,
        appStore
      });
      return res.json(payload);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load live dashboard data",
        error: error.message
      });
    }
  });

  router.get("/performance/summary", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const requestUser = resolveRequestUser(req);
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const runs = await appStore.listSimulationRuns({
        userId: requestUser.userId,
        decisionBackend: req.query.decision_backend || null,
        limit: 1000,
        offset: 0
      });
      if (!runs.length) {
        return res.json(
          emptyPerformancePayload({
            user_id: requestUser.userId,
            decision_backend: req.query.decision_backend || null
          })
        );
      }
      const payload = await loadPerformancePayload({
        runPythonModule,
        parsePythonJson,
        simulationRunIds: runs.map((run) => run.simulation_run_id),
        requestUser,
        decisionBackend: req.query.decision_backend,
        topn: req.query.topn
      });
      return res.json(payload);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load performance summary",
        error: error.message
      });
    }
  });

  router.get("/performance/queries", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const requestUser = resolveRequestUser(req);
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const savedQueries = await appStore.listSavedQueries({
        userId: requestUser.userId,
        queryType: req.query.query_type || null,
        limit: parsePositiveInt(req.query.limit, 100)
      });
      return res.json({ saved_queries: savedQueries });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load saved queries",
        error: error.message
      });
    }
  });

  router.post("/performance/query", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const parsed = performanceQueryRequestSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const requestUser = resolveRequestUser(req, { allowBody: true });
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const filters = parsed.data.filters || {};
      const groupBy = parsed.data.group_by || [];
      const sort = parsed.data.sort || null;
      const runs = await appStore.listSimulationRuns({
        userId: requestUser.userId,
        status: filters.status || null,
        decisionBackend: filters.decision_backend || null,
        heroAgentDefinitionId: filters.hero_agent_definition_id || null,
        heroAgentVersionId: filters.hero_agent_version_id || null,
        limit: 5000,
        offset: 0
      });
      const matchedRuns = filterRunRecordsForQuery(runs, filters);

      let savedQuery = null;
      if (parsed.data.save) {
        if (!parsed.data.name) {
          return res.status(400).json({
            message: "A query name is required when save=true"
          });
        }
        savedQuery = await appStore.createSavedQuery({
          userId: requestUser.userId,
          name: parsed.data.name,
          queryType: parsed.data.query_type,
          filters: {
            ...filters,
            topn: parsed.data.topn,
            group_by: groupBy,
            sort,
            query_version: 2
          }
        });
      }

      if (!matchedRuns.length) {
        return res.status(parsed.data.save ? 201 : 200).json({
          saved_query: savedQuery,
          matched_run_ids: [],
          ...emptyPerformancePayload({
            ...filters,
            user_id: requestUser.userId
          }, {
            groupBy,
            sort,
            topn: parsed.data.topn
          })
        });
      }

      const payload = await loadPerformanceQueryPayload({
        runPythonModule,
        parsePythonJson,
        simulationRunIds: matchedRuns.map((run) => run.simulation_run_id),
        requestUser,
        filters,
        groupBy,
        sort,
        topn: parsed.data.topn
      });

      return res.status(parsed.data.save ? 201 : 200).json({
        saved_query: savedQuery,
        matched_run_ids: matchedRuns.map((run) => run.simulation_run_id),
        ...payload
      });
    } catch (error) {
      const statusCode = /date_from|date_to/i.test(String(error.message || "")) ? 400 : 500;
      return res.status(statusCode).json({
        message: "Failed to execute performance query",
        error: error.message
      });
    }
  });

  router.get("/performance/query/:savedQueryId", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const requestUser = resolveRequestUser(req);
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const savedQuery = await appStore.getSavedQuery(req.params.savedQueryId, {
        userId: requestUser.userId
      });
      if (!savedQuery) {
        return res.status(404).json({ message: "Saved query not found" });
      }

      const savedDefinition = parseSavedQueryDefinition(savedQuery, 25);
      const filters = savedDefinition.filters || {};
      const runs = await appStore.listSimulationRuns({
        userId: requestUser.userId,
        status: filters.status || null,
        decisionBackend: filters.decision_backend || null,
        heroAgentDefinitionId: filters.hero_agent_definition_id || null,
        heroAgentVersionId: filters.hero_agent_version_id || null,
        limit: 5000,
        offset: 0
      });
      const matchedRuns = filterRunRecordsForQuery(runs, filters);
      if (!matchedRuns.length) {
        return res.json({
          saved_query: savedQuery,
          matched_run_ids: [],
          ...emptyPerformancePayload({
            ...filters,
            user_id: requestUser.userId
          }, {
            groupBy: savedDefinition.groupBy,
            sort: savedDefinition.sort,
            topn: req.query.topn || savedDefinition.topn || 25
          })
        });
      }
      const payload = await loadPerformanceQueryPayload({
        runPythonModule,
        parsePythonJson,
        simulationRunIds: matchedRuns.map((run) => run.simulation_run_id),
        requestUser,
        filters,
        groupBy: savedDefinition.groupBy,
        sort: savedDefinition.sort,
        topn: req.query.topn || savedDefinition.topn || 25
      });

      return res.json({
        saved_query: savedQuery,
        matched_run_ids: matchedRuns.map((run) => run.simulation_run_id),
        ...payload
      });
    } catch (error) {
      const statusCode = /date_from|date_to/i.test(String(error.message || "")) ? 400 : 500;
      return res.status(statusCode).json({
        message: "Failed to load saved performance query",
        error: error.message
      });
    }
  });

  router.post("/agents", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const parsed = createAgentSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const requestUser = resolveRequestUser(req, { allowBody: true });
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      await ensureUserRecord(appStore, requestUser);
      const created = await appStore.createAgent({
        userId: requestUser.userId,
        name: parsed.data.name,
        description: parsed.data.description || null,
        contextText: parsed.data.context,
        decisionBackendDefault:
          parsed.data.decision_backend_default || "llm",
        metadata: parsed.data.metadata || {}
      });
      return res.status(201).json(created);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to create agent",
        error: error.message
      });
    }
  });

  router.get("/agents", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const requestUser = resolveRequestUser(req);
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const agents = await appStore.listAgents({
        userId: requestUser.userId
      });
      return res.json({ agents });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load agents",
        error: error.message
      });
    }
  });

  router.get("/agents/:agentDefinitionId", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const requestUser = resolveRequestUser(req);
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const agent = await appStore.getAgent(req.params.agentDefinitionId, {
        userId: requestUser.userId
      });
      if (!agent) {
        return res.status(404).json({ message: "Agent not found" });
      }
      return res.json({ agent });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load agent",
        error: error.message
      });
    }
  });

  router.delete("/agents/:agentDefinitionId", async (req, res) => {
    if (!appStore?.deleteAgent) {
      return appDatabaseUnavailable(res);
    }

    const requestUser = resolveRequestUser(req);
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const agent = await appStore.getAgent(req.params.agentDefinitionId, {
        userId: requestUser.userId
      });
      if (!agent) {
        return res.status(404).json({ message: "Agent not found" });
      }

      const deletedAgent = await appStore.deleteAgent(req.params.agentDefinitionId, {
        userId: requestUser.userId
      });
      if (!deletedAgent) {
        return res.status(404).json({ message: "Agent not found" });
      }

      return res.json({
        ok: true,
        agent_definition_id: req.params.agentDefinitionId,
        deleted_agent: deletedAgent
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to delete agent",
        error: error.message
      });
    }
  });

  router.get("/agents/:agentDefinitionId/performance", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const requestUser = resolveRequestUser(req);
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      const agent = await appStore.getAgent(req.params.agentDefinitionId, {
        userId: requestUser.userId
      });
      if (!agent) {
        return res.status(404).json({ message: "Agent not found" });
      }

      const runs = await appStore.listSimulationRuns({
        userId: requestUser.userId,
        heroAgentDefinitionId: req.params.agentDefinitionId,
        decisionBackend: req.query.decision_backend || null,
        limit: 1000,
        offset: 0
      });
      if (!runs.length) {
        return res.json({
          agent: {
          agent_definition_id: agent.agent_definition_id,
          name: agent.name,
          latest_version: agent.latest_version || null,
          versions: agent.versions || []
        },
        version_summaries: (agent.versions || []).map((version) => ({
          version,
          label: `Version ${version.version_number}`,
          is_latest_version:
            version.agent_version_id === agent.latest_version?.agent_version_id,
          run_count: 0,
          status_counts: summarizeRunStatuses([]),
          latest_requested_at: null,
          latest_simulation_run_id: null,
          simulation_runs: [],
          performance_summary: null,
          profile_overview: emptyProfileOverview(),
          recent_runs: []
        })),
          ...emptyPerformancePayload({
            user_id: requestUser.userId,
            decision_backend: req.query.decision_backend || null,
            hero_agent_definition_id: req.params.agentDefinitionId
          })
        });
      }
      const versionSummaries = await buildAgentVersionSummaries({
        agent,
        runs,
        requestUser,
        decisionBackend: req.query.decision_backend || null,
        topn: parsePositiveInt(req.query.topn, 5),
        runPythonModule,
        parsePythonJson
      });
      const payload = await loadPerformancePayload({
        runPythonModule,
        parsePythonJson,
        simulationRunIds: runs.map((run) => run.simulation_run_id),
        requestUser,
        decisionBackend: req.query.decision_backend,
        topn: req.query.topn
      });
      return res.json({
        agent: {
          agent_definition_id: agent.agent_definition_id,
          name: agent.name,
          latest_version: agent.latest_version || null,
          versions: agent.versions || []
        },
        version_summaries: versionSummaries,
        ...payload
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load agent performance",
        error: error.message
      });
    }
  });

  router.post("/agents/:agentDefinitionId/versions", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    const parsed = createAgentVersionSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const requestUser = resolveRequestUser(req, { allowBody: true });
    if (!requestUser?.userId) {
      return missingUserError(res);
    }

    try {
      await ensureUserRecord(appStore, requestUser);
      const version = await appStore.createAgentVersion({
        agentDefinitionId: req.params.agentDefinitionId,
        userId: requestUser.userId,
        contextText: parsed.data.context,
        decisionBackendDefault:
          parsed.data.decision_backend_default || "llm",
        metadata: parsed.data.metadata || {}
      });
      if (!version) {
        return res.status(404).json({ message: "Agent not found" });
      }
      return res.status(201).json({ version });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to create agent version",
        error: error.message
      });
    }
  });

  router.get("/simulations", async (req, res) => {
    if (!appStore) {
      return appDatabaseUnavailable(res);
    }

    try {
      const requestUser = resolveRequestUser(req);
      const runs = await appStore.listSimulationRuns({
        userId: requestUser?.userId || null,
        status: req.query.status || null,
        decisionBackend: req.query.decision_backend || null,
        heroAgentDefinitionId: req.query.hero_agent_definition_id || null,
        heroAgentVersionId: req.query.hero_agent_version_id || null,
        limit: parsePositiveInt(req.query.limit, 50),
        offset: parseNonNegativeInt(req.query.offset, 0)
      });
      return res.json({ runs });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load simulation runs",
        error: error.message
      });
    }
  });

  router.get("/simulations/:simulationRunId/live", async (req, res) => {
    try {
      const requestUser = resolveRequestUser(req);
      let appRun = null;
      if (appStore && requestUser?.userId) {
        appRun = await appStore.getSimulationRun(req.params.simulationRunId, {
          userId: requestUser.userId
        });
        if (!appRun) {
          return res.status(404).json({ message: "Simulation run not found" });
        }
      }
      if (
        appRun &&
        hasKnownZeroLiveRunProgress(appRun) &&
        ["queued", "running"].includes(normalizedRunStatus(appRun.status))
      ) {
        return res.json(
          buildLivePayloadFromAppRuns({
            runs: [appRun],
            selectedRunId: req.params.simulationRunId,
            topn: req.query.topn
          })
        );
      }
      const payload = await loadLivePayload({
        runPythonModule,
        parsePythonJson,
        simulationRunId: req.params.simulationRunId,
        requestUser,
        decisionBackend: req.query.decision_backend,
        topn: req.query.topn
      });
      let reconciled = await reconcileLivePayloadWithAppRuns({
        payload,
        appStore,
        requestUser,
        selectedRunId: req.params.simulationRunId,
        appActiveRuns: appRun ? [appRun] : []
      });
      reconciled = await hydrateSelectedLiveRunRows({
        payload: reconciled,
        simulationRunId: req.params.simulationRunId,
        selectedAppRun: appRun,
        requestUser,
        decisionBackend: req.query.decision_backend,
        topn: req.query.topn,
        runPythonModule,
        parsePythonJson
      });
      reconciled = await hydrateLiveLeaderboardsFromAppRuns({
        payload: reconciled,
        appStore,
        requestUser,
        topn: req.query.topn,
        appActiveRuns: appRun ? [appRun] : []
      });
      return res.json(reconciled);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load live simulation data",
        error: error.message
      });
    }
  });

  router.get("/simulations/:simulationRunId/results", async (req, res) => {
    try {
      const requestUser = resolveRequestUser(req);
      let run = await loadOwnedRun(req.params.simulationRunId, requestUser);
      if (!run) {
        return res.status(404).json({ message: "Simulation run not found" });
      }
      const initialRunStatus = String(run.status || "").trim().toLowerCase();
      run = await promoteExecutionCompletedRun(req.params.simulationRunId, run);
      let warehouseStatusPayload = null;
      let normalizedRunStatus = String(run.status || "").trim().toLowerCase();
      const shouldProbeWarehouseStatus = normalizedRunStatus !== "failed";
      if (shouldProbeWarehouseStatus) {
        try {
          warehouseStatusPayload = await loadSimulationResultsStatusPayload({
            runPythonModule,
            parsePythonJson,
            simulationRunId: req.params.simulationRunId
          });
          warehouseStatusPayload = markStaleWarehousePayload(
            warehouseStatusPayload,
            run,
            "Final results belong to an older run with the same simulation run name"
          );
        } catch (_error) {
          warehouseStatusPayload = null;
        }
      }

      if (normalizedRunStatus !== "failed" && warehouseStatusPayload?.ready === true) {
        if (appStore?.updateSimulationRunLifecycle) {
          run =
            (await appStore.updateSimulationRunLifecycle(
              req.params.simulationRunId,
              {
                status: "completed",
                completedAt:
                  warehouseStatusPayload?.run_summary?.finished_at ||
                  warehouseStatusPayload?.run_summary?.completed_at ||
                  run.completed_at ||
                  new Date().toISOString(),
                clearErrorMessage: true
              }
            )) || run;
        } else {
          run = {
            ...run,
            status: "completed"
          };
        }
        normalizedRunStatus = "completed";
      }

      if (["queued", "running"].includes(normalizedRunStatus)) {
        return res.status(409).json({
          message: "Simulation run is not complete yet",
          simulation_run_id: req.params.simulationRunId,
          status: run.status
        });
      }

      if (
        normalizedRunStatus === "completed" &&
        warehouseStatusPayload &&
        warehouseStatusPayload?.ready !== true
      ) {
        const warehouseRunSummary = warehouseStatusPayload?.run_summary || {};
        const provisionalPlayerSummaries = normalizePlayerSummaries(
          run.player_summaries
        );
        const provisionalRunMeta = {
          ...warehouseRunSummary,
          simulation_run_id: run.simulation_run_id,
          status: "completed",
          materialization_status: "materializing",
          published_hand_summaries:
            run.published_hand_summaries ??
            warehouseRunSummary.published_hand_summaries ??
            0
        };
        const provisionalRows = withDashboardRunFields(
          provisionalPlayerSummaries,
          provisionalRunMeta
        );
        const provisionalHeroProfitTimeseries = chooseHeroProfitTimeseries({
          appRows: normalizeHeroProfitTimeseriesRows(
            run.hero_profit_timeseries,
            provisionalRunMeta
          ),
          coarseRows: buildCoarseHeroProfitTimeseries(
            provisionalRunMeta,
            provisionalRows
          ),
          selectedRunMeta: provisionalRunMeta,
          sourceRows: provisionalRows
        });
        return res.status(202).json({
          simulation_run_id: run.simulation_run_id,
          status: "completed",
          materialization_status: "materializing",
          source:
            provisionalPlayerSummaries.length > 0
              ? "app_store"
              : warehouseStatusPayload?.source || "pending",
          message:
            provisionalPlayerSummaries.length > 0
              ? "Execution finished. Showing the latest run summary while final results are prepared."
              : warehouseStatusPayload?.message ||
                "Final results are still being prepared",
          run_summary: {
            ...warehouseRunSummary,
            simulation_run_id: run.simulation_run_id,
            status: "completed",
            materialization_status: "materializing",
            requested_at:
              run.requested_at ||
              warehouseRunSummary.requested_at ||
              null,
            started_at:
              run.started_at ||
              warehouseRunSummary.started_at ||
              null,
            finished_at:
              warehouseRunSummary.finished_at ||
              run.completed_at ||
              null,
            completed_at:
              warehouseRunSummary.completed_at ||
              run.completed_at ||
              null,
            updated_at:
              run.updated_at ||
              warehouseRunSummary.updated_at ||
              null,
            user_id:
              run.user_id ||
              warehouseRunSummary.user_id ||
              null,
            decision_backend:
              run.decision_backend ||
              warehouseRunSummary.decision_backend ||
              null,
            backend_type:
              run.backend_type ||
              warehouseRunSummary.backend_type ||
              null,
            model_name:
              run.model_name ||
              warehouseRunSummary.model_name ||
              null,
            request_mode:
              run.request_mode ||
              warehouseRunSummary.request_mode ||
              null,
            hero_context_hash:
              run.hero_context_hash ||
              warehouseRunSummary.hero_context_hash ||
              null,
            hero_agent_definition_id:
              run.hero_agent_definition_id ||
              warehouseRunSummary.hero_agent_definition_id ||
              null,
            hero_agent_version_id:
              run.hero_agent_version_id ||
              warehouseRunSummary.hero_agent_version_id ||
              null,
            published_actions:
              run.published_actions ??
              warehouseRunSummary.published_actions ??
              0,
            published_hand_summaries:
              run.published_hand_summaries ??
              warehouseRunSummary.published_hand_summaries ??
              0
          },
          result: buildResultPayload(provisionalPlayerSummaries),
          player_summaries: provisionalPlayerSummaries,
          run_profit_leaderboard: buildPlayerSummaryLeaderboard(
            provisionalPlayerSummaries,
            "total_bb_won",
            req.query.topn
          ),
          run_bb_per_100_leaderboard: buildPlayerSummaryLeaderboard(
            provisionalPlayerSummaries,
            "bb_per_100",
            req.query.topn
          ),
          run_high_hand_leaderboard: [],
          agent_metrics: [],
          profile_assignments: [],
          hero_profit_timeseries: provisionalHeroProfitTimeseries
        });
      }

      let warehousePayload = null;
      try {
        warehousePayload = await loadSimulationResultsPayload({
          runPythonModule,
          parsePythonJson,
          simulationRunId: req.params.simulationRunId,
          topn: req.query.topn
        });
        if (!payloadMatchesAppRun(warehousePayload, run)) {
          warehousePayload = null;
        }
      } catch (_error) {
        warehousePayload = null;
      }

      const legacyRun = !warehousePayload && !appStore
        ? await loadLegacyRun(req.params.simulationRunId)
        : null;

      if (
        appStore?.updateSimulationRunLifecycle &&
        warehousePayload?.status === "ready" &&
        normalizedRunStatus !== "completed"
      ) {
        run =
          (await appStore.updateSimulationRunLifecycle(
            req.params.simulationRunId,
            {
              status: "completed",
              completedAt:
                warehousePayload?.run_summary?.finished_at ||
                warehousePayload?.run_summary?.completed_at ||
                run.completed_at ||
                new Date().toISOString(),
              clearErrorMessage: true
            }
          )) || run;
      }

      const playerSummaries =
        warehousePayload?.player_summaries?.length
          ? warehousePayload.player_summaries
          : legacyRun?.player_summaries || [];
      const fallbackResult = buildResultPayload(playerSummaries);
      const warehouseResult = warehousePayload?.result || legacyRun?.result || {};
      const result = {
        ...fallbackResult,
        ...warehouseResult,
        performance:
          warehouseResult?.performance?.length
            ? warehouseResult.performance
            : fallbackResult.performance,
        summary: warehouseResult?.summary || fallbackResult.summary
      };
      const warehouseRunSummary = warehousePayload?.run_summary || {};
      const runSummary = {
        ...warehouseRunSummary,
        simulation_run_id: run.simulation_run_id,
        status:
          run.status ||
          warehouseRunSummary.status ||
          warehousePayload?.status ||
          null,
        requested_at:
          run.requested_at ||
          warehouseRunSummary.requested_at ||
          null,
        started_at:
          run.started_at ||
          warehouseRunSummary.started_at ||
          null,
        finished_at:
          run.completed_at ||
          warehouseRunSummary.finished_at ||
          null,
        completed_at:
          run.completed_at ||
          warehouseRunSummary.completed_at ||
          null,
        updated_at:
          run.updated_at ||
          warehouseRunSummary.updated_at ||
          null,
        user_id:
          run.user_id ||
          warehouseRunSummary.user_id ||
          null,
        decision_backend:
          run.decision_backend ||
          warehouseRunSummary.decision_backend ||
          null,
        backend_type:
          run.backend_type ||
          warehouseRunSummary.backend_type ||
          null,
        model_name:
          run.model_name ||
          warehouseRunSummary.model_name ||
          null,
        request_mode:
          run.request_mode ||
          warehouseRunSummary.request_mode ||
          null,
        hero_context_hash:
          run.hero_context_hash ||
          warehouseRunSummary.hero_context_hash ||
          null,
        hero_agent_definition_id:
          run.hero_agent_definition_id ||
          warehouseRunSummary.hero_agent_definition_id ||
          null,
        hero_agent_version_id:
          run.hero_agent_version_id ||
          warehouseRunSummary.hero_agent_version_id ||
          null,
        published_actions:
          run.published_actions ??
          warehouseRunSummary.published_actions ??
          0,
        published_hand_summaries:
          run.published_hand_summaries ??
          warehouseRunSummary.published_hand_summaries ??
          0
      };
      const resultRows = normalizeDashboardResultRows(
        warehousePayload?.agent_metrics?.length
          ? warehousePayload.agent_metrics
          : playerSummaries,
        runSummary
      );
      const heroProfitTimeseries = chooseHeroProfitTimeseries({
        existingRows: normalizeHeroProfitTimeseriesRows(
          warehousePayload?.hero_profit_timeseries,
          runSummary
        ),
        appRows: normalizeHeroProfitTimeseriesRows(run.hero_profit_timeseries, runSummary),
        selectedRunMeta: runSummary,
        sourceRows: resultRows
      });

      return res.json({
        simulation_run_id: run.simulation_run_id,
        status: run.status,
        source: warehousePayload?.source || "legacy",
        run_summary: runSummary,
        result,
        player_summaries: playerSummaries,
        run_profit_leaderboard: warehousePayload?.run_profit_leaderboard || [],
        run_bb_per_100_leaderboard:
          warehousePayload?.run_bb_per_100_leaderboard || [],
        run_high_hand_leaderboard:
          warehousePayload?.run_high_hand_leaderboard || [],
        agent_metrics: warehousePayload?.agent_metrics || [],
        profile_assignments: warehousePayload?.profile_assignments || [],
        hero_profit_timeseries: heroProfitTimeseries
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load simulation results",
        error: error.message
      });
    }
  });

  router.get("/simulations/:simulationRunId", async (req, res) => {
    try {
      const requestUser = resolveRequestUser(req);
      let run = await loadOwnedRun(req.params.simulationRunId, requestUser);
      if (!run) {
        return res.status(404).json({ message: "Simulation run not found" });
      }
      run = await promoteExecutionCompletedRun(req.params.simulationRunId, run);
      return res.json(run);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load simulation run",
        error: error.message
      });
    }
  });

  router.post("/simulations/:simulationRunId/cancel", async (req, res) => {
    try {
      if (!appStore?.updateSimulationRunLifecycle) {
        return appDatabaseUnavailable(res);
      }

      const requestUser = resolveRequestUser(req, { allowBody: true });
      if (!requestUser?.userId) {
        return missingUserError(res);
      }

      const run = await loadOwnedRun(req.params.simulationRunId, requestUser);
      if (!run) {
        return res.status(404).json({ message: "Simulation run not found" });
      }

      const runStatus = normalizedRunStatus(run.status);
      if (["completed", "failed"].includes(runStatus)) {
        return res.status(409).json({
          message: "Simulation run is already terminal",
          simulation_run_id: req.params.simulationRunId,
          status: run.status
        });
      }

      const canceledRun = await appStore.updateSimulationRunLifecycle(
        req.params.simulationRunId,
        {
          status: "failed",
          completedAt: new Date().toISOString(),
          errorMessage: "Canceled by user"
        }
      );
      if (!canceledRun) {
        return res.status(404).json({ message: "Simulation run not found" });
      }

      return res.json({
        ok: true,
        simulation_run_id: canceledRun.simulation_run_id,
        status: canceledRun.status,
        run: canceledRun
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to cancel simulation run",
        error: error.message
      });
    }
  });

  router.delete("/simulations/:simulationRunId", async (req, res) => {
    try {
      if (!appStore?.deleteSimulationRun) {
        return appDatabaseUnavailable(res);
      }

      const requestUser = resolveRequestUser(req);
      if (!requestUser?.userId) {
        return missingUserError(res);
      }

      const run = await loadOwnedRun(req.params.simulationRunId, requestUser);
      if (!run) {
        return res.status(404).json({ message: "Simulation run not found" });
      }

      const runStatus = normalizedRunStatus(run.status);
      const force = parseBooleanQuery(req.query.force);
      if (
        !["completed", "failed", "finalizing"].includes(runStatus) &&
        !(force && ["queued", "running"].includes(runStatus))
      ) {
        return res.status(409).json({
          message:
            "Only completed, failed, or finalizing runs can be deleted. Use force=true to stop and clear queued or running runs.",
          simulation_run_id: req.params.simulationRunId,
          status: run.status
        });
      }

      const deletedRun = await appStore.deleteSimulationRun(req.params.simulationRunId, {
        userId: requestUser.userId
      });
      if (!deletedRun) {
        return res.status(404).json({ message: "Simulation run not found" });
      }

      return res.json({
        ok: true,
        simulation_run_id: deletedRun.simulation_run_id,
        deleted_run: deletedRun
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to delete simulation run",
        error: error.message
      });
    }
  });

  router.get("/simulations/:simulationRunId/profile-jobs", async (req, res) => {
    try {
      if (!appStore?.listProfileJobs) {
        return appDatabaseUnavailable(res);
      }

      const requestUser = resolveRequestUser(req);
      const run = await loadOwnedRun(req.params.simulationRunId, requestUser);
      if (!run) {
        return res.status(404).json({ message: "Simulation run not found" });
      }

      const profileJobs = await appStore.listProfileJobs({
        simulationRunId: req.params.simulationRunId,
        userId: requestUser?.userId || run.user_id || null,
        limit: parsePositiveInt(req.query.limit, 20)
      });

      return res.json({
        simulation_run_id: run.simulation_run_id,
        profile_jobs: profileJobs
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load profile jobs",
        error: error.message
      });
    }
  });

  router.get("/simulations/:simulationRunId/profiles", async (req, res) => {
    try {
      if (req.query.refresh != null) {
        return res.status(400).json({
          message:
            "Use POST /api/simulations/:simulationRunId/profiles/request to refresh profile results"
        });
      }

      const requestUser = resolveRequestUser(req);
      let run = await loadOwnedRun(req.params.simulationRunId, requestUser);
      if (!run) {
        return res.status(404).json({ message: "Simulation run not found" });
      }
      const readiness = await resolveProfileReadyRun({
        simulationRunId: req.params.simulationRunId,
        requestUser,
        run
      });
      run = readiness.run;
      if (!readiness.ready) {
        return res.status(409).json({
          message:
            readiness.status === "completed" &&
            isMaterializingStatus(readiness.materializationStatus)
              ? "Final results are still being prepared"
              : "Simulation run is not complete yet",
          simulation_run_id: req.params.simulationRunId,
          status: run.status
        });
      }

      const requestedPlayerId =
        String(req.query.player_id || "").trim() || null;
      const requestedAgentId =
        String(req.query.agent_id || "").trim() || null;
      const response = await executeProfileLookup({
        simulationRunId: req.params.simulationRunId,
        requestUser,
        run,
        playerId: requestedPlayerId,
        agentId: requestedAgentId,
        cachedOnly: true,
        persistJob: false,
        warehouseStatusPayload: readiness.warehouseStatusPayload,
        profileSourceStatusPayload: readiness.profileSourceStatusPayload
      });
      return res.status(response.statusCode).json(response.body);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to load simulation profiles",
        error: error.message
      });
    }
  });

  router.post("/simulations/:simulationRunId/profiles/request", async (req, res) => {
    const parsed = profileRequestSchema.safeParse(req.body || {});
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    try {
      const requestUser = resolveRequestUser(req, { allowBody: true });
      let run = await loadOwnedRun(req.params.simulationRunId, requestUser);
      if (!run) {
        return res.status(404).json({ message: "Simulation run not found" });
      }
      const readiness = await resolveProfileReadyRun({
        simulationRunId: req.params.simulationRunId,
        requestUser,
        run
      });
      run = readiness.run;
      if (!readiness.ready) {
        return res.status(409).json({
          message:
            readiness.status === "completed" &&
            isMaterializingStatus(readiness.materializationStatus)
              ? "Final results are still being prepared"
              : "Simulation run is not complete yet",
          simulation_run_id: req.params.simulationRunId,
          status: run.status
        });
      }

      const response = await executeProfileLookup({
        simulationRunId: req.params.simulationRunId,
        requestUser,
        run,
        playerId: parsed.data.player_id || null,
        agentId: parsed.data.agent_id || null,
        refreshRequested: parsed.data.refresh === true,
        cachedOnly: false,
        persistJob: true,
        warehouseStatusPayload: readiness.warehouseStatusPayload,
        profileSourceStatusPayload: readiness.profileSourceStatusPayload,
        useProvisionalProfileFeatures:
          readiness.materializationStatus === "provisional_ready"
      });
      return res.status(response.statusCode).json(response.body);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to request simulation profiles",
        error: error.message
      });
    }
  });

  router.post("/actions", async (req, res) => {
    const parsed = actionEventSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const event = {
      ...parsed.data,
      event_type: "action",
      event_ts: parsed.data.event_ts || new Date().toISOString()
    };
    const count = await publishActionEvents([event]);
    return res.json({ message: "Action event published", count });
  });

  router.post("/actions/batch", async (req, res) => {
    const parsed = actionBatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const events = parsed.data.events.map((event) => ({
      ...event,
      event_type: "action",
      event_ts: event.event_ts || new Date().toISOString()
    }));
    const count = await publishActionEvents(events);
    return res.json({ message: "Action batch published", count });
  });

  router.post("/simulations", async (req, res) => {
    const parsed = simulationRequestSchema.safeParse(req.body);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const requestUser = resolveRequestUser(req, { allowBody: true });
    const resolvedUserId =
      requestUser?.userId || parsed.data.user_id || null;

    let agentVersion = null;
    if (parsed.data.agent_version_id) {
      if (!appStore) {
        return appDatabaseUnavailable(res);
      }
      try {
        agentVersion = await appStore.getAgentVersion(parsed.data.agent_version_id, {
          userId: resolvedUserId
        });
      } catch (error) {
        return res.status(500).json({
          message: "Failed to resolve agent version",
          error: error.message
        });
      }
      if (!agentVersion) {
        return res.status(404).json({
          message: "Agent version not found",
          agent_version_id: parsed.data.agent_version_id
        });
      }
    }

    const resolvedContext =
      String(parsed.data.context || "").trim() || agentVersion?.context_text || "";
    if (!resolvedContext) {
      return res.status(400).json({
        message: "Validation failed",
        issues: [
          {
            path: ["context"],
            message: "Either context or agent_version_id is required"
          }
        ]
      });
    }

    const decisionBackend = normalizedDecisionBackend(
      parsed.data.decision_backend || agentVersion?.decision_backend_default || "llm"
    );
    const backendConfig =
      DECISION_BACKEND_OPTIONS[decisionBackend] || DECISION_BACKEND_OPTIONS.llm;
    const resolvedHeroContextHash = heroContextHash(resolvedContext);
    const requestMode = agentVersion ? "saved_agent" : "context";
    const heroAgentDefinitionId =
      agentVersion?.agent_definition_id || parsed.data.agent_definition_id || null;
    const heroAgentVersionId =
      agentVersion?.agent_version_id || parsed.data.agent_version_id || null;

    const requestPayload = {
      ...parsed.data,
      context: resolvedContext,
      user_id: resolvedUserId,
      decision_backend: decisionBackend,
      hero_agent_definition_id: heroAgentDefinitionId,
      hero_agent_version_id: heroAgentVersionId,
      event_type: "simulation_request",
      payload_version: "1",
      table_id: "table_1",
      request_mode: requestMode,
      hero_context_hash: resolvedHeroContextHash,
      backend_type: backendConfig.backendType,
      model_name: backendConfig.modelName,
      reasoning_effort: backendConfig.reasoningEffort,
      requested_at: new Date().toISOString()
    };

    try {
      if (appStore) {
        await ensureUserRecord(appStore, requestUser);
        await appStore.createSimulationRun({
          simulationRunId: parsed.data.simulation_run_id,
          userId: resolvedUserId,
          heroAgentDefinitionId,
          heroAgentVersionId,
          status: "queued",
          decisionBackend,
          backendType: backendConfig.backendType,
          modelName: backendConfig.modelName,
          requestMode,
          handCount: parsed.data.hand_count,
          seed: parsed.data.seed,
          heroSeat: parsed.data.hero_seat,
          heroContextHash: resolvedHeroContextHash,
          heroContextPreview: resolvedContext.slice(0, 80),
          requestPayload,
          requestedAt: requestPayload.requested_at
        });
      } else {
        await syncLegacyQueuedRun(requestPayload, { required: true });
      }
    } catch (error) {
      const message =
        error.message ||
        (appStore
          ? "Failed to persist simulation request"
          : "Simulation queueing failed");
      await bestEffortMarkLegacyRunFailed(
        parsed.data.simulation_run_id,
        message
      );
      if (appStore) {
        await appStore
          .markSimulationRunFailed(
            parsed.data.simulation_run_id,
            message
          )
          .catch(() => null);
      }
      if (
        error instanceof AppStoreConflictError ||
        String(message).includes("already exists")
      ) {
        return res.status(409).json({
          message: "Simulation run already exists",
          simulation_run_id: parsed.data.simulation_run_id
        });
      }
      return res.status(500).json({
        message: appStore
          ? "Failed to persist simulation request"
          : "Simulation queueing failed",
        error: message
      });
    }

    try {
      const lifecycleEvent = {
        event_type: "simulation_requested",
        simulation_run_id: parsed.data.simulation_run_id,
        table_id: requestPayload.table_id,
        requested_at: requestPayload.requested_at,
        hand_count: parsed.data.hand_count,
        request_mode: requestMode,
        user_id: resolvedUserId,
        decision_backend: decisionBackend,
        hero_context_hash: resolvedHeroContextHash,
        hero_context_preview: resolvedContext.slice(0, 80),
        hero_agent_definition_id: heroAgentDefinitionId,
        hero_agent_version_id: heroAgentVersionId,
        backend_type: backendConfig.backendType,
        model_name: backendConfig.modelName,
        status: "queued"
      };
      await Promise.all([
        publishSimulationRequests([requestPayload]),
        publishSimulationEvents([lifecycleEvent])
      ]);

      const statusUrl = `/api/simulations/${parsed.data.simulation_run_id}`;
      return res.status(202).location(statusUrl).json({
        message: "Simulation run queued",
        simulation_run_id: parsed.data.simulation_run_id,
        status: "queued",
        status_url: statusUrl,
        hero_agent_definition_id: heroAgentDefinitionId,
        hero_agent_version_id: heroAgentVersionId
      });
    } catch (error) {
      await bestEffortMarkLegacyRunFailed(
        parsed.data.simulation_run_id,
        error.message || "Failed to publish simulation request"
      );
      if (appStore) {
        await appStore
          .markSimulationRunFailed(
            parsed.data.simulation_run_id,
            error.message || "Failed to publish simulation request"
          )
          .catch(() => null);
      }
      return res.status(500).json({
        message: "Simulation queueing failed",
        error: error.message
      });
    }
  });

  router.post("/hand-summaries", async (req, res) => {
    const payload = Array.isArray(req.body) ? req.body : [req.body];
    const parsed = z.array(handSummarySchema).safeParse(payload);
    if (!parsed.success) {
      return validationError(res, parsed.error);
    }

    const summaries = parsed.data.map((summary) => ({
      ...summary,
      event_type: "hand_summary"
    }));
    const count = await publishHandSummaries(summaries);
    return res.json({ message: "Hand summaries published", count });
  });

  return router;
}

const router = createEventsRouter();

export default router;
