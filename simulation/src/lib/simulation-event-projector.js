import { AppStoreConflictError } from "./app-store.js";

const SUPPORTED_EVENT_TYPES = new Set([
  "simulation_requested",
  "simulation_started",
  "simulation_progress",
  "simulation_completed",
  "simulation_failed"
]);

const DEFAULT_STATUS_BY_EVENT_TYPE = {
  simulation_requested: "queued",
  simulation_started: "running",
  simulation_progress: "running",
  simulation_completed: "completed",
  simulation_failed: "failed"
};

function nonEmpty(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const normalized = String(value).trim();
  return normalized || null;
}

function normalizedDecisionBackend(value) {
  return nonEmpty(value) === "heuristic" ? "heuristic" : "llm";
}

function normalizedOptionalDecisionBackend(value) {
  return nonEmpty(value) ? normalizedDecisionBackend(value) : null;
}

function parseInteger(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function parsePlayerSummaries(value) {
  if (Array.isArray(value)) {
    return value.map((row) => (row && typeof row === "object" ? { ...row } : row));
  }
  if (typeof value === "string") {
    const normalized = value.trim();
    if (!normalized) {
      return null;
    }
    try {
      const parsed = JSON.parse(normalized);
      return Array.isArray(parsed)
        ? parsed.map((row) => (row && typeof row === "object" ? { ...row } : row))
        : null;
    } catch (_error) {
      return null;
    }
  }
  return null;
}

function parseProfileSessionFeatures(value) {
  if (Array.isArray(value)) {
    return value.map((row) => (row && typeof row === "object" ? { ...row } : row));
  }
  if (typeof value === "string") {
    const normalized = value.trim();
    if (!normalized) {
      return null;
    }
    try {
      const parsed = JSON.parse(normalized);
      return Array.isArray(parsed)
        ? parsed.map((row) => (row && typeof row === "object" ? { ...row } : row))
        : null;
    } catch (_error) {
      return null;
    }
  }
  return null;
}

function isoTimestamp(value) {
  const normalized = nonEmpty(value);
  if (!normalized) {
    return null;
  }
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return normalized;
  }
  return parsed.toISOString();
}

function timestampMs(value) {
  const normalized = nonEmpty(value);
  if (!normalized) {
    return null;
  }
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed.getTime();
}

function sameWhenPresent(left, right, normalize = (value) => nonEmpty(value)) {
  const normalizedLeft = normalize(left);
  const normalizedRight = normalize(right);
  return !normalizedLeft || !normalizedRight || normalizedLeft === normalizedRight;
}

function existingRunMatchesEvent(existingRun, event) {
  const existingRequestedAtMs = timestampMs(existingRun?.requested_at);
  const eventTimestampMs = timestampMs(event.eventTimestamp || event.requestedAt);
  if (
    existingRequestedAtMs != null &&
    eventTimestampMs != null &&
    eventTimestampMs < existingRequestedAtMs - 1000
  ) {
    return false;
  }

  return (
    sameWhenPresent(existingRun?.user_id, event.userId) &&
    sameWhenPresent(
      existingRun?.decision_backend,
      event.decisionBackend,
      normalizedOptionalDecisionBackend
    ) &&
    sameWhenPresent(existingRun?.hero_context_hash, event.heroContextHash) &&
    sameWhenPresent(existingRun?.hero_agent_definition_id, event.heroAgentDefinitionId) &&
    sameWhenPresent(existingRun?.hero_agent_version_id, event.heroAgentVersionId)
  );
}

function inferBackendType(decisionBackend, explicitBackendType = null) {
  const normalizedBackendType = nonEmpty(explicitBackendType);
  if (normalizedBackendType) {
    return normalizedBackendType;
  }
  if (decisionBackend === "heuristic") {
    return "heuristic_persona";
  }
  if (decisionBackend === "llm") {
    return "llm_gated_nano";
  }
  return null;
}

function findHeroSummary(rows = []) {
  const normalizedRows = Array.isArray(rows) ? rows : [];
  for (const row of normalizedRows) {
    if (!row || typeof row !== "object") {
      continue;
    }
    if (row.is_hero_player) {
      return row;
    }
    const playerId = nonEmpty(row.player_id);
    const agentId = nonEmpty(row.agent_id);
    if (playerId === "user_agent" || agentId === "user_agent") {
      return row;
    }
  }
  return null;
}

function parseFiniteNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function heroProfitPointFromEvent(event) {
  const heroSummary = findHeroSummary(event.playerSummaries);
  if (!heroSummary) {
    return null;
  }
  const handNumber = Math.max(
    0,
    event.publishedHandSummaries ?? 0,
    parseInteger(heroSummary.hands_played) ?? 0,
    parseInteger(heroSummary.observed_hands) ?? 0
  );
  const cumulativeBbWon = parseFiniteNumber(
    heroSummary.total_bb_won ?? heroSummary.bb_won
  );
  if (handNumber <= 0 || cumulativeBbWon == null) {
    return null;
  }
  return {
    simulation_run_id: event.simulationRunId,
    hand_number: handNumber,
    cumulative_bb_won: cumulativeBbWon,
    event_ts: event.eventTimestamp || null
  };
}

export function normalizeSimulationLifecycleEvent(rawEvent) {
  if (!rawEvent || typeof rawEvent !== "object") {
    return null;
  }

  const eventType = nonEmpty(rawEvent.event_type)?.toLowerCase() || null;
  const simulationRunId = nonEmpty(rawEvent.simulation_run_id);
  if (!eventType || !SUPPORTED_EVENT_TYPES.has(eventType) || !simulationRunId) {
    return null;
  }

  const decisionBackend = normalizedDecisionBackend(rawEvent.decision_backend);
  return {
    eventType,
    simulationRunId,
    status:
      nonEmpty(rawEvent.status)?.toLowerCase() ||
      DEFAULT_STATUS_BY_EVENT_TYPE[eventType] ||
      "queued",
    userId: nonEmpty(rawEvent.user_id),
    heroAgentDefinitionId: nonEmpty(rawEvent.hero_agent_definition_id),
    heroAgentVersionId: nonEmpty(rawEvent.hero_agent_version_id),
    decisionBackend,
    backendType: inferBackendType(decisionBackend, rawEvent.backend_type),
    modelName: nonEmpty(rawEvent.model_name),
    requestMode: nonEmpty(rawEvent.request_mode),
    handCount: parseInteger(rawEvent.hand_count),
    publishedActions: parseInteger(rawEvent.published_actions),
    publishedHandSummaries: parseInteger(rawEvent.published_hand_summaries),
    playerSummaries: parsePlayerSummaries(rawEvent.player_summaries),
    profileSessionFeatures: parseProfileSessionFeatures(
      rawEvent.profile_session_features
    ),
    seed: parseInteger(rawEvent.seed),
    heroSeat: parseInteger(rawEvent.hero_seat),
    heroContextHash: nonEmpty(rawEvent.hero_context_hash),
    heroContextPreview: nonEmpty(rawEvent.hero_context_preview),
    requestedAt: isoTimestamp(rawEvent.requested_at),
    eventTimestamp: isoTimestamp(rawEvent.event_ts),
    errorMessage: nonEmpty(rawEvent.error_message)
  };
}

function createRunPayloadFromEvent(event) {
  const heroProfitPoint = heroProfitPointFromEvent(event);
  return {
    simulationRunId: event.simulationRunId,
    userId: event.userId,
    heroAgentDefinitionId: event.heroAgentDefinitionId,
    heroAgentVersionId: event.heroAgentVersionId,
    status: event.status || "queued",
    decisionBackend: event.decisionBackend,
    backendType: event.backendType,
    modelName: event.modelName,
    requestMode: event.requestMode || "context",
    handCount: event.handCount ?? 500,
    publishedActions: event.publishedActions ?? 0,
    publishedHandSummaries: event.publishedHandSummaries ?? 0,
    playerSummaries: event.playerSummaries || [],
    profileSessionFeatures: event.profileSessionFeatures || [],
    heroProfitTimeseries: heroProfitPoint ? [heroProfitPoint] : [],
    seed: event.seed,
    heroSeat: event.heroSeat,
    heroContextHash: event.heroContextHash,
    heroContextPreview: event.heroContextPreview,
    requestPayload: {},
    errorMessage: event.errorMessage,
    requestedAt: event.requestedAt || event.eventTimestamp || new Date().toISOString()
  };
}

function lifecyclePatchFromEvent(event) {
  const heroProfitPoint = heroProfitPointFromEvent(event);
  const patch = {
    userId: event.userId,
    heroAgentDefinitionId: event.heroAgentDefinitionId,
    heroAgentVersionId: event.heroAgentVersionId,
    status: event.status,
    decisionBackend: event.decisionBackend,
    backendType: event.backendType,
    modelName: event.modelName,
    requestMode: event.requestMode,
    handCount: event.handCount,
    publishedActions: event.publishedActions,
    publishedHandSummaries: event.publishedHandSummaries,
    playerSummaries: event.playerSummaries,
    profileSessionFeatures: event.profileSessionFeatures,
    heroProfitTimeseries: heroProfitPoint ? [heroProfitPoint] : null,
    seed: event.seed,
    heroSeat: event.heroSeat,
    heroContextHash: event.heroContextHash,
    heroContextPreview: event.heroContextPreview
  };

  if (event.eventType === "simulation_requested") {
    patch.requestedAt =
      event.requestedAt || event.eventTimestamp || new Date().toISOString();
    patch.clearErrorMessage = true;
    return patch;
  }

  if (event.eventType === "simulation_started") {
    patch.startedAt = event.eventTimestamp || new Date().toISOString();
    patch.clearErrorMessage = true;
    patch.clearCompletedAt = true;
    return patch;
  }

  if (event.eventType === "simulation_progress") {
    patch.startedAt = event.eventTimestamp || new Date().toISOString();
    patch.clearErrorMessage = true;
    patch.clearCompletedAt = true;
    return patch;
  }

  patch.completedAt = event.eventTimestamp || new Date().toISOString();
  if (event.eventType === "simulation_completed") {
    patch.clearErrorMessage = true;
  } else if (event.eventType === "simulation_failed") {
    patch.errorMessage = event.errorMessage || "Simulation failed";
  }
  return patch;
}

export async function projectSimulationLifecycleEvent(
  appStore,
  rawEvent,
  { logger = console } = {}
) {
  const event = normalizeSimulationLifecycleEvent(rawEvent);
  if (!event) {
    return {
      applied: false,
      reason: "unsupported_event",
      event: rawEvent
    };
  }

  if (!appStore?.getSimulationRun || !appStore?.updateSimulationRunLifecycle) {
    throw new Error("App store does not support simulation lifecycle projection");
  }

  const existingRun = await appStore.getSimulationRun(event.simulationRunId);
  if (existingRun && !existingRunMatchesEvent(existingRun, event)) {
    logger?.warn?.(
      `[simulation-event-projector] skipping stale or incompatible ${event.eventType} for existing run ${event.simulationRunId}`
    );
    return {
      applied: false,
      reason: "stale_or_incompatible_event",
      event
    };
  }

  if (event.userId && appStore.ensureUser) {
    await appStore.ensureUser({ userId: event.userId });
  }

  if (!existingRun && event.eventType === "simulation_requested") {
    try {
      const createdRun = await appStore.createSimulationRun(
        createRunPayloadFromEvent(event)
      );
      return {
        applied: true,
        action: "created",
        event,
        run: createdRun
      };
    } catch (error) {
      if (
        !(error instanceof AppStoreConflictError) &&
        !String(error.message || "").includes("already exists")
      ) {
        throw error;
      }
      logger?.warn?.(
        `[simulation-event-projector] create race for ${event.simulationRunId}; falling back to update`
      );
    }
  } else if (!existingRun) {
    logger?.warn?.(
      `[simulation-event-projector] skipping ${event.eventType} for missing run ${event.simulationRunId}`
    );
    return {
      applied: false,
      reason: "run_not_found",
      event
    };
  }

  const updatedRun = await appStore.updateSimulationRunLifecycle(
    event.simulationRunId,
    lifecyclePatchFromEvent(event)
  );
  if (!updatedRun) {
    return {
      applied: false,
      reason: "run_not_found",
      event
    };
  }

  return {
    applied: true,
    action: "updated",
    event,
    run: updatedRun
  };
}
