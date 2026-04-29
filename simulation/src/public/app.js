const state = {
  session: null,
  dashboardUrl: "http://localhost:8501",
  agents: [],
  runs: [],
  selectedRunId: null,
  selectedAgentId: null,
  agentDetail: null,
  runDetail: null,
  runResults: null,
  runProfiles: null,
  runLive: null,
  savedQueries: [],
  explorerResults: null,
  selectedSavedQueryId: null,
  busy: {
    session: false,
    agents: false,
    agentDetail: false,
    deletingAgentId: null,
    runs: false,
    runDetail: false,
    deletingRunId: null,
    cancelingRunId: null,
    profileRequest: false,
    explorer: false
  },
  message: null,
  error: null
};

let runRefreshTimer = null;
let runDetailRequestToken = 0;
let agentDetailRequestToken = 0;
const runDetailCache = new Map();
const COLUMN_LABELS = {
  simulation_run_id: "Simulation Run",
  decision_backend: "Strategy Type",
  profile_status: "Profile Status",
  cluster_label: "Profile",
  summary_text: "Summary",
  confidence_score: "Confidence",
  total_bb_won: "Total BB Won",
  avg_bb_per_100: "Avg BB / 100",
  bb_per_100: "BB / 100",
  final_rank: "Finish",
  hands_played: "Hands",
  observed_hands: "Observed Hands",
  observed_actions: "Observed Actions",
  aggression_frequency: "Aggression",
  showdown_hand_category: "Best Hand",
  showdown_hand_score: "Hand Score",
  run_count: "Runs",
  first_place_rate: "First Place Rate",
  avg_confidence_score: "Avg Confidence",
  requested_at: "Requested",
  status: "Status"
};

const el = {};

document.addEventListener("DOMContentLoaded", () => {
  captureElements();
  bindEvents();
  renderAll();
  initialize();
});

function captureElements() {
  el.authShell = document.querySelector("#auth-shell");
  el.appShell = document.querySelector("#app-shell");
  el.loginForm = document.querySelector("#login-form");
  el.registerForm = document.querySelector("#register-form");
  el.sessionClear = document.querySelector("#session-clear");
  el.agentForm = document.querySelector("#agent-form");
  el.agentVersionForm = document.querySelector("#agent-version-form");
  el.runForm = document.querySelector("#run-form");
  el.refreshRuns = document.querySelector("#refresh-runs");
  el.refreshRun = document.querySelector("#refresh-run");
  el.runsContainer = document.querySelector("#runs-container");
  el.agentsContainer = document.querySelector("#agents-container");
  el.agentDetail = document.querySelector("#agent-detail");
  el.runDetail = document.querySelector("#run-detail");
  el.explorerForm = document.querySelector("#explorer-form");
  el.savedQueries = document.querySelector("#saved-queries");
  el.explorerResults = document.querySelector("#explorer-results");
  el.sessionStatus = document.querySelector("#session-status");
  el.message = document.querySelector("#global-message");
  el.error = document.querySelector("#global-error");
  el.dashboardLink = document.querySelector("#dashboard-link");
  el.authDashboardLink = document.querySelector("#auth-dashboard-link");
  el.agentSelect = document.querySelector("#run-agent-version");
  el.selectedAgentName = document.querySelector("#selected-agent-name");
}

function bindEvents() {
  el.loginForm.addEventListener("submit", onLoginSubmit);
  el.registerForm.addEventListener("submit", onRegisterSubmit);
  el.sessionClear.addEventListener("click", clearSession);
  el.agentForm.addEventListener("submit", onAgentSubmit);
  el.agentVersionForm.addEventListener("submit", onAgentVersionSubmit);
  el.runForm.addEventListener("submit", onRunSubmit);
  if (el.explorerForm) {
    el.explorerForm.addEventListener("submit", onExplorerSubmit);
  }
  el.refreshRuns.addEventListener("click", () => refreshRuns());
  el.refreshRun.addEventListener("click", () => {
    if (state.selectedRunId) {
      loadRunDetail(state.selectedRunId, { forceReload: true });
    }
  });
}

async function initialize() {
  state.busy.session = true;
  renderSession();
  try {
    const payload = await fetchJson("/api/session");
    state.session = payload.user || null;
    state.dashboardUrl = payload.dashboard_url || state.dashboardUrl;
    if (state.session?.user_id) {
      await hydrateAuthenticatedState();
    }
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.session = false;
    renderAll();
  }
}

function resetUserScopedState() {
  clearRunRefreshTimer();
  runDetailCache.clear();
  state.session = null;
  state.agents = [];
  state.runs = [];
  state.selectedRunId = null;
  state.selectedAgentId = null;
  state.agentDetail = null;
  state.runDetail = null;
  state.runResults = null;
  state.runProfiles = null;
  state.runLive = null;
  state.savedQueries = [];
  state.explorerResults = null;
  state.selectedSavedQueryId = null;
}

async function hydrateAuthenticatedState() {
  await Promise.all([refreshAgents(), refreshRuns()]);
}

async function onLoginSubmit(event) {
  event.preventDefault();
  state.busy.session = true;
  renderSession();

  try {
    const form = new FormData(el.loginForm);
    const payload = await fetchJson("/api/session/login", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: String(form.get("email") || "").trim(),
        password: String(form.get("password") || "")
      })
    });
    resetUserScopedState();
    state.session = payload.user || null;
    state.dashboardUrl = payload.dashboard_url || state.dashboardUrl;
    el.loginForm.reset();
    await hydrateAuthenticatedState();
    setMessage(`Signed in as ${state.session.display_name || state.session.email || state.session.user_id}`);
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.session = false;
    renderAll();
  }
}

async function onRegisterSubmit(event) {
  event.preventDefault();
  const form = new FormData(el.registerForm);
  const password = String(form.get("password") || "");
  const passwordConfirm = String(form.get("password_confirm") || "");
  if (password !== passwordConfirm) {
    setError("Passwords do not match.");
    renderAll();
    return;
  }

  state.busy.session = true;
  renderSession();
  try {
    const payload = await fetchJson("/api/session/register", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        display_name: String(form.get("display_name") || "").trim(),
        email: String(form.get("email") || "").trim(),
        password
      })
    });
    resetUserScopedState();
    state.session = payload.user || null;
    state.dashboardUrl = payload.dashboard_url || state.dashboardUrl;
    el.registerForm.reset();
    await hydrateAuthenticatedState();
    setMessage(`Account ready for ${state.session.display_name || state.session.email || state.session.user_id}`);
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.session = false;
    renderAll();
  }
}

async function clearSession() {
  state.busy.session = true;
  renderSession();
  try {
    await fetchJson("/api/session/logout", {
      method: "POST"
    });
    resetUserScopedState();
    setMessage("Signed out.");
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.session = false;
    renderAll();
  }
}

async function onAgentSubmit(event) {
  event.preventDefault();
  if (!requireSession()) {
    return;
  }
  const form = new FormData(el.agentForm);
  const payload = {
    name: String(form.get("name") || "").trim(),
    description: String(form.get("description") || "").trim() || null,
    context: String(form.get("context") || "").trim(),
    decision_backend_default:
      String(form.get("decision_backend_default") || "llm").trim() || "llm"
  };
  if (!payload.name || !payload.context) {
    setError("Agent name and context are required.");
    renderMessages();
    return;
  }

  try {
    const created = await api("/api/agents", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    el.agentForm.reset();
    setMessage(`Created agent ${created.agent.name}`);
    state.selectedAgentId = created.agent.agent_definition_id;
    await refreshAgents();
  } catch (error) {
    setError(error.message);
  } finally {
    renderAll();
  }
}

async function onExplorerSubmit(event) {
  event.preventDefault();
  if (!requireSession()) {
    return;
  }

  const submitter = event.submitter;
  const shouldSave = submitter?.dataset.save === "true";
  const form = new FormData(el.explorerForm);
  const compareBy = String(form.get("compare_by") || "").trim();
  const profileStatus = String(form.get("profile_status") || "").trim();
  const payload = {
    query_type: "hero_performance",
    name: String(form.get("name") || "").trim() || null,
    save: shouldSave,
    topn: Number(form.get("topn") || 25),
    group_by: compareBy ? [compareBy] : [],
    filters: {
      simulation_run_ids: parseCsv(String(form.get("simulation_run_ids") || "")),
      cluster_labels: parseCsv(String(form.get("cluster_labels") || "")),
      profile_statuses: profileStatus ? [profileStatus] : [],
      decision_backend:
        String(form.get("decision_backend") || "").trim() || null,
      status: String(form.get("status") || "").trim() || null,
      date_from: normalizeDateTimeLocal(form.get("date_from")),
      date_to: normalizeDateTimeLocal(form.get("date_to")),
      min_hands_played: parseOptionalNumber(form.get("min_hands_played")),
      max_final_rank: parseOptionalNumber(form.get("max_final_rank")),
      min_total_bb_won: parseOptionalNumber(form.get("min_total_bb_won")),
      min_bb_per_100: parseOptionalNumber(form.get("min_bb_per_100"))
    }
  };

  if (shouldSave && !payload.name) {
    setError("Add a query name before saving it.");
    renderMessages();
    return;
  }

  state.busy.explorer = true;
  renderExplorer();
  try {
    state.explorerResults = await api("/api/performance/query", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    if (state.explorerResults.saved_query?.saved_query_id) {
      state.selectedSavedQueryId = state.explorerResults.saved_query.saved_query_id;
      await refreshSavedQueries();
      setMessage(`Saved query ${state.explorerResults.saved_query.name}.`);
    } else {
      setMessage("Performance query completed.");
    }
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.explorer = false;
    renderAll();
  }
}

async function onAgentVersionSubmit(event) {
  event.preventDefault();
  if (!requireSession()) {
    return;
  }
  if (!state.selectedAgentId) {
    setError("Select an agent before creating a new version.");
    renderMessages();
    return;
  }
  const form = new FormData(el.agentVersionForm);
  const payload = {
    context: String(form.get("context") || "").trim(),
    decision_backend_default:
      String(form.get("decision_backend_default") || "llm").trim() || "llm"
  };
  if (!payload.context) {
    setError("Version context is required.");
    renderMessages();
    return;
  }

  try {
    const created = await api(`/api/agents/${state.selectedAgentId}/versions`, {
      method: "POST",
      body: JSON.stringify(payload)
    });
    el.agentVersionForm.reset();
    setMessage(`Created agent version ${created.version.version_number}.`);
    await refreshAgents();
  } catch (error) {
    setError(error.message);
  } finally {
    renderAll();
  }
}

async function onRunSubmit(event) {
  event.preventDefault();
  if (!requireSession()) {
    return;
  }
  const form = new FormData(el.runForm);
  const simulationRunId =
    String(form.get("simulation_run_id") || "").trim() || generateRunId();
  const agentVersionId = String(form.get("agent_version_id") || "").trim() || null;
  const resolvedContext = String(form.get("context") || "").trim() || null;
  const payload = {
    simulation_run_id: simulationRunId,
    hand_count: Number(form.get("hand_count") || 250),
    reset_stacks_each_hand: true,
    decision_backend: String(form.get("decision_backend") || "llm").trim(),
    context: resolvedContext,
    agent_version_id: agentVersionId
  };

  if (!payload.context && !payload.agent_version_id) {
    setError("Provide a raw context or choose a saved agent version.");
    renderMessages();
    return;
  }

  const optimisticRun = {
    simulation_run_id: simulationRunId,
    status: "queued",
    decision_backend: payload.decision_backend,
    request_mode: payload.agent_version_id ? "saved_agent" : "context",
    hand_count: payload.hand_count,
    requested_at: new Date().toISOString(),
    hero_agent_version_id: payload.agent_version_id,
    hero_context_hash: null,
    hero_context_preview: resolvedContext ? resolvedContext.slice(0, 80) : null,
    request_payload: {
      context: resolvedContext
    },
    model_name: null
  };
  upsertRunRecord(optimisticRun);
  renderRuns();

  try {
    const created = await api("/api/simulations", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    el.runForm.querySelector('[name="simulation_run_id"]').value = "";
    upsertRunRecord({
      ...optimisticRun,
      simulation_run_id: created.simulation_run_id,
      status: created.status || "queued"
    });
    setMessage(`Queued simulation ${created.simulation_run_id}.`);
    await refreshRuns();
  } catch (error) {
    state.runs = state.runs.filter(
      (run) => run.simulation_run_id !== simulationRunId
    );
    setError(error.message);
  } finally {
    renderAll();
  }
}

async function refreshAgents() {
  if (!state.session?.user_id) {
    state.agents = [];
    state.agentDetail = null;
    renderAgents();
    renderAgentDetail();
    return;
  }
  state.busy.agents = true;
  renderAgents();
  try {
    const payload = await api("/api/agents");
    state.agents = payload.agents || [];
    const hasSelectedAgent = state.agents.some(
      (agent) => agent.agent_definition_id === state.selectedAgentId
    );
    if (!hasSelectedAgent) {
      state.selectedAgentId = state.agents[0]?.agent_definition_id || null;
    }
    if (state.selectedAgentId) {
      await loadAgentDetail(state.selectedAgentId);
    } else {
      state.agentDetail = null;
    }
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.agents = false;
    renderAgents();
    renderAgentDetail();
    renderRunForm();
  }
}

async function refreshRuns() {
  if (!state.session?.user_id) {
    clearRunRefreshTimer();
    state.runs = [];
    renderRuns();
    return;
  }
  state.busy.runs = true;
  renderRuns();
  try {
    const payload = await api("/api/simulations?limit=30");
    state.runs = payload.runs || [];
    if (state.selectedRunId) {
      const stillExists = state.runs.some(
        (run) => run.simulation_run_id === state.selectedRunId
      );
      if (stillExists) {
        void loadRunDetail(state.selectedRunId, { background: true });
      } else {
        state.selectedRunId = null;
        state.runDetail = null;
        state.runResults = null;
        state.runProfiles = null;
        state.runLive = null;
        clearRunRefreshTimer();
      }
    }
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.runs = false;
    renderRuns();
  }
}

async function refreshSavedQueries() {
  if (!state.session?.user_id) {
    state.savedQueries = [];
    renderExplorer();
    return;
  }
  state.busy.explorer = true;
  renderExplorer();
  try {
    const payload = await api("/api/performance/queries");
    state.savedQueries = payload.saved_queries || [];
    if (
      !state.selectedSavedQueryId &&
      state.savedQueries.length &&
      !state.explorerResults
    ) {
      state.selectedSavedQueryId = state.savedQueries[0].saved_query_id;
    }
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.explorer = false;
    renderExplorer();
  }
}

async function loadSavedQuery(savedQueryId) {
  if (!savedQueryId) {
    return;
  }
  state.selectedSavedQueryId = savedQueryId;
  state.busy.explorer = true;
  renderExplorer();
  try {
    state.explorerResults = await api(`/api/performance/query/${savedQueryId}`);
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.explorer = false;
    renderExplorer();
  }
}

async function selectRun(simulationRunId) {
  state.selectedRunId = simulationRunId;
  const hydratedFromCache = applyCachedRunState(simulationRunId);
  renderRuns();
  if (hydratedFromCache) {
    renderRunDetail();
  }
  await loadRunDetail(simulationRunId, { background: hydratedFromCache });
}

function runApiPath(simulationRunId, suffix = "") {
  return `/api/simulations/${encodeURIComponent(simulationRunId)}${suffix}`;
}

function isActiveRunStatus(status) {
  return ["queued", "running"].includes(String(status || "").trim().toLowerCase());
}

function runStatusForAction(simulationRunId) {
  const run =
    state.runs.find((item) => item.simulation_run_id === simulationRunId) ||
    (state.runDetail?.simulation_run_id === simulationRunId ? state.runDetail : null);
  return String(run?.status || "").trim().toLowerCase();
}

async function cancelRun(simulationRunId) {
  if (!requireSession() || !simulationRunId) {
    return;
  }

  const runStatus = runStatusForAction(simulationRunId);
  if (!["queued", "running", "finalizing"].includes(runStatus)) {
    setError("Only queued, running, or finalizing runs can be stopped.");
    renderMessages();
    return;
  }

  const confirmed = window.confirm(
    `Stop run ${simulationRunId}? Partial progress already published may remain visible.`
  );
  if (!confirmed) {
    return;
  }

  state.busy.cancelingRunId = simulationRunId;
  renderRuns();
  renderRunDetail();

  try {
    const payload = await api(runApiPath(simulationRunId, "/cancel"), {
      method: "POST"
    });
    const updated = payload.run || {
      simulation_run_id: simulationRunId,
      status: payload.status || "failed"
    };
    state.runs = state.runs.map((item) =>
      item.simulation_run_id === simulationRunId ? { ...item, ...updated } : item
    );
    if (state.runDetail?.simulation_run_id === simulationRunId) {
      state.runDetail = { ...state.runDetail, ...updated };
      writeRunCache(simulationRunId, { detail: state.runDetail });
    }
    setMessage(`Stopped run ${simulationRunId}.`);
    clearRunRefreshTimer();
    await loadRuns();
    if (state.selectedRunId === simulationRunId) {
      await loadRunDetail(simulationRunId, { forceReload: true });
    }
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.cancelingRunId = null;
    renderAll();
  }
}

async function deleteRun(simulationRunId) {
  if (!requireSession() || !simulationRunId) {
    return;
  }

  const runStatus = runStatusForAction(simulationRunId);
  const forceActiveDelete = isActiveRunStatus(runStatus);
  if (!canDeleteRunStatus(runStatus)) {
    setError("Only known simulation runs can be removed.");
    renderMessages();
    return;
  }

  const confirmed = window.confirm(
    forceActiveDelete
      ? `Stop and remove run ${simulationRunId}? The current simulation will stop as soon as possible.`
      : `Remove recorded run ${simulationRunId}? This will hide it from the app workspace.`
  );
  if (!confirmed) {
    return;
  }

  state.busy.deletingRunId = simulationRunId;
  renderRuns();
  renderRunDetail();

  try {
    await api(runApiPath(simulationRunId, forceActiveDelete ? "?force=true" : ""), {
      method: "DELETE"
    });

    state.runs = state.runs.filter((item) => item.simulation_run_id !== simulationRunId);
    clearRunCache(simulationRunId);
    if (state.selectedRunId === simulationRunId) {
      state.selectedRunId = null;
      state.runDetail = null;
      state.runResults = null;
      state.runProfiles = null;
      state.runLive = null;
      runDetailRequestToken += 1;
      clearRunRefreshTimer();
    }

    setMessage(`Removed run ${simulationRunId}.`);
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.deletingRunId = null;
    renderAll();
  }
}

async function selectAgent(agentDefinitionId) {
  state.selectedAgentId = agentDefinitionId;
  renderAgents();
  renderAgentDetail();
  renderRunForm();
  await loadAgentDetail(agentDefinitionId);
}

async function deleteAgent(agentDefinitionId) {
  if (!requireSession() || !agentDefinitionId) {
    return;
  }

  const agent = state.agents.find((item) => item.agent_definition_id === agentDefinitionId);
  const label = agent?.name || agentDefinitionId;
  const confirmed = window.confirm(
    `Remove saved agent ${label}? Existing runs will stay, but this saved agent and its versions will be removed.`
  );
  if (!confirmed) {
    return;
  }

  state.busy.deletingAgentId = agentDefinitionId;
  renderAgents();
  renderAgentDetail();

  try {
    await api(`/api/agents/${agentDefinitionId}`, {
      method: "DELETE"
    });

    state.agents = state.agents.filter((item) => item.agent_definition_id !== agentDefinitionId);
    if (state.selectedAgentId === agentDefinitionId) {
      agentDetailRequestToken += 1;
      state.selectedAgentId = null;
      state.agentDetail = null;
      state.busy.agentDetail = false;
    }

    await refreshAgents();
    setMessage(`Removed agent ${label}.`);
  } catch (error) {
    setError(error.message);
  } finally {
    state.busy.deletingAgentId = null;
    renderAll();
  }
}

async function loadAgentDetail(agentDefinitionId) {
  if (!agentDefinitionId || !state.session?.user_id) {
    agentDetailRequestToken += 1;
    state.agentDetail = null;
    renderAgentDetail();
    renderRunForm();
    return;
  }

  const requestToken = ++agentDetailRequestToken;
  const requestedAgentId = agentDefinitionId;
  state.busy.agentDetail = true;
  renderAgentDetail();
  try {
    const payload = await api(`/api/agents/${agentDefinitionId}`);
    if (
      requestToken !== agentDetailRequestToken ||
      state.selectedAgentId !== requestedAgentId
    ) {
      return;
    }
    state.agentDetail = payload.agent || null;
  } catch (error) {
    if (
      requestToken !== agentDetailRequestToken ||
      state.selectedAgentId !== requestedAgentId
    ) {
      return;
    }
    state.agentDetail = null;
    setError(error.message);
  } finally {
    if (
      requestToken !== agentDetailRequestToken ||
      state.selectedAgentId !== requestedAgentId
    ) {
      return;
    }
    state.busy.agentDetail = false;
    renderAgentDetail();
    renderRunForm();
  }
}

function clearRunRefreshTimer() {
  if (runRefreshTimer) {
    window.clearTimeout(runRefreshTimer);
    runRefreshTimer = null;
  }
}

function normalizeExecutionDate(value) {
  if (!value) {
    return "";
  }
  const parsed = new Date(String(value));
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString();
}

function executionFingerprint(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  return [
    record.simulation_run_id || "",
    record.user_id || "",
    record.decision_backend || "",
    record.hero_context_hash || "",
    record.hero_agent_definition_id || "",
    record.hero_agent_version_id || "",
    normalizeExecutionDate(record.requested_at),
    normalizeExecutionDate(record.started_at)
  ].join("|");
}

function cacheMatchesRunRecord(cached, listedRun) {
  if (!cached || !listedRun) {
    return true;
  }
  const cachedFingerprint = executionFingerprint(cached.detail || cached.results?.run_summary);
  const listedFingerprint = executionFingerprint(listedRun);
  return !cachedFingerprint || !listedFingerprint || cachedFingerprint === listedFingerprint;
}

function readRunCache(simulationRunId) {
  return runDetailCache.get(simulationRunId) || null;
}

function writeRunCache(simulationRunId, patch) {
  if (!simulationRunId || !patch || typeof patch !== "object") {
    return;
  }
  const existing = readRunCache(simulationRunId) || {};
  runDetailCache.set(simulationRunId, {
    ...existing,
    ...patch,
    updatedAt: Date.now()
  });
}

function clearRunCache(simulationRunId) {
  if (!simulationRunId) {
    return;
  }
  runDetailCache.delete(simulationRunId);
}

function applyCachedRunState(simulationRunId) {
  const cached = readRunCache(simulationRunId);
  if (!cached) {
    return false;
  }

  state.runDetail = cached.detail || null;
  state.runResults = cached.results || null;
  state.runProfiles = cached.profiles || null;
  state.runLive = cached.live || null;
  return true;
}

function updateRunRecord(updatedRun) {
  if (!updatedRun?.simulation_run_id) {
    return;
  }
  state.runs = state.runs.map((run) =>
    run.simulation_run_id === updatedRun.simulation_run_id
      ? { ...run, ...updatedRun }
      : run
  );
}

function upsertRunRecord(runRecord) {
  if (!runRecord?.simulation_run_id) {
    return;
  }
  const existingIndex = state.runs.findIndex(
    (run) => run.simulation_run_id === runRecord.simulation_run_id
  );
  if (existingIndex >= 0) {
    state.runs = state.runs.map((run, index) =>
      index === existingIndex ? { ...run, ...runRecord } : run
    );
    return;
  }
  state.runs = [runRecord, ...state.runs];
}

function effectiveRunStatus(detail, results) {
  const normalizedResultStatus = String(
    results?.status || results?.run_summary?.status || ""
  )
    .trim()
    .toLowerCase();
  if (normalizedResultStatus) {
    return normalizedResultStatus;
  }
  return String(detail?.status || "")
    .trim()
    .toLowerCase();
}

function isMaterializingResultsPayload(results) {
  return String(results?.materialization_status || results?.run_summary?.materialization_status || "")
    .trim()
    .toLowerCase() === "materializing";
}

function coerceFiniteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function playerObservedHands(row) {
  return (
    coerceFiniteNumber(row?.observed_hands) ??
    coerceFiniteNumber(row?.hands_played) ??
    coerceFiniteNumber(row?.hand_count) ??
    0
  );
}

function expectedRunHands(detail, results, heroSummary = null) {
  return Math.max(
    0,
    coerceFiniteNumber(detail?.hand_count) || 0,
    coerceFiniteNumber(detail?.published_hand_summaries) || 0,
    coerceFiniteNumber(results?.run_summary?.published_hand_summaries) || 0,
    coerceFiniteNumber(heroSummary?.hands_played) || 0,
    coerceFiniteNumber(heroSummary?.observed_hands) || 0
  );
}

function mergeResultRows(...rows) {
  const normalizedRows = rows.filter((row) => row && typeof row === "object");
  if (!normalizedRows.length) {
    return null;
  }
  return normalizedRows.reduce((merged, row) => ({ ...merged, ...row }), {});
}

function timelineLastHandNumber(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return 0;
  }
  return Math.max(
    ...rows.map((row, index) => {
      const handNumber = coerceFiniteNumber(row?.hand_number);
      return handNumber == null ? index + 1 : handNumber;
    })
  );
}

function timelineMatchesHeroTotal(rows, heroSummary = null) {
  if (!Array.isArray(rows) || !rows.length) {
    return false;
  }
  const expectedTotal =
    coerceFiniteNumber(heroSummary?.total_bb_won) ??
    coerceFiniteNumber(heroSummary?.bb_won);
  if (expectedTotal == null) {
    return true;
  }
  const finalPoint = rows[rows.length - 1];
  const actualTotal = coerceFiniteNumber(finalPoint?.cumulative_bb_won);
  if (actualTotal == null) {
    return false;
  }
  return Math.abs(actualTotal - expectedTotal) < 0.01;
}

function profitTimelineLooksComplete(rows, expectedHands, heroSummary = null) {
  if (!Array.isArray(rows) || !rows.length) {
    return false;
  }
  const observedHands = timelineLastHandNumber(rows);
  if (expectedHands > 0 && observedHands < expectedHands) {
    return false;
  }
  return timelineMatchesHeroTotal(rows, heroSummary);
}

function buildCoarseProfitTimeline(detail, results, heroSummary = null) {
  const expectedHands = expectedRunHands(detail, results, heroSummary);
  const totalBbWon =
    coerceFiniteNumber(heroSummary?.total_bb_won) ??
    coerceFiniteNumber(heroSummary?.bb_won);
  if (expectedHands <= 0 || totalBbWon == null) {
    return null;
  }
  const simulationRunId =
    heroSummary?.simulation_run_id || detail?.simulation_run_id || null;
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

function chooseProfitTimeline({
  primaryRows,
  fallbackRows,
  detail,
  results,
  heroSummary,
  agentRows
}) {
  const expectedHands = expectedRunHands(detail, results, heroSummary);
  const primary = normalizeProfitTimeline(primaryRows);
  if (profitTimelineLooksComplete(primary, expectedHands, heroSummary)) {
    return primary;
  }
  const fallback = normalizeProfitTimeline(fallbackRows);
  if (profitTimelineLooksComplete(fallback, expectedHands, heroSummary)) {
    return fallback;
  }
  const liveHeroRow = findBestUserAggressionRow(agentRows || []);
  const liveObservedHands = playerObservedHands(liveHeroRow);
  const liveProfit = coerceFiniteNumber(liveHeroRow?.bb_won);
  if (
    liveObservedHands > Math.max(10, timelineLastHandNumber(primary) * 2) &&
    liveProfit != null
  ) {
    return [
      {
        simulation_run_id: liveHeroRow?.simulation_run_id || detail?.simulation_run_id || null,
        hand_number: 0,
        cumulative_bb_won: 0
      },
      {
        simulation_run_id: liveHeroRow?.simulation_run_id || detail?.simulation_run_id || null,
        hand_number: liveObservedHands,
        cumulative_bb_won: liveProfit
      }
    ];
  }
  return buildCoarseProfitTimeline(detail, results, heroSummary) || primary || fallback;
}

function completedResultsVisualsLookFresh(detail, results) {
  if (!results || isMaterializingResultsPayload(results)) {
    return false;
  }
  const summary = buildProvisionalResultSummary(results.player_summaries || []);
  const detailSummary = buildProvisionalResultSummary(detail?.player_summaries || []);
  const heroSummary = mergeResultRows(
    detailSummary.hero,
    summary.hero,
    results?.result?.summary?.hero
  );
  const expectedHands = expectedRunHands(detail, results, heroSummary);
  const timeline = normalizeProfitTimeline(results.hero_profit_timeseries);
  if (
    timeline &&
    !profitTimelineLooksComplete(timeline, expectedHands, heroSummary)
  ) {
    return false;
  }
  return true;
}

function canDeleteRunStatus(status) {
  return ["queued", "running", "finalizing", "completed", "failed"].includes(
    String(status || "").trim().toLowerCase()
  );
}

function scheduleRunRefresh(status, results = null) {
  clearRunRefreshTimer();
  if (!state.selectedRunId) {
    return;
  }
  const normalizedStatus = String(status || "").toLowerCase();
  const keepRefreshing = ["queued", "running", "finalizing"].includes(normalizedStatus);
  const materializing = isMaterializingResultsPayload(results);
  if (!keepRefreshing && !materializing) {
    return;
  }
  const delayMs =
    materializing
      ? 5000
      : normalizedStatus === "queued"
      ? 3000
      : normalizedStatus === "finalizing"
        ? 5000
        : 2000;
  runRefreshTimer = window.setTimeout(() => {
    runRefreshTimer = null;
    if (state.selectedRunId) {
      void loadRunDetail(state.selectedRunId, { background: true });
    }
  }, delayMs);
}

async function loadRunDetail(simulationRunId, { background = false, forceReload = false } = {}) {
  if (!simulationRunId) {
    return;
  }
  const requestToken = ++runDetailRequestToken;
  clearRunRefreshTimer();
  const isSameRun = state.runDetail?.simulation_run_id === simulationRunId;
  const listedRun = state.runs.find((run) => run.simulation_run_id === simulationRunId) || null;
  let cachedRun = forceReload ? null : readRunCache(simulationRunId);
  if (cachedRun && !cacheMatchesRunRecord(cachedRun, listedRun)) {
    clearRunCache(simulationRunId);
    cachedRun = null;
    if (isSameRun) {
      state.runResults = null;
      state.runProfiles = null;
      state.runLive = null;
    }
  }
  const cachedStatus = effectiveRunStatus(cachedRun?.detail, cachedRun?.results);
  const canServeFromCache = Boolean(
    cachedRun &&
      ["completed", "failed"].includes(cachedStatus) &&
      !isMaterializingResultsPayload(cachedRun?.results) &&
      (cachedStatus === "failed" ||
        completedResultsVisualsLookFresh(cachedRun.detail, cachedRun.results))
  );
  const hydratedFromCache = !isSameRun && applyCachedRunState(simulationRunId);
  if (!isSameRun && !hydratedFromCache) {
    state.runDetail = null;
    state.runResults = null;
    state.runProfiles = null;
    state.runLive = null;
  }
  if ((!background || !isSameRun) && !hydratedFromCache && !canServeFromCache) {
    state.busy.runDetail = true;
    renderRunDetail();
  }
  if (hydratedFromCache || canServeFromCache) {
    renderRunDetail();
  }

  if (canServeFromCache) {
    scheduleRunRefresh(cachedStatus, cachedRun?.results);
    return;
  }

  const loadResultsPayload = async () => {
    try {
      return await api(`/api/simulations/${simulationRunId}/results`);
    } catch (error) {
      if (error.status === 202 && error.payload) {
        return error.payload;
      }
      if (error.status === 409 && error.payload) {
        return error.payload;
      }
      throw error;
    }
  };

  try {
    let detail = await api(`/api/simulations/${simulationRunId}`);
    if (requestToken !== runDetailRequestToken || state.selectedRunId !== simulationRunId) {
      return;
    }
    state.runDetail = detail;
    writeRunCache(simulationRunId, { detail });
    updateRunRecord(detail);
    renderRuns();
    renderRunDetail();

    const normalizedStatus = String(detail?.status || "").toLowerCase();
    let results = null;
    let live = null;

    if (["queued", "running"].includes(normalizedStatus)) {
      live = await api(`/api/simulations/${simulationRunId}/live`).catch(() => null);
    } else if (normalizedStatus === "finalizing") {
      results = await loadResultsPayload();
    } else if (["completed", "failed"].includes(normalizedStatus)) {
      results = await loadResultsPayload().catch(() => null);
    }

    if (requestToken !== runDetailRequestToken || state.selectedRunId !== simulationRunId) {
      return;
    }

    state.runResults = results;
    state.runLive = live;
    writeRunCache(simulationRunId, {
      detail,
      results,
      live
    });

    const resolvedStatus = effectiveRunStatus(detail, results);
    if (resolvedStatus && resolvedStatus !== normalizedStatus) {
      detail = {
        ...detail,
        status: resolvedStatus,
        completed_at:
          results?.run_summary?.completed_at ||
          results?.run_summary?.finished_at ||
          detail.completed_at
      };
      state.runDetail = detail;
      writeRunCache(simulationRunId, { detail });
      updateRunRecord(detail);
      renderRuns();
    }

    if (["queued", "running"].includes(resolvedStatus)) {
      state.runProfiles = null;
    }
    scheduleRunRefresh(resolvedStatus, results);
  } catch (error) {
    setError(error.message);
  } finally {
    if (requestToken === runDetailRequestToken) {
      state.busy.runDetail = false;
      renderRunDetail();
      renderRuns();
    }
  }
}

async function loadCachedRunProfiles() {
  if (!state.selectedRunId || !state.runDetail) {
    return;
  }

  state.busy.profileRequest = true;
  setMessage("Loading ML profile results...");
  renderMessages();
  renderRunDetail();
  try {
    const payload = await api(`/api/simulations/${state.selectedRunId}/profiles`);
    state.runProfiles = payload;
    writeRunCache(state.selectedRunId, { profiles: payload });
    setMessage("Loaded ML profile results.");
  } catch (error) {
    if (Array.isArray(error.payload?.results)) {
      state.runProfiles = error.payload;
      writeRunCache(state.selectedRunId, { profiles: error.payload });
    }
    setError(error.message);
  } finally {
    state.busy.profileRequest = false;
    renderMessages();
    renderRunDetail();
  }
}

async function requestRunProfiles({ refresh = false } = {}) {
  if (!state.selectedRunId || !state.runDetail) {
    return;
  }

  state.busy.profileRequest = true;
  setMessage(refresh ? "Refreshing ML profiling..." : "Running ML profiling...");
  renderMessages();
  renderRunDetail();
  try {
    const payload = await api(
      `/api/simulations/${state.selectedRunId}/profiles/request`,
      {
        method: "POST",
        body: JSON.stringify({
          refresh
        })
      }
    );
    state.runProfiles = payload;
    writeRunCache(state.selectedRunId, { profiles: payload });
    setMessage(
      payload.cached
        ? "Loaded ML profile results."
        : refresh
          ? "Refreshed ML profiling results."
          : "ML profiling completed."
    );
  } catch (error) {
    if (Array.isArray(error.payload?.results)) {
      state.runProfiles = error.payload;
      writeRunCache(state.selectedRunId, { profiles: error.payload });
    }
    setError(error.message);
  } finally {
    state.busy.profileRequest = false;
    renderMessages();
    renderRunDetail();
  }
}

function renderAll() {
  renderSession();
  renderMessages();
  renderAgents();
  renderAgentDetail();
  renderRuns();
  renderRunDetail();
  renderRunForm();
  renderExplorer();
}

function syncRouteWithSession() {
  const targetPath = state.session?.user_id ? "/app" : "/app/login";
  if (window.location.pathname !== targetPath) {
    window.history.replaceState({}, "", targetPath);
  }
}

function renderSession() {
  const user = state.session;
  const dashboardUrl = state.dashboardUrl || "http://localhost:8501";
  el.dashboardLink.href = dashboardUrl;
  el.authDashboardLink.href = dashboardUrl;
  el.authShell.classList.toggle("is-hidden", Boolean(user?.user_id));
  el.appShell.classList.toggle("is-hidden", !user?.user_id);
  syncRouteWithSession();

  if (!user?.user_id) {
    el.sessionStatus.innerHTML = `
      <div class="empty">
        Sign in to create saved agents, launch simulations, and scope the workspace to your account.
      </div>
    `;
    return;
  }

  el.sessionStatus.innerHTML = `
    <div class="user-chip">
      <strong>${escapeHtml(user.display_name || user.email || user.user_id)}</strong>
      <span>${escapeHtml(user.email || user.user_id)}</span>
    </div>
  `;
}

function renderMessages() {
  el.message.innerHTML = state.message
    ? `<div class="success">${escapeHtml(state.message)}</div>`
    : "";
  el.error.innerHTML = state.error
    ? `<div class="error">${escapeHtml(state.error)}</div>`
    : "";
}

function renderAgents() {
  if (!state.session?.user_id) {
    el.agentsContainer.innerHTML = `<div class="empty">Sign in to load saved agents.</div>`;
    return;
  }
  if (state.busy.agents) {
    el.agentsContainer.innerHTML = `<div class="empty">Loading agents...</div>`;
    return;
  }
  if (!state.agents.length) {
    el.agentsContainer.innerHTML = `<div class="empty">No saved agents yet. Create one from a context string to get started.</div>`;
    return;
  }

  el.agentsContainer.innerHTML = state.agents
    .map((agent) => {
      const active = agent.agent_definition_id === state.selectedAgentId ? "active" : "";
      const deleting = state.busy.deletingAgentId === agent.agent_definition_id;
      return `
        <article class="list-item ${active}">
          <h4>${escapeHtml(agent.name)}</h4>
          <div class="list-meta">
            <span>${escapeHtml(agent.description || "No description")}</span>
            <span>Latest v${agent.latest_version?.version_number ?? "?"}</span>
            <span>${escapeHtml(decisionModeLabel(agent.latest_version?.decision_backend_default || "llm"))}</span>
          </div>
          <div class="row" style="margin-top: 12px;">
            <button class="subtle" data-action="select-agent" data-agent-id="${escapeHtml(
              agent.agent_definition_id
            )}">Inspect Agent</button>
            ${
              agent.latest_version?.agent_version_id
                ? `<button class="subtle" data-action="use-version" data-version-id="${escapeHtml(
                    agent.latest_version.agent_version_id
                  )}">Use Latest Version</button>`
                : ""
            }
            <button
              class="danger"
              data-action="delete-agent"
              data-agent-id="${escapeHtml(agent.agent_definition_id)}"
              ${deleting ? "disabled" : ""}
            >${escapeHtml(deleting ? "Removing..." : "Remove Agent")}</button>
          </div>
        </article>
      `;
    })
    .join("");

  el.agentsContainer.querySelectorAll("[data-action='select-agent']").forEach((button) => {
    button.addEventListener("click", () => {
      selectAgent(button.dataset.agentId);
    });
  });
  el.agentsContainer.querySelectorAll("[data-action='use-version']").forEach((button) => {
    button.addEventListener("click", () => {
      el.agentSelect.value = button.dataset.versionId || "";
      renderRunForm();
    });
  });
  el.agentsContainer.querySelectorAll("[data-action='delete-agent']").forEach((button) => {
    button.addEventListener("click", () => {
      deleteAgent(button.dataset.agentId);
    });
  });

  const selected = state.agents.find(
    (agent) => agent.agent_definition_id === state.selectedAgentId
  );
  el.selectedAgentName.textContent = selected
    ? `${selected.name} · latest v${selected.latest_version?.version_number ?? "?"}`
    : "Select an agent above";
}

function renderAgentDetail() {
  if (!el.agentDetail) {
    return;
  }
  if (!state.session?.user_id) {
    el.agentDetail.innerHTML = `<div class="empty">Sign in to inspect agent versions and saved context history.</div>`;
    return;
  }
  if (state.busy.agentDetail) {
    el.agentDetail.innerHTML = `<div class="empty">Loading selected agent...</div>`;
    return;
  }
  if (!state.selectedAgentId) {
    el.agentDetail.innerHTML = `<div class="empty">Select an agent above to inspect its versions.</div>`;
    return;
  }
  if (!state.agentDetail) {
    el.agentDetail.innerHTML = `<div class="empty">The selected agent could not be loaded.</div>`;
    return;
  }

  const agent = state.agentDetail;
  const versions = Array.isArray(agent.versions) ? agent.versions : [];
  const latestVersionId = agent.latest_version?.agent_version_id || null;
  const deletingAgent = state.busy.deletingAgentId === agent.agent_definition_id;

  el.agentDetail.innerHTML = `
    <section class="stack">
      <div class="row">
        <span class="pill">${escapeHtml(String(versions.length))} saved versions</span>
        <span class="pill">${escapeHtml(decisionModeLabel(agent.latest_version?.decision_backend_default || "llm"))}</span>
        <button
          class="danger"
          type="button"
          data-action="delete-agent"
          ${deletingAgent ? "disabled" : ""}
        >
          ${escapeHtml(deletingAgent ? "Removing..." : "Remove Agent")}
        </button>
      </div>
      <div class="card-grid">
        <div class="mini-card">
          <h4>Name</h4>
          <strong>${escapeHtml(agent.name || "Untitled agent")}</strong>
        </div>
        <div class="mini-card">
          <h4>Latest Version</h4>
          <strong>${escapeHtml(
            agent.latest_version?.version_number != null
              ? `v${agent.latest_version.version_number}`
              : "n/a"
          )}</strong>
        </div>
        <div class="mini-card">
          <h4>Default Strategy</h4>
          <strong>${escapeHtml(decisionModeLabel(agent.latest_version?.decision_backend_default || "llm"))}</strong>
        </div>
      </div>
      <div class="muted">${escapeHtml(agent.description || "No description yet.")}</div>
      ${
        versions.length
          ? versions
              .map((version) => {
                const isLatest = version.agent_version_id === latestVersionId;
                return `
                  <article class="list-item ${isLatest ? "active" : ""}">
                    <h4>Version ${escapeHtml(String(version.version_number))}</h4>
                    <div class="list-meta">
                      <span>${escapeHtml(decisionModeLabel(version.decision_backend_default || "llm"))}</span>
                      ${isLatest ? `<span>latest</span>` : ""}
                    </div>
                    <div class="muted">${escapeHtml(truncateText(version.context_text, 220))}</div>
                    <div class="row" style="margin-top: 12px;">
                      <button
                        class="subtle"
                        data-action="use-agent-version"
                        data-version-id="${escapeHtml(version.agent_version_id)}"
                      >
                        Use This Version
                      </button>
                      ${isLatest ? `<span class="pill">latest</span>` : ""}
                    </div>
                  </article>
                `;
              })
              .join("")
          : `<div class="empty">No versions were found for this agent yet.</div>`
      }
    </section>
  `;

  el.agentDetail
    .querySelectorAll("[data-action='use-agent-version']")
    .forEach((button) => {
      button.addEventListener("click", () => {
        el.agentSelect.value = button.dataset.versionId || "";
        renderRunForm();
        setMessage(`Using saved agent version ${button.dataset.versionId}.`);
        renderMessages();
      });
    });
  el.agentDetail
    .querySelector("[data-action='delete-agent']")
    ?.addEventListener("click", () => {
      deleteAgent(agent.agent_definition_id);
    });
}

function renderRuns() {
  if (!state.session?.user_id) {
    el.runsContainer.innerHTML = `<div class="empty">Sign in to load runs.</div>`;
    return;
  }
  if (state.busy.runs) {
    el.runsContainer.innerHTML = `<div class="empty">Loading runs...</div>`;
    return;
  }
  if (!state.runs.length) {
    el.runsContainer.innerHTML = `<div class="empty">No runs yet. Queue a simulation from the launch form.</div>`;
    return;
  }

  el.runsContainer.innerHTML = state.runs
    .map((run) => {
      const active = run.simulation_run_id === state.selectedRunId ? "active" : "";
      const deleting = state.busy.deletingRunId === run.simulation_run_id;
      const canceling = state.busy.cancelingRunId === run.simulation_run_id;
      const canDelete = canDeleteRunStatus(run.status);
      const activeRun = isActiveRunStatus(run.status);
      const runContext = runContextText(run);
      return `
        <article class="list-item ${active}">
          <h4>${escapeHtml(run.simulation_run_id)}</h4>
          <div class="list-meta">
            <span class="pill status-${escapeHtml(String(run.status || "").toLowerCase())}">${escapeHtml(
              run.status || "unknown"
            )}</span>
            <span>${escapeHtml(decisionModeLabel(run.decision_backend || "llm"))}</span>
            <span>${escapeHtml(`${run.hand_count || 0} hands`)}</span>
            <span>${escapeHtml(run.requested_at || "")}</span>
          </div>
          ${
            runContext
              ? `<div class="muted" style="margin-top: 10px;">${escapeHtml(
                  truncateText(runContext, 140)
                )}</div>`
              : ""
          }
          <div class="row" style="margin-top: 12px;">
            <button class="subtle" data-action="open-run" data-run-id="${escapeHtml(
              run.simulation_run_id
            )}">Inspect Run</button>
            ${
              activeRun
                ? `<button class="danger" data-action="cancel-run" data-run-id="${escapeHtml(
                    run.simulation_run_id
                  )}" ${canceling ? "disabled" : ""}>${escapeHtml(
                    canceling ? "Stopping..." : "Stop Run"
                  )}</button>`
                : ""
            }
            ${
              canDelete
                ? `<button class="danger" data-action="delete-run" data-run-id="${escapeHtml(
                    run.simulation_run_id
                  )}" ${deleting ? "disabled" : ""}>${escapeHtml(
                    deleting ? "Removing..." : activeRun ? "Stop & Remove" : "Remove Run"
                  )}</button>`
                : ""
            }
          </div>
        </article>
      `;
    })
    .join("");

  el.runsContainer.querySelectorAll("[data-action='open-run']").forEach((button) => {
    button.addEventListener("click", () => {
      selectRun(button.dataset.runId);
    });
  });
  el.runsContainer.querySelectorAll("[data-action='delete-run']").forEach((button) => {
    button.addEventListener("click", () => {
      deleteRun(button.dataset.runId);
    });
  });
  el.runsContainer.querySelectorAll("[data-action='cancel-run']").forEach((button) => {
    button.addEventListener("click", () => {
      cancelRun(button.dataset.runId);
    });
  });
}

function renderRunForm() {
  const selectedVersion = el.agentSelect.value;
  const detailVersions =
    state.agentDetail?.agent_definition_id === state.selectedAgentId &&
    Array.isArray(state.agentDetail?.versions)
      ? state.agentDetail.versions
      : [];
  const seenVersionIds = new Set();
  const versionOptions = [];

  for (const version of detailVersions) {
    if (!version?.agent_version_id || seenVersionIds.has(version.agent_version_id)) {
      continue;
    }
    seenVersionIds.add(version.agent_version_id);
    versionOptions.push(`
      <option value="${escapeHtml(version.agent_version_id)}" ${
        selectedVersion === version.agent_version_id ? "selected" : ""
      }>
        ${escapeHtml(state.agentDetail?.name || "Selected agent")} · v${escapeHtml(
          String(version.version_number)
        )} · ${escapeHtml(decisionModeLabel(version.decision_backend_default || "llm"))}
      </option>
    `);
  }

  for (const agent of state.agents) {
    if (!agent.latest_version?.agent_version_id) {
      continue;
    }
    if (seenVersionIds.has(agent.latest_version.agent_version_id)) {
      continue;
    }
    seenVersionIds.add(agent.latest_version.agent_version_id);
    versionOptions.push(`
      <option value="${escapeHtml(agent.latest_version.agent_version_id)}" ${
        selectedVersion === agent.latest_version.agent_version_id ? "selected" : ""
      }>
        ${escapeHtml(agent.name)} · latest v${escapeHtml(
          String(agent.latest_version.version_number)
        )}
      </option>
    `);
  }

  const options = [`<option value="">Use ad-hoc context</option>`, ...versionOptions];
  el.agentSelect.innerHTML = options.join("");
  el.dashboardLink.href = state.dashboardUrl || "/api/session";
}

function renderRunDetail() {
  if (!state.session?.user_id) {
    el.runDetail.innerHTML = `
      <div class="splash">
        <h3>Live simulations, completed results, and profile outputs land here.</h3>
        <p>Once you sign in, this workspace will show live standings, completed run summaries, and profile results for your runs.</p>
      </div>
    `;
    return;
  }
  if (state.busy.runDetail) {
    el.runDetail.innerHTML = `<div class="empty">Loading run detail...</div>`;
    return;
  }
  if (!state.runDetail) {
    el.runDetail.innerHTML = `<div class="empty">Select a run to inspect live and completed results.</div>`;
    return;
  }

  const run = state.runDetail;
  const resultPayload = state.runResults || {};
  const runSummary = resultPayload.run_summary || {};
  const liveRows = withParticipantLabels(state.runLive?.run_profit_leaderboard || []);
  const detailRows = Array.isArray(run.player_summaries) ? run.player_summaries : [];
  const rawResultRows = resultPayload.player_summaries || run.player_summaries || [];
  const provisionalSummary = buildProvisionalResultSummary(rawResultRows);
  const detailSummary = buildProvisionalResultSummary(detailRows);
  const resultRows = withParticipantLabels(rawResultRows);
  const highHandRows = withParticipantLabels(
    resultPayload.run_high_hand_leaderboard?.length
      ? resultPayload.run_high_hand_leaderboard
      : sortRowsByMetric(
          rawResultRows.filter((row) => Number.isFinite(Number(row?.showdown_hand_score))),
          "showdown_hand_score"
        )
  );
  const liveAgentMetricRows = withParticipantLabels(state.runLive?.run_agents || []);
  const agentMetricRows = withParticipantLabels(resultPayload.agent_metrics || []);
  const profileRows = withParticipantLabels(state.runProfiles?.results || []);
  const hasCachedProfiles = Boolean(profileRows.length);
  const normalizedRunStatus = effectiveRunStatus(run, resultPayload);
  const deletingRun = state.busy.deletingRunId === run.simulation_run_id;
  const cancelingRun = state.busy.cancelingRunId === run.simulation_run_id;
  const canDeleteRun = canDeleteRunStatus(normalizedRunStatus);
  const canCancelRun = ["queued", "running", "finalizing"].includes(normalizedRunStatus);
  const canRequestProfiles = ["completed", "failed", "finalizing"].includes(
    normalizedRunStatus
  );
  const heroSummary = mergeResultRows(
    detailSummary.hero,
    provisionalSummary.hero,
    resultPayload.result?.summary?.hero
  );
  const winnerSummary = mergeResultRows(
    detailSummary.winner,
    provisionalSummary.winner,
    resultPayload.result?.summary?.winner
  );
  const runContext = runContextText(run);
  const isFinalizing = normalizedRunStatus === "finalizing";
  const isMaterializing = isMaterializingResultsPayload(resultPayload);
  const resultsMessage =
    resultPayload.message ||
    (isFinalizing || isMaterializing
      ? "Execution finished. Final results are still being prepared."
      : null);
  const detailMetricRows = withParticipantLabels(detailRows);
  const aggressionChartRow = ["queued", "running"].includes(normalizedRunStatus)
    ? findBestUserAggressionRow(liveAgentMetricRows, agentMetricRows, detailMetricRows, resultRows)
    : findBestUserAggressionRow(agentMetricRows, detailMetricRows, resultRows, liveAgentMetricRows);
  const aggressionChartCaption = ["queued", "running"].includes(normalizedRunStatus)
    ? "Near-real-time user-agent aggression profile for the active run."
    : isFinalizing || isMaterializing
      ? "Latest user-agent aggression profile while final results finish preparing."
      : "Final user-agent aggression profile for this run.";
  const profitTimeline = chooseProfitTimeline({
    primaryRows: ["queued", "running"].includes(normalizedRunStatus)
      ? state.runLive?.hero_profit_timeseries
      : resultPayload.hero_profit_timeseries,
    fallbackRows: ["queued", "running"].includes(normalizedRunStatus)
      ? resultPayload.hero_profit_timeseries
      : state.runLive?.hero_profit_timeseries,
    detail: run,
    results: resultPayload,
    heroSummary,
    agentRows: ["queued", "running"].includes(normalizedRunStatus)
      ? liveAgentMetricRows
      : agentMetricRows
  });
  const profitChartCaption = ["queued", "running"].includes(normalizedRunStatus)
    ? "Near-real-time cumulative BB won for the user agent during the active run."
    : isFinalizing || isMaterializing
      ? "Latest cumulative BB won curve while final results finish preparing."
      : "Final cumulative BB won curve for the user agent.";
  const profileMessage =
    state.runProfiles && !profileRows.length
      ? state.runProfiles.message || null
      : !hasCachedProfiles && canRequestProfiles
        ? normalizedRunStatus === "finalizing"
          ? "Run ML profiling and it will start as soon as final results are ready."
          : "Run ML profiling to classify the players from this finished simulation."
        : null;
  const profileSectionMessage =
    state.busy.profileRequest
      ? "ML profiling is running now. Results will appear here as soon as the request finishes."
      : profileMessage;

  el.runDetail.innerHTML = `
    <div class="stack">
      <div class="row">
        <span class="pill status-${escapeHtml(normalizedRunStatus)}">${escapeHtml(
          normalizedRunStatus || "unknown"
        )}</span>
        ${
          isMaterializing
            ? `<span class="pill">preparing results</span>`
            : ""
        }
        <span class="pill">${escapeHtml(decisionModeLabel(run.decision_backend || "llm"))}</span>
        ${
          run.hero_agent_version_id
            ? `<span class="pill">saved agent ${escapeHtml(run.hero_agent_version_id)}</span>`
            : ""
        }
        ${
          canCancelRun
            ? `<button class="danger" type="button" data-action="cancel-selected-run" ${
                cancelingRun ? "disabled" : ""
              }>${escapeHtml(cancelingRun ? "Stopping..." : "Stop Run")}</button>`
            : ""
        }
        ${
          canDeleteRun
            ? `<button class="danger" type="button" data-action="delete-selected-run" ${
                deletingRun ? "disabled" : ""
              }>${escapeHtml(
                deletingRun
                  ? "Removing..."
                  : isActiveRunStatus(normalizedRunStatus)
                    ? "Stop & Remove"
                    : "Remove Run"
              )}</button>`
            : ""
        }
      </div>
      <div class="card-grid">
        <div class="mini-card">
          <h4>Simulation Run</h4>
          <strong>${escapeHtml(run.simulation_run_id)}</strong>
        </div>
        <div class="mini-card">
          <h4>Hands</h4>
          <strong>${escapeHtml(String(run.hand_count || "n/a"))}</strong>
        </div>
        <div class="mini-card">
          <h4>Strategy Type</h4>
          <strong>${escapeHtml(decisionModeLabel(run.decision_backend || run.model_name || run.backend_type))}</strong>
        </div>
        <div class="mini-card">
          <h4>Finished</h4>
          <strong>${escapeHtml(runSummary.finished_at || run.completed_at || "pending")}</strong>
        </div>
      </div>
      ${
        runContext
          ? `<div class="status-ribbon"><strong>Context</strong><div>${escapeHtml(
              truncateText(runContext, 320)
            )}</div></div>`
          : ""
      }
      ${
        resultsMessage
          ? `<div class="status-ribbon">${escapeHtml(resultsMessage)}</div>`
          : ""
      }
      ${renderProfitTimelineSection(profitTimeline, { caption: profitChartCaption })}
      ${renderAggressionChartSection(aggressionChartRow, { caption: aggressionChartCaption })}
      ${
        heroSummary || winnerSummary
          ? `
            <div class="card-grid">
              ${
                heroSummary
                  ? `
                    <div class="mini-card">
                      <h4>Hero Result</h4>
                      <strong>${escapeHtml(
                        `${formatNumber(heroSummary.total_bb_won)} BB · ${formatNumber(
                          heroSummary.bb_per_100
                        )} BB/100`
                      )}</strong>
                    </div>
                    <div class="mini-card">
                      <h4>Hero Finish</h4>
                      <strong>${escapeHtml(
                        `#${heroSummary.final_rank ?? "?"} in ${heroSummary.hands_played ?? "?"} hands`
                      )}</strong>
                    </div>
                  `
                  : ""
              }
              ${
                winnerSummary
                  ? `
                    <div class="mini-card">
                      <h4>Winner</h4>
                      <strong>${escapeHtml(
                        participantDisplayName(winnerSummary)
                      )}</strong>
                    </div>
                    <div class="mini-card">
                      <h4>Winning Score</h4>
                      <strong>${escapeHtml(
                        `${formatNumber(winnerSummary.total_bb_won)} BB · rank #${
                          winnerSummary.final_rank ?? "?"
                        }`
                      )}</strong>
                    </div>
                  `
                  : ""
              }
            </div>
          `
          : ""
      }
      <section class="stack">
        <h3>ML Profiling</h3>
        <div class="muted">Use the trained model to classify the player styles from this run.</div>
        ${
          canRequestProfiles
            ? `
              <div class="row" style="margin-bottom: 12px;">
                <button
                  class="primary"
                  type="button"
                  data-action="${escapeHtml(hasCachedProfiles ? "load-cached-profiles" : "request-profiles")}"
                  data-refresh="false"
                  ${state.busy.profileRequest ? "disabled" : ""}
                >
                  ${escapeHtml(
                    hasCachedProfiles
                      ? "View ML Profiles"
                      : state.busy.profileRequest
                        ? "Running ML Profiling..."
                        : "Run ML Profiling"
                  )}
                </button>
                <button
                  class="subtle"
                  type="button"
                  data-action="request-profiles"
                  data-refresh="true"
                  ${state.busy.profileRequest ? "disabled" : ""}
                >
                  ${escapeHtml(
                    state.busy.profileRequest ? "Refreshing..." : "Refresh ML Profiling"
                  )}
                </button>
              </div>
            `
            : ""
        }
        ${
          profileSectionMessage
            ? `<div class="status-ribbon">${escapeHtml(profileSectionMessage)}</div>`
            : ""
        }
      </section>
      <div class="tabs">
        <span class="pill">Live leaderboard</span>
        <span class="pill">Final results</span>
        <span class="pill">ML profiles</span>
      </div>
      <div class="tab-content">
        ${
          liveRows.length && ["queued", "running", "finalizing"].includes(normalizedRunStatus)
            ? renderTableSection(
                "Live profit leaderboard",
                liveRows,
                ["player", "total_bb_won", "bb_per_100", "decision_backend"]
              )
            : ""
        }
        ${renderTableSection(
          "Completed player summaries",
          resultRows,
          ["player", "total_bb_won", "bb_per_100", "final_rank", "hands_played"]
        )}
        ${
          profileRows.length
            ? renderTableSection(
                "ML profile results",
                profileRows,
                ["player", "cluster_label", "summary_text", "confidence_score", "hands_observed", "status"]
              )
            : ""
        }
        ${
          agentMetricRows.length
            ? renderTableSection(
                "Run-wide player metrics",
                agentMetricRows,
                ["player", "bb_won", "vpip", "pfr", "aggression_frequency", "observed_hands"]
              )
            : ""
        }
        ${
          highHandRows.length
            ? renderTableSection(
                "Completed high-hand leaderboard",
                highHandRows,
                ["player", "showdown_hand_category", "showdown_hand_score"]
              )
            : ""
        }
      </div>
    </div>
  `;

  el.runDetail
    .querySelectorAll("[data-action='request-profiles']")
    .forEach((button) => {
      button.addEventListener("click", () => {
        requestRunProfiles({
          refresh: button.dataset.refresh === "true"
        });
      });
    });
  el.runDetail
    .querySelectorAll("[data-action='load-cached-profiles']")
    .forEach((button) => {
      button.addEventListener("click", () => {
        if (button.dataset.refresh === "true") {
          requestRunProfiles({ refresh: true });
          return;
        }
        loadCachedRunProfiles();
      });
    });
  el.runDetail
    .querySelector("[data-action='delete-selected-run']")
    ?.addEventListener("click", () => {
      deleteRun(run.simulation_run_id);
    });
  el.runDetail
    .querySelector("[data-action='cancel-selected-run']")
    ?.addEventListener("click", () => {
      cancelRun(run.simulation_run_id);
    });
}

function renderExplorer() {
  if (!el.savedQueries || !el.explorerResults) {
    return;
  }
  if (!state.session?.user_id) {
    el.savedQueries.innerHTML = `<div class="empty">Sign in to load saved performance queries.</div>`;
    el.explorerResults.innerHTML = `<div class="empty">Run a structured query once your account session is active.</div>`;
    return;
  }
  if (state.busy.explorer && !state.savedQueries.length && !state.explorerResults) {
    el.savedQueries.innerHTML = `<div class="empty">Loading saved queries...</div>`;
    el.explorerResults.innerHTML = `<div class="empty">Running explorer query...</div>`;
    return;
  }

  if (!state.savedQueries.length) {
    el.savedQueries.innerHTML = `<div class="empty">No saved queries yet. Run and save one from the form above.</div>`;
  } else {
    el.savedQueries.innerHTML = state.savedQueries
      .map((savedQuery) => {
        const active =
          savedQuery.saved_query_id === state.selectedSavedQueryId ? "active" : "";
        const filters = savedQuery.filters || {};
        const compareBy =
          Array.isArray(filters.group_by) && filters.group_by.length
            ? filters.group_by.map((column) => columnLabel(column)).join(", ")
            : "detail rows";
        return `
          <article class="list-item ${active}">
            <h4>${escapeHtml(savedQuery.name)}</h4>
            <div class="list-meta">
              <span>${escapeHtml(savedQuery.query_type)}</span>
              <span>${escapeHtml(decisionModeFilterLabel(filters.decision_backend))}</span>
              <span>${escapeHtml(compareBy)}</span>
              <span>top ${escapeHtml(String(filters.topn || 25))}</span>
            </div>
            <div class="row" style="margin-top: 12px;">
              <button class="subtle" data-action="load-saved-query" data-query-id="${escapeHtml(
                savedQuery.saved_query_id
              )}">Run Saved Query</button>
            </div>
          </article>
        `;
      })
      .join("");
    el.savedQueries
      .querySelectorAll("[data-action='load-saved-query']")
      .forEach((button) => {
        button.addEventListener("click", () => {
          loadSavedQuery(button.dataset.queryId);
        });
      });
  }

  const results = state.explorerResults;
  if (!results) {
    el.explorerResults.innerHTML = `<div class="empty">Run a performance query to see matched hero runs, strategy comparisons, and profile counts.</div>`;
    return;
  }

  const summary = results.summary;
  const profileOverview = results.profile_overview || {};
  const rows = results.rows || [];
  const comparisonRows = results.comparison_rows || [];
  const backendRows = results.breakdowns?.by_backend || [];
  const finishRows = results.breakdowns?.by_finish_rank || [];
  const profileStatusRows = results.breakdowns?.by_profile_status || [];
  const clusterRows = results.breakdowns?.by_cluster_label || [];
  const compareDimensions = Array.isArray(results.query?.group_by)
    ? results.query.group_by
    : [];
  const savedBadge = results.saved_query?.name
    ? `<span class="pill">Saved as ${escapeHtml(results.saved_query.name)}</span>`
    : "";
  const compareBadge = compareDimensions.length
    ? `<span class="pill">Compare by ${escapeHtml(
        compareDimensions.map((column) => columnLabel(column)).join(", ")
      )}</span>`
    : "";
  const comparisonColumns = comparisonRows.length
    ? availableColumns(comparisonRows, [
        ...compareDimensions,
        "run_count",
        "total_hands",
        "total_bb_won",
        "avg_bb_per_100",
        "avg_finish_rank",
        "first_place_rate",
        "avg_confidence_score"
      ])
    : [];

  el.explorerResults.innerHTML = `
    <div class="stack">
      <div class="row">
        <span class="pill">Matched runs: ${escapeHtml(
          String(results.matched_run_count || 0)
        )}</span>
        <span class="pill">Insufficient evidence: ${escapeHtml(
          String(profileOverview.insufficient_evidence || 0)
        )}</span>
        ${compareBadge}
        ${savedBadge}
      </div>
      ${
        summary
          ? `
            <div class="card-grid">
              <div class="mini-card"><h4>Runs</h4><strong>${escapeHtml(
                String(summary.run_count || 0)
              )}</strong></div>
              <div class="mini-card"><h4>Hands</h4><strong>${escapeHtml(
                String(summary.total_hands || 0)
              )}</strong></div>
              <div class="mini-card"><h4>Total BB Won</h4><strong>${escapeHtml(
                formatNumber(summary.total_bb_won)
              )}</strong></div>
              <div class="mini-card"><h4>Avg BB / 100</h4><strong>${escapeHtml(
                formatNumber(summary.avg_bb_per_100)
              )}</strong></div>
            </div>
          `
          : `<div class="empty">No rows matched the current filters.</div>`
      }
      ${
        comparisonColumns.length
          ? renderTableSection(
              "Grouped comparison",
              comparisonRows,
              comparisonColumns
            )
          : ""
      }
      ${renderTableSection(
        "Matched hero runs",
        rows,
        [
          "simulation_run_id",
          "decision_backend",
          "profile_status",
          "cluster_label",
          "total_bb_won",
          "bb_per_100",
          "final_rank",
          "hands_played"
        ]
      )}
      ${renderTableSection(
        "Breakdown by strategy",
        backendRows,
        ["decision_backend", "run_count", "total_hands", "total_bb_won", "avg_bb_per_100"]
      )}
      ${renderTableSection(
        "Breakdown by finish rank",
        finishRows,
        ["final_rank", "run_count"]
      )}
      ${renderTableSection(
        "Breakdown by profile status",
        profileStatusRows,
        ["profile_status", "run_count"]
      )}
      ${renderTableSection(
        "Breakdown by cluster label",
        clusterRows,
        ["cluster_label", "run_count", "total_bb_won", "avg_bb_per_100"]
      )}
    </div>
  `;
}

function renderTableSection(title, rows, columns) {
  if (!rows || !rows.length) {
    return `<section class="stack"><h3>${escapeHtml(
      title
    )}</h3><div class="empty">No data yet.</div></section>`;
  }
  const headers = columns
    .map((column) => `<th>${escapeHtml(columnLabel(column))}</th>`)
    .join("");
  const body = rows
    .map(
      (row) => `
        <tr>
          ${columns
            .map((column) => `<td>${escapeHtml(formatCellForColumn(column, row[column]))}</td>`)
            .join("")}
        </tr>
      `
    )
    .join("");
  return `
    <section class="stack">
      <h3>${escapeHtml(title)}</h3>
      <div class="table-wrap">
        <table>
          <thead><tr>${headers}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    </section>
  `;
}

function normalizeRatePercent(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) {
    return null;
  }
  if (number <= 1) {
    return number * 100;
  }
  if (number <= 100) {
    return number;
  }
  return null;
}

function formatCount(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return Math.round(number).toLocaleString();
}

function findUserAggressionRow(rows) {
  if (!Array.isArray(rows)) {
    return null;
  }
  return (
    rows.find(
      (row) =>
        row?.is_hero_player ||
        row?.player_id === "user_agent" ||
        row?.agent_id === "user_agent" ||
        participantDisplayName(row) === "Your Agent"
    ) || null
  );
}

function findBestUserAggressionRow(...rowSets) {
  const candidates = rowSets
    .flatMap((rows) => (Array.isArray(rows) ? rows : []))
    .filter((row) => {
      if (!row || typeof row !== "object") {
        return false;
      }
      const isHero =
        row.is_hero_player ||
        row.player_id === "user_agent" ||
        row.agent_id === "user_agent" ||
        participantDisplayName(row) === "Your Agent";
      return isHero && aggressionChartEntries(row).length > 0;
    });
  if (!candidates.length) {
    return null;
  }
  candidates.sort((left, right) => {
    const rightHands = playerObservedHands(right);
    const leftHands = playerObservedHands(left);
    if (rightHands !== leftHands) {
      return rightHands - leftHands;
    }
    return Number(right?.observed_actions || 0) - Number(left?.observed_actions || 0);
  });
  return candidates[0];
}

function aggressionChartEntries(row) {
  if (!row || typeof row !== "object") {
    return [];
  }
  return [
    { label: "VPIP", percent: normalizeRatePercent(row.vpip), tone: "vpip" },
    { label: "PFR", percent: normalizeRatePercent(row.pfr), tone: "pfr" },
    {
      label: "Aggression",
      percent: normalizeRatePercent(row.aggression_frequency),
      tone: "aggression"
    }
  ].filter((entry) => entry.percent !== null);
}

function renderAggressionChartSection(row, { title = "User Agent Aggression Profile", caption = "" } = {}) {
  const entries = aggressionChartEntries(row);
  if (!entries.length) {
    return "";
  }
  const metaParts = [];
  if (caption) {
    metaParts.push(caption);
  }
  if (Number.isFinite(Number(row?.observed_hands))) {
    metaParts.push(`Observed hands: ${formatCount(row.observed_hands)}`);
  }
  if (Number.isFinite(Number(row?.observed_actions))) {
    metaParts.push(`Observed actions: ${formatCount(row.observed_actions)}`);
  }
  return `
    <section class="stack">
      <h3>${escapeHtml(title)}</h3>
      ${
        metaParts.length
          ? `<div class="aggression-chart-meta">${escapeHtml(metaParts.join(" · "))}</div>`
          : ""
      }
      <div class="aggression-chart">
        ${entries
          .map(
            (entry) => `
              <div class="aggression-bar-row">
                <div class="aggression-bar-label">${escapeHtml(entry.label)}</div>
                <div class="aggression-bar-track" aria-hidden="true">
                  <div
                    class="aggression-bar-fill aggression-bar-fill-${escapeHtml(entry.tone)}"
                    style="width: ${escapeHtml(entry.percent.toFixed(1))}%"
                  ></div>
                </div>
                <div class="aggression-bar-value">${escapeHtml(entry.percent.toFixed(1))}%</div>
              </div>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

function normalizeProfitTimeline(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return null;
  }
  const normalized = rows
    .map((row, index) => {
      const handNumber = Number(row?.hand_number);
      const cumulative = Number(row?.cumulative_bb_won);
      if (!Number.isFinite(cumulative)) {
        return null;
      }
      return {
        hand_number: Number.isFinite(handNumber) ? handNumber : index + 1,
        cumulative_bb_won: cumulative,
        hand_id: row?.hand_id || null,
        result_bb: Number.isFinite(Number(row?.result_bb)) ? Number(row.result_bb) : null
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.hand_number - right.hand_number);
  if (!normalized.length) {
    return null;
  }
  return normalized;
}

function downsampleProfitTimeline(rows, maxPoints = 180) {
  if (!Array.isArray(rows) || rows.length <= maxPoints) {
    return rows || [];
  }
  const lastIndex = rows.length - 1;
  const sampled = [];
  for (let index = 0; index < maxPoints; index += 1) {
    const sourceIndex = Math.min(
      lastIndex,
      Math.round((index / Math.max(1, maxPoints - 1)) * lastIndex)
    );
    const row = rows[sourceIndex];
    if (!sampled.length || sampled[sampled.length - 1].hand_number !== row.hand_number) {
      sampled.push(row);
    }
  }
  const lastRow = rows[lastIndex];
  if (sampled[sampled.length - 1]?.hand_number !== lastRow.hand_number) {
    sampled.push(lastRow);
  }
  return sampled;
}

function renderProfitTimelineSection(
  rows,
  { title = "User Agent Profit Over Run", caption = "" } = {}
) {
  if (!Array.isArray(rows) || !rows.length) {
    return "";
  }
  const sampled = downsampleProfitTimeline(rows);
  const values = sampled.map((row) => row.cumulative_bb_won);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const range = maxValue - minValue || 1;
  const width = 100;
  const height = 40;
  const padding = 2;
  const baselineValue = Math.max(minValue, Math.min(maxValue, 0));
  const baselineY = height - padding - ((baselineValue - minValue) / range) * (height - padding * 2);
  const points = sampled
    .map((row, index) => {
      const x = sampled.length === 1
        ? width / 2
        : (index / Math.max(1, sampled.length - 1)) * width;
      const y = height - padding - ((row.cumulative_bb_won - minValue) / range) * (height - padding * 2);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
  const finalPoint = rows[rows.length - 1];
  const observedHands = timelineLastHandNumber(rows);
  const chartMeta = [
    caption,
    `Hands: ${formatCount(observedHands || rows.length)}`,
    `Current profit: ${formatNumber(finalPoint.cumulative_bb_won)} BB`
  ]
    .filter(Boolean)
    .join(" · ");
  return `
    <section class="stack">
      <h3>${escapeHtml(title)}</h3>
      ${
        chartMeta
          ? `<div class="aggression-chart-meta">${escapeHtml(chartMeta)}</div>`
          : ""
      }
      <div class="profit-chart">
        <svg
          class="profit-chart-svg"
          viewBox="0 0 ${width} ${height}"
          preserveAspectRatio="none"
          role="img"
          aria-label="${escapeHtml(title)}"
        >
          <line
            x1="0"
            y1="${escapeHtml(baselineY.toFixed(2))}"
            x2="${width}"
            y2="${escapeHtml(baselineY.toFixed(2))}"
            class="profit-chart-baseline"
          ></line>
          <polyline
            points="${escapeHtml(points)}"
            class="profit-chart-line"
          ></polyline>
        </svg>
        <div class="profit-chart-axis">
          <span>${escapeHtml(`${formatNumber(minValue)} BB`)}</span>
          <span>${escapeHtml(`${formatNumber(maxValue)} BB`)}</span>
        </div>
      </div>
    </section>
  `;
}

function runContextText(run) {
  const requestContext = String(run?.request_payload?.context || "").trim();
  if (requestContext) {
    return requestContext;
  }
  const preview = String(run?.hero_context_preview || "").trim();
  return preview || "";
}

function personaDisplayName(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "";
  }
  if (normalized === "tag") {
    return "TAG";
  }
  if (normalized === "lag") {
    return "LAG";
  }
  if (normalized === "calling_station") {
    return "Calling Station";
  }
  if (normalized === "shove_bot") {
    return "Shove Bot";
  }
  return humanize(normalized);
}

function participantDisplayName(row) {
  const participant = resolveParticipantMetadata(row);
  if (!row || typeof row !== "object") {
    return "Unknown";
  }
  const labeledRow = participant ? { ...participant, ...row } : row;
  if (
    labeledRow.is_hero_player ||
    labeledRow.player_id === "user_agent" ||
    labeledRow.agent_id === "user_agent"
  ) {
    return "Your Agent";
  }
  const persona = personaDisplayName(labeledRow.persona_name);
  if (persona) {
    return persona;
  }
  return labeledRow.player_id || labeledRow.agent_id || "Unknown";
}

function withParticipantLabels(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }
  return rows.map((row) => ({
    ...row,
    player: participantDisplayName(row)
  }));
}

function sortRowsByMetric(rows, metric) {
  if (!Array.isArray(rows)) {
    return [];
  }
  return [...rows].sort((left, right) => {
    const leftMetric = Number(left?.[metric]);
    const rightMetric = Number(right?.[metric]);
    const normalizedLeftMetric = Number.isFinite(leftMetric) ? leftMetric : Number.NEGATIVE_INFINITY;
    const normalizedRightMetric = Number.isFinite(rightMetric)
      ? rightMetric
      : Number.NEGATIVE_INFINITY;
    if (normalizedLeftMetric !== normalizedRightMetric) {
      return normalizedRightMetric - normalizedLeftMetric;
    }
    const leftProfit = Number(left?.total_bb_won);
    const rightProfit = Number(right?.total_bb_won);
    const normalizedLeftProfit = Number.isFinite(leftProfit) ? leftProfit : Number.NEGATIVE_INFINITY;
    const normalizedRightProfit = Number.isFinite(rightProfit)
      ? rightProfit
      : Number.NEGATIVE_INFINITY;
    if (normalizedLeftProfit !== normalizedRightProfit) {
      return normalizedRightProfit - normalizedLeftProfit;
    }
    return Number(left?.seat || 0) - Number(right?.seat || 0);
  });
}

function buildProvisionalResultSummary(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return { hero: null, winner: null };
  }
  const ordered = [...rows].sort((left, right) => {
    const leftRank = Number(left?.final_rank);
    const rightRank = Number(right?.final_rank);
    const normalizedLeftRank = Number.isFinite(leftRank) ? leftRank : Number.POSITIVE_INFINITY;
    const normalizedRightRank = Number.isFinite(rightRank) ? rightRank : Number.POSITIVE_INFINITY;
    if (normalizedLeftRank !== normalizedRightRank) {
      return normalizedLeftRank - normalizedRightRank;
    }
    return Number(right?.total_bb_won || 0) - Number(left?.total_bb_won || 0);
  });
  return {
    hero:
      rows.find(
        (row) =>
          row?.is_hero_player ||
          row?.player_id === "user_agent" ||
          row?.agent_id === "user_agent"
      ) || null,
    winner: ordered[0] || null
  };
}

function resolveParticipantMetadata(row) {
  if (!row || typeof row !== "object") {
    return null;
  }
  const referenceRows = [
    ...(Array.isArray(state.runResults?.player_summaries)
      ? state.runResults.player_summaries
      : []),
    ...(Array.isArray(state.runDetail?.player_summaries)
      ? state.runDetail.player_summaries
      : [])
  ];
  for (const candidate of referenceRows) {
    if (!candidate || typeof candidate !== "object") {
      continue;
    }
    if (row.player_id && candidate.player_id === row.player_id) {
      return candidate;
    }
    if (row.agent_id && candidate.agent_id === row.agent_id) {
      return candidate;
    }
  }
  return null;
}

function requireSession() {
  if (state.session?.user_id) {
    return true;
  }
  setError("Sign in first.");
  renderMessages();
  return false;
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  headers.set("Content-Type", headers.get("Content-Type") || "application/json");

  return fetchJson(path, {
    ...options,
    headers,
    credentials: "same-origin"
  });
}

async function fetchJson(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    ...options
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message =
      payload?.message ||
      payload?.error ||
      `Request failed with status ${response.status}`;
    const error = new Error(message);
    error.status = response.status;
    error.payload = payload;
    if (response.status === 401) {
      resetUserScopedState();
      renderAll();
    }
    throw error;
  }
  clearMessages();
  return payload;
}

function clearMessages() {
  state.error = null;
}

function setMessage(message) {
  state.message = message;
  state.error = null;
}

function setError(message) {
  state.error = message;
  state.message = null;
}

function parseCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseOptionalNumber(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeDateTimeLocal(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return null;
  }
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return normalized;
  }
  return parsed.toISOString();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatNumber(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return number.toFixed(2);
}

function formatPercent(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return `${(number * 100).toFixed(1)}%`;
}

function formatCell(value) {
  if (typeof value === "number") {
    return formatNumber(value);
  }
  return value ?? "n/a";
}

function decisionModeLabel(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "n/a";
  }
  if (normalized.includes("mixed")) {
    return "Mixed";
  }
  if (normalized === "llm" || normalized.includes("llm") || normalized.includes("model")) {
    return "LLM";
  }
  if (normalized === "heuristic" || normalized.includes("heuristic") || normalized.includes("rules")) {
    return "heuristic";
  }
  return humanize(normalized);
}

function decisionModeFilterLabel(value) {
  return String(value || "").trim() ? decisionModeLabel(value) : "All strategies";
}

function columnLabel(column) {
  return COLUMN_LABELS[column] || humanize(column);
}

function formatCellForColumn(column, value) {
  if (column === "decision_backend" || column === "backend_type") {
    return decisionModeLabel(value);
  }
  if (column === "status" || column === "profile_status") {
    return humanize(value || "n/a");
  }
  return formatCell(value);
}

function humanize(column) {
  return String(column)
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function truncateText(value, maxLength = 160) {
  const text = String(value || "").trim();
  if (!text) {
    return "No context text saved.";
  }
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}...`;
}

function availableColumns(rows, preferredColumns) {
  return preferredColumns.filter((column, index) => {
    if (preferredColumns.indexOf(column) !== index) {
      return false;
    }
    return rows.some((row) => row[column] !== undefined && row[column] !== null);
  });
}

function generateRunId() {
  return `sim_ui_${Date.now()}`;
}
