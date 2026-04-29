import { getAppStore as defaultGetAppStore } from "./app-store.js";
import {
  parsePythonJson as defaultParsePythonJson,
  runPythonModule as defaultRunPythonModule
} from "./python.js";

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function parseDateMaybe(value) {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function ageMsFromNow(value, now = new Date()) {
  const parsed = parseDateMaybe(value);
  if (!parsed) {
    return null;
  }
  return Math.max(0, now.getTime() - parsed.getTime());
}

function hasExecutionCompleteProgress(run) {
  const expectedHands = Number(run?.hand_count);
  const publishedHands = Number(run?.published_hand_summaries);
  if (!Number.isFinite(expectedHands) || expectedHands <= 0) {
    return false;
  }
  if (!Number.isFinite(publishedHands) || publishedHands < expectedHands) {
    return false;
  }
  return true;
}

function withTimeout(promise, timeoutMs, message) {
  let timeoutHandle = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(new Error(message));
    }, timeoutMs);
    timeoutHandle.unref?.();
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  });
}

async function loadSimulationResultsStatusPayload(
  simulationRunId,
  {
    runPythonModule = defaultRunPythonModule,
    parsePythonJson = defaultParsePythonJson
  } = {}
) {
  const rawResult = await runPythonModule("dashboard.api_bridge", [
    "simulation-results-status",
    "--simulation-run-id",
    String(simulationRunId)
  ]);
  const payload = parsePythonJson(rawResult);
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid simulation results status payload");
  }
  return payload;
}

export function createFinalizingRunReconciler({
  getAppStore = defaultGetAppStore,
  loadStatusPayload = loadSimulationResultsStatusPayload,
  intervalMs = process.env.FINALIZING_RUN_RECONCILE_INTERVAL_MS,
  batchSize = process.env.FINALIZING_RUN_RECONCILE_BATCH_SIZE,
  statusTimeoutMs = process.env.FINALIZING_RUN_RECONCILE_STATUS_TIMEOUT_MS,
  staleFinalizingMs = process.env.FINALIZING_RUN_STALE_COMPLETION_MS,
  staleRunningMs = process.env.STALE_RUNNING_RUN_FAILURE_MS,
  staleQueuedMs = process.env.STALE_QUEUED_RUN_FAILURE_MS,
  logger = console
} = {}) {
  const resolvedIntervalMs = Math.max(1000, parsePositiveInt(intervalMs, 5000));
  const resolvedBatchSize = Math.max(1, Math.min(parsePositiveInt(batchSize, 25), 250));
  const resolvedStatusTimeoutMs = Math.max(
    1000,
    parsePositiveInt(statusTimeoutMs, 12000)
  );
  const resolvedStaleFinalizingMs = Math.max(
    1000,
    parsePositiveInt(staleFinalizingMs, 180000)
  );
  const resolvedStaleRunningMs = Math.max(
    1000,
    parsePositiveInt(staleRunningMs, 600000)
  );
  const resolvedStaleQueuedMs = Math.max(
    1000,
    parsePositiveInt(staleQueuedMs, 600000)
  );

  let appStore = null;
  let stopped = true;
  let timer = null;
  let runningPromise = null;

  function scheduleNext() {
    if (stopped) {
      return;
    }
    timer = setTimeout(() => {
      timer = null;
      void tick();
    }, resolvedIntervalMs);
    timer.unref?.();
  }

  async function reconcileOnce() {
    appStore = appStore || getAppStore?.() || null;
    if (!appStore?.listSimulationRuns || !appStore?.updateSimulationRunLifecycle) {
      return { scanned: 0, promoted: 0, failed: 0 };
    }

    const [finalizingRuns, runningRuns, queuedRuns] = await Promise.all([
      appStore.listSimulationRuns({
        status: "finalizing",
        limit: resolvedBatchSize,
        offset: 0
      }),
      appStore.listSimulationRuns({
        status: "running",
        limit: resolvedBatchSize,
        offset: 0
      }),
      appStore.listSimulationRuns({
        status: "queued",
        limit: resolvedBatchSize,
        offset: 0
      })
    ]);

    const now = new Date();
    const promotions = await Promise.all(
      finalizingRuns.map(async (run) => {
        const simulationRunId = run?.simulation_run_id;
        if (!simulationRunId) {
          return null;
        }

        const finalizingAgeMs = ageMsFromNow(
          run?.updated_at || run?.completed_at || run?.requested_at,
          now
        );
        if (
          finalizingAgeMs != null &&
          finalizingAgeMs >= resolvedStaleFinalizingMs &&
          parseDateMaybe(run?.completed_at) &&
          hasExecutionCompleteProgress(run)
        ) {
          try {
            await appStore.updateSimulationRunLifecycle(simulationRunId, {
              status: "completed",
              completedAt: run.completed_at || now.toISOString(),
              publishedActions: run.published_actions ?? null,
              publishedHandSummaries: run.published_hand_summaries ?? null,
              clearErrorMessage: true
            });
            return "promoted";
          } catch (error) {
            logger?.warn?.(
              `[finalizing-run-reconciler] failed to auto-complete ${simulationRunId}: ${error.message}`
            );
            return null;
          }
        }

        let payload = null;
        try {
          payload = await withTimeout(
            loadStatusPayload(simulationRunId),
            resolvedStatusTimeoutMs,
            `status probe timed out after ${resolvedStatusTimeoutMs}ms`
          );
        } catch (error) {
          logger?.warn?.(
            `[finalizing-run-reconciler] failed to inspect ${simulationRunId}: ${error.message}`
          );
          return null;
        }

        if (payload?.ready !== true) {
          return null;
        }

        const runSummary = payload?.run_summary || {};
        try {
          await appStore.updateSimulationRunLifecycle(simulationRunId, {
            status: "completed",
            completedAt:
              runSummary.finished_at ||
              runSummary.completed_at ||
              run.completed_at ||
              now.toISOString(),
            publishedActions:
              runSummary.published_actions ?? run.published_actions ?? null,
            publishedHandSummaries:
              runSummary.published_hand_summaries ??
              run.published_hand_summaries ??
              null,
            clearErrorMessage: true
          });
          return "promoted";
        } catch (error) {
          logger?.warn?.(
            `[finalizing-run-reconciler] failed to promote ${simulationRunId}: ${error.message}`
          );
          return null;
        }
      })
    );

    const staleRunningFailures = await Promise.all(
      runningRuns.map(async (run) => {
        const simulationRunId = run?.simulation_run_id;
        if (!simulationRunId) {
          return null;
        }
        const runningAgeMs = ageMsFromNow(
          run?.updated_at || run?.started_at || run?.requested_at,
          now
        );
        if (runningAgeMs == null || runningAgeMs < resolvedStaleRunningMs) {
          return null;
        }
        if (hasExecutionCompleteProgress(run) && parseDateMaybe(run?.completed_at)) {
          try {
            await appStore.updateSimulationRunLifecycle(simulationRunId, {
              status: "completed",
              completedAt: run.completed_at || now.toISOString(),
              publishedActions: run.published_actions ?? null,
              publishedHandSummaries: run.published_hand_summaries ?? null,
              clearErrorMessage: true
            });
            return "promoted";
          } catch (error) {
            logger?.warn?.(
              `[finalizing-run-reconciler] failed to finalize stale running ${simulationRunId}: ${error.message}`
            );
            return null;
          }
        }
        try {
          await appStore.updateSimulationRunLifecycle(simulationRunId, {
            status: "failed",
            completedAt: now.toISOString(),
            errorMessage:
              "Run stopped making progress and was cleaned up automatically as stale.",
            publishedActions: run.published_actions ?? null,
            publishedHandSummaries: run.published_hand_summaries ?? null
          });
          return "failed";
        } catch (error) {
          logger?.warn?.(
            `[finalizing-run-reconciler] failed to fail stale running ${simulationRunId}: ${error.message}`
          );
          return null;
        }
      })
    );

    const staleQueuedFailures = await Promise.all(
      queuedRuns.map(async (run) => {
        const simulationRunId = run?.simulation_run_id;
        if (!simulationRunId) {
          return null;
        }
        const queuedAgeMs = ageMsFromNow(run?.updated_at || run?.requested_at, now);
        if (
          queuedAgeMs == null ||
          queuedAgeMs < resolvedStaleQueuedMs ||
          parseDateMaybe(run?.started_at)
        ) {
          return null;
        }
        try {
          await appStore.updateSimulationRunLifecycle(simulationRunId, {
            status: "failed",
            completedAt: now.toISOString(),
            errorMessage:
              "Queued run timed out before starting and was cleaned up automatically."
          });
          return "failed";
        } catch (error) {
          logger?.warn?.(
            `[finalizing-run-reconciler] failed to fail stale queued ${simulationRunId}: ${error.message}`
          );
          return null;
        }
      })
    );

    return {
      scanned: finalizingRuns.length + runningRuns.length + queuedRuns.length,
      promoted: [...promotions, ...staleRunningFailures].filter(
        (value) => value === "promoted"
      ).length,
      failed: [...staleRunningFailures, ...staleQueuedFailures].filter(
        (value) => value === "failed"
      ).length
    };
  }

  async function tick() {
    if (stopped) {
      return { scanned: 0, promoted: 0 };
    }
    if (runningPromise) {
      return runningPromise;
    }

    runningPromise = reconcileOnce()
      .catch((error) => {
        logger?.warn?.(
          `[finalizing-run-reconciler] reconcile loop failed: ${error.message}`
        );
        return { scanned: 0, promoted: 0, failed: 0, error: error.message };
      })
      .finally(() => {
        runningPromise = null;
        if (!stopped) {
          scheduleNext();
        }
      });

    return runningPromise;
  }

  function start() {
    if (!stopped) {
      return;
    }
    stopped = false;
    void tick();
  }

  async function stop() {
    stopped = true;
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    if (runningPromise) {
      await runningPromise.catch(() => null);
    }
  }

  return {
    start,
    stop,
    reconcileOnce
  };
}
