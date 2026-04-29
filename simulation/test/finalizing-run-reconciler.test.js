import test from "node:test";
import assert from "node:assert/strict";

import { createFinalizingRunReconciler } from "../src/lib/finalizing-run-reconciler.js";

test("reconcileOnce promotes ready finalizing runs to completed", async () => {
  const updates = [];
  const reconciler = createFinalizingRunReconciler({
    getAppStore: () => ({
      listSimulationRuns: async ({ status }) =>
        status === "finalizing"
          ? [
              {
                simulation_run_id: "sim_finalizing_1",
                status: "finalizing",
                completed_at: "2026-04-22T00:00:00.000Z",
                published_actions: 180,
                published_hand_summaries: 24
              }
            ]
          : [],
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        updates.push({ simulationRunId, patch });
        return {
          simulation_run_id: simulationRunId,
          status: patch.status
        };
      }
    }),
    loadStatusPayload: async () => ({
      ready: true,
      run_summary: {
        finished_at: "2026-04-22T00:00:10.000Z",
        published_actions: 240,
        published_hand_summaries: 30
      }
    })
  });

  const result = await reconciler.reconcileOnce();

  assert.deepEqual(result, { scanned: 1, promoted: 1, failed: 0 });
  assert.equal(updates.length, 1);
  assert.equal(updates[0].simulationRunId, "sim_finalizing_1");
  assert.equal(updates[0].patch.status, "completed");
  assert.equal(updates[0].patch.completedAt, "2026-04-22T00:00:10.000Z");
  assert.equal(updates[0].patch.publishedActions, 240);
  assert.equal(updates[0].patch.publishedHandSummaries, 30);
  assert.equal(updates[0].patch.clearErrorMessage, true);
});

test("reconcileOnce leaves pending finalizing runs alone", async () => {
  let updateCalls = 0;
  const reconciler = createFinalizingRunReconciler({
    getAppStore: () => ({
      listSimulationRuns: async ({ status }) =>
        status === "finalizing"
          ? [
              {
                simulation_run_id: "sim_finalizing_2",
                status: "finalizing"
              }
            ]
          : [],
      updateSimulationRunLifecycle: async () => {
        updateCalls += 1;
        return null;
      }
    }),
    loadStatusPayload: async () => ({
      ready: false,
      run_summary: {}
    })
  });

  const result = await reconciler.reconcileOnce();

  assert.deepEqual(result, { scanned: 1, promoted: 0, failed: 0 });
  assert.equal(updateCalls, 0);
});

test("reconcileOnce continues when a status lookup fails", async () => {
  const warnings = [];
  const updates = [];
  const reconciler = createFinalizingRunReconciler({
    getAppStore: () => ({
      listSimulationRuns: async ({ status }) =>
        status === "finalizing"
          ? [
              { simulation_run_id: "sim_lookup_fail", status: "finalizing" },
              { simulation_run_id: "sim_ready_after_fail", status: "finalizing" }
            ]
          : [],
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        updates.push({ simulationRunId, patch });
        return {
          simulation_run_id: simulationRunId,
          status: patch.status
        };
      }
    }),
    loadStatusPayload: async (simulationRunId) => {
      if (simulationRunId === "sim_lookup_fail") {
        throw new Error("snowflake unavailable");
      }
      return {
        ready: true,
        run_summary: {
          completed_at: "2026-04-22T00:01:00.000Z"
        }
      };
    },
    logger: {
      warn(message) {
        warnings.push(message);
      }
    }
  });

  const result = await reconciler.reconcileOnce();

  assert.deepEqual(result, { scanned: 2, promoted: 1, failed: 0 });
  assert.equal(updates.length, 1);
  assert.equal(updates[0].simulationRunId, "sim_ready_after_fail");
  assert.match(warnings[0], /sim_lookup_fail/);
});

test("reconcileOnce is a no-op when the app store is unavailable", async () => {
  const reconciler = createFinalizingRunReconciler({
    getAppStore: () => null
  });

  const result = await reconciler.reconcileOnce();

  assert.deepEqual(result, { scanned: 0, promoted: 0, failed: 0 });
});

test("reconcileOnce auto-completes stale finalizing runs with full execution progress", async () => {
  const updates = [];
  const reconciler = createFinalizingRunReconciler({
    getAppStore: () => ({
      listSimulationRuns: async ({ status }) => {
        if (status !== "finalizing") {
          return [];
        }
        return [
          {
            simulation_run_id: "sim_finalizing_stale",
            status: "finalizing",
            hand_count: 60,
            published_actions: 900,
            published_hand_summaries: 60,
            completed_at: "2026-04-22T00:00:00.000Z",
            updated_at: "2026-04-22T00:00:00.000Z"
          }
        ];
      },
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        updates.push({ simulationRunId, patch });
        return { simulation_run_id: simulationRunId, status: patch.status };
      }
    }),
    staleFinalizingMs: 1000,
    loadStatusPayload: async () => ({ ready: false, run_summary: {} })
  });

  const result = await reconciler.reconcileOnce();

  assert.deepEqual(result, { scanned: 1, promoted: 1, failed: 0 });
  assert.equal(updates.length, 1);
  assert.equal(updates[0].simulationRunId, "sim_finalizing_stale");
  assert.equal(updates[0].patch.status, "completed");
  assert.equal(updates[0].patch.publishedHandSummaries, 60);
});

test("reconcileOnce fails stale running runs that stopped making progress", async () => {
  const updates = [];
  const reconciler = createFinalizingRunReconciler({
    getAppStore: () => ({
      listSimulationRuns: async ({ status }) => {
        if (status !== "running") {
          return [];
        }
        return [
          {
            simulation_run_id: "sim_running_stale",
            status: "running",
            hand_count: 500,
            published_actions: 120,
            published_hand_summaries: 10,
            updated_at: "2026-04-22T00:00:00.000Z"
          }
        ];
      },
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        updates.push({ simulationRunId, patch });
        return { simulation_run_id: simulationRunId, status: patch.status };
      }
    }),
    staleRunningMs: 1000
  });

  const result = await reconciler.reconcileOnce();

  assert.deepEqual(result, { scanned: 1, promoted: 0, failed: 1 });
  assert.equal(updates.length, 1);
  assert.equal(updates[0].simulationRunId, "sim_running_stale");
  assert.equal(updates[0].patch.status, "failed");
  assert.match(updates[0].patch.errorMessage, /stopped making progress/i);
});

test("reconcileOnce fails stale queued runs that never started", async () => {
  const updates = [];
  const reconciler = createFinalizingRunReconciler({
    getAppStore: () => ({
      listSimulationRuns: async ({ status }) => {
        if (status !== "queued") {
          return [];
        }
        return [
          {
            simulation_run_id: "sim_queued_stale",
            status: "queued",
            requested_at: "2026-04-22T00:00:00.000Z",
            updated_at: "2026-04-22T00:00:00.000Z",
            started_at: null
          }
        ];
      },
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        updates.push({ simulationRunId, patch });
        return { simulation_run_id: simulationRunId, status: patch.status };
      }
    }),
    staleQueuedMs: 1000
  });

  const result = await reconciler.reconcileOnce();

  assert.deepEqual(result, { scanned: 1, promoted: 0, failed: 1 });
  assert.equal(updates.length, 1);
  assert.equal(updates[0].simulationRunId, "sim_queued_stale");
  assert.equal(updates[0].patch.status, "failed");
  assert.match(updates[0].patch.errorMessage, /timed out before starting/i);
});
