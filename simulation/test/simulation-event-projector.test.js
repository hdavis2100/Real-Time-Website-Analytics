import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeSimulationLifecycleEvent,
  projectSimulationLifecycleEvent
} from "../src/lib/simulation-event-projector.js";

test("normalizeSimulationLifecycleEvent normalizes supported lifecycle events", () => {
  const event = normalizeSimulationLifecycleEvent({
    event_type: "simulation_started",
    simulation_run_id: "sim_123",
    decision_backend: "heuristic",
    user_id: "user_1",
    hand_count: "250",
    hero_context_hash: "ctx_hash_1",
    event_ts: "2026-04-20T20:30:00Z"
  });

  assert.equal(event.eventType, "simulation_started");
  assert.equal(event.simulationRunId, "sim_123");
  assert.equal(event.status, "running");
  assert.equal(event.decisionBackend, "heuristic");
  assert.equal(event.backendType, "heuristic_persona");
  assert.equal(event.handCount, 250);
  assert.equal(event.userId, "user_1");
  assert.equal(event.heroContextHash, "ctx_hash_1");
  assert.equal(event.eventTimestamp, "2026-04-20T20:30:00.000Z");
});

test("projectSimulationLifecycleEvent creates missing queued runs from simulation_requested events", async () => {
  const ensuredUsers = [];
  const createdRuns = [];
  const appStore = {
    ensureUser: async (payload) => {
      ensuredUsers.push(payload);
      return payload;
    },
    getSimulationRun: async () => null,
    createSimulationRun: async (payload) => {
      createdRuns.push(payload);
      return {
        simulation_run_id: payload.simulationRunId,
        status: payload.status,
        request_mode: payload.requestMode
      };
    },
    updateSimulationRunLifecycle: async () => {
      throw new Error("create path should not update existing rows");
    }
  };

  const result = await projectSimulationLifecycleEvent(appStore, {
    event_type: "simulation_requested",
    simulation_run_id: "sim_requested_1",
    user_id: "user_projected_1",
    hand_count: 300,
    request_mode: "saved_agent",
    decision_backend: "llm",
    hero_agent_definition_id: "agent_def_1",
    hero_agent_version_id: "agent_ver_1",
    hero_context_hash: "ctx_hash_1",
    hero_context_preview: "pressure capped ranges",
    model_name: "gpt-5.4-nano",
    requested_at: "2026-04-20T20:35:00Z"
  });

  assert.equal(result.applied, true);
  assert.equal(result.action, "created");
  assert.equal(ensuredUsers.length, 1);
  assert.equal(ensuredUsers[0].userId, "user_projected_1");
  assert.equal(createdRuns.length, 1);
  assert.equal(createdRuns[0].simulationRunId, "sim_requested_1");
  assert.equal(createdRuns[0].status, "queued");
  assert.equal(createdRuns[0].requestMode, "saved_agent");
  assert.equal(createdRuns[0].handCount, 300);
  assert.equal(createdRuns[0].backendType, "llm_gated_nano");
  assert.equal(createdRuns[0].heroAgentDefinitionId, "agent_def_1");
  assert.equal(createdRuns[0].heroAgentVersionId, "agent_ver_1");
  assert.equal(createdRuns[0].requestedAt, "2026-04-20T20:35:00.000Z");
});

test("projectSimulationLifecycleEvent updates existing runs for started, progress, and terminal lifecycle events", async () => {
  const updates = [];
  const appStore = {
    getSimulationRun: async () => ({
      simulation_run_id: "sim_running_1",
      status: "queued"
    }),
    updateSimulationRunLifecycle: async (simulationRunId, patch) => {
      updates.push({ simulationRunId, patch });
      return {
        simulation_run_id: simulationRunId,
        status: patch.status
      };
    }
  };

  const started = await projectSimulationLifecycleEvent(appStore, {
    event_type: "simulation_started",
    simulation_run_id: "sim_running_1",
    decision_backend: "heuristic",
    model_name: null,
    hero_context_hash: "ctx_started",
    event_ts: "2026-04-20T20:36:00Z"
  });
  const progress = await projectSimulationLifecycleEvent(appStore, {
    event_type: "simulation_progress",
    simulation_run_id: "sim_running_1",
    decision_backend: "heuristic",
    model_name: null,
    hero_context_hash: "ctx_started",
    published_actions: 90,
    published_hand_summaries: 12,
    player_summaries: [
      {
        player_id: "hero_player",
        is_hero_player: true,
        total_bb_won: 2.25
      }
    ],
    event_ts: "2026-04-20T20:38:00Z"
  });
  const completed = await projectSimulationLifecycleEvent(appStore, {
    event_type: "simulation_completed",
    simulation_run_id: "sim_running_1",
    decision_backend: "heuristic",
    model_name: null,
    hero_context_hash: "ctx_started",
    hand_count: 300,
    published_actions: 180,
    published_hand_summaries: 24,
    player_summaries: [
      {
        player_id: "hero_player",
        is_hero_player: true,
        total_bb_won: 4.5,
      }
    ],
    event_ts: "2026-04-20T20:40:00Z"
  });

  assert.equal(started.applied, true);
  assert.equal(started.action, "updated");
  assert.equal(progress.applied, true);
  assert.equal(progress.action, "updated");
  assert.equal(completed.applied, true);
  assert.equal(completed.action, "updated");
  assert.equal(updates.length, 3);
  assert.equal(updates[0].simulationRunId, "sim_running_1");
  assert.equal(updates[0].patch.status, "running");
  assert.equal(updates[0].patch.startedAt, "2026-04-20T20:36:00.000Z");
  assert.equal(updates[0].patch.clearCompletedAt, true);
  assert.equal(updates[0].patch.backendType, "heuristic_persona");
  assert.equal(updates[1].patch.status, "running");
  assert.equal(updates[1].patch.startedAt, "2026-04-20T20:38:00.000Z");
  assert.equal(updates[1].patch.publishedActions, 90);
  assert.equal(updates[1].patch.publishedHandSummaries, 12);
  assert.equal(updates[1].patch.playerSummaries[0].player_id, "hero_player");
  assert.deepEqual(updates[1].patch.heroProfitTimeseries, [
    {
      simulation_run_id: "sim_running_1",
      hand_number: 12,
      cumulative_bb_won: 2.25,
      event_ts: "2026-04-20T20:38:00.000Z"
    }
  ]);
  assert.equal(updates[2].patch.status, "completed");
  assert.equal(updates[2].patch.completedAt, "2026-04-20T20:40:00.000Z");
  assert.equal(updates[2].patch.clearErrorMessage, true);
  assert.equal(updates[2].patch.publishedActions, 180);
  assert.equal(updates[2].patch.publishedHandSummaries, 24);
  assert.equal(updates[2].patch.playerSummaries[0].player_id, "hero_player");
  assert.deepEqual(updates[2].patch.heroProfitTimeseries, [
    {
      simulation_run_id: "sim_running_1",
      hand_number: 24,
      cumulative_bb_won: 4.5,
      event_ts: "2026-04-20T20:40:00.000Z"
    }
  ]);
});

test("projectSimulationLifecycleEvent skips replayed lifecycle events for a different run owner", async () => {
  const updates = [];
  const ensuredUsers = [];
  const warnings = [];
  const appStore = {
    ensureUser: async (payload) => {
      ensuredUsers.push(payload);
      return payload;
    },
    getSimulationRun: async () => ({
      simulation_run_id: "sim_reused_1",
      user_id: "fresh_user",
      status: "queued",
      decision_backend: "llm",
      hero_context_hash: "fresh_context",
      requested_at: "2026-04-20T20:40:00.000Z"
    }),
    updateSimulationRunLifecycle: async (simulationRunId, patch) => {
      updates.push({ simulationRunId, patch });
      return {
        simulation_run_id: simulationRunId,
        status: patch.status
      };
    }
  };

  const result = await projectSimulationLifecycleEvent(
    appStore,
    {
      event_type: "simulation_failed",
      simulation_run_id: "sim_reused_1",
      user_id: "old_user",
      decision_backend: "llm",
      hero_context_hash: "fresh_context",
      error_message: "old failure",
      event_ts: "2026-04-20T20:30:00Z"
    },
    {
      logger: {
        warn: (message) => warnings.push(message)
      }
    }
  );

  assert.equal(result.applied, false);
  assert.equal(result.reason, "stale_or_incompatible_event");
  assert.equal(updates.length, 0);
  assert.equal(ensuredUsers.length, 0);
  assert.equal(warnings.length, 1);
});

test("projectSimulationLifecycleEvent ignores unsupported events", async () => {
  const appStore = {
    getSimulationRun: async () => {
      throw new Error("unsupported events should not hit the store");
    },
    updateSimulationRunLifecycle: async () => {
      throw new Error("unsupported events should not hit the store");
    }
  };

  const result = await projectSimulationLifecycleEvent(appStore, {
    event_type: "hand_summary",
    simulation_run_id: "sim_ignore_1"
  });

  assert.equal(result.applied, false);
  assert.equal(result.reason, "unsupported_event");
});
