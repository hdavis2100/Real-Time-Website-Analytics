import test from "node:test";
import assert from "node:assert/strict";

import { createApp } from "../src/app.js";
import { hashPassword } from "../src/lib/auth.js";
import { AppStoreConflictError } from "../src/lib/app-store.js";

function withServer(app, fn) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, async () => {
      const address = server.address();
      const baseUrl = `http://127.0.0.1:${address.port}`;
      try {
        await fn(baseUrl);
        server.close((error) => (error ? reject(error) : resolve()));
      } catch (error) {
        server.close(() => reject(error));
      }
    });
  });
}

function cookieHeaderFromResponse(response) {
  return response.headers.get("set-cookie")?.split(";")[0] || null;
}

test("GET /healthz returns ok", async () => {
  await withServer(createApp(), async (baseUrl) => {
    const response = await fetch(`${baseUrl}/healthz`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
  });
});

test("POST /api/actions validates malformed payloads", async () => {
  await withServer(createApp(), async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/actions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        source_type: "simulated",
        hand_id: "hand_1"
      })
    });
    assert.equal(response.status, 400);
    const payload = await response.json();
    assert.equal(payload.message, "Validation failed");
    assert.ok(Array.isArray(payload.issues));
    assert.ok(payload.issues.length > 0);
  });
});

test("POST /api/simulations requires context", async () => {
  const app = createApp({
    queueSimulationRun: async () => ({ simulation_run_id: "unused" }),
    publishSimulationRequests: async () => 1,
    publishSimulationEvents: async () => 1
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        simulation_run_id: "sim_missing_context"
      })
    });
    assert.equal(response.status, 400);
    const payload = await response.json();
    assert.equal(payload.message, "Validation failed");
  });
});

test("POST /api/simulations rejects unknown public contract fields", async () => {
  const app = createApp({
    queueSimulationRun: async () => ({ simulation_run_id: "unused" }),
    publishSimulationRequests: async () => 1,
    publishSimulationEvents: async () => 1
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        simulation_run_id: "sim_rejects_unknown_fields",
        context: "balanced aggressive value bettor",
        table_id: "user_supplied_table"
      })
    });
    assert.equal(response.status, 400);
    const payload = await response.json();
    assert.equal(payload.message, "Validation failed");
    assert.ok(Array.isArray(payload.issues));
    assert.equal(payload.issues[0].code, "unrecognized_keys");
  });
});

test("POST /api/simulations returns 202 and publishes request plus lifecycle events", async () => {
  const queuedRuns = [];
  const requestMessages = [];
  const lifecycleEvents = [];
  const app = createApp({
    appStore: null,
    queueSimulationRun: async (payload) => {
      queuedRuns.push(payload);
      return payload;
    },
    publishSimulationRequests: async (payloads) => {
      requestMessages.push(...payloads);
      return payloads.length;
    },
    publishSimulationEvents: async (payloads) => {
      lifecycleEvents.push(...payloads);
      return payloads.length;
    },
    topics: {
      simulationRequests: "poker.simulation_requests",
      actions: "poker.actions",
      handSummaries: "poker.hand_summaries",
      simulationEvents: "poker.simulation_events"
    },
    isKafkaConnected: () => true
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        simulation_run_id: "sim_async_1",
        context: "balanced aggressive value bettor"
      })
    });

    assert.equal(response.status, 202);
    assert.equal(response.headers.get("location"), "/api/simulations/sim_async_1");
    const payload = await response.json();
    assert.equal(payload.status, "queued");
    assert.equal(payload.simulation_run_id, "sim_async_1");
  });

  assert.equal(queuedRuns.length, 1);
  assert.equal(requestMessages.length, 1);
  assert.equal(lifecycleEvents.length, 1);
  assert.equal(requestMessages[0].event_type, "simulation_request");
  assert.equal(requestMessages[0].context, "balanced aggressive value bettor");
  assert.equal(requestMessages[0].decision_backend, "llm");
  assert.equal(requestMessages[0].backend_type, "llm_gated_nano");
  assert.equal(requestMessages[0].model_name, "gpt-5.4-nano");
  assert.equal(requestMessages[0].reasoning_effort, "none");
  assert.equal(requestMessages[0].table_id, "table_1");
  assert.equal(lifecycleEvents[0].event_type, "simulation_requested");
  assert.equal(lifecycleEvents[0].status, "queued");
});

test("POST /api/simulations accepts heuristic backend mode", async () => {
  const requestMessages = [];
  const app = createApp({
    appStore: null,
    queueSimulationRun: async (payload) => payload,
    publishSimulationRequests: async (payloads) => {
      requestMessages.push(...payloads);
      return payloads.length;
    },
    publishSimulationEvents: async () => 1
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        simulation_run_id: "sim_heuristic_1",
        context: "play very tight",
        decision_backend: "heuristic"
      })
    });

    assert.equal(response.status, 202);
  });

  assert.equal(requestMessages.length, 1);
  assert.equal(requestMessages[0].decision_backend, "heuristic");
  assert.equal(requestMessages[0].backend_type, "heuristic_persona");
  assert.equal(requestMessages[0].model_name, null);
  assert.equal(requestMessages[0].reasoning_effort, null);
});

test("GET /api/simulations/:id returns durable run status", async () => {
  const app = createApp({
    appStore: null,
    getSimulationRun: async (simulationRunId) => ({
      simulation_run_id: simulationRunId,
      status: "completed",
      player_summaries: [
        {
          player_id: "user_agent",
          final_rank: 1,
          total_bb_won: 12.5
        }
      ]
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations/sim_done_1`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "completed");
    assert.equal(payload.player_summaries[0].player_id, "user_agent");
  });
});

test("DELETE /api/simulations/:id removes an owned completed run", async () => {
  const deleted = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "completed"
      }),
      deleteSimulationRun: async (simulationRunId, { userId }) => {
        deleted.push({ simulationRunId, userId });
        return {
          simulation_run_id: simulationRunId,
          user_id: userId,
          status: "completed"
        };
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations/sim_delete_1?user_id=user_1`, {
      method: "DELETE"
    });
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.simulation_run_id, "sim_delete_1");
  });

  assert.deepEqual(deleted, [
    {
      simulationRunId: "sim_delete_1",
      userId: "user_1"
    }
  ]);
});

test("DELETE /api/simulations/:id rejects active runs", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "running"
      }),
      deleteSimulationRun: async () => {
        throw new Error("deleteSimulationRun should not be called for active runs");
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations/sim_running_1?user_id=user_1`, {
      method: "DELETE"
    });
    assert.equal(response.status, 409);
    const payload = await response.json();
    assert.equal(payload.status, "running");
  });
});

test("POST /api/simulations/:id/cancel marks active runs failed", async () => {
  const updates = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "running"
      }),
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        updates.push({ simulationRunId, patch });
        return {
          simulation_run_id: simulationRunId,
          status: patch.status,
          error_message: patch.errorMessage
        };
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_running_1/cancel?user_id=user_1`,
      { method: "POST" }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.status, "failed");
    assert.equal(payload.run.error_message, "Canceled by user");
  });

  assert.equal(updates.length, 1);
  assert.equal(updates[0].simulationRunId, "sim_running_1");
  assert.equal(updates[0].patch.status, "failed");
  assert.ok(updates[0].patch.completedAt);
});

test("DELETE /api/simulations/:id force removes active runs", async () => {
  const deleted = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "running"
      }),
      deleteSimulationRun: async (simulationRunId, { userId }) => {
        deleted.push({ simulationRunId, userId });
        return {
          simulation_run_id: simulationRunId,
          user_id: userId,
          status: "running"
        };
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_running_1?user_id=user_1&force=true`,
      { method: "DELETE" }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.simulation_run_id, "sim_running_1");
  });

  assert.deepEqual(deleted, [
    {
      simulationRunId: "sim_running_1",
      userId: "user_1"
    }
  ]);
});

test("GET /api/simulations/:id/profiles returns 409 for incomplete runs", async () => {
  const app = createApp({
    appStore: null,
    getSimulationRun: async (simulationRunId) => ({
      simulation_run_id: simulationRunId,
      status: "running"
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations/sim_live/profiles`);
    assert.equal(response.status, 409);
    const payload = await response.json();
    assert.equal(payload.status, "running");
  });
});

test("GET /api/simulations/:id/profiles keeps running runs incomplete when only progress summaries exist", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        status: "running",
        hand_count: 500,
        published_actions: 200,
        published_hand_summaries: 50,
        player_summaries: [
          {
            player_id: "hero_player",
            total_bb_won: 3.5,
            bb_per_100: 7.0,
            hands_played: 50
          }
        ]
      }),
      updateSimulationRunLifecycle: async () => {
        throw new Error("running progress should not be promoted to finalizing");
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations/sim_live_progress/profiles`);
    assert.equal(response.status, 409);
    const payload = await response.json();
    assert.equal(payload.status, "running");
  });
});

test("GET /api/simulations/:id/profiles returns cached profile payloads without creating jobs", async () => {
  const runPythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_hero_1",
        hero_agent_version_id: "agent_ver_hero_1",
        status: "completed"
      }),
      createProfileJob: async () => {
        throw new Error("GET /profiles should not create profile jobs");
      }
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_2",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_done_2",
              finished_at: "2026-04-20T20:10:04Z",
              completed_at: "2026-04-20T20:10:04Z"
            }
          })
        };
      }
      if (moduleName === "dashboard.api_bridge" && args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_2",
            ready: true,
            has_actions: true,
            has_players: true,
            run_summary: {
              simulation_run_id: "sim_done_2",
              finished_at: "2026-04-20T20:10:04Z",
              completed_at: "2026-04-20T20:10:04Z"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "ready",
          cached: true,
          model_run_id: "model_ready",
          feature_version: "v1",
          simulation_run_id: "sim_done_2",
          results: [
            {
              status: "ready",
              player_id: "hero_player",
              agent_id: "hero_agent",
              cluster_id: 1,
              cluster_label: "LAG",
              confidence_score: 0.87,
              hands_observed: 120,
              summary_text: "Aggressive profile",
              feature_vector: { vpip: 0.5 },
              top_feature_deltas: [],
              prototype_matches: [],
              model_run_id: "model_ready",
              simulation_run_id: "sim_done_2"
            }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_done_2/profiles?user_id=user_1&player_id=hero_player`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "ready");
    assert.equal(payload.cached, true);
    assert.equal(payload.results[0].player_id, "hero_player");
    assert.equal(payload.results[0].cluster_label, "LAG");
  });

  assert.equal(runPythonCalls.length, 3);
  assert.equal(runPythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.equal(runPythonCalls[0].args[0], "simulation-results-status");
  assert.equal(runPythonCalls[1].moduleName, "dashboard.api_bridge");
  assert.equal(runPythonCalls[1].args[0], "profile-source-status");
  assert.equal(runPythonCalls[2].moduleName, "profiles.score_session");
  assert.ok(runPythonCalls[2].args.includes("--all-subjects"));
  assert.ok(runPythonCalls[2].args.includes("--cached-only"));
  assert.ok(runPythonCalls[2].args.includes("--player-id"));
  assert.ok(!runPythonCalls[2].args.includes("--refresh"));
});

test("POST /api/simulations/:id/profiles/request requests session profiles and persists jobs", async () => {
  const runPythonCalls = [];
  const createdProfileJobs = [];
  const updatedProfileJobs = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_hero_1",
        hero_agent_version_id: "agent_ver_hero_1",
        status: "completed"
      }),
      createProfileJob: async (payload) => {
        createdProfileJobs.push(payload);
        return {
          profile_job_id: "profile_job_1",
          simulation_run_id: payload.simulationRunId,
          user_id: payload.userId,
          agent_definition_id: payload.agentDefinitionId,
          agent_version_id: payload.agentVersionId,
          player_id: payload.playerId,
          agent_id: payload.agentId,
          status: payload.status,
          refresh_requested: payload.refreshRequested,
          requested_at: "2026-04-20T20:10:00Z",
          completed_at: null,
          error_message: null
        };
      },
      updateProfileJob: async (profileJobId, payload) => {
        updatedProfileJobs.push({ profileJobId, payload });
        return {
          profile_job_id: profileJobId,
          simulation_run_id: "sim_done_2",
          user_id: "user_1",
          agent_definition_id: "agent_def_hero_1",
          agent_version_id: "agent_ver_hero_1",
          player_id: "hero_player",
          agent_id: null,
          status: payload.status,
          refresh_requested: true,
          requested_at: "2026-04-20T20:10:00Z",
          completed_at: payload.completedAt,
          error_message: payload.errorMessage
        };
      }
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_2",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_done_2",
              finished_at: "2026-04-20T20:10:04Z",
              completed_at: "2026-04-20T20:10:04Z"
            }
          })
        };
      }
      if (moduleName === "dashboard.api_bridge" && args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_2",
            ready: true,
            has_actions: true,
            has_players: true,
            run_summary: {
              simulation_run_id: "sim_done_2",
              finished_at: "2026-04-20T20:10:04Z",
              completed_at: "2026-04-20T20:10:04Z"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "ready",
          cached: false,
          model_run_id: "model_ready",
          feature_version: "v1",
          simulation_run_id: "sim_done_2",
          results: [
            {
              status: "ready",
              player_id: "hero_player",
              agent_id: "hero_agent",
              cluster_id: 1,
              cluster_label: "LAG",
              confidence_score: 0.87,
              hands_observed: 120,
              summary_text: "Aggressive profile",
              feature_vector: { vpip: 0.5 },
              top_feature_deltas: [],
              prototype_matches: [],
              model_run_id: "model_ready",
              simulation_run_id: "sim_done_2"
            }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_done_2/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          player_id: "hero_player",
          refresh: true
        })
      }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "ready");
    assert.equal(payload.results[0].player_id, "hero_player");
    assert.equal(payload.results[0].cluster_label, "LAG");
    assert.equal(payload.profile_job.profile_job_id, "profile_job_1");
    assert.equal(payload.profile_job.status, "completed");
  });

  assert.equal(createdProfileJobs.length, 1);
  assert.equal(createdProfileJobs[0].simulationRunId, "sim_done_2");
  assert.equal(createdProfileJobs[0].userId, "user_1");
  assert.equal(createdProfileJobs[0].agentDefinitionId, "agent_def_hero_1");
  assert.equal(createdProfileJobs[0].agentVersionId, "agent_ver_hero_1");
  assert.equal(createdProfileJobs[0].playerId, "hero_player");
  assert.equal(createdProfileJobs[0].refreshRequested, true);
  assert.equal(updatedProfileJobs.length, 1);
  assert.equal(updatedProfileJobs[0].profileJobId, "profile_job_1");
  assert.equal(updatedProfileJobs[0].payload.status, "completed");
  assert.equal(updatedProfileJobs[0].payload.errorMessage, null);
  assert.equal(runPythonCalls.length, 3);
  assert.equal(runPythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.equal(runPythonCalls[0].args[0], "simulation-results-status");
  assert.equal(runPythonCalls[1].moduleName, "dashboard.api_bridge");
  assert.equal(runPythonCalls[1].args[0], "profile-source-status");
  assert.equal(runPythonCalls[2].moduleName, "profiles.score_session");
  assert.ok(runPythonCalls[2].args.includes("--all-subjects"));
  assert.ok(!runPythonCalls[2].args.includes("--cached-only"));
  assert.ok(runPythonCalls[2].args.includes("--player-id"));
  assert.ok(runPythonCalls[2].args.includes("--refresh"));
});

test("POST /api/simulations/:id/profiles/request refreshes stale cached completed-run profiles", async () => {
  const runPythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_stale_profile",
        hero_agent_version_id: "agent_ver_stale_profile",
        status: "completed",
        hand_count: 5000,
        published_hand_summaries: 5000
      }),
      createProfileJob: async () => ({
        profile_job_id: "profile_job_stale",
        status: "pending"
      }),
      updateProfileJob: async (_id, payload) => ({
        profile_job_id: "profile_job_stale",
        ...payload
      })
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_stale_profile",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_done_stale_profile",
              hand_count: 5000,
              published_hand_summaries: 5000
            }
          })
        };
      }
      if (moduleName === "dashboard.api_bridge" && args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_stale_profile",
            ready: true,
            has_actions: true,
            has_players: true,
            run_summary: {
              simulation_run_id: "sim_done_stale_profile",
              hand_count: 5000,
              published_hand_summaries: 5000
            }
          })
        };
      }
      if (moduleName === "profiles.score_session") {
        const refreshed = args.includes("--refresh");
        return {
          stdout: JSON.stringify({
            status: "ready",
            cached: !refreshed,
            simulation_run_id: "sim_done_stale_profile",
            results: [
              {
                simulation_run_id: "sim_done_stale_profile",
                subject_id: "player:user_agent",
                player_id: "user_agent",
                agent_id: "user_agent",
                cluster_label: refreshed ? "Polarized caller" : "Loose caller",
                status: "ready",
                hands_observed: refreshed ? 5000 : 54
              }
            ]
          })
        };
      }
      throw new Error(`Unexpected module call: ${moduleName} ${args[0]}`);
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_done_stale_profile/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.cached, false);
    assert.equal(payload.results[0].cluster_label, "Polarized caller");
    assert.equal(payload.results[0].hands_observed, 5000);
  });

  const scoreCalls = runPythonCalls.filter((call) => call.moduleName === "profiles.score_session");
  assert.equal(scoreCalls.length, 2);
  assert.ok(!scoreCalls[0].args.includes("--refresh"));
  assert.ok(scoreCalls[1].args.includes("--refresh"));
});

test("POST /api/simulations/:id/profiles/request refreshes cached profiles scored before this run", async () => {
  const runPythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_reused_cache",
        hero_agent_version_id: "agent_ver_reused_cache",
        status: "completed",
        requested_at: "2026-04-22T20:10:00Z",
        started_at: "2026-04-22T20:10:02Z",
        completed_at: "2026-04-22T20:12:00Z",
        decision_backend: "heuristic",
        hero_context_hash: "fresh_context_hash",
        hand_count: 5000,
        published_hand_summaries: 5000
      }),
      createProfileJob: async () => ({
        profile_job_id: "profile_job_reused_cache",
        status: "pending"
      }),
      updateProfileJob: async (_id, payload) => ({
        profile_job_id: "profile_job_reused_cache",
        ...payload
      })
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_reused_cache",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_reused_cache",
              user_id: "user_1",
              decision_backend: "heuristic",
              hero_context_hash: "fresh_context_hash",
              requested_at: "2026-04-22T20:10:00Z",
              finished_at: "2026-04-22T20:12:00Z",
              hand_count: 5000,
              published_hand_summaries: 5000
            }
          })
        };
      }
      if (moduleName === "dashboard.api_bridge" && args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_reused_cache",
            ready: true,
            has_actions: true,
            has_players: true,
            run_summary: {
              simulation_run_id: "sim_reused_cache",
              user_id: "user_1",
              decision_backend: "heuristic",
              hero_context_hash: "fresh_context_hash",
              requested_at: "2026-04-22T20:10:00Z",
              finished_at: "2026-04-22T20:12:00Z",
              hand_count: 5000,
              published_hand_summaries: 5000
            }
          })
        };
      }
      if (moduleName === "profiles.score_session") {
        const refreshed = args.includes("--refresh");
        return {
          stdout: JSON.stringify({
            status: "ready",
            cached: !refreshed,
            simulation_run_id: "sim_reused_cache",
            results: [
              {
                simulation_run_id: "sim_reused_cache",
                subject_id: "player:user_agent",
                player_id: "user_agent",
                agent_id: "user_agent",
                cluster_label: refreshed ? "Fresh profile" : "Old profile",
                status: "ready",
                hands_observed: 5000,
                scored_at: refreshed
                  ? "2026-04-22T20:13:00Z"
                  : "2026-04-20T20:13:00Z"
              }
            ]
          })
        };
      }
      throw new Error(`Unexpected module call: ${moduleName} ${args[0]}`);
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_reused_cache/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.cached, false);
    assert.equal(payload.results[0].cluster_label, "Fresh profile");
  });

  const scoreCalls = runPythonCalls.filter((call) => call.moduleName === "profiles.score_session");
  assert.equal(scoreCalls.length, 2);
  assert.ok(scoreCalls[0].args.includes("--min-event-ts"));
  assert.ok(scoreCalls[0].args.includes("2026-04-22T20:10:00.000Z"));
  assert.ok(scoreCalls[1].args.includes("--refresh"));
});

test("POST /api/simulations/:id/profiles/request promotes finalizing runs once durable results are ready", async () => {
  const runPythonCalls = [];
  const lifecycleUpdates = [];
  const updatedProfileJobs = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_hero_finalizing",
        hero_agent_version_id: "agent_ver_hero_finalizing",
        status: "finalizing",
        completed_at: "2026-04-22T20:09:59Z"
      }),
      updateSimulationRunLifecycle: async (simulationRunId, payload) => {
        lifecycleUpdates.push({ simulationRunId, payload });
        return {
          simulation_run_id: simulationRunId,
          user_id: "user_1",
          hero_agent_definition_id: "agent_def_hero_finalizing",
          hero_agent_version_id: "agent_ver_hero_finalizing",
          status: payload.status,
          completed_at: payload.completedAt
        };
      },
      createProfileJob: async (payload) => ({
        profile_job_id: "profile_job_finalizing_ready",
        simulation_run_id: payload.simulationRunId,
        user_id: payload.userId,
        agent_definition_id: payload.agentDefinitionId,
        agent_version_id: payload.agentVersionId,
        player_id: null,
        agent_id: null,
        status: payload.status,
        refresh_requested: false,
        requested_at: "2026-04-22T20:10:00Z",
        completed_at: null,
        error_message: null
      }),
      updateProfileJob: async (profileJobId, payload) => {
        updatedProfileJobs.push({ profileJobId, payload });
        return {
          profile_job_id: profileJobId,
          simulation_run_id: "sim_finalizing_profile_ready",
          user_id: "user_1",
          agent_definition_id: "agent_def_hero_finalizing",
          agent_version_id: "agent_ver_hero_finalizing",
          player_id: null,
          agent_id: null,
          status: payload.status,
          refresh_requested: false,
          requested_at: "2026-04-22T20:10:00Z",
          completed_at: payload.completedAt,
          error_message: payload.errorMessage
        };
      }
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge") {
        return {
          stdout: JSON.stringify({
            ready: true,
            run_summary: {
              simulation_run_id: "sim_finalizing_profile_ready",
              finished_at: "2026-04-22T20:10:04Z",
              completed_at: "2026-04-22T20:10:04Z"
            }
          })
        };
      }
      if (moduleName === "profiles.score_session") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            cached: false,
            model_run_id: "model_ready",
            feature_version: "v1",
            simulation_run_id: "sim_finalizing_profile_ready",
            results: [
              {
                status: "ready",
                player_id: "hero_player",
                agent_id: "hero_agent",
                cluster_id: 1,
                cluster_label: "LAG",
                confidence_score: 0.87,
                hands_observed: 120,
                summary_text: "Aggressive profile",
                feature_vector: { vpip: 0.5 },
                top_feature_deltas: [],
                prototype_matches: [],
                model_run_id: "model_ready",
                simulation_run_id: "sim_finalizing_profile_ready"
              }
            ]
          })
        };
      }
      throw new Error(`Unexpected module ${moduleName}`);
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_finalizing_profile_ready/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "ready");
    assert.equal(payload.results[0].cluster_label, "LAG");
    assert.equal(payload.profile_job.profile_job_id, "profile_job_finalizing_ready");
    assert.equal(payload.profile_job.status, "completed");
  });

  assert.equal(lifecycleUpdates.length, 1);
  assert.equal(lifecycleUpdates[0].simulationRunId, "sim_finalizing_profile_ready");
  assert.equal(lifecycleUpdates[0].payload.status, "completed");
  assert.equal(updatedProfileJobs.length, 1);
  assert.equal(updatedProfileJobs[0].profileJobId, "profile_job_finalizing_ready");
  assert.equal(updatedProfileJobs[0].payload.status, "completed");
  assert.equal(runPythonCalls.length, 3);
  assert.equal(runPythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.ok(runPythonCalls[0].args.includes("simulation-results-status"));
  assert.equal(runPythonCalls[1].moduleName, "dashboard.api_bridge");
  assert.ok(runPythonCalls[1].args.includes("profile-source-status"));
  assert.equal(runPythonCalls[2].moduleName, "profiles.score_session");
});

test("POST /api/simulations/:id/profiles/request promotes queued runs once durable results are ready", async () => {
  const lifecycleUpdates = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_hero_queued",
        hero_agent_version_id: "agent_ver_hero_queued",
        status: "queued",
        completed_at: null
      }),
      updateSimulationRunLifecycle: async (simulationRunId, payload) => {
        lifecycleUpdates.push({ simulationRunId, payload });
        return {
          simulation_run_id: simulationRunId,
          user_id: "user_1",
          hero_agent_definition_id: "agent_def_hero_queued",
          hero_agent_version_id: "agent_ver_hero_queued",
          status: payload.status,
          completed_at: payload.completedAt
        };
      },
      createProfileJob: async () => ({
        profile_job_id: "profile_job_queued_ready",
        simulation_run_id: "sim_queued_profile_ready",
        user_id: "user_1",
        agent_definition_id: "agent_def_hero_queued",
        agent_version_id: "agent_ver_hero_queued",
        player_id: null,
        agent_id: null,
        status: "running",
        refresh_requested: false,
        requested_at: "2026-04-22T20:10:00Z",
        completed_at: null,
        error_message: null
      }),
      updateProfileJob: async (profileJobId, payload) => ({
        profile_job_id: profileJobId,
        simulation_run_id: "sim_queued_profile_ready",
        user_id: "user_1",
        agent_definition_id: "agent_def_hero_queued",
        agent_version_id: "agent_ver_hero_queued",
        player_id: null,
        agent_id: null,
        status: payload.status,
        refresh_requested: false,
        requested_at: "2026-04-22T20:10:00Z",
        completed_at: payload.completedAt,
        error_message: payload.errorMessage
      })
    },
    runPythonModule: async (moduleName) => {
      if (moduleName === "dashboard.api_bridge") {
        return {
          stdout: JSON.stringify({
            ready: true,
            run_summary: {
              simulation_run_id: "sim_queued_profile_ready",
              finished_at: "2026-04-22T20:10:04Z",
              completed_at: "2026-04-22T20:10:04Z"
            }
          })
        };
      }
      if (moduleName === "profiles.score_session") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            cached: false,
            model_run_id: "model_ready",
            feature_version: "v1",
            simulation_run_id: "sim_queued_profile_ready",
            results: [
              {
                status: "ready",
                player_id: "hero_player",
                agent_id: "hero_agent",
                cluster_id: 1,
                cluster_label: "LAG",
                confidence_score: 0.87,
                hands_observed: 120,
                summary_text: "Aggressive profile",
                feature_vector: { vpip: 0.5 },
                top_feature_deltas: [],
                prototype_matches: [],
                model_run_id: "model_ready",
                simulation_run_id: "sim_queued_profile_ready"
              }
            ]
          })
        };
      }
      throw new Error(`Unexpected module ${moduleName}`);
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_queued_profile_ready/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "ready");
    assert.equal(payload.results[0].cluster_label, "LAG");
    assert.equal(payload.profile_job.profile_job_id, "profile_job_queued_ready");
    assert.equal(payload.profile_job.status, "completed");
  });

  assert.equal(lifecycleUpdates.length, 1);
  assert.equal(lifecycleUpdates[0].simulationRunId, "sim_queued_profile_ready");
  assert.equal(lifecycleUpdates[0].payload.status, "completed");
});

test("POST /api/simulations/:id/profiles/request records not_found profile jobs", async () => {
  const updatedProfileJobs = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_hero_2",
        hero_agent_version_id: "agent_ver_hero_2",
        status: "completed"
      }),
      createProfileJob: async (payload) => ({
        profile_job_id: "profile_job_nf",
        simulation_run_id: payload.simulationRunId,
        user_id: payload.userId,
        agent_definition_id: payload.agentDefinitionId,
        agent_version_id: payload.agentVersionId,
        player_id: null,
        agent_id: null,
        status: "running",
        refresh_requested: false,
        requested_at: "2026-04-20T20:11:00Z",
        completed_at: null,
        error_message: null
      }),
      updateProfileJob: async (profileJobId, payload) => {
        updatedProfileJobs.push({ profileJobId, payload });
        return {
          profile_job_id: profileJobId,
          simulation_run_id: "sim_done_missing_profiles",
          user_id: "user_1",
          agent_definition_id: "agent_def_hero_2",
          agent_version_id: "agent_ver_hero_2",
          player_id: null,
          agent_id: null,
          status: payload.status,
          refresh_requested: false,
          requested_at: "2026-04-20T20:11:00Z",
          completed_at: payload.completedAt,
          error_message: payload.errorMessage
        };
      }
    },
    runPythonModule: async (moduleName, args) => {
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_missing_profiles",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_done_missing_profiles",
              finished_at: "2026-04-20T20:11:04Z",
              completed_at: "2026-04-20T20:11:04Z"
            }
          })
        };
      }
      if (moduleName === "dashboard.api_bridge" && args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_missing_profiles",
            ready: true,
            has_actions: true,
            has_players: true,
            run_summary: {
              simulation_run_id: "sim_done_missing_profiles",
              finished_at: "2026-04-20T20:11:04Z",
              completed_at: "2026-04-20T20:11:04Z"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "not_found",
          message: "No completed profile results were found"
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_done_missing_profiles/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 404);
    const payload = await response.json();
    assert.equal(payload.message, "No completed profile results were found");
    assert.equal(payload.profile_job.profile_job_id, "profile_job_nf");
    assert.equal(payload.profile_job.status, "not_found");
  });

  assert.equal(updatedProfileJobs.length, 1);
  assert.equal(updatedProfileJobs[0].profileJobId, "profile_job_nf");
  assert.equal(updatedProfileJobs[0].payload.status, "not_found");
  assert.equal(
    updatedProfileJobs[0].payload.errorMessage,
    "No completed profile results were found"
  );
});

test("POST /api/simulations/:id/profiles/request blocks completed runs until durable results materialize", async () => {
  const runPythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_completed_materializing",
        hero_agent_version_id: "agent_ver_completed_materializing",
        status: "completed",
        completed_at: "2026-04-22T20:10:04Z"
      })
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "pending",
            source: "snowflake",
            simulation_run_id: "sim_completed_profile_materializing",
            ready: false,
            message: "Profile inputs are still being prepared",
            has_actions: false,
            has_players: false,
            run_summary: {
              simulation_run_id: "sim_completed_profile_materializing",
              status: "completed"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "pending",
          source: "snowflake",
          simulation_run_id: "sim_completed_profile_materializing",
          ready: false,
          message: "Final results are still being prepared",
          run_summary: {
            simulation_run_id: "sim_completed_profile_materializing",
            status: "completed"
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_completed_profile_materializing/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 409);
    const payload = await response.json();
    assert.equal(payload.message, "Final results are still being prepared");
    assert.equal(payload.status, "completed");
  });

  assert.equal(runPythonCalls.length, 2);
  assert.deepEqual(
    runPythonCalls.map((call) => [call.moduleName, call.args[0]]),
    [
      ["dashboard.api_bridge", "simulation-results-status"],
      ["dashboard.api_bridge", "profile-source-status"]
    ]
  );
});

test("POST /api/simulations/:id/profiles/request ignores profile sources from an older reused run id", async () => {
  const runPythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_reused_profile",
        hero_agent_version_id: "agent_ver_reused_profile",
        status: "completed",
        requested_at: "2026-04-22T20:10:00Z",
        started_at: "2026-04-22T20:10:02Z",
        completed_at: "2026-04-22T20:12:00Z",
        decision_backend: "heuristic",
        hero_context_hash: "fresh_context_hash"
      })
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_reused_profile",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_reused_profile",
              user_id: "user_1",
              decision_backend: "heuristic",
              hero_context_hash: "fresh_context_hash",
              requested_at: "2026-04-20T20:10:00Z",
              finished_at: "2026-04-20T20:12:00Z"
            }
          })
        };
      }
      if (moduleName === "dashboard.api_bridge" && args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_reused_profile",
            ready: true,
            has_actions: true,
            has_players: true,
            run_summary: {
              simulation_run_id: "sim_reused_profile",
              user_id: "user_1",
              decision_backend: "heuristic",
              hero_context_hash: "fresh_context_hash",
              requested_at: "2026-04-20T20:10:00Z",
              finished_at: "2026-04-20T20:12:00Z"
            }
          })
        };
      }
      throw new Error(`Unexpected module call: ${moduleName} ${args[0]}`);
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_reused_profile/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 409);
    const payload = await response.json();
    assert.equal(payload.message, "Final results are still being prepared");
    assert.equal(payload.status, "completed");
  });

  assert.deepEqual(
    runPythonCalls.map((call) => [call.moduleName, call.args[0]]),
    [
      ["dashboard.api_bridge", "simulation-results-status"],
      ["dashboard.api_bridge", "profile-source-status"]
    ]
  );
});

test("POST /api/simulations/:id/profiles/request allows completed runs when profile source rows are ready", async () => {
  const runPythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_profile_ready",
        hero_agent_version_id: "agent_ver_profile_ready",
        status: "completed",
        completed_at: "2026-04-22T20:10:04Z"
      }),
      createProfileJob: async () => ({
        profile_job_id: "profile_job_ready",
        status: "pending"
      }),
      updateProfileJob: async (_id, payload) => ({
        profile_job_id: "profile_job_ready",
        ...payload
      })
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "pending",
            source: "snowflake",
            simulation_run_id: "sim_completed_profile_ready",
            ready: false,
            message: "Final results are still being prepared",
            run_summary: {
              simulation_run_id: "sim_completed_profile_ready",
              status: "completed"
            }
          })
        };
      }
      if (moduleName === "dashboard.api_bridge" && args[0] === "profile-source-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_completed_profile_ready",
            ready: true,
            has_actions: true,
            has_players: true,
            run_summary: {
              simulation_run_id: "sim_completed_profile_ready",
              status: "completed"
            }
          })
        };
      }
      if (moduleName === "profiles.score_session") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            cached: false,
            simulation_run_id: "sim_completed_profile_ready",
            results: [
              {
                simulation_run_id: "sim_completed_profile_ready",
                subject_id: "player:user_agent",
                player_id: "user_agent",
                agent_id: "user_agent",
                cluster_label: "Balanced regular",
                status: "ready"
              }
            ]
          })
        };
      }
      throw new Error(`Unexpected module call: ${moduleName} ${args[0]}`);
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_completed_profile_ready/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "ready");
    assert.equal(payload.results.length, 1);
    assert.equal(payload.results[0].cluster_label, "Balanced regular");
  });

  assert.deepEqual(
    runPythonCalls.map((call) => [call.moduleName, call.args[0]]),
    [
      ["dashboard.api_bridge", "simulation-results-status"],
      ["dashboard.api_bridge", "profile-source-status"],
      ["profiles.score_session", "--simulation-run-id"]
    ]
  );
});

test("POST /api/simulations/:id/profiles/request uses provisional profile features immediately", async () => {
  const runPythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        hero_agent_definition_id: "agent_def_provisional",
        hero_agent_version_id: "agent_ver_provisional",
        status: "completed",
        completed_at: "2026-04-22T20:10:04Z",
        profile_session_features: [
          {
            source_type: "simulated",
            source_dataset: "persona_simulation",
            source_run_id: "sim_completed_provisional_profile",
            simulation_run_id: "sim_completed_provisional_profile",
            subject_id: "user_agent",
            player_id: "user_agent",
            agent_id: "user_agent",
            hands_observed: 120,
            aggressive_actions: 40,
            passive_actions: 20,
            flop_cbet_opportunities: 10,
            turn_barrel_opportunities: 5,
            late_position_hands: 20,
            blind_position_hands: 20,
            short_stack_hands: 0,
            deep_stack_hands: 120,
            vpip_rate: 0.3,
            pfr_rate: 0.2,
            aggression_frequency: 0.66,
            call_preference: 0.34,
            flop_cbet_rate: 0.5,
            turn_barrel_rate: 0.4,
            river_seen_rate: 0.2,
            all_in_rate: 0.02,
            avg_preflop_raise_bb: 3.2,
            avg_postflop_bet_pot_ratio: 0.6,
            late_position_vpip_rate: 0.35,
            blind_defend_rate: 0.2,
            short_stack_aggression_rate: 0.0,
            deep_stack_looseness: 0.3
          }
        ]
      }),
      createProfileJob: async () => ({
        profile_job_id: "profile_job_provisional",
        status: "pending"
      }),
      updateProfileJob: async (_id, payload) => ({
        profile_job_id: "profile_job_provisional",
        ...payload
      })
    },
    runPythonModule: async (moduleName, args) => {
      runPythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge") {
        return {
          stdout: JSON.stringify({
            status: "pending",
            source: "snowflake",
            ready: false,
            simulation_run_id: "sim_completed_provisional_profile",
            run_summary: {
              simulation_run_id: "sim_completed_provisional_profile"
            },
            has_actions: false,
            has_players: false
          })
        };
      }
      if (moduleName === "profiles.score_session") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            cached: false,
            simulation_run_id: "sim_completed_provisional_profile",
            results: [
              {
                simulation_run_id: "sim_completed_provisional_profile",
                subject_id: "user_agent",
                player_id: "user_agent",
                agent_id: "user_agent",
                cluster_label: "Balanced regular",
                status: "ready"
              }
            ]
          })
        };
      }
      throw new Error(`Unexpected module call: ${moduleName} ${args[0]}`);
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_completed_provisional_profile/profiles/request?user_id=user_1`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({})
      }
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "ready");
    assert.equal(payload.results[0].cluster_label, "Balanced regular");
  });

  assert.equal(runPythonCalls.length, 3);
  assert.deepEqual(
    runPythonCalls.slice(0, 2).map((call) => [call.moduleName, call.args[0]]),
    [
      ["dashboard.api_bridge", "simulation-results-status"],
      ["dashboard.api_bridge", "profile-source-status"]
    ]
  );
  assert.equal(runPythonCalls[2].moduleName, "profiles.score_session");
  assert.ok(
    runPythonCalls[2].args.includes("--session-features-json")
  );
});

test("GET /api/simulations/:id/profile-jobs returns persisted profile job history", async () => {
  const listedProfileJobs = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "completed"
      }),
      listProfileJobs: async (filters) => {
        listedProfileJobs.push(filters);
        return [
          {
            profile_job_id: "profile_job_hist_1",
            simulation_run_id: filters.simulationRunId,
            user_id: filters.userId,
            status: "completed",
            refresh_requested: true,
            requested_at: "2026-04-20T20:12:00Z",
            completed_at: "2026-04-20T20:12:04Z",
            error_message: null
          }
        ];
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_done_2/profile-jobs?user_id=user_1&limit=5`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.simulation_run_id, "sim_done_2");
    assert.equal(payload.profile_jobs.length, 1);
    assert.equal(payload.profile_jobs[0].profile_job_id, "profile_job_hist_1");
  });

  assert.equal(listedProfileJobs.length, 1);
  assert.equal(listedProfileJobs[0].simulationRunId, "sim_done_2");
  assert.equal(listedProfileJobs[0].userId, "user_1");
  assert.equal(listedProfileJobs[0].limit, 5);
});

test("POST /api/agents creates an agent with an initial version", async () => {
  const ensuredUsers = [];
  const createdAgents = [];
  const app = createApp({
    appStore: {
      ensureUser: async (user) => {
        ensuredUsers.push(user);
        return user;
      },
      createAgent: async (payload) => {
        createdAgents.push(payload);
        return {
          agent: {
            agent_definition_id: "agent_def_1",
            name: payload.name,
            latest_version: {
              agent_version_id: "agent_ver_1",
              version_number: 1
            }
          },
          version: {
            agent_version_id: "agent_ver_1",
            version_number: 1,
            context_text: payload.contextText
          }
        };
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/agents`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        user_id: "user_1",
        user_email: "user_1@example.com",
        name: "River Captain",
        context: "balanced aggressive value bettor"
      })
    });

    assert.equal(response.status, 201);
    const payload = await response.json();
    assert.equal(payload.agent.agent_definition_id, "agent_def_1");
    assert.equal(payload.version.agent_version_id, "agent_ver_1");
  });

  assert.equal(ensuredUsers.length, 1);
  assert.equal(ensuredUsers[0].userId, "user_1");
  assert.equal(createdAgents.length, 1);
  assert.equal(createdAgents[0].userId, "user_1");
  assert.equal(createdAgents[0].name, "River Captain");
});

test("GET /api/agents lists user agents from the app store", async () => {
  const app = createApp({
    appStore: {
      listAgents: async ({ userId }) => [
        {
          agent_definition_id: "agent_def_1",
          user_id: userId,
          name: "River Captain"
        }
      ]
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/agents?user_id=user_1`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.agents.length, 1);
    assert.equal(payload.agents[0].user_id, "user_1");
  });
});

test("GET /api/agents/:id returns agent detail with version history", async () => {
  const app = createApp({
    appStore: {
      getAgent: async (agentDefinitionId, { userId }) => ({
        agent_definition_id: agentDefinitionId,
        user_id: userId,
        name: "River Captain",
        description: "Pressure capped ranges",
        latest_version: {
          agent_version_id: "agent_ver_2",
          version_number: 2,
          context_hash: "ctx_hash_2"
        },
        versions: [
          {
            agent_version_id: "agent_ver_2",
            version_number: 2,
            context_text: "Updated river pressure profile",
            context_hash: "ctx_hash_2",
            decision_backend_default: "llm"
          },
          {
            agent_version_id: "agent_ver_1",
            version_number: 1,
            context_text: "Original balanced profile",
            context_hash: "ctx_hash_1",
            decision_backend_default: "heuristic"
          }
        ]
      })
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/agents/agent_def_1?user_id=user_1`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.agent.agent_definition_id, "agent_def_1");
    assert.equal(payload.agent.versions.length, 2);
    assert.equal(payload.agent.versions[0].agent_version_id, "agent_ver_2");
  });
});

test("DELETE /api/agents/:id removes an owned agent", async () => {
  const deleted = [];
  const app = createApp({
    appStore: {
      getAgent: async (agentDefinitionId, { userId }) => ({
        agent_definition_id: agentDefinitionId,
        user_id: userId,
        name: "River Captain"
      }),
      deleteAgent: async (agentDefinitionId, { userId }) => {
        deleted.push({ agentDefinitionId, userId });
        return {
          agent_definition_id: agentDefinitionId,
          user_id: userId,
          name: "River Captain"
        };
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/agents/agent_def_1?user_id=user_1`, {
      method: "DELETE"
    });
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.agent_definition_id, "agent_def_1");
  });

  assert.deepEqual(deleted, [
    {
      agentDefinitionId: "agent_def_1",
      userId: "user_1"
    }
  ]);
});

test("POST /api/agents/:id/versions creates a new agent version", async () => {
  const app = createApp({
    appStore: {
      ensureUser: async (user) => user,
      createAgentVersion: async ({ agentDefinitionId, contextText }) => ({
        agent_version_id: "agent_ver_2",
        agent_definition_id: agentDefinitionId,
        version_number: 2,
        context_text: contextText
      })
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/agents/agent_def_1/versions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        user_id: "user_1",
        context: "tight exploitative closer"
      })
    });

    assert.equal(response.status, 201);
    const payload = await response.json();
    assert.equal(payload.version.agent_definition_id, "agent_def_1");
    assert.equal(payload.version.version_number, 2);
  });
});

test("POST /api/simulations can launch from a saved agent version", async () => {
  const requestMessages = [];
  const lifecycleEvents = [];
  const persistedRuns = [];
  const app = createApp({
    appStore: {
      getAgentVersion: async (agentVersionId) => ({
        agent_version_id: agentVersionId,
        agent_definition_id: "agent_def_saved",
        context_text: "profiled postflop pressure",
        decision_backend_default: "heuristic"
      }),
      createSimulationRun: async (payload) => {
        persistedRuns.push(payload);
        return payload;
      }
    },
    queueSimulationRun: async (payload) => payload,
    publishSimulationRequests: async (payloads) => {
      requestMessages.push(...payloads);
      return payloads.length;
    },
    publishSimulationEvents: async (payloads) => {
      lifecycleEvents.push(...payloads);
      return payloads.length;
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        simulation_run_id: "sim_saved_agent_1",
        agent_version_id: "agent_ver_saved"
      })
    });

    assert.equal(response.status, 202);
    const payload = await response.json();
    assert.equal(payload.hero_agent_version_id, "agent_ver_saved");
  });

  assert.equal(persistedRuns.length, 1);
  assert.equal(persistedRuns[0].requestMode, "saved_agent");
  assert.equal(persistedRuns[0].heroAgentDefinitionId, "agent_def_saved");
  assert.equal(requestMessages.length, 1);
  assert.equal(requestMessages[0].context, "profiled postflop pressure");
  assert.equal(requestMessages[0].decision_backend, "heuristic");
  assert.equal(lifecycleEvents[0].hero_agent_version_id, "agent_ver_saved");
});

test("POST /api/simulations keeps app-owned queueing authoritative when legacy sync fails", async () => {
  const persistedRuns = [];
  const requestMessages = [];
  const lifecycleEvents = [];
  const legacyFailureMarks = [];
  const app = createApp({
    appStore: {
      ensureUser: async (user) => user,
      createSimulationRun: async (payload) => {
        persistedRuns.push(payload);
        return payload;
      }
    },
    queueSimulationRun: async () => {
      throw new Error("legacy registry unavailable");
    },
    markSimulationRunFailed: async (...args) => {
      legacyFailureMarks.push(args);
      return null;
    },
    publishSimulationRequests: async (payloads) => {
      requestMessages.push(...payloads);
      return payloads.length;
    },
    publishSimulationEvents: async (payloads) => {
      lifecycleEvents.push(...payloads);
      return payloads.length;
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        simulation_run_id: "sim_app_owned_compat_1",
        user_id: "user_app_owned_1",
        context: "patient exploitative closer",
        decision_backend: "heuristic"
      })
    });

    assert.equal(response.status, 202);
    const payload = await response.json();
    assert.equal(payload.simulation_run_id, "sim_app_owned_compat_1");
    assert.equal(payload.status, "queued");
  });

  assert.equal(persistedRuns.length, 1);
  assert.equal(persistedRuns[0].simulationRunId, "sim_app_owned_compat_1");
  assert.equal(persistedRuns[0].userId, "user_app_owned_1");
  assert.equal(requestMessages.length, 1);
  assert.equal(requestMessages[0].decision_backend, "heuristic");
  assert.equal(lifecycleEvents.length, 1);
  assert.equal(lifecycleEvents[0].event_type, "simulation_requested");
  assert.equal(legacyFailureMarks.length, 0);
});

test("GET /api/simulations lists app-backed runs without overriding status from the legacy registry", async () => {
  const app = createApp({
    appStore: {
      listSimulationRuns: async () => [
        {
          simulation_run_id: "sim_list_1",
          status: "queued",
          request_mode: "context",
          decision_backend: "llm",
          requested_at: "2026-04-20T20:00:00Z"
        }
      ]
    },
    getSimulationRun: async () => ({
      simulation_run_id: "sim_list_1",
      status: "completed",
      player_summaries: [
        {
          player_id: "hero_player",
          total_bb_won: 18.5
        }
      ]
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.runs.length, 1);
    assert.equal(payload.runs[0].status, "queued");
    assert.equal(payload.runs[0].request_mode, "context");
    assert.equal(payload.runs[0].player_summaries, undefined);
  });
});

test("GET /api/simulations/:id prefers app-backed metadata when the app database is configured", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "queued",
        request_mode: "saved_agent",
        decision_backend: "llm"
      })
    },
    getSimulationRun: async () => ({
      simulation_run_id: "sim_owned_1",
      status: "completed",
      player_summaries: [
        {
          player_id: "hero_player",
          total_bb_won: 18.5
        }
      ]
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_owned_1?user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "queued");
    assert.equal(payload.request_mode, "saved_agent");
    assert.equal(payload.player_summaries, undefined);
  });
});

test("GET /api/simulations/:id/live proxies live dashboard payloads", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "running"
      })
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          available: true,
          source: "redis",
          selected_run_id: "sim_live_1",
          run_profit_leaderboard: [
            {
              player_id: "hero_player",
              total_profit_bb: 12.5
            }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_live_1/live?user_id=user_1&decision_backend=llm`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.available, true);
    assert.equal(payload.run_profit_leaderboard[0].player_id, "hero_player");
  });

  assert.equal(pythonCalls.length, 2);
  assert.equal(pythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.ok(pythonCalls[0].args.includes("--simulation-run-id"));
  assert.ok(pythonCalls[0].args.includes("--user-id"));
  assert.ok(pythonCalls[0].args.includes("--decision-backend"));
  assert.ok(pythonCalls[1].args.includes("--simulation-run-id"));
  assert.ok(pythonCalls[1].args.includes("sim_live_1"));
});

test("GET /api/simulations/:id/live ignores Redis rows from an older reused run id", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "running",
        requested_at: "2026-04-21T04:00:00.000Z",
        started_at: "2026-04-21T04:00:02.000Z",
        decision_backend: "heuristic",
        hero_context_hash: "ctx_current",
        hand_count: 300,
        published_actions: 10,
        published_hand_summaries: 2,
        player_summaries: [
          {
            simulation_run_id: simulationRunId,
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            total_bb_won: 3,
            bb_per_100: 150,
            observed_hands: 2,
            hands_played: 2
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: simulationRunId,
            hand_number: 1,
            cumulative_bb_won: 1
          },
          {
            simulation_run_id: simulationRunId,
            hand_number: 2,
            cumulative_bb_won: 3
          }
        ]
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_reused_live_endpoint"],
        active_runs: [
          {
            simulation_run_id: "sim_reused_live_endpoint",
            status: "finalizing",
            user_id: "user_1",
            decision_backend: "heuristic",
            hero_context_hash: "ctx_current",
            requested_at: "2026-04-21T03:00:00.000Z",
            started_at: "2026-04-21T03:00:02.000Z",
            finished_at: "2026-04-21T03:30:00.000Z",
            published_hand_summaries: 250
          }
        ],
        selected_run_id: "sim_reused_live_endpoint",
        selected_run_meta: {
          simulation_run_id: "sim_reused_live_endpoint",
          status: "finalizing",
          user_id: "user_1",
          decision_backend: "heuristic",
          hero_context_hash: "ctx_current",
          requested_at: "2026-04-21T03:00:00.000Z",
          started_at: "2026-04-21T03:00:02.000Z",
          finished_at: "2026-04-21T03:30:00.000Z",
          published_hand_summaries: 250
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_reused_live_endpoint",
            player_id: "old_player",
            total_bb_won: 999,
            observed_hands: 250
          }
        ],
        run_agents: [
          {
            simulation_run_id: "sim_reused_live_endpoint",
            player_id: "old_player",
            agent_id: "old_agent",
            total_bb_won: 999,
            observed_hands: 250,
            hands_played: 250
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: "sim_reused_live_endpoint",
            hand_number: 250,
            cumulative_bb_won: 999
          }
        ]
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_reused_live_endpoint/live?user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_meta.status, "running");
    assert.equal(payload.selected_run_meta.published_hand_summaries, 2);
    assert.deepEqual(
      payload.hero_profit_timeseries.map((row) => row.hand_number),
      [1, 2]
    );
    assert.deepEqual(
      payload.run_agents.map((row) => row.player_id),
      ["user_agent"]
    );
    assert.deepEqual(
      payload.run_profit_leaderboard.map((row) => row.player_id),
      ["user_agent"]
    );
  });
});

test("GET /api/dashboard/live filters out stale Redis runs that are terminal in the app store", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "completed",
        completed_at: "2026-04-21T03:47:51.625Z",
        updated_at: "2026-04-21T03:47:51.634Z",
        decision_backend: "heuristic"
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_stale_live_1"],
        active_runs: [
          {
            simulation_run_id: "sim_stale_live_1",
            status: "queued",
            user_id: "user_1",
            decision_backend: "heuristic"
          }
        ],
        selected_run_id: "sim_stale_live_1",
        selected_run_meta: {
          simulation_run_id: "sim_stale_live_1",
          status: "queued",
          user_id: "user_1",
          decision_backend: "heuristic"
        }
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live?user_id=user_1`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.available, false);
    assert.deepEqual(payload.active_run_ids, []);
    assert.deepEqual(payload.active_runs, []);
    assert.equal(payload.selected_run_id, null);
    assert.equal(payload.selected_run_meta, null);
  });
});

test("GET /api/dashboard/live filters out stale Redis runs even without a user filter", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        status: "completed",
        completed_at: "2026-04-21T03:47:51.625Z",
        updated_at: "2026-04-21T03:47:51.634Z",
        decision_backend: "heuristic"
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_stale_live_2"],
        active_runs: [
          {
            simulation_run_id: "sim_stale_live_2",
            status: "queued",
            decision_backend: "heuristic"
          }
        ],
        selected_run_id: "sim_stale_live_2",
        selected_run_meta: {
          simulation_run_id: "sim_stale_live_2",
          status: "queued",
          decision_backend: "heuristic"
        }
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.available, false);
    assert.deepEqual(payload.active_run_ids, []);
    assert.deepEqual(payload.active_runs, []);
    assert.equal(payload.selected_run_id, null);
    assert.equal(payload.selected_run_meta, null);
  });
});

test("GET /api/dashboard/live filters out orphaned Redis runs that no longer exist in the app store", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async () => null
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_missing_live_1"],
        active_runs: [
          {
            simulation_run_id: "sim_missing_live_1",
            status: "running",
            decision_backend: "llm"
          }
        ],
        selected_run_id: "sim_missing_live_1",
        selected_run_meta: {
          simulation_run_id: "sim_missing_live_1",
          status: "running",
          decision_backend: "llm"
        },
        hero_profit_timeseries: [
          {
            hand_number: 1,
            cumulative_bb_won: 2.5
          }
        ]
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.available, false);
    assert.deepEqual(payload.active_run_ids, []);
    assert.deepEqual(payload.active_runs, []);
    assert.equal(payload.selected_run_id, null);
    assert.equal(payload.selected_run_meta, null);
    assert.deepEqual(payload.hero_profit_timeseries, []);
  });
});

test("GET /api/dashboard/live keeps only the selected live run rows and active global rows", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => {
        if (simulationRunId === "sim_live_1") {
          return {
            simulation_run_id: "sim_live_1",
            user_id: "user_live",
            status: "running",
            requested_at: "2026-04-21T03:47:51.625Z",
            started_at: "2026-04-21T03:48:00.000Z",
            updated_at: "2026-04-21T03:48:10.000Z",
            decision_backend: "llm",
            hero_context_hash: "ctx_live"
          };
        }
        return {
          simulation_run_id: simulationRunId,
          user_id: "user_old",
          status: "completed",
          completed_at: "2026-04-21T03:47:51.625Z",
          updated_at: "2026-04-21T03:47:51.634Z",
          decision_backend: "heuristic"
        };
      }
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_stale_live_1", "sim_live_1"],
        active_runs: [
          {
            simulation_run_id: "sim_stale_live_1",
            status: "queued",
            user_id: "user_old",
            decision_backend: "heuristic"
          },
          {
            simulation_run_id: "sim_live_1",
            status: "running",
            user_id: "user_live",
            decision_backend: "llm",
            hero_context_hash: "ctx_live",
            requested_at: "2026-04-21T03:47:51.625Z",
            started_at: "2026-04-21T03:48:00.000Z"
          }
        ],
        selected_run_id: "sim_stale_live_1",
        selected_run_meta: {
          simulation_run_id: "sim_stale_live_1",
          status: "queued",
          user_id: "user_old",
          decision_backend: "heuristic",
          requested_at: "2026-04-21T03:40:00.000Z"
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_stale_live_1",
            player_id: "old_player"
          },
          {
            simulation_run_id: "sim_live_1",
            player_id: "live_player"
          }
        ],
        run_bb_per_100_leaderboard: [
          {
            simulation_run_id: "sim_stale_live_1",
            player_id: "old_player"
          },
          {
            simulation_run_id: "sim_live_1",
            player_id: "live_player"
          }
        ],
        run_high_hand_leaderboard: [
          {
            simulation_run_id: "sim_stale_live_1",
            player_id: "old_player"
          },
          {
            simulation_run_id: "sim_live_1",
            player_id: "live_player"
          }
        ],
        run_agents: [
          {
            simulation_run_id: "sim_stale_live_1",
            player_id: "old_player"
          },
          {
            simulation_run_id: "sim_live_1",
            player_id: "live_player"
          }
        ],
        global_profit_leaderboard: [
          {
            simulation_run_id: "sim_stale_live_1",
            player_id: "old_player"
          },
          {
            simulation_run_id: "sim_live_1",
            player_id: "live_player"
          }
        ],
        global_high_hand_leaderboard: [
          {
            simulation_run_id: "sim_stale_live_1",
            player_id: "old_player"
          },
          {
            simulation_run_id: "sim_live_1",
            player_id: "live_player"
          }
        ],
        global_hero_context_leaderboard: [
          {
            simulation_run_id: "sim_stale_live_1",
            player_id: "old_player"
          },
          {
            simulation_run_id: "sim_live_1",
            player_id: "live_player"
          }
        ]
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/dashboard/live?simulation_run_id=sim_live_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.available, true);
    assert.deepEqual(payload.active_run_ids, ["sim_live_1"]);
    assert.equal(payload.selected_run_id, "sim_live_1");
    assert.equal(payload.selected_run_meta.simulation_run_id, "sim_live_1");
    assert.equal(payload.selected_run_meta.user_id, "user_live");
    assert.deepEqual(
      payload.run_profit_leaderboard.map((row) => row.simulation_run_id),
      ["sim_live_1"]
    );
    assert.deepEqual(
      payload.run_bb_per_100_leaderboard.map((row) => row.simulation_run_id),
      ["sim_live_1"]
    );
    assert.deepEqual(
      payload.run_high_hand_leaderboard.map((row) => row.simulation_run_id),
      ["sim_live_1"]
    );
    assert.deepEqual(
      payload.run_agents.map((row) => row.simulation_run_id),
      ["sim_live_1"]
    );
    assert.deepEqual(
      payload.global_profit_leaderboard.map((row) => row.simulation_run_id),
      ["sim_live_1"]
    );
    assert.deepEqual(
      payload.global_high_hand_leaderboard.map((row) => row.simulation_run_id),
      ["sim_live_1"]
    );
    assert.deepEqual(payload.global_hero_context_leaderboard, []);
  });
});

test("GET /api/dashboard/live discards Redis visual rows from an older reused run id", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        user_id: "user_current",
        status: "running",
        requested_at: "2026-04-21T04:00:00.000Z",
        started_at: "2026-04-21T04:00:02.000Z",
        updated_at: "2026-04-21T04:00:08.000Z",
        decision_backend: "heuristic",
        hero_context_hash: "ctx_current",
        published_actions: 10,
        published_hand_summaries: 2,
        player_summaries: [
          {
            simulation_run_id: simulationRunId,
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            total_bb_won: 3,
            bb_per_100: 150,
            observed_hands: 2,
            hands_played: 2,
            observed_actions: 3,
            vpip: 0.5,
            pfr: 0.5,
            aggression_frequency: 0.25
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: simulationRunId,
            hand_number: 1,
            cumulative_bb_won: 1,
            event_ts: "2026-04-21T04:00:04.000Z"
          },
          {
            simulation_run_id: simulationRunId,
            hand_number: 2,
            cumulative_bb_won: 3,
            event_ts: "2026-04-21T04:00:08.000Z"
          }
        ]
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_reused_live"],
        active_runs: [
          {
            simulation_run_id: "sim_reused_live",
            status: "finalizing",
            user_id: "user_current",
            decision_backend: "heuristic",
            hero_context_hash: "ctx_current",
            requested_at: "2026-04-21T03:00:00.000Z",
            started_at: "2026-04-21T03:00:02.000Z",
            finished_at: "2026-04-21T03:30:00.000Z",
            updated_at: "2026-04-21T03:30:00.000Z",
            published_hand_summaries: 250
          }
        ],
        selected_run_id: "sim_reused_live",
        selected_run_meta: {
          simulation_run_id: "sim_reused_live",
          status: "finalizing",
          user_id: "user_current",
          decision_backend: "heuristic",
          hero_context_hash: "ctx_current",
          requested_at: "2026-04-21T03:00:00.000Z",
          started_at: "2026-04-21T03:00:02.000Z",
          finished_at: "2026-04-21T03:30:00.000Z",
          updated_at: "2026-04-21T03:30:00.000Z",
          published_hand_summaries: 250
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_reused_live",
            player_id: "old_player",
            total_bb_won: 999,
            observed_hands: 250
          }
        ],
        run_bb_per_100_leaderboard: [
          {
            simulation_run_id: "sim_reused_live",
            player_id: "old_player",
            bb_per_100: 399.6,
            observed_hands: 250
          }
        ],
        run_high_hand_leaderboard: [
          {
            simulation_run_id: "sim_reused_live",
            player_id: "old_player",
            showdown_hand_score: 999999
          }
        ],
        run_agents: [
          {
            simulation_run_id: "sim_reused_live",
            player_id: "old_player",
            agent_id: "old_agent",
            total_bb_won: 999,
            observed_hands: 250,
            hands_played: 250,
            vpip: 1,
            pfr: 1,
            aggression_frequency: 1
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: "sim_reused_live",
            hand_number: 250,
            cumulative_bb_won: 999
          }
        ],
        global_profit_leaderboard: [
          {
            simulation_run_id: "sim_reused_live",
            player_id: "old_player",
            total_bb_won: 999
          }
        ],
        global_high_hand_leaderboard: [
          {
            simulation_run_id: "sim_reused_live",
            player_id: "old_player",
            showdown_hand_score: 999999
          }
        ]
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/dashboard/live?simulation_run_id=sim_reused_live`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_meta.status, "running");
    assert.equal(payload.selected_run_meta.published_hand_summaries, 2);
    assert.deepEqual(
      payload.hero_profit_timeseries.map((row) => row.hand_number),
      [1, 2]
    );
    assert.equal(payload.hero_profit_timeseries.at(-1).cumulative_bb_won, 3);
    assert.deepEqual(
      payload.run_agents.map((row) => row.player_id),
      ["user_agent"]
    );
    assert.deepEqual(
      payload.run_profit_leaderboard.map((row) => row.player_id),
      ["user_agent"]
    );
    assert.deepEqual(
      payload.global_profit_leaderboard.map((row) => row.player_id),
      ["user_agent"]
    );
  });
});

test("GET /api/dashboard/live reloads selected live run rows and rebuilds global leaderboards", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => {
        if (simulationRunId === "sim_live_1") {
          return {
            simulation_run_id: "sim_live_1",
            user_id: "user_live",
            status: "running",
            requested_at: "2026-04-21T03:47:51.625Z",
            started_at: "2026-04-21T03:48:00.000Z",
            updated_at: "2026-04-21T03:48:10.000Z",
            decision_backend: "llm",
            hero_context_hash: "ctx_live"
          };
        }
        return {
          simulation_run_id: simulationRunId,
          user_id: "user_old",
          status: "completed",
          completed_at: "2026-04-21T03:47:51.625Z",
          updated_at: "2026-04-21T03:47:51.634Z",
          decision_backend: "heuristic"
        };
      }
    },
    runPythonModule: async (_moduleName, args) => {
      pythonCalls.push(args);
      const selectedRunIndex = args.indexOf("--simulation-run-id");
      const selectedRunId = selectedRunIndex >= 0 ? args[selectedRunIndex + 1] : null;
      if (selectedRunId === "sim_live_1") {
        return {
          stdout: JSON.stringify({
            available: true,
            source: "redis",
            active_run_ids: ["sim_stale_live_1", "sim_live_1"],
            active_runs: [
              {
                simulation_run_id: "sim_stale_live_1",
                status: "queued",
                user_id: "user_old",
                decision_backend: "heuristic"
              },
              {
                simulation_run_id: "sim_live_1",
                status: "running",
                user_id: "user_live",
                decision_backend: "llm",
                hero_context_hash: "ctx_live",
                requested_at: "2026-04-21T03:47:51.625Z",
                started_at: "2026-04-21T03:48:00.000Z"
              }
            ],
            selected_run_id: "sim_live_1",
            selected_run_meta: {
              simulation_run_id: "sim_live_1",
              status: "running",
              user_id: "user_live",
              decision_backend: "llm",
              hero_context_hash: "ctx_live",
              requested_at: "2026-04-21T03:47:51.625Z",
              started_at: "2026-04-21T03:48:00.000Z"
            },
            run_profit_leaderboard: [
              { simulation_run_id: "sim_live_1", player_id: "p1", total_bb_won: 42 },
              { simulation_run_id: "sim_live_1", player_id: "p2", total_bb_won: 21 },
              { simulation_run_id: "sim_live_1", player_id: "p3", total_bb_won: 4 },
              { simulation_run_id: "sim_live_1", player_id: "p4", total_bb_won: -2 },
              { simulation_run_id: "sim_live_1", player_id: "p5", total_bb_won: -30 },
              { simulation_run_id: "sim_live_1", player_id: "p6", total_bb_won: -35 }
            ],
            run_bb_per_100_leaderboard: [
              { simulation_run_id: "sim_live_1", player_id: "p1", bb_per_100: 84 },
              { simulation_run_id: "sim_live_1", player_id: "p2", bb_per_100: 42 }
            ],
            run_high_hand_leaderboard: [
              {
                simulation_run_id: "sim_live_1",
                player_id: "p2",
                showdown_hand_score: 700000
              },
              {
                simulation_run_id: "sim_live_1",
                player_id: "p5",
                showdown_hand_score: 680000
              }
            ],
            run_agents: [
              { simulation_run_id: "sim_live_1", player_id: "p1" },
              { simulation_run_id: "sim_live_1", player_id: "p2" }
            ],
            global_profit_leaderboard: [
              { simulation_run_id: "sim_stale_live_1", player_id: "old_player", total_bb_won: 999 }
            ],
            global_high_hand_leaderboard: [
              {
                simulation_run_id: "sim_stale_live_1",
                player_id: "old_player",
                showdown_hand_score: 999999
              }
            ],
            global_hero_context_leaderboard: [
              { simulation_run_id: "sim_stale_live_1", player_id: "old_player" }
            ]
          })
        };
      }
      return {
        stdout: JSON.stringify({
          available: true,
          source: "redis",
          active_run_ids: ["sim_stale_live_1", "sim_live_1"],
          active_runs: [
            {
              simulation_run_id: "sim_stale_live_1",
              status: "queued",
              user_id: "user_old",
              decision_backend: "heuristic"
            },
            {
              simulation_run_id: "sim_live_1",
              status: "running",
              user_id: "user_live",
              decision_backend: "llm"
            }
          ],
          selected_run_id: "sim_stale_live_1",
          selected_run_meta: {
            simulation_run_id: "sim_stale_live_1",
            status: "queued",
            user_id: "user_old",
            decision_backend: "heuristic"
          },
          run_profit_leaderboard: [
            { simulation_run_id: "sim_stale_live_1", player_id: "old_player", total_bb_won: 999 }
          ],
          run_bb_per_100_leaderboard: [
            { simulation_run_id: "sim_stale_live_1", player_id: "old_player", bb_per_100: 999 }
          ],
          run_high_hand_leaderboard: [
            {
              simulation_run_id: "sim_stale_live_1",
              player_id: "old_player",
              showdown_hand_score: 999999
            }
          ],
          run_agents: [{ simulation_run_id: "sim_stale_live_1", player_id: "old_player" }],
          global_profit_leaderboard: [
            { simulation_run_id: "sim_stale_live_1", player_id: "old_player", total_bb_won: 999 }
          ],
          global_high_hand_leaderboard: [
            {
              simulation_run_id: "sim_stale_live_1",
              player_id: "old_player",
              showdown_hand_score: 999999
            }
          ],
          global_hero_context_leaderboard: [
            { simulation_run_id: "sim_stale_live_1", player_id: "old_player" }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.available, true);
    assert.deepEqual(payload.active_run_ids, ["sim_live_1"]);
    assert.equal(payload.selected_run_id, "sim_live_1");
    assert.deepEqual(
      payload.run_profit_leaderboard.map((row) => row.simulation_run_id),
      ["sim_live_1", "sim_live_1", "sim_live_1", "sim_live_1", "sim_live_1", "sim_live_1"]
    );
    assert.equal(payload.global_profit_leaderboard.length, 6);
    assert.deepEqual(
      payload.global_profit_leaderboard.map((row) => row.simulation_run_id),
      ["sim_live_1", "sim_live_1", "sim_live_1", "sim_live_1", "sim_live_1", "sim_live_1"]
    );
    assert.deepEqual(payload.global_hero_context_leaderboard, []);
  });

  assert.equal(pythonCalls.length, 2);
  assert.ok(pythonCalls[1].includes("--simulation-run-id"));
  assert.ok(pythonCalls[1].includes("sim_live_1"));
});

test("GET /api/dashboard/live falls back to app-store player summaries when live leaderboards lag", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        user_id: "user_live",
        status: "finalizing",
        player_summaries: [
          {
            simulation_run_id: simulationRunId,
            player_id: "hero_player",
            is_hero_player: true,
            total_bb_won: 18,
            bb_per_100: 36,
            showdown_hand_score: 520000,
            final_rank: 1,
            seat: 1
          },
          {
            simulation_run_id: simulationRunId,
            player_id: "villain_1",
            total_bb_won: 3,
            bb_per_100: 6,
            showdown_hand_score: 410000,
            final_rank: 2,
            seat: 2
          },
          {
            simulation_run_id: simulationRunId,
            player_id: "villain_2",
            total_bb_won: -21,
            bb_per_100: -42,
            showdown_hand_score: 390000,
            final_rank: 6,
            seat: 6
          }
        ]
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_live_lagging"],
        active_runs: [
          {
            simulation_run_id: "sim_live_lagging",
            status: "finalizing",
            user_id: "user_live",
            decision_backend: "heuristic"
          }
        ],
        selected_run_id: "sim_live_lagging",
        selected_run_meta: {
          simulation_run_id: "sim_live_lagging",
          status: "finalizing",
          user_id: "user_live",
          decision_backend: "heuristic"
        },
        run_profit_leaderboard: [],
        run_bb_per_100_leaderboard: [],
        run_high_hand_leaderboard: [],
        run_agents: [],
        global_profit_leaderboard: [],
        global_high_hand_leaderboard: [],
        global_hero_context_leaderboard: [
          {
            simulation_run_id: "sim_live_lagging",
            player_id: "hero_player"
          }
        ]
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_id, "sim_live_lagging");
    assert.deepEqual(
      payload.run_profit_leaderboard.map((row) => row.player_id),
      ["hero_player", "villain_1", "villain_2"]
    );
    assert.deepEqual(
      payload.global_profit_leaderboard.map((row) => row.player_id),
      ["hero_player", "villain_1", "villain_2"]
    );
    assert.deepEqual(
      payload.run_high_hand_leaderboard.map((row) => row.player_id),
      ["hero_player", "villain_1", "villain_2"]
    );
    assert.deepEqual(payload.global_hero_context_leaderboard, []);
  });
});

test("GET /api/dashboard/live replaces partial live leaderboards with app-store player summaries", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        user_id: "user_live",
        status: "running",
        player_summaries: [
          {
            simulation_run_id: simulationRunId,
            player_id: "hero_player",
            is_hero_player: true,
            total_bb_won: 18,
            bb_per_100: 36,
            showdown_hand_score: 520000,
            final_rank: 1,
            seat: 1
          },
          {
            simulation_run_id: simulationRunId,
            player_id: "villain_1",
            total_bb_won: 3,
            bb_per_100: 6,
            showdown_hand_score: 410000,
            final_rank: 2,
            seat: 2
          },
          {
            simulation_run_id: simulationRunId,
            player_id: "villain_2",
            total_bb_won: -21,
            bb_per_100: -42,
            showdown_hand_score: 390000,
            final_rank: 6,
            seat: 6
          }
        ]
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_live_partial"],
        active_runs: [
          {
            simulation_run_id: "sim_live_partial",
            status: "running",
            user_id: "user_live",
            decision_backend: "heuristic"
          }
        ],
        selected_run_id: "sim_live_partial",
        selected_run_meta: {
          simulation_run_id: "sim_live_partial",
          status: "running",
          user_id: "user_live",
          decision_backend: "heuristic"
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_partial",
            player_id: "hero_player",
            total_bb_won: 18
          }
        ],
        run_bb_per_100_leaderboard: [],
        run_high_hand_leaderboard: [],
        run_agents: [
          {
            simulation_run_id: "sim_live_partial",
            player_id: "hero_player"
          }
        ],
        global_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_partial",
            player_id: "hero_player",
            total_bb_won: 18
          }
        ],
        global_high_hand_leaderboard: [],
        global_hero_context_leaderboard: []
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_id, "sim_live_partial");
    assert.deepEqual(
      payload.run_profit_leaderboard.map((row) => row.player_id),
      ["hero_player", "villain_1", "villain_2"]
    );
    assert.deepEqual(
      payload.global_profit_leaderboard.map((row) => row.player_id),
      ["hero_player", "villain_1", "villain_2"]
    );
    assert.deepEqual(
      payload.run_agents.map((row) => row.player_id),
      ["hero_player", "villain_1", "villain_2"]
    );
  });
});

test("GET /api/dashboard/live promotes selected run status to running when live rows already exist", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        user_id: "user_live",
        status: "queued",
        published_actions: 0,
        published_hand_summaries: 0,
        player_summaries: []
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_live_materializing"],
        active_runs: [
          {
            simulation_run_id: "sim_live_materializing",
            status: "queued",
            user_id: "user_live",
            decision_backend: "llm"
          }
        ],
        selected_run_id: "sim_live_materializing",
        selected_run_meta: {
          simulation_run_id: "sim_live_materializing",
          status: "queued",
          user_id: "user_live",
          decision_backend: "llm"
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_materializing",
            player_id: "hero_player",
            total_bb_won: 12
          }
        ],
        run_bb_per_100_leaderboard: [],
        run_high_hand_leaderboard: [],
        run_agents: [
          {
            simulation_run_id: "sim_live_materializing",
            player_id: "hero_player",
            is_hero_player: true,
            vpip: 0.22,
            pfr: 0.17,
            aggression_frequency: 0.36
          }
        ],
        global_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_materializing",
            player_id: "hero_player",
            total_bb_won: 12
          }
        ],
        global_high_hand_leaderboard: [],
        global_hero_context_leaderboard: []
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_id, "sim_live_materializing");
    assert.equal(payload.selected_run_meta.status, "running");
    assert.equal(payload.active_runs[0].status, "running");
  });
});

test("GET /api/dashboard/live reloads the selected run when chart series are missing from the overview payload", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        user_id: "user_live",
        status: "running"
      })
    },
    runPythonModule: async (_moduleName, args) => {
      pythonCalls.push(args);
      const selectedRunIndex = args.indexOf("--simulation-run-id");
      const selectedRunId = selectedRunIndex >= 0 ? args[selectedRunIndex + 1] : null;
      if (selectedRunId === "sim_live_charts") {
        return {
          stdout: JSON.stringify({
            available: true,
            source: "redis",
            active_run_ids: ["sim_live_charts"],
            active_runs: [
              {
                simulation_run_id: "sim_live_charts",
                status: "running",
                user_id: "user_live",
                decision_backend: "llm"
              }
            ],
            selected_run_id: "sim_live_charts",
            selected_run_meta: {
              simulation_run_id: "sim_live_charts",
              status: "running",
              user_id: "user_live",
              decision_backend: "llm"
            },
            run_profit_leaderboard: [
              {
                simulation_run_id: "sim_live_charts",
                player_id: "hero_player",
                total_bb_won: 5
              }
            ],
            run_bb_per_100_leaderboard: [],
            run_high_hand_leaderboard: [],
            run_agents: [
              {
                simulation_run_id: "sim_live_charts",
                player_id: "hero_player",
                is_hero_player: true,
                vpip: 0.2,
                pfr: 0.15,
                aggression_frequency: 0.3
              }
            ],
            hero_profit_timeseries: [
              {
                hand_number: 1,
                cumulative_bb_won: 1.5
              },
              {
                hand_number: 2,
                cumulative_bb_won: 5
              }
            ],
            global_profit_leaderboard: [
              {
                simulation_run_id: "sim_live_charts",
                player_id: "hero_player",
                total_bb_won: 5
              }
            ],
            global_high_hand_leaderboard: [],
            global_hero_context_leaderboard: []
          })
        };
      }
      return {
        stdout: JSON.stringify({
          available: true,
          source: "redis",
          active_run_ids: ["sim_live_charts"],
          active_runs: [
            {
              simulation_run_id: "sim_live_charts",
              status: "running",
              user_id: "user_live",
              decision_backend: "llm"
            }
          ],
          selected_run_id: "sim_live_charts",
          selected_run_meta: {
            simulation_run_id: "sim_live_charts",
            status: "running",
            user_id: "user_live",
            decision_backend: "llm"
          },
          run_profit_leaderboard: [
            {
              simulation_run_id: "sim_live_charts",
              player_id: "hero_player",
              total_bb_won: 5
            }
          ],
          run_bb_per_100_leaderboard: [],
          run_high_hand_leaderboard: [],
          run_agents: [
            {
              simulation_run_id: "sim_live_charts",
              player_id: "hero_player",
              is_hero_player: true,
              vpip: 0.2,
              pfr: 0.15,
              aggression_frequency: 0.3
            }
          ],
          hero_profit_timeseries: [],
          global_profit_leaderboard: [
            {
              simulation_run_id: "sim_live_charts",
              player_id: "hero_player",
              total_bb_won: 5
            }
          ],
          global_high_hand_leaderboard: [],
          global_hero_context_leaderboard: []
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_id, "sim_live_charts");
    assert.equal(payload.hero_profit_timeseries.length, 2);
    assert.equal(payload.hero_profit_timeseries[1].cumulative_bb_won, 5);
  });

  assert.equal(pythonCalls.length, 2);
  assert.ok(pythonCalls[1].includes("--simulation-run-id"));
  assert.ok(pythonCalls[1].includes("sim_live_charts"));
});

test("GET /api/dashboard/live replaces stale short live profit series with live agent totals", async () => {
  const app = createApp({
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_live_short_chart"],
        active_runs: [
          {
            simulation_run_id: "sim_live_short_chart",
            status: "running",
            user_id: "user_live",
            decision_backend: "heuristic",
            published_hand_summaries: 1000
          }
        ],
        selected_run_id: "sim_live_short_chart",
        selected_run_meta: {
          simulation_run_id: "sim_live_short_chart",
          status: "running",
          user_id: "user_live",
          decision_backend: "heuristic",
          published_hand_summaries: 1000
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_short_chart",
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            total_bb_won: 50
          }
        ],
        run_bb_per_100_leaderboard: [],
        run_high_hand_leaderboard: [],
        run_agents: [
          {
            simulation_run_id: "sim_live_short_chart",
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            observed_hands: 1000,
            observed_actions: 2400,
            bb_won: 50,
            vpip: 0.2,
            pfr: 0.1,
            aggression_frequency: 0.4
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: "sim_live_short_chart",
            hand_number: 1,
            cumulative_bb_won: 1
          },
          {
            simulation_run_id: "sim_live_short_chart",
            hand_number: 2,
            cumulative_bb_won: 5
          }
        ],
        global_profit_leaderboard: [],
        global_high_hand_leaderboard: [],
        global_hero_context_leaderboard: []
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.deepEqual(payload.hero_profit_timeseries, [
      {
        simulation_run_id: "sim_live_short_chart",
        hand_number: 0,
        cumulative_bb_won: 0
      },
      {
        simulation_run_id: "sim_live_short_chart",
        hand_number: 1000,
        cumulative_bb_won: 50
      }
    ]);
  });
});

test("GET /api/dashboard/live prefers app progress snapshots over coarse live profit lines", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        user_id: "user_live",
        status: "running",
        published_hand_summaries: 1000,
        player_summaries: [
          {
            simulation_run_id: simulationRunId,
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            total_bb_won: 50,
            observed_hands: 1000
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: simulationRunId,
            hand_number: 250,
            cumulative_bb_won: 12
          },
          {
            simulation_run_id: simulationRunId,
            hand_number: 500,
            cumulative_bb_won: 30
          },
          {
            simulation_run_id: simulationRunId,
            hand_number: 1000,
            cumulative_bb_won: 50
          }
        ]
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_live_app_snapshots"],
        active_runs: [
          {
            simulation_run_id: "sim_live_app_snapshots",
            status: "running",
            user_id: "user_live",
            decision_backend: "heuristic",
            published_hand_summaries: 1000
          }
        ],
        selected_run_id: "sim_live_app_snapshots",
        selected_run_meta: {
          simulation_run_id: "sim_live_app_snapshots",
          status: "running",
          user_id: "user_live",
          decision_backend: "heuristic",
          published_hand_summaries: 1000
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_app_snapshots",
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            total_bb_won: 50
          }
        ],
        run_bb_per_100_leaderboard: [],
        run_high_hand_leaderboard: [],
        run_agents: [
          {
            simulation_run_id: "sim_live_app_snapshots",
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            observed_hands: 1000,
            bb_won: 50
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: "sim_live_app_snapshots",
            hand_number: 0,
            cumulative_bb_won: 0
          },
          {
            simulation_run_id: "sim_live_app_snapshots",
            hand_number: 1000,
            cumulative_bb_won: 50
          }
        ],
        global_profit_leaderboard: [],
        global_high_hand_leaderboard: [],
        global_hero_context_leaderboard: []
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.deepEqual(payload.hero_profit_timeseries, [
      {
        simulation_run_id: "sim_live_app_snapshots",
        hand_number: 250,
        cumulative_bb_won: 12,
        event_ts: null
      },
      {
        simulation_run_id: "sim_live_app_snapshots",
        hand_number: 500,
        cumulative_bb_won: 30,
        event_ts: null
      },
      {
        simulation_run_id: "sim_live_app_snapshots",
        hand_number: 1000,
        cumulative_bb_won: 50,
        event_ts: null
      }
    ]);
  });
});

test("GET /api/dashboard/live hydrates completed selected run charts from durable results", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "completed",
        decision_backend: "heuristic",
        hand_count: 5000,
        published_actions: 74000,
        published_hand_summaries: 5000,
        completed_at: "2026-04-27T23:58:44.604Z"
      })
    },
    runPythonModule: async (_moduleName, args) => {
      pythonCalls.push(args);
      if (args[0] === "simulation-results") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_done_dashboard",
            run_summary: {
              simulation_run_id: "sim_done_dashboard",
              user_id: "user_1",
              decision_backend: "heuristic",
              status: "completed",
              finished_at: "2026-04-27T23:58:44.604Z",
              published_actions: 74000,
              published_hand_summaries: 5000
            },
            player_summaries: [
              {
                simulation_run_id: "sim_done_dashboard",
                player_id: "user_agent",
                agent_id: "user_agent",
                is_hero_player: true,
                hands_played: 5000,
                total_bb_won: 120,
                bb_per_100: 2.4,
                final_rank: 1
              }
            ],
            run_profit_leaderboard: [
              {
                simulation_run_id: "sim_done_dashboard",
                player_id: "user_agent",
                agent_id: "user_agent",
                is_hero_player: true,
                hands_played: 5000,
                total_bb_won: 120,
                bb_per_100: 2.4,
                final_rank: 1
              }
            ],
            run_bb_per_100_leaderboard: [],
            run_high_hand_leaderboard: [],
            agent_metrics: [
              {
                simulation_run_id: "sim_done_dashboard",
                player_id: "user_agent",
                agent_id: "user_agent",
                is_hero_player: true,
                observed_hands: 5000,
                observed_actions: 11608,
                vpip: 0.44,
                pfr: 0.14,
                aggression_frequency: 0.41,
                bb_won: 120
              }
            ],
            hero_profit_timeseries: [
              {
                simulation_run_id: "sim_done_dashboard",
                hand_number: 1,
                cumulative_bb_won: 1
              },
              {
                simulation_run_id: "sim_done_dashboard",
                hand_number: 5000,
                cumulative_bb_won: 120
              }
            ]
          })
        };
      }
      return {
        stdout: JSON.stringify({
          available: true,
          source: "redis",
          active_run_ids: ["sim_done_dashboard"],
          active_runs: [
            {
              simulation_run_id: "sim_done_dashboard",
              status: "running",
              user_id: "user_1",
              decision_backend: "heuristic"
            }
          ],
          selected_run_id: "sim_done_dashboard",
          selected_run_meta: {
            simulation_run_id: "sim_done_dashboard",
            status: "running",
            user_id: "user_1",
            decision_backend: "heuristic",
            published_hand_summaries: 54
          },
          run_profit_leaderboard: [
            {
              simulation_run_id: "sim_done_dashboard",
              player_id: "user_agent",
              agent_id: "user_agent",
              is_hero_player: true,
              total_bb_won: 9
            }
          ],
          run_bb_per_100_leaderboard: [],
          run_high_hand_leaderboard: [],
          run_agents: [
            {
              simulation_run_id: "sim_done_dashboard",
              player_id: "user_agent",
              agent_id: "user_agent",
              is_hero_player: true,
              observed_hands: 7,
              observed_actions: 15,
              vpip: 0.28,
              pfr: 0.08,
              aggression_frequency: 0.11
            }
          ],
          hero_profit_timeseries: [
            {
              simulation_run_id: "sim_done_dashboard",
              hand_number: 54,
              cumulative_bb_won: 9
            }
          ],
          global_profit_leaderboard: [],
          global_high_hand_leaderboard: [],
          global_hero_context_leaderboard: []
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/dashboard/live?simulation_run_id=sim_done_dashboard&user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_id, "sim_done_dashboard");
    assert.equal(payload.selected_run_meta.status, "completed");
    assert.equal(payload.source, "redis+snowflake");
    assert.equal(payload.hero_profit_timeseries.length, 2);
    assert.equal(payload.hero_profit_timeseries[1].hand_number, 5000);
    assert.equal(payload.run_agents[0].observed_hands, 5000);
    assert.equal(payload.run_agents[0].vpip, 0.44);
    assert.equal(payload.run_profit_leaderboard[0].bb_won, 120);
  });

  assert.ok(pythonCalls.some((args) => args[0] === "simulation-results"));
});

test("GET /api/dashboard/live prefers current app rows over stale completed-run warehouse rows", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "completed",
        decision_backend: "heuristic",
        hero_context_hash: "ctx_current",
        hand_count: 80,
        published_actions: 1200,
        published_hand_summaries: 80,
        requested_at: "2026-04-28T03:11:15.659Z",
        started_at: "2026-04-28T03:11:15.699Z",
        completed_at: "2026-04-28T03:11:23.252Z",
        player_summaries: [
          {
            simulation_run_id: simulationRunId,
            player_id: "user_agent",
            agent_id: "user_agent",
            is_hero_player: true,
            observed_hands: 80,
            hands_played: 80,
            observed_actions: 110,
            vpip: 0.31,
            pfr: 0.12,
            aggression_frequency: 0.42,
            total_bb_won: -121.41,
            bb_per_100: -151.76
          }
        ],
        hero_profit_timeseries: [
          {
            simulation_run_id: simulationRunId,
            hand_number: 1,
            cumulative_bb_won: -1
          },
          {
            simulation_run_id: simulationRunId,
            hand_number: 80,
            cumulative_bb_won: -121.41
          }
        ]
      })
    },
    runPythonModule: async (_moduleName, args) => {
      pythonCalls.push(args);
      if (args[0] === "simulation-results") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "h",
            run_summary: {
              simulation_run_id: "h",
              user_id: "user_1",
              decision_backend: "heuristic",
              status: "completed",
              published_hand_summaries: 5000
            },
            agent_metrics: [
              {
                simulation_run_id: "h",
                player_id: "user_agent",
                agent_id: "user_agent",
                is_hero_player: true,
                observed_hands: 5000,
                vpip: 0.99,
                pfr: 0.88,
                aggression_frequency: 0.77,
                bb_won: 1825
              }
            ],
            hero_profit_timeseries: [
              {
                simulation_run_id: "h",
                hand_number: 5000,
                cumulative_bb_won: 1825
              }
            ]
          })
        };
      }
      return {
        stdout: JSON.stringify({
          available: true,
          source: "redis",
          active_run_ids: [],
          active_runs: [],
          selected_run_id: "h",
          selected_run_meta: {
            simulation_run_id: "h",
            status: "completed",
            user_id: "user_1",
            decision_backend: "heuristic",
            hero_context_hash: "ctx_current",
            published_hand_summaries: 5000
          },
          run_agents: [
            {
              simulation_run_id: "h",
              player_id: "user_agent",
              agent_id: "user_agent",
              is_hero_player: true,
              observed_hands: 5000,
              vpip: 0.99,
              pfr: 0.88,
              aggression_frequency: 0.77
            }
          ],
          hero_profit_timeseries: [
            {
              simulation_run_id: "h",
              hand_number: 5000,
              cumulative_bb_won: 1825
            }
          ],
          global_profit_leaderboard: [],
          global_high_hand_leaderboard: [],
          global_hero_context_leaderboard: []
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/dashboard/live?simulation_run_id=h&user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.selected_run_id, "h");
    assert.equal(payload.selected_run_meta.published_hand_summaries, 80);
    assert.equal(payload.hero_profit_timeseries.at(-1).hand_number, 80);
    assert.equal(payload.run_agents[0].observed_hands, 80);
    assert.equal(payload.run_agents[0].vpip, 0.31);
  });

  assert.ok(!pythonCalls.some((args) => args[0] === "simulation-results"));
});

test("GET /api/dashboard/live preserves live aggression metrics and synthesizes a coarse hero profit line when needed", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId) => ({
        simulation_run_id: simulationRunId,
        user_id: "user_live",
        status: "running",
        published_hand_summaries: 40,
        player_summaries: [
          {
            simulation_run_id: simulationRunId,
            player_id: "hero_player",
            agent_id: "user_agent",
            is_hero_player: true,
            total_bb_won: 18,
            bb_per_100: 45,
            seat: 1
          },
          {
            simulation_run_id: simulationRunId,
            player_id: "villain_1",
            agent_id: "baseline_agent_2",
            total_bb_won: -18,
            bb_per_100: -45,
            seat: 2
          }
        ]
      })
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        available: true,
        source: "redis",
        active_run_ids: ["sim_live_visuals"],
        active_runs: [
          {
            simulation_run_id: "sim_live_visuals",
            status: "running",
            user_id: "user_live",
            decision_backend: "heuristic"
          }
        ],
        selected_run_id: "sim_live_visuals",
        selected_run_meta: {
          simulation_run_id: "sim_live_visuals",
          status: "running",
          user_id: "user_live",
          decision_backend: "heuristic",
          published_hand_summaries: 40
        },
        run_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_visuals",
            player_id: "hero_player",
            agent_id: "user_agent",
            is_hero_player: true,
            total_bb_won: 18
          }
        ],
        run_bb_per_100_leaderboard: [],
        run_high_hand_leaderboard: [],
        run_agents: [
          {
            simulation_run_id: "sim_live_visuals",
            player_id: "hero_player",
            agent_id: "user_agent",
            is_hero_player: true,
            vpip: 0.28,
            pfr: 0.17,
            aggression_frequency: 0.44
          }
        ],
        hero_profit_timeseries: [],
        global_profit_leaderboard: [
          {
            simulation_run_id: "sim_live_visuals",
            player_id: "hero_player",
            total_bb_won: 18
          }
        ],
        global_high_hand_leaderboard: [],
        global_hero_context_leaderboard: []
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.run_agents.length, 1);
    assert.equal(payload.run_agents[0].vpip, 0.28);
    assert.equal(payload.run_agents[0].pfr, 0.17);
    assert.equal(payload.run_agents[0].aggression_frequency, 0.44);
    assert.deepEqual(payload.hero_profit_timeseries, [
      {
        simulation_run_id: "sim_live_visuals",
        hand_number: 0,
        cumulative_bb_won: 0
      },
      {
        simulation_run_id: "sim_live_visuals",
        hand_number: 40,
        cumulative_bb_won: 18
      }
    ]);
  });
});

test("GET /api/dashboard/live surfaces running app runs before first live hand arrives", async () => {
  let pythonCalls = 0;
  const app = createApp({
    appStore: {
      listUsersByIds: async (userIds) =>
        userIds.map((userId) => ({
          user_id: userId,
          email: "hannah@example.com",
          display_name: "Hannah Davis"
        })),
      listLiveSimulationRuns: async () => [
        {
          simulation_run_id: "sim_live_booting",
          user_id: "user_live",
          status: "running",
          requested_at: "2026-04-22T21:14:07.000Z",
          started_at: "2026-04-22T21:14:08.000Z",
          updated_at: "2026-04-22T21:14:08.100Z",
          decision_backend: "llm",
          backend_type: "llm_gated_nano",
          model_name: "gpt-5.4-nano",
          hero_context_hash: "ctx_live_booting",
          hero_context_preview: "Play Aggressive",
          published_actions: 0,
          published_hand_summaries: 0,
          player_summaries: []
        }
      ]
    },
    runPythonModule: async () => {
      pythonCalls += 1;
      throw new Error("python live payload should not be loaded for zero-progress app runs");
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/dashboard/live`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.available, true);
    assert.deepEqual(payload.active_run_ids, ["sim_live_booting"]);
    assert.equal(payload.selected_run_id, "sim_live_booting");
    assert.equal(payload.selected_run_meta.status, "running");
    assert.equal(payload.selected_run_meta.account_name, "Hannah Davis");
    assert.equal(payload.active_runs[0].account_name, "Hannah Davis");
    assert.equal(payload.selected_run_meta.published_actions, 0);
    assert.equal(payload.selected_run_meta.published_hand_summaries, 0);
    assert.deepEqual(payload.run_profit_leaderboard, []);
    assert.deepEqual(payload.global_profit_leaderboard, []);
    assert.deepEqual(payload.global_hero_context_leaderboard, []);
  });

  assert.equal(pythonCalls, 0);
});

test("GET /api/simulations/:id/results returns warehouse-backed completed run data", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: null,
    getSimulationRun: async (simulationRunId) => ({
      simulation_run_id: simulationRunId,
      status: "completed",
      player_summaries: [
        {
          player_id: "legacy_player",
          total_bb_won: 1.5
        }
      ]
    }),
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_results_1",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_results_1",
              finished_at: "2026-04-21T01:49:02.931Z",
              completed_at: "2026-04-21T01:49:02.931Z"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "ready",
          source: "snowflake",
          simulation_run_id: "sim_results_1",
          run_summary: {
            simulation_run_id: "sim_results_1",
            status: "completed",
            published_actions: 384,
            published_hand_summaries: 30
          },
          result: {
            summary: {
              hero: {
                player_id: "hero_player",
                total_bb_won: 12.0
              }
            }
          },
          player_summaries: [
            {
              player_id: "hero_player",
              total_bb_won: 12.0,
              bb_per_100: 40.0,
              final_rank: 1,
              hands_played: 30
            }
          ],
          run_profit_leaderboard: [
            {
              player_id: "hero_player",
              total_bb_won: 12.0
            }
          ],
          run_bb_per_100_leaderboard: [
            {
              player_id: "hero_player",
              bb_per_100: 40.0
            }
          ],
          run_high_hand_leaderboard: [
            {
              player_id: "hero_player",
              showdown_hand_category: "straight"
            }
          ],
          agent_metrics: [
            {
              agent_id: "hero_agent",
              player_id: "hero_player"
            }
          ],
          profile_assignments: [
            {
              agent_id: "hero_agent",
              nearest_cluster_label: "LAG"
            }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations/sim_results_1/results`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.source, "snowflake");
    assert.equal(payload.run_summary.published_actions, 384);
    assert.equal(payload.result.summary.hero.player_id, "hero_player");
    assert.equal(payload.player_summaries[0].player_id, "hero_player");
    assert.equal(payload.run_high_hand_leaderboard[0].showdown_hand_category, "straight");
    assert.equal(payload.agent_metrics[0].agent_id, "hero_agent");
    assert.equal(payload.profile_assignments[0].nearest_cluster_label, "LAG");
  });

  assert.equal(pythonCalls.length, 2);
  assert.equal(pythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.equal(pythonCalls[0].args[0], "simulation-results-status");
  assert.equal(pythonCalls[1].moduleName, "dashboard.api_bridge");
  assert.equal(pythonCalls[1].args[0], "simulation-results");
  assert.ok(pythonCalls[1].args.includes("--simulation-run-id"));
  assert.ok(pythonCalls[1].args.includes("sim_results_1"));
});

test("GET /api/simulations/:id/results keeps app-owned run metadata authoritative over stale warehouse status", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "completed",
        requested_at: "2026-04-21T01:48:52.592Z",
        started_at: "2026-04-21T01:49:02.693Z",
        completed_at: "2026-04-21T01:49:02.931Z",
        updated_at: "2026-04-21T01:49:02.935Z",
        decision_backend: "heuristic",
        backend_type: "heuristic_persona",
        model_name: null,
        request_mode: "context",
        hero_context_hash: "ctx_hash_results_1",
        hero_agent_definition_id: null,
        hero_agent_version_id: null
      })
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      if (moduleName === "dashboard.api_bridge" && args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_results_app_owned_1",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_results_app_owned_1",
              finished_at: "2026-04-21T01:49:02.931Z",
              completed_at: "2026-04-21T01:49:02.931Z"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "ready",
          source: "snowflake",
          simulation_run_id: "sim_results_app_owned_1",
          run_summary: {
            simulation_run_id: "sim_results_app_owned_1",
            status: "queued",
            started_at: null,
            finished_at: null,
            decision_backend: "heuristic"
          },
          result: {
            summary: {
              hero: {
                player_id: "hero_player",
                total_bb_won: 6.5
              }
            }
          },
          player_summaries: [
            {
              player_id: "hero_player",
              total_bb_won: 6.5,
              bb_per_100: 21.7,
              final_rank: 1,
              hands_played: 30
            }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_results_app_owned_1/results?user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.source, "snowflake");
    assert.equal(payload.status, "completed");
    assert.equal(payload.run_summary.status, "completed");
    assert.equal(payload.run_summary.started_at, "2026-04-21T01:49:02.693Z");
    assert.equal(payload.run_summary.finished_at, "2026-04-21T01:49:02.931Z");
    assert.equal(payload.player_summaries[0].player_id, "hero_player");
  });

  assert.equal(pythonCalls.length, 2);
  assert.equal(pythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.equal(pythonCalls[0].args[0], "simulation-results-status");
  assert.equal(pythonCalls[1].moduleName, "dashboard.api_bridge");
  assert.equal(pythonCalls[1].args[0], "simulation-results");
});

test("GET /api/simulations/:id/results returns completed execution with materializing durable results", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "finalizing",
        requested_at: "2026-04-21T02:00:00.000Z",
        started_at: "2026-04-21T02:00:03.000Z",
        completed_at: "2026-04-21T02:00:09.000Z",
        decision_backend: "llm",
        backend_type: "llm_gated_nano",
        model_name: "gpt-5.4-nano",
        request_mode: "context",
        hero_context_hash: "ctx_hash_finalizing_1",
        published_actions: 180,
        published_hand_summaries: 24,
        player_summaries: [
          {
            player_id: "hero_player",
            agent_id: "hero_agent",
            seat: 1,
            is_hero_player: true,
            total_bb_won: 6.5,
            bb_per_100: 27.1,
            final_rank: 1,
            hands_played: 24
          },
          {
            player_id: "villain_player",
            agent_id: "villain_agent",
            seat: 2,
            is_hero_player: false,
            total_bb_won: -6.5,
            bb_per_100: -27.1,
            final_rank: 6,
            hands_played: 24
          }
        ]
      })
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          status: "pending",
          source: "snowflake",
          simulation_run_id: "sim_finalizing_1",
          ready: false,
          message: "Final results are still being prepared",
          run_summary: {
            simulation_run_id: "sim_finalizing_1",
            status: "finalizing"
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_finalizing_1/results?user_id=user_1`
    );
    assert.equal(response.status, 202);
    const payload = await response.json();
    assert.equal(payload.status, "completed");
    assert.equal(payload.materialization_status, "materializing");
    assert.equal(
      payload.message,
      "Execution finished. Showing the latest run summary while final results are prepared."
    );
    assert.equal(payload.run_summary.status, "completed");
    assert.equal(payload.run_summary.materialization_status, "materializing");
    assert.equal(payload.run_summary.published_actions, 180);
    assert.equal(payload.run_summary.published_hand_summaries, 24);
    assert.equal(payload.player_summaries.length, 2);
    assert.equal(payload.player_summaries[0].player_id, "hero_player");
    assert.equal(payload.run_profit_leaderboard[0].player_id, "hero_player");
    assert.equal(payload.run_bb_per_100_leaderboard[0].player_id, "hero_player");
    assert.equal(payload.result.summary.hero.player_id, "hero_player");
    assert.equal(payload.result.summary.winner.player_id, "hero_player");
  });

  assert.equal(pythonCalls.length, 1);
  assert.equal(pythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.equal(pythonCalls[0].args[0], "simulation-results-status");
});

test("GET /api/simulations/:id/results keeps provisional data for completed runs while durable rows materialize", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "completed",
        requested_at: "2026-04-21T02:00:00.000Z",
        started_at: "2026-04-21T02:00:03.000Z",
        completed_at: "2026-04-21T02:00:09.000Z",
        decision_backend: "heuristic",
        backend_type: "heuristic_persona",
        model_name: null,
        request_mode: "context",
        hero_context_hash: "ctx_hash_completed_materializing_1",
        published_actions: 180,
        published_hand_summaries: 24,
        player_summaries: [
          {
            player_id: "hero_player",
            agent_id: "hero_agent",
            seat: 1,
            is_hero_player: true,
            total_bb_won: 6.5,
            bb_per_100: 27.1,
            final_rank: 1,
            hands_played: 24
          }
        ]
      })
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          status: "pending",
          source: "snowflake",
          simulation_run_id: "sim_completed_materializing_1",
          ready: false,
          message: "Final results are still being prepared",
          run_summary: {
            simulation_run_id: "sim_completed_materializing_1",
            status: "completed"
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_completed_materializing_1/results?user_id=user_1`
    );
    assert.equal(response.status, 202);
    const payload = await response.json();
    assert.equal(payload.status, "completed");
    assert.equal(payload.materialization_status, "materializing");
    assert.equal(payload.player_summaries.length, 1);
    assert.equal(payload.player_summaries[0].player_id, "hero_player");
    assert.equal(payload.run_profit_leaderboard[0].player_id, "hero_player");
  });

  assert.equal(pythonCalls.length, 1);
  assert.equal(pythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.equal(pythonCalls[0].args[0], "simulation-results-status");
});

test("GET /api/simulations/:id promotes execution-finished runs to completed before durable materialization finishes", async () => {
  const lifecycleUpdates = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "finalizing",
        completed_at: "2026-04-21T02:00:09.000Z",
        published_hand_summaries: 24,
        hand_count: 24
      }),
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        lifecycleUpdates.push({ simulationRunId, patch });
        return {
          simulation_run_id: simulationRunId,
          user_id: "user_1",
          status: patch.status,
          completed_at: patch.completedAt,
          published_hand_summaries: 24,
          hand_count: 24
        };
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_finished_1?user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "completed");
    assert.equal(payload.completed_at, "2026-04-21T02:00:09.000Z");
  });

  assert.equal(lifecycleUpdates.length, 1);
  assert.equal(lifecycleUpdates[0].simulationRunId, "sim_finished_1");
  assert.equal(lifecycleUpdates[0].patch.status, "completed");
});

test("GET /api/simulations/:id/results promotes finalizing runs to completed once warehouse results are ready", async () => {
  const pythonCalls = [];
  const lifecycleUpdates = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "finalizing",
        requested_at: "2026-04-21T02:10:00.000Z",
        started_at: "2026-04-21T02:10:03.000Z",
        completed_at: "2026-04-21T02:10:09.000Z",
        decision_backend: "heuristic",
        backend_type: "heuristic_persona",
        model_name: null,
        request_mode: "context",
        hero_context_hash: "ctx_hash_finalizing_ready_1"
      }),
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        lifecycleUpdates.push({ simulationRunId, patch });
        return {
          simulation_run_id: simulationRunId,
          user_id: "user_1",
          status: patch.status,
          requested_at: "2026-04-21T02:10:00.000Z",
          started_at: "2026-04-21T02:10:03.000Z",
          completed_at: patch.completedAt,
          decision_backend: "heuristic",
          backend_type: "heuristic_persona",
          model_name: null,
          request_mode: "context",
          hero_context_hash: "ctx_hash_finalizing_ready_1"
        };
      }
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      if (args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_finalizing_ready_1",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_finalizing_ready_1",
              finished_at: "2026-04-21T02:10:10.000Z",
              completed_at: "2026-04-21T02:10:10.000Z"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "ready",
          source: "snowflake",
          simulation_run_id: "sim_finalizing_ready_1",
          run_summary: {
            simulation_run_id: "sim_finalizing_ready_1",
            status: "completed",
            finished_at: "2026-04-21T02:10:10.000Z",
            completed_at: "2026-04-21T02:10:10.000Z",
            published_actions: 240,
            published_hand_summaries: 30
          },
          result: {
            summary: {
              hero: {
                player_id: "hero_player",
                total_bb_won: 8.5
              }
            }
          },
          player_summaries: [
            {
              player_id: "hero_player",
              total_bb_won: 8.5,
              bb_per_100: 28.3,
              final_rank: 1,
              hands_played: 30
            }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_finalizing_ready_1/results?user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "completed");
    assert.equal(payload.run_summary.status, "completed");
    assert.equal(payload.player_summaries[0].player_id, "hero_player");
  });

  assert.equal(pythonCalls.length, 2);
  assert.equal(pythonCalls[0].args[0], "simulation-results-status");
  assert.equal(pythonCalls[1].args[0], "simulation-results");
  assert.equal(lifecycleUpdates.length, 2);
  assert.equal(lifecycleUpdates[0].simulationRunId, "sim_finalizing_ready_1");
  assert.equal(lifecycleUpdates[0].patch.status, "completed");
  assert.equal(
    lifecycleUpdates[0].patch.completedAt,
    "2026-04-21T02:10:09.000Z"
  );
  assert.equal(lifecycleUpdates[1].simulationRunId, "sim_finalizing_ready_1");
  assert.equal(lifecycleUpdates[1].patch.status, "completed");
  assert.equal(
    lifecycleUpdates[1].patch.completedAt,
    "2026-04-21T02:10:10.000Z"
  );
});

test("GET /api/simulations/:id/results promotes queued runs to completed once warehouse results are ready", async () => {
  const lifecycleUpdates = [];
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "queued",
        requested_at: "2026-04-21T02:10:00.000Z",
        started_at: null,
        completed_at: null,
        decision_backend: "heuristic",
        backend_type: "heuristic_persona",
        model_name: null,
        request_mode: "context",
        hero_context_hash: "ctx_hash_queued_ready_1"
      }),
      updateSimulationRunLifecycle: async (simulationRunId, patch) => {
        lifecycleUpdates.push({ simulationRunId, patch });
        return {
          simulation_run_id: simulationRunId,
          user_id: "user_1",
          status: patch.status,
          requested_at: "2026-04-21T02:10:00.000Z",
          started_at: null,
          completed_at: patch.completedAt,
          decision_backend: "heuristic",
          backend_type: "heuristic_persona",
          model_name: null,
          request_mode: "context",
          hero_context_hash: "ctx_hash_queued_ready_1"
        };
      }
    },
    runPythonModule: async (_moduleName, args) => {
      if (args[0] === "simulation-results-status") {
        return {
          stdout: JSON.stringify({
            status: "ready",
            source: "snowflake",
            simulation_run_id: "sim_queued_ready_1",
            ready: true,
            run_summary: {
              simulation_run_id: "sim_queued_ready_1",
              finished_at: "2026-04-21T02:10:10.000Z",
              completed_at: "2026-04-21T02:10:10.000Z"
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          status: "ready",
          source: "snowflake",
          simulation_run_id: "sim_queued_ready_1",
          run_summary: {
            simulation_run_id: "sim_queued_ready_1",
            status: "completed",
            finished_at: "2026-04-21T02:10:10.000Z",
            completed_at: "2026-04-21T02:10:10.000Z",
            published_actions: 120,
            published_hand_summaries: 20
          },
          result: {
            summary: {
              hero: {
                player_id: "hero_player",
                total_bb_won: 4.5
              }
            }
          },
          player_summaries: [
            {
              player_id: "hero_player",
              total_bb_won: 4.5,
              bb_per_100: 22.5,
              final_rank: 1,
              hands_played: 20
            }
          ]
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/simulations/sim_queued_ready_1/results?user_id=user_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.status, "completed");
    assert.equal(payload.run_summary.status, "completed");
    assert.equal(payload.player_summaries[0].player_id, "hero_player");
  });

  assert.equal(lifecycleUpdates.length, 1);
  assert.equal(lifecycleUpdates[0].simulationRunId, "sim_queued_ready_1");
  assert.equal(lifecycleUpdates[0].patch.status, "completed");
});

test("GET /app serves the operator shell", async () => {
  await withServer(createApp(), async (baseUrl) => {
    const response = await fetch(`${baseUrl}/app`);
    assert.equal(response.status, 200);
    const body = await response.text();
    assert.match(body, /Simulation Console/);
    assert.match(body, /Run agents, inspect live edges/);
  });
});

test("GET /api/simulations/:id/results keeps running runs running when only progress summaries exist", async () => {
  const app = createApp({
    appStore: {
      getSimulationRun: async (simulationRunId, { userId }) => ({
        simulation_run_id: simulationRunId,
        user_id: userId,
        status: "running",
        hand_count: 500,
        started_at: "2026-04-21T02:00:03.000Z",
        published_actions: 200,
        published_hand_summaries: 50,
        player_summaries: [
          {
            player_id: "hero_player",
            agent_id: "hero_agent",
            seat: 1,
            is_hero_player: true,
            total_bb_won: 6.5,
            bb_per_100: 13.0,
            final_rank: 1,
            hands_played: 50
          }
        ],
      }),
      updateSimulationRunLifecycle: async () => {
        throw new Error("running progress should not be promoted to finalizing");
      }
    },
    runPythonModule: async () => ({
      stdout: JSON.stringify({
        ready: false,
        source: "snowflake",
        status: "not_ready",
        run_summary: {
          simulation_run_id: "sim_running_progress_1"
        }
      })
    })
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/simulations/sim_running_progress_1/results?user_id=user_1`);
    assert.equal(response.status, 409);
    const payload = await response.json();
    assert.equal(payload.status, "running");
  });
});

test("POST /api/session/dev persists and returns the dev session identity", async () => {
  const ensuredUsers = [];
  const app = createApp({
    appStore: {
      ensureUser: async (user) => {
        ensuredUsers.push(user);
        return user;
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/session/dev`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        user_id: "user_session_1",
        user_email: "session@example.com",
        user_display_name: "River Captain"
      })
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.user.user_id, "user_session_1");
    assert.equal(payload.user.email, "session@example.com");
    assert.equal(payload.user.display_name, "River Captain");
    assert.match(payload.user.dashboard_url, /8501$/);
  });

  assert.equal(ensuredUsers.length, 1);
  assert.equal(ensuredUsers[0].userId, "user_session_1");
});

test("POST /api/session/register creates a password account, sets a cookie, and resolves via GET /api/session", async () => {
  const createdUsers = [];
  const sessions = new Map();
  const app = createApp({
    appStore: {
      registerPasswordUser: async ({
        email,
        displayName,
        passwordHash,
        sessionToken,
        expiresAt
      }) => {
        const user = {
          user_id: "user_password_1",
          email,
          display_name: displayName
        };
        createdUsers.push({ email, displayName, passwordHash, sessionToken, expiresAt });
        sessions.set(sessionToken, {
          session: {
            session_id: "session_password_1",
            user_id: user.user_id,
            expires_at: expiresAt
          },
          user
        });
        return {
          user,
          session: {
            session_id: "session_password_1",
            user_id: user.user_id,
            expires_at: expiresAt
          }
        };
      },
      getSessionByToken: async (sessionToken) => sessions.get(sessionToken) || null
    }
  });

  await withServer(app, async (baseUrl) => {
    const registerResponse = await fetch(`${baseUrl}/api/session/register`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: "account@example.com",
        display_name: "Account Owner",
        password: "RiverPass9"
      })
    });

    assert.equal(registerResponse.status, 201);
    const registerPayload = await registerResponse.json();
    assert.equal(registerPayload.user.auth_mode, "password");
    assert.equal(registerPayload.user.email, "account@example.com");
    const cookieHeader = cookieHeaderFromResponse(registerResponse);
    assert.ok(cookieHeader);
    assert.match(cookieHeader, /^poker_app_session=/);

    const sessionResponse = await fetch(`${baseUrl}/api/session`, {
      headers: {
        Cookie: cookieHeader
      }
    });
    assert.equal(sessionResponse.status, 200);
    const sessionPayload = await sessionResponse.json();
    assert.equal(sessionPayload.user.user_id, "user_password_1");
    assert.equal(sessionPayload.user.auth_mode, "password");
    assert.equal(sessionPayload.user.email, "account@example.com");
  });

  assert.equal(createdUsers.length, 1);
  assert.equal(createdUsers[0].email, "account@example.com");
  assert.equal(createdUsers[0].displayName, "Account Owner");
  assert.notEqual(createdUsers[0].passwordHash, "RiverPass9");
  assert.match(createdUsers[0].passwordHash, /^scrypt\$/);
  assert.ok(createdUsers[0].sessionToken);
});

test("POST /api/session/register returns 409 for duplicate emails and does not set a cookie", async () => {
  const app = createApp({
    appStore: {
      registerPasswordUser: async () => {
        throw new AppStoreConflictError("A user with that email already exists");
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/session/register`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: "account@example.com",
        display_name: "Account Owner",
        password: "RiverPass9"
      })
    });

    assert.equal(response.status, 409);
    const payload = await response.json();
    assert.equal(payload.message, "A user with that email already exists");
    assert.equal(response.headers.get("set-cookie"), null);
  });
});

test("POST /api/session/register rejects weak passwords before any account write", async () => {
  let registerCalls = 0;
  const app = createApp({
    appStore: {
      registerPasswordUser: async () => {
        registerCalls += 1;
        return null;
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/session/register`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: "weak@example.com",
        display_name: "Weak Password",
        password: "short"
      })
    });

    assert.equal(response.status, 400);
    const payload = await response.json();
    assert.equal(payload.message, "Validation failed");
    assert.ok(Array.isArray(payload.issues));
    assert.equal(payload.issues[0]?.path?.[0], "password");
  });

  assert.equal(registerCalls, 0);
});

test("POST /api/session/register does not authenticate the browser when registration fails", async () => {
  const app = createApp({
    appStore: {
      registerPasswordUser: async () => {
        throw new Error("database write failed");
      },
      getSessionByToken: async () => null
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/session/register`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: "broken@example.com",
        display_name: "Broken Account",
        password: "RiverPass9"
      })
    });

    assert.equal(response.status, 500);
    assert.equal(response.headers.get("set-cookie"), null);

    const sessionResponse = await fetch(`${baseUrl}/api/session`);
    assert.equal(sessionResponse.status, 200);
    const sessionPayload = await sessionResponse.json();
    assert.equal(sessionPayload.user, null);
  });
});

test("POST /api/session/login rejects invalid credentials", async () => {
  const createSessionCalls = [];
  const app = createApp({
    appStore: {
      getPasswordAuthIdentityByEmail: async () => ({
        password_hash: await hashPassword("ValidPass9"),
        user: {
          user_id: "user_password_2",
          email: "login@example.com",
          display_name: "Login User"
        }
      }),
      createSession: async (payload) => {
        createSessionCalls.push(payload);
        return {
          session_id: "session_unused"
        };
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/session/login`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: "login@example.com",
        password: "WrongPass9"
      })
    });

    assert.equal(response.status, 401);
    const payload = await response.json();
    assert.equal(payload.message, "Invalid email or password");
  });

  assert.equal(createSessionCalls.length, 0);
});

test("POST /api/session/logout clears the session cookie and removes the session", async () => {
  const deletedTokens = [];
  const app = createApp({
    appStore: {
      getSessionByToken: async (sessionToken) => ({
        session: {
          session_id: "session_existing_1",
          user_id: "user_logout_1"
        },
        user: {
          user_id: "user_logout_1",
          email: "logout@example.com",
          display_name: "Logout User"
        }
      }),
      deleteSessionByToken: async (sessionToken) => {
        deletedTokens.push(sessionToken);
        return true;
      }
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/session/logout`, {
      method: "POST",
      headers: {
        Cookie: "poker_app_session=logout_token_1"
      }
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    const cookieHeader = response.headers.get("set-cookie") || "";
    assert.match(cookieHeader, /poker_app_session=/);
    assert.match(cookieHeader, /Max-Age=0/);
  });

  assert.deepEqual(deletedTokens, ["logout_token_1"]);
});

test("GET /api/performance/summary returns user-scoped performance payloads", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      listSimulationRuns: async ({ userId }) => [
        { simulation_run_id: `${userId}_run_1` },
        { simulation_run_id: `${userId}_run_2` }
      ]
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          summary: {
            run_count: 2,
            total_hands: 300,
            total_bb_won: 18.5,
            avg_bb_per_100: 6.2,
            first_place_rate: 0.5
          },
          recent_runs: [
            {
              simulation_run_id: "user_perf_1_run_1",
              total_bb_won: 18.5
            }
          ],
          profile_overview: {
            ready_profiles: 1,
            insufficient_evidence: 1,
            not_found: 0,
            other: 0
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/performance/summary?user_id=user_perf_1&decision_backend=heuristic`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.summary.run_count, 2);
    assert.equal(payload.profile_overview.ready_profiles, 1);
  });

  assert.equal(pythonCalls.length, 1);
  assert.equal(pythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.equal(pythonCalls[0].args[0], "performance-summary");
  assert.ok(pythonCalls[0].args.includes("--simulation-run-id"));
  assert.ok(pythonCalls[0].args.includes("--decision-backend"));
  assert.ok(pythonCalls[0].args.includes("heuristic"));
});

test("GET /api/agents/:id/performance scopes to the selected agent's runs", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getAgent: async (agentDefinitionId, { userId }) => ({
        agent_definition_id: agentDefinitionId,
        user_id: userId,
        name: "River Captain",
        latest_version: {
          agent_version_id: "agent_ver_7",
          version_number: 7
        },
        versions: [
          {
            agent_version_id: "agent_ver_7",
            version_number: 7,
            context_text: "Pressure capped ranges",
            decision_backend_default: "llm"
          }
        ]
      }),
      listSimulationRuns: async ({ heroAgentDefinitionId }) => [
        {
          simulation_run_id: `${heroAgentDefinitionId}_run_1`,
          hero_agent_version_id: "agent_ver_7",
          status: "completed",
          decision_backend: "llm",
          requested_at: "2026-04-20T20:15:00Z"
        }
      ]
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          summary: {
            run_count: 1,
            total_hands: 120,
            total_bb_won: 9.5,
            avg_bb_per_100: 7.9,
            first_place_rate: 1
          },
          recent_runs: [
            {
              simulation_run_id: "agent_def_perf_1_run_1",
              total_bb_won: 9.5
            }
          ],
          profile_overview: {
            ready_profiles: 1,
            insufficient_evidence: 0,
            not_found: 0,
            other: 0
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/agents/agent_def_perf_1/performance?user_id=user_perf_1`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.agent.agent_definition_id, "agent_def_perf_1");
    assert.equal(payload.summary.run_count, 1);
    assert.equal(payload.recent_runs[0].simulation_run_id, "agent_def_perf_1_run_1");
    assert.equal(payload.version_summaries.length, 1);
    assert.equal(payload.version_summaries[0].version.agent_version_id, "agent_ver_7");
  });

  assert.equal(pythonCalls.length, 2);
  assert.equal(pythonCalls[0].moduleName, "dashboard.api_bridge");
  assert.ok(pythonCalls[0].args.includes("agent_def_perf_1_run_1"));
});

test("GET /api/agents/:id/performance includes version-specific summaries", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getAgent: async (agentDefinitionId, { userId }) => ({
        agent_definition_id: agentDefinitionId,
        user_id: userId,
        name: "River Captain",
        latest_version: {
          agent_version_id: "agent_ver_2",
          version_number: 2
        },
        versions: [
          {
            agent_version_id: "agent_ver_2",
            version_number: 2,
            context_text: "Updated river pressure",
            decision_backend_default: "llm"
          },
          {
            agent_version_id: "agent_ver_1",
            version_number: 1,
            context_text: "Original baseline",
            decision_backend_default: "heuristic"
          }
        ]
      }),
      listSimulationRuns: async ({ heroAgentDefinitionId }) => [
        {
          simulation_run_id: `${heroAgentDefinitionId}_overall_1`,
          hero_agent_version_id: "agent_ver_2",
          status: "completed",
          decision_backend: "llm",
          requested_at: "2026-04-20T20:05:00Z"
        },
        {
          simulation_run_id: `${heroAgentDefinitionId}_overall_2`,
          hero_agent_version_id: "agent_ver_1",
          status: "running",
          decision_backend: "heuristic",
          requested_at: "2026-04-20T19:55:00Z"
        }
      ]
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      const runIds = args.filter((value, index) => args[index - 1] === "--simulation-run-id");
      if (runIds.includes("agent_def_perf_versions_overall_1") && runIds.length === 1) {
        return {
          stdout: JSON.stringify({
            summary: {
              run_count: 1,
              total_hands: 180,
              total_bb_won: 16.0,
              avg_bb_per_100: 8.9
            },
            recent_runs: [
              {
                simulation_run_id: "agent_def_perf_versions_overall_1",
                total_bb_won: 16.0
              }
            ],
            profile_overview: {
              ready_profiles: 1,
              insufficient_evidence: 0,
              not_found: 0,
              other: 0
            }
          })
        };
      }
      if (runIds.includes("agent_def_perf_versions_overall_2") && runIds.length === 1) {
        return {
          stdout: JSON.stringify({
            summary: {
              run_count: 1,
              total_hands: 80,
              total_bb_won: 1.5,
              avg_bb_per_100: 1.9
            },
            recent_runs: [
              {
                simulation_run_id: "agent_def_perf_versions_overall_2",
                total_bb_won: 1.5
              }
            ],
            profile_overview: {
              ready_profiles: 0,
              insufficient_evidence: 1,
              not_found: 0,
              other: 0
            }
          })
        };
      }
      return {
        stdout: JSON.stringify({
          summary: {
            run_count: 2,
            total_hands: 260,
            total_bb_won: 17.5,
            avg_bb_per_100: 5.4
          },
          recent_runs: [
            {
              simulation_run_id: "agent_def_perf_versions_overall_1",
              total_bb_won: 16.0
            },
            {
              simulation_run_id: "agent_def_perf_versions_overall_2",
              total_bb_won: 1.5
            }
          ],
          profile_overview: {
            ready_profiles: 1,
            insufficient_evidence: 1,
            not_found: 0,
            other: 0
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/agents/agent_def_perf_versions/performance?user_id=user_perf_2`
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.version_summaries.length, 2);
    assert.equal(payload.version_summaries[0].version.agent_version_id, "agent_ver_2");
    assert.equal(payload.version_summaries[0].performance_summary.total_hands, 180);
    assert.equal(payload.version_summaries[0].profile_overview.ready_profiles, 1);
    assert.equal(payload.version_summaries[1].status_counts.running, 1);
  });

  assert.equal(pythonCalls.length, 3);
});

test("POST /api/performance/query executes a structured explorer query and saves it", async () => {
  const pythonCalls = [];
  const savedQueries = [];
  const app = createApp({
    appStore: {
      listSimulationRuns: async () => [
        {
          simulation_run_id: "sim_query_1",
          requested_at: "2026-04-20T20:00:00Z"
        },
        {
          simulation_run_id: "sim_query_2",
          requested_at: "2026-04-20T20:10:00Z"
        }
      ],
      createSavedQuery: async (payload) => {
        savedQueries.push(payload);
        return {
          saved_query_id: "saved_query_1",
          name: payload.name,
          query_type: payload.queryType,
          filters: payload.filters
        };
      }
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          status: "ready",
          matched_run_count: 2,
          summary: {
            run_count: 2,
            total_hands: 220,
            total_bb_won: 15.0,
            avg_bb_per_100: 6.8,
            first_place_rate: 0.5
          },
          rows: [
            {
              simulation_run_id: "sim_query_1",
              total_bb_won: 10.0
            }
          ],
          breakdowns: {
            by_backend: [{ decision_backend: "heuristic", run_count: 2 }],
            by_finish_rank: [{ final_rank: 1, run_count: 1 }]
          },
          profile_overview: {
            ready_profiles: 1,
            insufficient_evidence: 1,
            not_found: 0,
            other: 0
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/performance/query?user_id=user_query_1`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        name: "My heuristic winners",
        save: true,
        topn: 15,
        filters: {
          decision_backend: "heuristic",
          min_hands_played: 50
        }
      })
    });

    assert.equal(response.status, 201);
    const payload = await response.json();
    assert.equal(payload.saved_query.saved_query_id, "saved_query_1");
    assert.equal(payload.summary.run_count, 2);
    assert.equal(payload.rows[0].simulation_run_id, "sim_query_1");
  });

  assert.equal(savedQueries.length, 1);
  assert.equal(savedQueries[0].userId, "user_query_1");
  assert.equal(savedQueries[0].queryType, "hero_performance");
  assert.equal(savedQueries[0].filters.topn, 15);
  assert.equal(pythonCalls.length, 1);
  assert.equal(pythonCalls[0].args[0], "performance-query");
  assert.ok(pythonCalls[0].args.includes("--min-hands-played"));
  assert.ok(pythonCalls[0].args.includes("50"));
});

test("GET /api/performance/queries lists saved explorer queries", async () => {
  const app = createApp({
    appStore: {
      listSavedQueries: async ({ userId }) => [
        {
          saved_query_id: "saved_query_1",
          user_id: userId,
          name: "Recent winners",
          query_type: "hero_performance",
          filters: {
            decision_backend: "llm"
          }
        }
      ]
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/performance/queries?user_id=user_query_1`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.saved_queries.length, 1);
    assert.equal(payload.saved_queries[0].name, "Recent winners");
  });
});

test("GET /api/performance/query/:id loads and executes a saved explorer query", async () => {
  const pythonCalls = [];
  const app = createApp({
    appStore: {
      getSavedQuery: async (savedQueryId, { userId }) => ({
        saved_query_id: savedQueryId,
        user_id: userId,
        name: "Top finishes",
        query_type: "hero_performance",
        filters: {
          decision_backend: "llm",
          max_final_rank: 2,
          topn: 10
        }
      }),
      listSimulationRuns: async () => [
        {
          simulation_run_id: "sim_saved_1",
          requested_at: "2026-04-20T19:00:00Z"
        }
      ]
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          status: "ready",
          matched_run_count: 1,
          summary: {
            run_count: 1,
            total_hands: 120,
            total_bb_won: 12.5,
            avg_bb_per_100: 10.4,
            first_place_rate: 1
          },
          rows: [
            {
              simulation_run_id: "sim_saved_1",
              final_rank: 1
            }
          ],
          breakdowns: {
            by_backend: [{ decision_backend: "llm", run_count: 1 }],
            by_finish_rank: [{ final_rank: 1, run_count: 1 }]
          },
          profile_overview: {
            ready_profiles: 1,
            insufficient_evidence: 0,
            not_found: 0,
            other: 0
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/performance/query/saved_query_1?user_id=user_query_1`);
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.saved_query.saved_query_id, "saved_query_1");
    assert.equal(payload.rows[0].simulation_run_id, "sim_saved_1");
  });

  assert.equal(pythonCalls.length, 1);
  assert.equal(pythonCalls[0].args[0], "performance-query");
  assert.ok(pythonCalls[0].args.includes("--max-final-rank"));
  assert.ok(pythonCalls[0].args.includes("2"));
});

test("POST /api/performance/query forwards grouped comparison queries to the Snowflake bridge", async () => {
  const pythonCalls = [];
  const savedQueries = [];
  const app = createApp({
    appStore: {
      listSimulationRuns: async () => [
        {
          simulation_run_id: "sim_group_1",
          requested_at: "2026-04-20T18:00:00Z",
          hero_context_hash: "ctx_hash_a"
        },
        {
          simulation_run_id: "sim_group_2",
          requested_at: "2026-04-20T18:30:00Z",
          hero_context_hash: "ctx_hash_b"
        }
      ],
      createSavedQuery: async (payload) => {
        savedQueries.push(payload);
        return {
          saved_query_id: "saved_query_grouped",
          name: payload.name,
          query_type: payload.queryType,
          filters: payload.filters
        };
      }
    },
    runPythonModule: async (moduleName, args) => {
      pythonCalls.push({ moduleName, args });
      return {
        stdout: JSON.stringify({
          status: "ready",
          matched_run_count: 2,
          summary: {
            run_count: 2,
            total_hands: 240,
            total_bb_won: 28.0,
            avg_bb_per_100: 11.6,
            first_place_rate: 0.5
          },
          rows: [
            {
              simulation_run_id: "sim_group_1",
              cluster_label: "LAG",
              profile_status: "ready",
              total_bb_won: 20.0
            }
          ],
          comparison_rows: [
            {
              cluster_label: "LAG",
              run_count: 1,
              total_bb_won: 20.0,
              avg_bb_per_100: 14.3,
              ready_profiles: 1
            }
          ],
          breakdowns: {
            by_backend: [{ decision_backend: "llm", run_count: 1 }],
            by_finish_rank: [{ final_rank: 1, run_count: 1 }],
            by_profile_status: [{ profile_status: "ready", run_count: 1, ready_profiles: 1 }],
            by_cluster_label: [{ cluster_label: "LAG", run_count: 1, total_bb_won: 20.0 }]
          },
          profile_overview: {
            ready_profiles: 1,
            insufficient_evidence: 1,
            not_found: 0,
            other: 0
          },
          query: {
            group_by: ["cluster_label"],
            topn: 20
          }
        })
      };
    }
  });

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/performance/query?user_id=user_group_1`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        name: "Grouped clusters",
        save: true,
        topn: 20,
        group_by: ["cluster_label"],
        filters: {
          hero_context_hashes: ["ctx_hash_a"],
          profile_statuses: ["ready"],
          min_total_bb_won: 5
        }
      })
    });

    assert.equal(response.status, 201);
    const payload = await response.json();
    assert.equal(payload.comparison_rows.length, 1);
    assert.equal(payload.comparison_rows[0].cluster_label, "LAG");
    assert.equal(payload.query.group_by[0], "cluster_label");
  });

  assert.equal(savedQueries.length, 1);
  assert.deepEqual(savedQueries[0].filters.group_by, ["cluster_label"]);
  assert.equal(savedQueries[0].filters.topn, 20);
  assert.equal(pythonCalls.length, 1);
  assert.ok(pythonCalls[0].args.includes("--query-json"));
  const queryJsonIndex = pythonCalls[0].args.indexOf("--query-json");
  assert.ok(queryJsonIndex >= 0);
  const forwardedQuery = JSON.parse(pythonCalls[0].args[queryJsonIndex + 1]);
  assert.deepEqual(forwardedQuery.group_by, ["cluster_label"]);
  assert.deepEqual(forwardedQuery.filters.hero_context_hashes, ["ctx_hash_a"]);
  assert.deepEqual(forwardedQuery.filters.profile_statuses, ["ready"]);
});
