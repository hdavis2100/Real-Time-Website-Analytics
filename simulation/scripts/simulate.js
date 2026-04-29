const DEFAULTS = {
  baseUrl: "http://localhost:3000",
  hands: 500,
  seed: 42,
  heroSeat: 1,
  context: "balanced aggressive value bettor",
  decisionBackend: "llm",
  pollIntervalMs: 1000,
  timeoutMs: 600000
};

function parseArgs() {
  const args = process.argv.slice(2);
  const config = { ...DEFAULTS };

  for (const arg of args) {
    const [key, value] = arg.replace(/^--/, "").split("=");
    if (key === "baseUrl" && value) config.baseUrl = value;
    if (key === "hands" && value) config.hands = Number(value);
    if (key === "seed" && value) config.seed = Number(value);
    if (key === "heroSeat" && value) config.heroSeat = Number(value);
    if (key === "context" && value) config.context = value;
    if (key === "decisionBackend" && value) config.decisionBackend = value;
    if (key === "pollIntervalMs" && value) config.pollIntervalMs = Number(value);
    if (key === "timeoutMs" && value) config.timeoutMs = Number(value);
  }

  return config;
}

async function pollSimulation(baseUrl, runId, { pollIntervalMs, timeoutMs }) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const response = await fetch(`${baseUrl}/api/simulations/${runId}`);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Simulation status failed: ${response.status} ${text}`);
    }

    const payload = await response.json();
    if (payload.status === "completed") {
      return payload;
    }
    if (payload.status === "failed") {
      throw new Error(JSON.stringify(payload, null, 2));
    }

    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  throw new Error(`Timed out waiting for simulation ${runId}`);
}

async function run(config) {
  const runId = `sim_api_${Date.now()}`;
  const requestBody = {
    simulation_run_id: runId,
    hand_count: config.hands,
    seed: config.seed,
    small_blind_bb: 0.5,
    big_blind_bb: 1.0,
    starting_stack_bb: 100,
    context: config.context,
    hero_seat: config.heroSeat,
    decision_backend: config.decisionBackend
  };

  const response = await fetch(`${config.baseUrl}/api/simulations`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(requestBody)
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Simulation request failed: ${response.status} ${text}`);
  }

  const payload = await response.json();
  const finalPayload = await pollSimulation(config.baseUrl, runId, config);
  console.log(JSON.stringify({ queued: payload, final: finalPayload }, null, 2));
}

run(parseArgs()).catch((error) => {
  console.error(error.message);
  process.exit(1);
});
