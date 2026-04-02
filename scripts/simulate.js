// Generates synthetic events and sends them to the server at a specified rate.
const DEFAULTS = {
  baseUrl: "http://localhost:3000",
  rps: 10,
  logEvery: 5
};

let shouldStop = false;

function parseArgs() {
  const args = process.argv.slice(2);
  const config = { ...DEFAULTS };

  for (const arg of args) {
    const [key, value] = arg.replace(/^--/, "").split("=");

    if (key === "baseUrl" && value) config.baseUrl = value;
    if (key === "rps" && value) config.rps = Number(value);
    if (key === "logEvery" && value) config.logEvery = Number(value);
  }

  return config;
}

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateEvent() {
  const eventTypes = ["page_view", "click", "purchase", "signup"];
  const regions = ["us-east", "us-west", "eu-central", "ap-south"];

  return {
    event_type: randomChoice(eventTypes),
    timestamp: Date.now(),
    region: randomChoice(regions)
  };
}

async function sendEvent(baseUrl) {
  const event = generateEvent();

  const res = await fetch(`${baseUrl}/api/events`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(event)
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed: ${res.status} ${text}`);
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function printStats(stats, startedAt) {
  const elapsedSeconds = Math.max(1, (Date.now() - startedAt) / 1000);
  const totalRequests = stats.success + stats.failed;
  const actualRps = (totalRequests / elapsedSeconds).toFixed(2);

  console.log("\n--- stats ---");
  console.log(`elapsed: ${elapsedSeconds.toFixed(1)}s`);
  console.log(`successful requests: ${stats.success}`);
  console.log(`failed requests: ${stats.failed}`);
  console.log(`actual request rate: ${actualRps}/sec`);
}

async function run(config) {
  const delayMs = 1000 / config.rps;
  const stats = {
    success: 0,
    failed: 0
  };

  const startedAt = Date.now();
  let lastLogAt = Date.now();

  console.log("Starting simulator with config:");
  console.log(config);
  console.log("Press Ctrl+C to stop.\n");

  while (!shouldStop) {
    const loopStartedAt = Date.now();

    try {
      await sendEvent(config.baseUrl);
      stats.success += 1;
    } catch (err) {
      stats.failed += 1;
      console.error(`[error] ${err.message}`);
    }

    const now = Date.now();

    if (now - lastLogAt >= config.logEvery * 1000) {
      printStats(stats, startedAt);
      lastLogAt = now;
    }

    const elapsedThisLoop = Date.now() - loopStartedAt;
    const remainingDelay = Math.max(0, delayMs - elapsedThisLoop);
    await sleep(remainingDelay);
  }

  console.log("\nStopping simulator...");
  printStats(stats, startedAt);
}

process.on("SIGINT", () => {
  shouldStop = true;
});

process.on("SIGTERM", () => {
  shouldStop = true;
});

run(parseArgs()).catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});