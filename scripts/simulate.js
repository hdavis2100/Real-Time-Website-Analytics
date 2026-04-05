// Generates synthetic poker hand events and sends them to the server at a specified rate.
const DEFAULTS = {
  baseUrl: "http://localhost:3000",
  rps: 10,
  logEvery: 5
};

let shouldStop = false;
let nextHandId = 1;

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

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function buildDeck() {
  const ranks = ["A", "K", "Q", "J", "T", "9", "8", "7", "6", "5", "4", "3", "2"];
  const suits = ["s", "h", "d", "c"];
  const deck = [];

  for (const rank of ranks) {
    for (const suit of suits) {
      deck.push(`${rank}${suit}`);
    }
  }

  return deck;
}

function drawStartingHand() {
  const deck = buildDeck();

  const firstIndex = randomInt(0, deck.length - 1);
  const firstCard = deck.splice(firstIndex, 1)[0];

  const secondIndex = randomInt(0, deck.length - 1);
  const secondCard = deck[secondIndex];

  return `${firstCard}${secondCard}`;
}

function getPositions(playerCount) {
  if (playerCount <= 2) return ["SB", "BB"];
  if (playerCount === 3) return ["BTN", "SB", "BB"];
  if (playerCount === 4) return ["CO", "BTN", "SB", "BB"];
  if (playerCount === 5) return ["HJ", "CO", "BTN", "SB", "BB"];
  return ["UTG", "HJ", "CO", "BTN", "SB", "BB"];
}

function generateStakePlayed() {
  const examples = [
    "1sb",
    "1 sb",
    "1bb",
    "1 bb",
    "2bb",
    "2 bb",
    "2.5bb",
    "2.5 bb",
    "3bb",
    "3 bb",
    "4bb",
    "4 bb",
    "5bb",
    "5 bb",
    "10bb"
  ];

  return randomChoice(examples);
}

function generateOutcome() {
  const possibleOutcomes = [
    -20,
    -15,
    -12,
    -10,
    -8,
    -6,
    -5,
    -4,
    -3,
    -2,
    -1.5,
    -1,
    -0.5,
    0,
    0.5,
    1,
    1.5,
    2,
    3,
    4,
    5,
    6,
    8,
    10,
    12,
    15,
    20
  ];

  return randomChoice(possibleOutcomes);
}

function generateHandId() {
  const handId = `hand_${nextHandId}`;
  nextHandId += 1;
  return handId;
}

function generateEvent() {
  const numberOfPlayers = randomInt(2, 6);
  const positions = getPositions(numberOfPlayers);

  return {
    handId: generateHandId(),
    userId: `user_${randomInt(1, 1000000)}`,
    startingHand: drawStartingHand(),
    position: randomChoice(positions),
    numberOfPlayers,
    stakePlayed: generateStakePlayed(),
    outcome: generateOutcome(),
    timestamp: Date.now()
  };
}

async function sendEvent(baseUrl) {
  const event = generateEvent();

  const res = await fetch(`${baseUrl}/api/poker-hands`, {
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
  return new Promise((resolve) => setTimeout(resolve, ms));
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

run(parseArgs()).catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});