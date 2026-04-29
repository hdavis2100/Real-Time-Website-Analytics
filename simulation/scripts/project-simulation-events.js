import dotenv from "dotenv";
import { Kafka, logLevel } from "kafkajs";

import { bootstrapAppDatabase, closeAppDatabase } from "../src/lib/app-db.js";
import { getAppStore } from "../src/lib/app-store.js";
import { projectSimulationLifecycleEvent } from "../src/lib/simulation-event-projector.js";

dotenv.config();

function parseBoolean(value, fallback = false) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }
  return ["1", "true", "t", "yes", "y"].includes(normalized);
}

function sleep(ms) {
  return new Promise((resolve) => {
    const handle = setTimeout(resolve, ms);
    handle.unref?.();
  });
}

function parseArgs() {
  const args = process.argv.slice(2);
  const config = {
    topic:
      process.env.KAFKA_TOPIC_SIMULATION_EVENTS || "poker.simulation_events",
    groupId:
      process.env.KAFKA_SIMULATION_EVENT_PROJECTOR_GROUP_ID ||
      "poker-simulation-event-projector",
    clientId:
      process.env.KAFKA_CLIENT_ID ||
      "poker-simulation-event-projector",
    fromBeginning: parseBoolean(
      process.env.KAFKA_SIMULATION_EVENT_PROJECTOR_FROM_BEGINNING,
      false
    )
  };

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    const [key, inlineValue] = arg.replace(/^--/, "").split("=");
    const nextValue = inlineValue ?? args[index + 1];
    if (key === "topic" && nextValue) {
      config.topic = nextValue;
      if (inlineValue == null) {
        index += 1;
      }
    } else if (key === "group-id" && nextValue) {
      config.groupId = nextValue;
      if (inlineValue == null) {
        index += 1;
      }
    } else if (key === "client-id" && nextValue) {
      config.clientId = nextValue;
      if (inlineValue == null) {
        index += 1;
      }
    } else if (key === "from-beginning") {
      config.fromBeginning = inlineValue == null
        ? true
        : parseBoolean(inlineValue, true);
    }
  }

  return config;
}

async function run() {
  const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
  const config = parseArgs();

  if (!brokers.length) {
    throw new Error("KAFKA_BROKERS must include at least one broker");
  }

  await bootstrapAppDatabase();
  const appStore = getAppStore();
  if (!appStore) {
    throw new Error("App database is not configured");
  }

  const kafka = new Kafka({
    clientId: config.clientId,
    brokers,
    logLevel: logLevel.INFO
  });
  let consumer = null;
  let stopping = false;
  const retryBaseMs = 2000;
  const retryMaxMs = 30000;

  const shutdown = async () => {
    stopping = true;
    await consumer?.disconnect().catch(() => null);
    await closeAppDatabase().catch(() => null);
  };

  process.on("SIGINT", async () => {
    await shutdown();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    await shutdown();
    process.exit(0);
  });

  let attempt = 0;
  while (!stopping) {
    consumer = kafka.consumer({
      groupId: config.groupId
    });
    let removeCrashListener = null;
    let removeDisconnectListener = null;
    let removeStopListener = null;
    try {
      const terminalEvent = new Promise((resolve) => {
        removeCrashListener = consumer.on(consumer.events.CRASH, ({ payload }) => {
          resolve({
            type: "crash",
            error: payload?.error || new Error("Kafka consumer crashed")
          });
        });
        removeDisconnectListener = consumer.on(
          consumer.events.DISCONNECT,
          () => {
            resolve({
              type: "disconnect"
            });
          }
        );
        removeStopListener = consumer.on(consumer.events.STOP, () => {
          resolve({
            type: "stop"
          });
        });
      });
      await consumer.connect();
      await consumer.subscribe({
        topic: config.topic,
        fromBeginning: config.fromBeginning
      });

      console.log(
        `[simulation-event-projector] listening to ${config.topic} with group ${config.groupId}`
      );
      attempt = 0;

      await consumer.run({
        eachMessage: async ({ partition, message }) => {
          const rawValue = message.value?.toString() || "";
          if (!rawValue) {
            return;
          }

          let payload = null;
          try {
            payload = JSON.parse(rawValue);
          } catch (error) {
            console.error(
              `[simulation-event-projector] failed to parse offset ${message.offset} on partition ${partition}: ${error.message}`
            );
            return;
          }

          const result = await projectSimulationLifecycleEvent(appStore, payload, {
            logger: console
          });
          if (!result.applied) {
            if (result.reason !== "unsupported_event") {
              console.warn(
                `[simulation-event-projector] skipped ${payload?.event_type || "unknown"} for ${payload?.simulation_run_id || "unknown"}: ${result.reason}`
              );
            }
            return;
          }

          console.log(
            `[simulation-event-projector] ${result.action} ${result.run?.simulation_run_id || result.event.simulationRunId} -> ${result.run?.status || result.event.status}`
          );
        }
      });

      attempt = 0;
      const outcome = await terminalEvent;
      if (stopping) {
        break;
      }
      if (outcome.type !== "stop" && outcome.type !== "disconnect") {
        throw outcome.error || new Error("Kafka consumer stopped unexpectedly");
      }
      break;
    } catch (error) {
      await consumer.disconnect().catch(() => null);
      consumer = null;
      if (stopping) {
        break;
      }
      attempt += 1;
      const retryDelayMs = Math.min(retryBaseMs * 2 ** Math.max(0, attempt - 1), retryMaxMs);
      console.warn(
        `[simulation-event-projector] retrying after startup/runtime failure in ${retryDelayMs}ms: ${error.message}`
      );
      await sleep(retryDelayMs);
    } finally {
      removeCrashListener?.();
      removeDisconnectListener?.();
      removeStopListener?.();
    }
  }
}

run().catch(async (error) => {
  console.error("[simulation-event-projector] fatal error:", error);
  await closeAppDatabase().catch(() => null);
  process.exit(1);
});
