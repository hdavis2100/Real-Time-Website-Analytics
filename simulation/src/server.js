import dotenv from "dotenv";
import app from "./app.js";
import { bootstrapAppDatabase } from "./lib/app-db.js";
import { createFinalizingRunReconciler } from "./lib/finalizing-run-reconciler.js";
import { connectKafka, disconnectKafka } from "./lib/kafka.js";

dotenv.config();

const PORT = process.env.PORT || 3000;

let server;
let finalizingRunReconciler;

// Bring up external dependencies first so the API only starts accepting traffic
// once it can actually publish incoming events to Kafka.
async function start() {
  process.env.POKER_PLATFORM_SKIP_SCHEMA_BOOTSTRAP = "1";
  await bootstrapAppDatabase();
  await connectKafka();
  finalizingRunReconciler = createFinalizingRunReconciler();
  finalizingRunReconciler.start();

  server = app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
  });
}

// Close the HTTP listener before disconnecting Kafka so in-flight requests stop
// entering the system while the producer is draining.
async function shutdown(signal) {
  console.log(`\nReceived ${signal}. Shutting down...`);

  try {
    if (finalizingRunReconciler) {
      await finalizingRunReconciler.stop();
    }

    if (server) {
      await new Promise((resolve, reject) => {
        server.close((err) => {
          if (err) reject(err);
          else resolve();
        });
      });
    }

    await disconnectKafka();
    process.exit(0);
  } catch (error) {
    console.error("Shutdown error:", error);
    process.exit(1);
  }
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

start().catch((error) => {
  console.error("Failed to start server:", error);
  process.exit(1);
});
