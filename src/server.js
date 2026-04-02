import dotenv from "dotenv";
import app from "./app.js";
import { connectKafka, disconnectKafka } from "./lib/kafka.js";

dotenv.config();

const PORT = process.env.PORT || 3000;

let server;

async function start() {
  await connectKafka();

  server = app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
  });
}

async function shutdown(signal) {
  console.log(`\nReceived ${signal}. Shutting down...`);

  try {
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