import dotenv from "dotenv";

import { bootstrapAppDatabase, hasAppDatabaseConfig } from "../src/lib/app-db.js";

dotenv.config();

async function main() {
  if (!hasAppDatabaseConfig()) {
    console.log("APP_DATABASE_URL is not configured. Skipping app database bootstrap.");
    return;
  }

  await bootstrapAppDatabase();
  console.log("App database schema is ready.");
}

main().catch((error) => {
  console.error("Failed to bootstrap app database:", error);
  process.exit(1);
});
