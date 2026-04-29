import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";

import pg from "pg";

const { Pool } = pg;

let pool = null;
let bootstrapPromise = null;
let bootstrapped = false;

function schemaPath() {
  return fileURLToPath(
    new URL("../../../sql/ddl/postgres/create_tables.sql", import.meta.url)
  );
}

function resolveConnectionConfig() {
  const connectionString =
    process.env.APP_DATABASE_URL || process.env.DATABASE_URL || null;

  if (connectionString) {
    return { connectionString };
  }

  return {
    host: process.env.APP_DB_HOST || process.env.POSTGRES_HOST || process.env.PGHOST,
    port: Number(
      process.env.APP_DB_PORT ||
        process.env.POSTGRES_PORT ||
        process.env.PGPORT ||
        5432
    ),
    database:
      process.env.APP_DB_NAME ||
      process.env.POSTGRES_DB ||
      process.env.PGDATABASE ||
      "poker_app",
    user:
      process.env.APP_DB_USER ||
      process.env.POSTGRES_USER ||
      process.env.PGUSER ||
      "poker",
    password:
      process.env.APP_DB_PASSWORD ||
      process.env.POSTGRES_PASSWORD ||
      process.env.PGPASSWORD ||
      "poker"
  };
}

export function hasAppDatabaseConfig() {
  return Boolean(
    process.env.APP_DATABASE_URL ||
      process.env.DATABASE_URL ||
      process.env.APP_DB_HOST ||
      process.env.POSTGRES_HOST ||
      process.env.PGHOST
  );
}

export function getAppDatabase() {
  if (!hasAppDatabaseConfig()) {
    return null;
  }

  if (!pool) {
    pool = new Pool({
      ...resolveConnectionConfig(),
      max: Number(process.env.APP_DB_POOL_SIZE || 10),
      idleTimeoutMillis: Number(process.env.APP_DB_IDLE_TIMEOUT_MS || 10_000)
    });
    pool.on("error", (error) => {
      console.error("Postgres pool error:", error);
    });
  }

  return pool;
}

export async function query(text, params = []) {
  const db = getAppDatabase();
  if (!db) {
    throw new Error("App database is not configured");
  }
  return db.query(text, params);
}

export async function withTransaction(callback) {
  const db = getAppDatabase();
  if (!db) {
    throw new Error("App database is not configured");
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => null);
    throw error;
  } finally {
    client.release();
  }
}

export async function bootstrapAppDatabase() {
  if (!hasAppDatabaseConfig()) {
    return false;
  }
  if (bootstrapped) {
    return true;
  }

  if (!bootstrapPromise) {
    bootstrapPromise = (async () => {
      const sql = await fs.readFile(schemaPath(), "utf-8");
      if (!sql.trim()) {
        throw new Error("Postgres schema file is empty");
      }
      const db = getAppDatabase();
      await db.query(sql);
      bootstrapped = true;
      return true;
    })().catch((error) => {
      bootstrapPromise = null;
      throw error;
    });
  }

  return bootstrapPromise;
}

export async function closeAppDatabase() {
  if (!pool) {
    return;
  }
  const currentPool = pool;
  pool = null;
  bootstrapped = false;
  bootstrapPromise = null;
  await currentPool.end();
}
