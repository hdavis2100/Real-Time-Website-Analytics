import express from "express";
import cors from "cors";
import morgan from "morgan";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createEventsRouter } from "./routes/events.js";

const srcDir = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(srcDir, "public");

export function createApp(deps = {}) {
  const app = express();

  // The API is the orchestration edge for action events, completed-run profile
  // reads, and persona-backed simulation runs.
  app.use(cors());
  app.use(express.json());
  app.use(morgan("dev"));

  app.get("/", (req, res) => {
    res.status(200).json({
      message: "Poker action platform API is running"
    });
  });

  app.get("/healthz", (req, res) => {
    res.status(200).json({
      ok: true
    });
  });

  app.use("/app/assets", express.static(publicDir, { index: false }));
  app.get(["/app", "/app/", "/app/login"], (_req, res) => {
    res.sendFile(path.join(publicDir, "index.html"));
  });

  app.use("/api", createEventsRouter(deps));
  return app;
}

const app = createApp();

export default app;
