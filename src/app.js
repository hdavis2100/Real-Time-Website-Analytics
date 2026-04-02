import express from "express";
import cors from "cors";
import morgan from "morgan";
import eventsRouter from "./routes/events.js";

const app = express();

app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

app.get("/", (req, res) => {
  res.status(200).json({
    message: "Real-time analytics ingestion API is running"
  });
});

app.use("/api", eventsRouter);

export default app;