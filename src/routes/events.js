import express from "express";

const router = express.Router();

router.post("/events", (req, res) => {
  res.status(501).json({
    message: "Event ingestion not implemented yet"
  });
});

router.post("/events/batch", (req, res) => {
  res.status(501).json({
    message: "Batch event ingestion not implemented yet"
  });
});

export default router;