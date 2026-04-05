import express from "express";
import { publishEvents } from "../lib/kafka.js";

const router = express.Router();

function enrichPokerHandEvent(event, receivedAt, responseSentAt) {
  return {
    ...event,
    timestamp: event.timestamp || Date.now(),
    metrics: {
      received_at: receivedAt,
      response_sent_at: responseSentAt,
      request_to_handled_by_backend_latency_ms: responseSentAt - receivedAt
    }
  };
}

async function handleSinglePokerHand(req, res) {
  try {
    const receivedAt = Date.now();
    const event = req.body;

    if (!event || typeof event !== "object" || Array.isArray(event)) {
      return res.status(400).json({
        message: "Invalid payload"
      });
    }

    const responseSentAt = Date.now();
    const enrichedEvent = enrichPokerHandEvent(
      event,
      receivedAt,
      responseSentAt
    );

    console.log("Single poker hand received:", enrichedEvent);

    await publishEvents([enrichedEvent]);

    res.json({
      message: "Single poker hand received",
      event: enrichedEvent
    });
  } catch (error) {
    console.error("Failed to publish single poker hand to Kafka:", error);

    res.status(500).json({
      message: "Failed to publish poker hand to Kafka",
      error: error.message
    });
  }
}

async function handlePokerHandBatch(req, res) {
  try {
    const receivedAt = Date.now();
    const { events } = req.body;

    if (!Array.isArray(events)) {
      return res.status(400).json({
        message: "Payload must include an events array"
      });
    }

    const responseSentAt = Date.now();
    const enrichedEvents = events.map((event) =>
      enrichPokerHandEvent(event, receivedAt, responseSentAt)
    );

    console.log("Batch poker hands received:", {
      count: enrichedEvents.length,
      events: enrichedEvents
    });

    await publishEvents(enrichedEvents);

    res.json({
      message: "Batch poker hands received",
      count: enrichedEvents.length,
      events: enrichedEvents
    });
  } catch (error) {
    console.error("Failed to publish poker hand batch to Kafka:", error);

    res.status(500).json({
      message: "Failed to publish poker hand batch to Kafka",
      error: error.message
    });
  }
}

router.post("/poker-hands", handleSinglePokerHand);
router.post("/poker-hands/batch", handlePokerHandBatch);

export default router;