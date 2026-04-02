import express from "express";
import { publishEvents } from "../lib/kafka.js";

const router = express.Router();

router.post("/events", async (req, res) => {
  try {
    const receivedAt = Date.now();
    const event = req.body;

    const responseSentAt = Date.now();
    const requestToHandledByBackendLatencyMs = responseSentAt - receivedAt;

    const enrichedEvent = {
      event_type: event.event_type,
      timestamp: event.timestamp,
      region: event.region,
      metrics: {
        received_at: receivedAt,
        response_sent_at: responseSentAt,
        request_to_handled_by_backend_latency_ms:
          requestToHandledByBackendLatencyMs
      }
    };

    console.log("Single event received:", enrichedEvent);

    await publishEvents([enrichedEvent]);

    res.json({
      message: "Single event received",
      event: enrichedEvent
    });
  } catch (error) {
    console.error("Failed to publish single event to Kafka:", error);

    res.status(500).json({
      message: "Failed to publish event to Kafka",
      error: error.message
    });
  }
});

router.post("/events/batch", async (req, res) => {
  try {
    const receivedAt = Date.now();
    const { events } = req.body;

    const responseSentAt = Date.now();
    const requestToHandledByBackendLatencyMs = responseSentAt - receivedAt;

    const enrichedEvents = (events || []).map((event) => ({
      event_type: event.event_type,
      timestamp: event.timestamp,
      region: event.region,
      metrics: {
        received_at: receivedAt,
        response_sent_at: responseSentAt,
        request_to_handled_by_backend_latency_ms:
          requestToHandledByBackendLatencyMs
      }
    }));

    console.log("Batch events received:", {
      count: enrichedEvents.length,
      events: enrichedEvents
    });

    await publishEvents(enrichedEvents);

    res.json({
      message: "Batch received",
      count: enrichedEvents.length,
      events: enrichedEvents
    });
  } catch (error) {
    console.error("Failed to publish batch to Kafka:", error);

    res.status(500).json({
      message: "Failed to publish batch to Kafka",
      error: error.message
    });
  }
});

export default router;