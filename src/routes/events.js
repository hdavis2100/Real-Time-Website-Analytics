import express from "express";

const router = express.Router();

router.post("/events", (req, res) => {
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

  // TODO: publish enrichedEvent to Kafka
  // TODO: add response contract once ingestion behavior is finalized
  res.json({
    message: "Single event received",
    event: enrichedEvent
  });
});

router.post("/events/batch", (req, res) => {
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

  // TODO: publish enrichedEvents to Kafka
  // TODO: add response contract once batch ingestion behavior is finalized
  res.json({
    message: "Batch received",
    count: enrichedEvents.length,
    events: enrichedEvents
  });
});

export default router;