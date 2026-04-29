import { Kafka, logLevel } from "kafkajs";
const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

const clientId = process.env.KAFKA_CLIENT_ID || "poker-action-platform-api";
const shouldCreateTopic = process.env.KAFKA_CREATE_TOPIC !== "false";
const numPartitions = Number(process.env.KAFKA_TOPIC_PARTITIONS || 6);
const replicationFactor = Number(
  process.env.KAFKA_TOPIC_REPLICATION_FACTOR || 1
);

const topics = {
  simulationRequests:
    process.env.KAFKA_TOPIC_SIMULATION_REQUESTS || "poker.simulation_requests",
  actions: process.env.KAFKA_TOPIC_ACTIONS || "poker.actions",
  handSummaries:
    process.env.KAFKA_TOPIC_HAND_SUMMARIES || "poker.hand_summaries",
  simulationEvents:
    process.env.KAFKA_TOPIC_SIMULATION_EVENTS || "poker.simulation_events"
};

const kafka = new Kafka({
  clientId,
  brokers,
  logLevel: logLevel.INFO
});

const producer = kafka.producer({
  allowAutoTopicCreation: false
});

let producerConnected = false;

async function ensureTopic(admin, topicName) {
  const metadata = await admin.fetchTopicMetadata({
    topics: [topicName]
  });
  const topicMetadata = metadata.topics.find(
    (topicInfo) => topicInfo.name === topicName
  );

  if (!topicMetadata) {
    throw new Error(`Topic metadata for ${topicName} was not returned by Kafka`);
  }

  const currentPartitionCount = topicMetadata.partitions.length;

  if (currentPartitionCount >= numPartitions) {
    return;
  }

  await admin.createPartitions({
    topicPartitions: [
      {
        topic: topicName,
        count: numPartitions
      }
    ],
    validateOnly: false,
    waitForLeaders: true
  });
}

async function ensureTopics(admin) {
  const topicNames = Object.values(topics);
  await admin.createTopics({
    waitForLeaders: true,
    topics: topicNames.map((topicName) => ({
      topic: topicName,
      numPartitions,
      replicationFactor
    }))
  });

  for (const topicName of topicNames) {
    await ensureTopic(admin, topicName);
  }
}

export async function connectKafka() {
  if (producerConnected) return;

  if (shouldCreateTopic) {
    const admin = kafka.admin();

    try {
      await admin.connect();
      await ensureTopics(admin);
    } finally {
      await admin.disconnect();
    }
  }

  await producer.connect();
  producerConnected = true;
}

function stableSimulationRequestPartition(runId) {
  const partitionCount = Number.isInteger(numPartitions) && numPartitions > 0
    ? numPartitions
    : 1;
  const text = String(runId || "simulation");
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash % partitionCount;
}

async function publishRecords(
  topicName,
  records,
  { keyBuilder, headerBuilder, partitionBuilder } = {}
) {
  if (!Array.isArray(records) || records.length === 0) {
    return 0;
  }

  if (!producerConnected) {
    throw new Error(`Kafka producer is not connected for topic ${topicName}`);
  }

  await producer.send({
    topic: topicName,
    acks: -1,
    messages: records.map((record, index) => {
      const partition = partitionBuilder?.(record, index);
      const message = {
        key: String(keyBuilder?.(record) || record.hand_id || record.handId || ""),
        value: JSON.stringify(record),
        timestamp: String(Date.now()),
        headers: headerBuilder?.(record)
      };
      if (Number.isInteger(partition) && partition >= 0) {
        message.partition = partition;
      }
      return message;
    })
  });

  return records.length;
}

function handScopedKey(record) {
  const runId =
    record.simulation_run_id ||
    record.simulationRunId ||
    record.source_run_id ||
    record.sourceRunId ||
    "unknown-run";
  const tableId = record.table_id || record.tableId || "table_1";
  const handId = record.hand_id || record.handId || "unknown-hand";
  return `${runId}|${tableId}|${handId}`;
}

export async function publishActionEvents(events) {
  return publishRecords(topics.actions, events, {
    keyBuilder: handScopedKey,
    headerBuilder: (event) => ({
      event_type: "action",
      hand_id: String(event.hand_id || event.handId || ""),
      player_id: String(event.player_id || event.playerId || ""),
      street: String(event.street || "")
    })
  });
}

export async function publishSimulationRequests(requests) {
  // Keep a run id on the same partition across app restarts. This still spreads
  // distinct run names, but avoids a reused id being routed behind unrelated
  // in-flight work because an in-memory round-robin cursor reset.
  return publishRecords(topics.simulationRequests, requests, {
    keyBuilder: (request) =>
      String(request.simulation_run_id || request.simulationRunId || "simulation"),
    partitionBuilder: (request) =>
      stableSimulationRequestPartition(
        request.simulation_run_id || request.simulationRunId
      ),
    headerBuilder: () => ({
      event_type: "simulation_request"
    })
  });
}

export async function publishHandSummaries(summaries) {
  return publishRecords(topics.handSummaries, summaries, {
    keyBuilder: handScopedKey,
    headerBuilder: (summary) => ({
      event_type: "hand_summary",
      hand_id: String(summary.hand_id || summary.handId || "")
    })
  });
}

export async function publishSimulationEvents(events) {
  return publishRecords(topics.simulationEvents, events, {
    keyBuilder: (event) =>
      String(event.simulation_run_id || event.simulationRunId || "simulation"),
    headerBuilder: () => ({
      event_type: "simulation_event"
    })
  });
}

export async function disconnectKafka() {
  if (!producerConnected) return;

  await producer.disconnect();
  producerConnected = false;
}

export function isKafkaConnected() {
  return producerConnected;
}

export { topics };
