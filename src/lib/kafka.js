import { Kafka, logLevel } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

const clientId = process.env.KAFKA_CLIENT_ID || "real-time-analytics-api";
const topic = process.env.KAFKA_TOPIC || "analytics-events";
const shouldCreateTopic = process.env.KAFKA_CREATE_TOPIC !== "false";
const numPartitions = Number(process.env.KAFKA_TOPIC_PARTITIONS || 3);
const replicationFactor = Number(
  process.env.KAFKA_TOPIC_REPLICATION_FACTOR || 1
);

const kafka = new Kafka({
  clientId,
  brokers,
  logLevel: logLevel.INFO
});

const producer = kafka.producer({
  allowAutoTopicCreation: false
});

let producerConnected = false;

export async function connectKafka() {
  if (producerConnected) return;

  if (shouldCreateTopic) {
    const admin = kafka.admin();

    try {
      await admin.connect();

      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic,
            numPartitions,
            replicationFactor
          }
        ]
      });
    } finally {
      await admin.disconnect();
    }
  }

  await producer.connect();
  producerConnected = true;

  console.log(
    `Kafka producer connected. brokers=${brokers.join(",")} topic=${topic}`
  );
}

export async function publishEvents(events) {
  if (!producerConnected) {
    throw new Error("Kafka producer is not connected");
  }

  if (!Array.isArray(events) || events.length === 0) {
    return;
  }

  await producer.send({
    topic,
    acks: -1,
    messages: events.map((event) => ({
      key: String(event.userId ?? ""),
      value: JSON.stringify(event),
      timestamp: String(Date.now()),
      headers: {
        hand_id: String(event.handId ?? ""),
        user_id: String(event.userId ?? ""),
        position: String(event.position ?? "")
      }
    }))
  });
}

export async function disconnectKafka() {
  if (!producerConnected) return;

  await producer.disconnect();
  producerConnected = false;
}

export { topic };