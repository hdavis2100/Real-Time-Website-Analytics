import dotenv from "dotenv";
import { Kafka } from "kafkajs";

dotenv.config();

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

const topic = process.env.KAFKA_TOPIC || "analytics-events";

const kafka = new Kafka({
  clientId: "real-time-analytics-debug-consumer",
  brokers
});

const consumer = kafka.consumer({
  groupId: `real-time-analytics-debug-${Date.now()}`
});

async function run() {
  await consumer.connect();
  await consumer.subscribe({
    topics: [topic],
    fromBeginning: true
  });

  console.log(`Listening to Kafka topic: ${topic}`);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const value = message.value?.toString() || "";

      console.log("\n--- message ---");
      console.log("partition:", partition);
      console.log("offset:", message.offset);
      console.log("value:", JSON.parse(value));
    }
  });
}

run().catch(async (error) => {
  console.error("Consumer failed:", error);
  await consumer.disconnect();
  process.exit(1);
});

process.on("SIGINT", async () => {
  await consumer.disconnect();
  process.exit(0);
});