# Real-Time Analytics Pipeline

## Overview

This project is a simulated real-time website analytics system for a data management course.

We will generate stateless events while putting random/varying levels of stress on our server. We will record associated latency metrics/event details in kafka, and use flink to create a real time dashboard showcasing things like end to end latency and other metrics. We will run schedule ETL job with airflow to clean and load our data from kafka to Snowflake, and run some aggregations/higher-scale analytics in snowflake.
---

## Event Format

### Single event request

```json
{
  "event_type": "page_view",
  "timestamp": 1710000004000,
  "region": "us-east"
}

---

## Setup

1. Install Node.js and Docker Desktop
2. Clone the repo
3. Run `npm install`
4. Run `cp .env.example .env`
5. Start Kafka: `docker compose -f docker-compose.kafka.yml up -d`
6. Start the API: `npm run start`

Optional:
- Consume Kafka messages: `npm run consume`
- Send test traffic: `npm run simulate`