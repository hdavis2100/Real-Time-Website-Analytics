from __future__ import annotations

"""Shared Kafka publishing helpers for Python-based replay and simulation."""

import json
from typing import Callable, Iterable


def build_producer(brokers: str):
    try:
        from kafka import KafkaProducer
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "kafka-python is required for Kafka publishing; install with pip install -e .[kafka]"
        ) from exc

    return KafkaProducer(
        bootstrap_servers=[broker.strip() for broker in brokers.split(",") if broker.strip()],
        acks="all",
        linger_ms=25,
        batch_size=131072,
        compression_type="gzip",
        retries=5,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )


def publish_records(
    *,
    producer,
    topic: str,
    records: Iterable[dict],
    key_builder: Callable[[dict], str],
    flush: bool = True,
) -> int:
    count = 0
    for record in records:
        producer.send(topic, key=key_builder(record), value=record)
        count += 1
    if flush:
        producer.flush()
    return count
