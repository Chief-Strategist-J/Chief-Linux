from __future__ import annotations

from typing import Any

from infrastructure.messaging.config.kafka_capacity_client import KafkaCapacityProfile


def build_topic_config(profile: KafkaCapacityProfile) -> dict[str, int]:
    kafka = profile.kafka
    return {
        "num_partitions": int(kafka["topic_partitions"]),
        "replication_factor": int(kafka["replication_factor"]),
        "min_insync_replicas": int(kafka["min_insync_replicas"]),
    }


def build_producer_config(profile: KafkaCapacityProfile, bootstrap_servers: str) -> dict[str, Any]:
    producer = profile.kafka["producer"]
    reliability = profile.kafka["reliability"]
    return {
        "bootstrap.servers": bootstrap_servers,
        "acks": producer["acks"],
        "batch.size": int(producer["batch_size"]),
        "linger.ms": int(producer["linger_ms"]),
        "compression.type": producer["compression_type"],
        "retries": int(reliability["retries"]),
        "retry.backoff.ms": int(reliability["retry_backoff_ms"]),
        "request.timeout.ms": int(reliability["request_timeout_ms"]),
    }


def build_consumer_config(
    profile: KafkaCapacityProfile,
    bootstrap_servers: str,
    group_id: str,
    auto_offset_reset: str = "latest",
) -> dict[str, Any]:
    consumer = profile.kafka["consumer"]
    reliability = profile.kafka["reliability"]
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "max.poll.records": int(consumer["max_poll_records"]),
        "fetch.min.bytes": int(consumer["fetch_min_bytes"]),
        "fetch.max.wait.ms": int(consumer["fetch_max_wait_ms"]),
        "request.timeout.ms": int(reliability["request_timeout_ms"]),
    }


def build_env_overrides(profile: KafkaCapacityProfile) -> dict[str, str]:
    topic = build_topic_config(profile)
    return {
        "KAFKA_TOPIC_PARTITIONS": str(topic["num_partitions"]),
        "KAFKA_REPLICATION_FACTOR": str(topic["replication_factor"]),
        "KAFKA_MIN_INSYNC_REPLICAS": str(topic["min_insync_replicas"]),
        "KAFKA_REQUIRED_RPS": f"{profile.required_rps:.2f}",
    }
