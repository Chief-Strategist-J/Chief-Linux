# Kafka Capacity Configuration

Kafka profiles are stored as one file per profile in `profiles/*.yaml`.

`kafka_capacity_client.py` selects profiles using:

- `--profile <name>` (highest priority)
- `KAFKA_CAPACITY_PROFILE` environment variable
- fallback: `standard`

## CLI Usage

```bash
python3 infrastructure/messaging/config/kafka_capacity_client.py --list
python3 infrastructure/messaging/config/kafka_capacity_client.py --profile 1m-in-10m
```

## API Usage

Run API server:

```bash
python3 infrastructure/messaging/api/kafka_capacity_api.py --host 0.0.0.0 --port 8080
```

Endpoints:

- `GET /health`
- `GET /profiles`
- `GET /profiles/{name}`

## How Kafka gets configured

These profile files do not change brokers automatically by themselves.
You must read a selected profile and apply values in your deployment/runtime code.

`kafka_runtime_config.py` converts a selected profile into:

- topic settings (`num_partitions`, `replication_factor`, `min_insync_replicas`)
- producer client config (`acks`, `batch.size`, `linger.ms`, etc.)
- consumer client config (`max.poll.records`, fetch settings)
- environment overrides for deployment pipelines

Example:

```python
from infrastructure.messaging.config.kafka_capacity_client import KafkaCapacityClient
from infrastructure.messaging.config.kafka_runtime_config import (
    build_consumer_config,
    build_producer_config,
    build_topic_config,
)

profile = KafkaCapacityClient().load("1m-in-10m")
topic_config = build_topic_config(profile)
producer_config = build_producer_config(profile, "kafka:9092")
consumer_config = build_consumer_config(profile, "kafka:9092", "events-group")
```
