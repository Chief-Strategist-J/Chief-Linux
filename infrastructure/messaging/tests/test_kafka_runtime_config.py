import unittest

from infrastructure.messaging.config.kafka_capacity_client import KafkaCapacityClient
from infrastructure.messaging.config.kafka_runtime_config import (
    build_consumer_config,
    build_env_overrides,
    build_producer_config,
    build_topic_config,
)


class KafkaRuntimeConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.profile = KafkaCapacityClient().load("1m-in-10m")

    def test_build_topic_config(self) -> None:
        topic = build_topic_config(self.profile)
        self.assertEqual(topic["num_partitions"], 48)
        self.assertEqual(topic["replication_factor"], 3)

    def test_build_producer_config(self) -> None:
        producer = build_producer_config(self.profile, "kafka:9092")
        self.assertEqual(producer["bootstrap.servers"], "kafka:9092")
        self.assertEqual(producer["batch.size"], 131072)

    def test_build_consumer_config(self) -> None:
        consumer = build_consumer_config(self.profile, "kafka:9092", "events-group")
        self.assertEqual(consumer["group.id"], "events-group")
        self.assertEqual(consumer["max.poll.records"], 1000)

    def test_build_env_overrides(self) -> None:
        env = build_env_overrides(self.profile)
        self.assertEqual(env["KAFKA_TOPIC_PARTITIONS"], "48")
        self.assertIn("KAFKA_REQUIRED_RPS", env)

    def test_default_auto_offset_reset_latest(self) -> None:
        consumer = build_consumer_config(self.profile, "kafka:9092", "events-group")
        self.assertEqual(consumer["auto.offset.reset"], "latest")

    def test_custom_auto_offset_reset(self) -> None:
        consumer = build_consumer_config(
            self.profile,
            "kafka:9092",
            "events-group",
            auto_offset_reset="earliest",
        )
        self.assertEqual(consumer["auto.offset.reset"], "earliest")


if __name__ == "__main__":
    unittest.main()
