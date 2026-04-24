import os
import tempfile
import unittest
from pathlib import Path

from infrastructure.messaging.config.kafka_capacity_client import (
    DEFAULT_PROFILE,
    KafkaCapacityClient,
)


class KafkaCapacityClientTest(unittest.TestCase):
    def test_lists_profiles(self):
        client = KafkaCapacityClient()
        profiles = client.available_profiles()
        self.assertIn("standard", profiles)
        self.assertIn("1m-in-10m", profiles)

    def test_load_profile(self):
        client = KafkaCapacityClient()
        profile = client.load("1m-in-10m")
        self.assertEqual(profile.name, "1m-in-10m")
        self.assertEqual(profile.kafka["topic_partitions"], 48)
        self.assertGreater(profile.required_rps, 0)

    def test_default_profile_from_env_fallback(self):
        original = os.environ.get("KAFKA_CAPACITY_PROFILE")
        try:
            os.environ.pop("KAFKA_CAPACITY_PROFILE", None)
            profile = KafkaCapacityClient().load()
            self.assertEqual(profile.name, DEFAULT_PROFILE)
        finally:
            if original is not None:
                os.environ["KAFKA_CAPACITY_PROFILE"] = original

    def test_missing_profile_raises_key_error(self):
        with self.assertRaises(KeyError):
            KafkaCapacityClient().load("does-not-exist")

    def test_invalid_yaml_top_level_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            profiles = base / "profiles"
            profiles.mkdir(parents=True, exist_ok=True)
            (profiles / "broken.yaml").write_text("- not-a-map\n", encoding="utf-8")

            with self.assertRaises(ValueError):
                KafkaCapacityClient(base).load("broken")


if __name__ == "__main__":
    unittest.main()
