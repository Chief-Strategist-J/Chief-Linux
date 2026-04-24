#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PROFILE = "standard"
PROFILES_DIRNAME = "profiles"


@dataclass
class KafkaCapacityProfile:
    name: str
    target_total_requests: int
    target_window_seconds: int
    headroom_factor: float
    kafka: dict[str, Any]

    @property
    def required_rps(self) -> float:
        return (self.target_total_requests / self.target_window_seconds) * self.headroom_factor

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "target_total_requests": self.target_total_requests,
            "target_window_seconds": self.target_window_seconds,
            "headroom_factor": self.headroom_factor,
            "required_rps": self.required_rps,
            "kafka": self.kafka,
        }


class KafkaCapacityClient:
    def __init__(self, base_path: Path | None = None) -> None:
        self.base_path = base_path or Path(__file__).resolve().parent
        self.profiles_dir = self.base_path / PROFILES_DIRNAME

    def load(self, profile: str | None = None) -> KafkaCapacityProfile:
        selected = profile or os.getenv("KAFKA_CAPACITY_PROFILE", DEFAULT_PROFILE)
        profile_data = self._load_from_directory(selected)
        return KafkaCapacityProfile(
            name=selected,
            target_total_requests=int(profile_data["target_total_requests"]),
            target_window_seconds=int(profile_data["target_window_seconds"]),
            headroom_factor=float(profile_data["headroom_factor"]),
            kafka=dict(profile_data["kafka"]),
        )

    def available_profiles(self) -> list[str]:
        if not self.profiles_dir.exists():
            return []
        return sorted(profile_file.stem for profile_file in self.profiles_dir.glob("*.yaml"))

    def _load_from_directory(self, profile: str) -> dict[str, Any]:
        profile_file = self.profiles_dir / f"{profile}.yaml"
        if not profile_file.exists():
            available = ", ".join(self.available_profiles())
            raise KeyError(
                f"Profile '{profile}' not found in {self.profiles_dir}. Available profiles: {available}"
            )
        return self._read_yaml_file(profile_file)

    @staticmethod
    def _read_yaml_file(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with path.open("r", encoding="utf-8") as file_handle:
            data = yaml.safe_load(file_handle) or {}

        if not isinstance(data, dict):
            raise ValueError(f"YAML top-level object must be a mapping: {path}")
        return data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Kafka capacity profile")
    parser.add_argument("--profile", help="Profile name (default: env KAFKA_CAPACITY_PROFILE or standard)")
    parser.add_argument("--list", action="store_true", help="List available profiles")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    client = KafkaCapacityClient()

    if args.list:
        for profile_name in client.available_profiles():
            print(profile_name)
        return 0

    selected_profile = client.load(args.profile)
    print(json.dumps(selected_profile.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
