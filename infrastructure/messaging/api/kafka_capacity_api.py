#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from infrastructure.messaging.config.kafka_capacity_client import KafkaCapacityClient

ROUTE_HEALTH = "/health"
ROUTE_PROFILES = "/profiles"


class KafkaCapacityRequestHandler(BaseHTTPRequestHandler):
    client = KafkaCapacityClient()

    def do_GET(self) -> None:  # noqa: N802
        parsed_path = urlparse(self.path)
        normalized_path = parsed_path.path.rstrip("/") or "/"

        if normalized_path == ROUTE_HEALTH:
            self._send_json(HTTPStatus.OK, {"status": "ok"})
            return

        if normalized_path == ROUTE_PROFILES:
            self._send_json(HTTPStatus.OK, {"profiles": self.client.available_profiles()})
            return

        if normalized_path.startswith(f"{ROUTE_PROFILES}/"):
            profile_name = normalized_path.split("/", 2)[2]
            try:
                profile = self.client.load(profile_name)
            except KeyError as error:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": str(error)})
                return
            self._send_json(HTTPStatus.OK, profile.to_dict())
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_server(host: str = "0.0.0.0", port: int = 8080) -> None:
    server = ThreadingHTTPServer((host, port), KafkaCapacityRequestHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka capacity profile API")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    run_server(args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
