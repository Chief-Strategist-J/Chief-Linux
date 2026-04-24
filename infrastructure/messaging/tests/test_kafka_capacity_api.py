import json
import socket
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import urlopen

from infrastructure.messaging.api.kafka_capacity_api import KafkaCapacityRequestHandler


class KafkaCapacityApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        cls.port = sock.getsockname()[1]
        sock.close()

        cls.server = ThreadingHTTPServer(("127.0.0.1", cls.port), KafkaCapacityRequestHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _get_json(self, path: str) -> dict:
        with urlopen(f"http://127.0.0.1:{self.port}{path}") as response:
            return json.loads(response.read().decode("utf-8"))

    def test_health_endpoint(self) -> None:
        body = self._get_json("/health")
        self.assertEqual(body["status"], "ok")

    def test_profiles_endpoint(self) -> None:
        body = self._get_json("/profiles")
        self.assertIn("profiles", body)
        self.assertIn("standard", body["profiles"])

    def test_profile_endpoint(self) -> None:
        body = self._get_json("/profiles/1m-in-10m")
        self.assertEqual(body["name"], "1m-in-10m")
        self.assertEqual(body["kafka"]["topic_partitions"], 48)

    def test_missing_profile_returns_404(self) -> None:
        with self.assertRaises(HTTPError) as context:
            self._get_json("/profiles/not-found")
        self.assertEqual(context.exception.code, 404)

    def test_unknown_path_returns_404(self) -> None:
        with self.assertRaises(HTTPError) as context:
            self._get_json("/unknown")
        self.assertEqual(context.exception.code, 404)


if __name__ == "__main__":
    unittest.main()
