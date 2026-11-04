from __future__ import annotations

from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_COMPONENT_ROOT = WORKSPACE_ROOT / "b2"
COMPONENT_ROOT = Path(
    os.environ.get("B2_COMPONENT_ROOT", DEFAULT_COMPONENT_ROOT)
).resolve()


class FakeMqttClient:
    def __init__(self) -> None:
        self.subscriptions: list[str] = []
        self.published: list[tuple[str, str]] = []

    def subscribe(self, topic: str) -> None:
        self.subscriptions.append(topic)

    def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))


class FakeMqttMessage:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload


class B2IntegrationTests(unittest.TestCase):
    def test_time_query_produces_correlated_mqtt_reply(self) -> None:
        service_path = COMPONENT_ROOT / "src" / "main.py"
        if not service_path.is_file():
            self.skipTest(
                f"Implementação B2 ausente no checkout: {service_path}. "
                "Defina B2_COMPONENT_ROOT para testar outra revisão."
            )

        sys.path.insert(0, str(service_path.parent))
        try:
            with patch.dict(sys.modules):
                sys.modules.pop("main", None)
                sys.modules.pop("time_service", None)
                service = importlib.import_module("main")

                client = FakeMqttClient()
                service.on_connect(client, None, None, 0)
                self.assertEqual(client.subscriptions, [service.TOPIC_REQUEST])

                correlation_id = "b4-b2-correlation"
                request = {
                    "message_type": "query",
                    "producer": "B4",
                    "payload": {
                        "action": "get_global_time",
                        "correlation_id": correlation_id,
                    },
                }
                service.on_message(
                    client,
                    None,
                    FakeMqttMessage(json.dumps(request).encode("utf-8")),
                )
        finally:
            sys.path.remove(str(service_path.parent))

        self.assertEqual(len(client.published), 1)
        topic, raw_reply = client.published[0]
        self.assertEqual(topic, service.TOPIC_RESPONSE)
        reply = json.loads(raw_reply)
        self.assertEqual(reply["schema"], "ods.system.time")
        self.assertEqual(reply["schema_version"], "1.0")
        self.assertEqual(reply["message_type"], "reply")
        self.assertEqual(reply["producer"], "global-time-service")
        self.assertEqual(reply["payload"]["correlation_id"], correlation_id)
        self.assertEqual(reply["payload"]["status"], "success")

        capture_time = reply["payload"]["capture_time"]
        utc_timestamp = datetime.fromisoformat(
            capture_time["utc_timestamp"].replace("Z", "+00:00")
        )
        self.assertEqual(utc_timestamp.utcoffset(), timezone.utc.utcoffset(None))
        self.assertIsInstance(capture_time["epoch_ms"], int)
