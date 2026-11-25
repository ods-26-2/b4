from __future__ import annotations

import json
import os
import sys
import types
import unittest
from unittest.mock import patch

from tests.integration.kafka_client import publish_and_wait


class KafkaClientTests(unittest.TestCase):
    def test_publishes_to_input_topic_and_waits_for_matching_reply(self):
        state: dict[str, object] = {}
        reply = {
            "schema": "ods.s3.alert.reply",
            "correlation_id": "command-id",
            "payload": {"status": "OK"},
        }

        class FakeRecord:
            def error(self):
                return None

            def value(self):
                return json.dumps(reply).encode("utf-8")

        class FakeConsumer:
            def __init__(self, config):
                state["consumer_config"] = config
                self.polled = False

            def subscribe(self, topics):
                state["subscribed_topics"] = topics

            def assignment(self):
                return ["partition"]

            def poll(self, _timeout):
                if self.polled:
                    return None
                self.polled = True
                return FakeRecord()

            def close(self):
                state["consumer_closed"] = True

        class FakeProducer:
            def __init__(self, config):
                state["producer_config"] = config

            def produce(self, topic, *, key, value, callback):
                state["produced"] = (topic, key, json.loads(value.decode("utf-8")))
                callback(None, None)

            def flush(self, _timeout):
                return 0

        fake_kafka = types.ModuleType("confluent_kafka")
        fake_kafka.Consumer = FakeConsumer
        fake_kafka.Producer = FakeProducer
        fake_kafka.KafkaError = type("KafkaError", (), {"_PARTITION_EOF": -191})
        request = {"message_id": "command-id", "payload": {}}
        with patch.dict(os.environ, {"ODS_KAFKA_BOOTSTRAP_SERVERS": "fake:9092"}):
            with patch.dict(sys.modules, {"confluent_kafka": fake_kafka}):
                result = publish_and_wait(
                    "ods.s3.alert.command",
                    request,
                    "ods.s3.alert.reply",
                    lambda message: message.get("correlation_id") == request["message_id"],
                    timeout=1,
                )

        self.assertEqual(state["subscribed_topics"], ["ods.s3.alert.reply"])
        self.assertEqual(
            state["produced"],
            ("ods.s3.alert.command", b"command-id", request),
        )
        self.assertEqual(result, reply)
        self.assertTrue(state["consumer_closed"])
