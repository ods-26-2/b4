from __future__ import annotations

import unittest
import uuid

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_request,
    publish_and_wait,
)


class S4IntegrationTests(unittest.TestCase):
    def test_query_for_unknown_clip_on_kafka_returns_empty_result(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        query = new_request(
            "ods.s4.clip.query",
            "query",
            "A4",
            "S4",
            {"clip_id": str(uuid.uuid4())},
        )
        try:
            reply = publish_and_wait(
                "ods.s4.clip.query",
                query,
                "ods.s4.clip.reply",
                lambda message: message.get("correlation_id") == query["message_id"],
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            reply,
            schema="ods.s4.clip.reply",
            message_type="reply",
            producer="S4",
            payload_fields=("status", "clips"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply S4 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["payload"]["status"], "OK")
        self.assertEqual(reply["payload"]["clips"], [])
