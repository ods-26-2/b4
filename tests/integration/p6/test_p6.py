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


class P6IntegrationTests(unittest.TestCase):
    def test_replay_command_on_kafka_returns_correlated_error_for_missing_recording(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        recording_id = f"b4-missing-{uuid.uuid4()}"
        command = new_request(
            "ods.p6.replay.command",
            "command",
            "B4",
            "P6",
            {
                "action": "SEEK",
                "recording_id": recording_id,
                "position_ms": 1,
            },
        )
        try:
            reply = publish_and_wait(
                "ods.p6.replay.command",
                command,
                "ods.p6.replay.reply",
                lambda message: message.get("correlation_id") == command["message_id"],
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            reply,
            schema="ods.p6.replay.reply",
            message_type="reply",
            producer="P6",
            payload_fields=("status",),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply P6 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["payload"]["status"], "ERROR")
        self.assertEqual(reply["payload"]["error"]["code"], "RECORDING_NOT_FOUND")
