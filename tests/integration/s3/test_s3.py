from __future__ import annotations

import unittest

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_request,
    publish_and_wait,
)


class S3IntegrationTests(unittest.TestCase):
    def test_invalid_alert_command_on_kafka_returns_correlated_error(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        command = new_request(
            "ods.s3.alert.command",
            "command",
            "A5",
            "S3",
            {
                "title": "B4 integration test",
                "message": "Invalid channel test; no alert should be created.",
                "severity": "MEDIUM",
                "recipients": ["group:portaria"],
                "channels": ["B4_INVALID_CHANNEL"],
            },
        )
        try:
            reply = publish_and_wait(
                "ods.s3.alert.command",
                command,
                "ods.s3.alert.reply",
                lambda message: message.get("correlation_id") == command["message_id"],
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            reply,
            schema="ods.s3.alert.reply",
            message_type="reply",
            producer="S3",
            payload_fields=("status", "error"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply S3 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["payload"]["status"], "ERROR")
        self.assertEqual(reply["payload"]["error"]["code"], "INVALID_CHANNEL")
