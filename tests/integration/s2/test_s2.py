from __future__ import annotations

import unittest

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_request,
    publish_and_wait,
)


class S2IntegrationTests(unittest.TestCase):
    def test_invalid_rules_command_on_kafka_returns_correlated_error(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        command = new_request(
            "ods.s2.rules.command",
            "command",
            "A5",
            "S2",
            {
                "artifact_version": "b4-invalid-rules",
                "rules": [
                    {
                        "rule_id": "b4-invalid-rule",
                        "event_type": "B4_INVALID_EVENT_TYPE",
                        "min_confidence": 0.7,
                        "uncertain_below": 0.85,
                        "min_duration_ms": 1000,
                        "debounce_ms": 0,
                    }
                ],
            },
        )
        try:
            reply = publish_and_wait(
                "ods.s2.rules.command",
                command,
                "ods.s2.rules.reply",
                lambda message: message.get("correlation_id") == command["message_id"],
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            reply,
            schema="ods.s2.rules.reply",
            message_type="reply",
            producer="S2",
            payload_fields=("status", "error"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply S2 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["payload"]["status"], "ERROR")
        self.assertEqual(reply["payload"]["error"]["code"], "UNKNOWN_EVENT_TYPE")
