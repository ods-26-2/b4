from __future__ import annotations

import unittest

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_request,
    publish_and_wait,
)


class S6IntegrationTests(unittest.TestCase):
    def test_unknown_lexicon_command_on_kafka_returns_error(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        command = new_request(
            "ods.s6.synthesis.command",
            "command",
            "A6",
            "S6",
            {
                "glosses": ["B4_UNKNOWN_SIGN"],
                "speed_factor": 1.0,
                "lexicon_version": "b4-no-such-lexicon",
            },
        )
        try:
            reply = publish_and_wait(
                "ods.s6.synthesis.command",
                command,
                "ods.s6.synthesis.reply",
                lambda message: message.get("correlation_id") == command["message_id"],
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            reply,
            schema="ods.s6.synthesis.reply",
            message_type="reply",
            producer="S6",
            payload_fields=("status", "error"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply S6 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["payload"]["status"], "ERROR")
        self.assertEqual(reply["payload"]["error"]["code"], "UNKNOWN_LEXICON_VERSION")
