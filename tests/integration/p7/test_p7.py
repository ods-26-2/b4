from __future__ import annotations

import unittest

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_request,
    publish_and_wait,
)


class P7IntegrationTests(unittest.TestCase):
    def test_out_of_range_move_command_on_kafka_returns_error_without_motion(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        command = new_request(
            "ods.p7.move.command",
            "command",
            "A4",
            "P7",
            {
                "source_id": "pt0",
                "pan_deg": 0.0,
                "tilt_deg": 90.0,
                "speed_deg_s": 10.0,
            },
        )
        try:
            reply = publish_and_wait(
                "ods.p7.move.command",
                command,
                "ods.p7.move.reply",
                lambda message: message.get("correlation_id") == command["message_id"],
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            reply,
            schema="ods.p7.move.reply",
            message_type="reply",
            producer="P7",
            payload_fields=("status", "error"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply P7 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["payload"]["status"], "ERROR")
        self.assertEqual(reply["payload"]["error"]["code"], "OUT_OF_RANGE")
