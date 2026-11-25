from __future__ import annotations

from datetime import datetime, timezone
import unittest

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_request,
    publish_and_wait,
)


class S5IntegrationTests(unittest.TestCase):
    def test_metrics_query_on_kafka_returns_correlated_reply(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        query = new_request(
            "ods.s5.metrics.query",
            "query",
            "A8",
            "S5",
            {
                "metric": "EVENT",
                "period_start_at": start.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "period_end_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "granularity": "1h",
            },
        )
        try:
            reply = publish_and_wait(
                "ods.s5.metrics.query",
                query,
                "ods.s5.metrics.reply",
                lambda message: message.get("correlation_id") == query["message_id"],
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            reply,
            schema="ods.s5.metrics.reply",
            message_type="reply",
            producer="S5",
            payload_fields=("status", "series"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply S5 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["payload"]["status"], "OK")
