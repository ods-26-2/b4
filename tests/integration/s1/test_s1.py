from __future__ import annotations

import os
import unittest
import uuid

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_event,
    publish_and_wait,
)


class S1IntegrationTests(unittest.TestCase):
    def test_i2_tracks_on_kafka_produce_s1_position(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")
        source_id = os.environ.get("ODS_TEST_SOURCE_ID", "").strip()
        if not source_id:
            self.skipTest("Defina ODS_TEST_SOURCE_ID para uma fonte de teste cadastrada no ZONES do S1.")

        session_id = str(uuid.uuid4())
        tracks = new_event(
            "ods.i2.tracks",
            "I2",
            source_id,
            session_id,
            0,
            "b4-test-model",
            {"frame_index": 0, "tracks": []},
        )
        try:
            position = publish_and_wait(
                "ods.i2.tracks",
                tracks,
                "ods.s1.position",
                lambda message: (
                    message.get("schema") == "ods.s1.position"
                    and message.get("source_id") == source_id
                    and message.get("session_id") == session_id
                    and isinstance(message.get("payload"), dict)
                    and message["payload"].get("frame_index") == 0
                ),
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            position,
            schema="ods.s1.position",
            message_type="event",
            producer="S1",
            payload_fields=("frame_index", "positions"),
            context_fields=(
                "captured_at",
                "source_id",
                "session_id",
                "sequence",
                "artifact_version",
            ),
        )
        self.assertFalse(errors, f"Evento S1 inválido: {'; '.join(errors)}")
