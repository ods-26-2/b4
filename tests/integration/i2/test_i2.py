from __future__ import annotations

import unittest
import uuid

from tests.integration.contract_validation import validate_message
from tests.integration.kafka_client import (
    KafkaIntegrationError,
    kafka_configured,
    new_event,
    publish_and_wait,
)


class I2IntegrationTests(unittest.TestCase):
    def test_i1_detections_on_kafka_produce_i2_tracks(self):
        if not kafka_configured():
            self.skipTest("Defina ODS_KAFKA_BOOTSTRAP_SERVERS para o broker Kafka de testes.")

        session_id = str(uuid.uuid4())
        source_id = f"b4-test-{uuid.uuid4().hex[:8]}"
        detections = new_event(
            "ods.i1.detections",
            "I1",
            source_id,
            session_id,
            0,
            "b4-test-model",
            {
                "frame_index": 0,
                "image_width": 640,
                "image_height": 480,
                "detections": [
                    {
                        "detection_id": 0,
                        "class": "person",
                        "confidence": 0.99,
                        "bbox": [100, 40, 240, 450],
                    }
                ],
            },
        )

        try:
            tracks = publish_and_wait(
                "ods.i1.detections",
                detections,
                "ods.i2.tracks",
                lambda message: (
                    message.get("schema") == "ods.i2.tracks"
                    and message.get("source_id") == source_id
                    and message.get("session_id") == session_id
                    and isinstance(message.get("payload"), dict)
                    and message["payload"].get("frame_index") == 0
                ),
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            tracks,
            schema="ods.i2.tracks",
            message_type="event",
            producer="I2",
            payload_fields=("frame_index", "tracks"),
            context_fields=(
                "captured_at",
                "source_id",
                "session_id",
                "sequence",
                "artifact_version",
            ),
        )
        self.assertFalse(errors, f"Evento I2 inválido: {'; '.join(errors)}")
