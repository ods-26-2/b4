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


class I9IntegrationTests(unittest.TestCase):
    def test_i1_detections_on_kafka_produce_i9_compliance(self):
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
                        "bbox": [100, 40, 360, 470],
                    }
                ],
            },
        )

        try:
            compliance = publish_and_wait(
                "ods.i1.detections",
                detections,
                "ods.i9.ppe_compliance",
                lambda message: (
                    message.get("schema") == "ods.i9.ppe_compliance"
                    and message.get("source_id") == source_id
                    and message.get("session_id") == session_id
                    and isinstance(message.get("payload"), dict)
                    and message["payload"].get("frame_index") == 0
                    and message["payload"].get("detection_id") == 0
                ),
            )
        except KafkaIntegrationError as error:
            self.fail(str(error))

        errors = validate_message(
            compliance,
            schema="ods.i9.ppe_compliance",
            message_type="event",
            producer="I9",
            payload_fields=(
                "frame_index",
                "detection_id",
                "bbox",
                "calibrated",
                "catalog_version",
                "items",
                "uncertain",
            ),
            context_fields=(
                "captured_at",
                "source_id",
                "session_id",
                "sequence",
                "artifact_version",
            ),
        )
        self.assertFalse(errors, f"Evento I9 inválido: {'; '.join(errors)}")
