from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest
import uuid

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "s3"
sys.path.insert(0, str(COMPONENT_ROOT / "src"))

from s3.commands import ProcessadorComandos
from s3.service import AlertService


class S3IntegrationTests(unittest.TestCase):
    def test_valid_alert_command_returns_contract_reply(self):
        message_id = str(uuid.uuid4())
        command = {
            "message_type": "command",
            "message_id": message_id,
            "schema": "ods.s3.alert.command",
            "schema_version": "1.0",
            "producer": "A5",
            "published_at": "2026-09-23T14:31:00.000Z",
            "target": "S3",
            "payload": {
                "title": "Acesso negado",
                "message": "Crachá de teste recusado na portaria",
                "severity": "MEDIUM",
                "recipients": ["group:portaria"],
                "channels": ["SCREEN"],
            },
        }
        reply = json.loads(ProcessadorComandos(AlertService()).receber(json.dumps(command)))
        errors = validate_message(
            reply,
            schema="ods.s3.alert.reply",
            message_type="reply",
            producer="S3",
            payload_fields=("status", "alert_id"),
            context_fields=("correlation_id",),
        )
        self.assertFalse(errors, f"Reply do S3 inválida: {'; '.join(errors)}")
        self.assertEqual(reply["correlation_id"], message_id)
        self.assertEqual(reply["payload"]["status"], "OK")
