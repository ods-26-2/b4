from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "i9" / "I9"
INPUT_PATH = COMPONENT_ROOT / "examples" / "i1_detections.json"
ENTRYPOINT = COMPONENT_ROOT / "main.py"


class I9IntegrationTests(unittest.TestCase):
    def test_i1_frame_produces_manual_ppe_compliance_event(self):
        if not ENTRYPOINT.is_file() or not INPUT_PATH.is_file():
            self.skipTest("Código/fixture de entrada do I9 não está disponível.")

        result = subprocess.run(
            [
                sys.executable,
                str(ENTRYPOINT),
                "--input",
                str(INPUT_PATH),
                "--json",
            ],
            cwd=COMPONENT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        output = result.stdout
        start = output.find("{")
        self.assertNotEqual(start, -1, f"I9 não imprimiu evento JSON: {output}")
        message, _ = json.JSONDecoder().raw_decode(output[start:])
        errors = validate_message(
            message,
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
        self.assertFalse(errors, f"I9 diverge de ods.i9.ppe_compliance: {'; '.join(errors)}")
