from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest
import uuid

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "i2"


class I2IntegrationTests(unittest.TestCase):
    def test_csv_detections_produce_contract_tracks(self):
        session_id = str(uuid.uuid4())
        csv_input = (
            "quadro,t_s,x1,y1,x2,y2,classe,confianca\n"
            "0,0.0,10,10,50,100,pessoa,0.9\n"
            "1,0.1,11,10,51,100,pessoa,0.9\n"
            "2,0.2,12,10,52,100,pessoa,0.9\n"
            "3,0.3,13,10,53,100,pessoa,0.9\n"
        )
        result = subprocess.run(
            [
                sys.executable,
                str(COMPONENT_ROOT / "main.py"),
                "--camera-id",
                "cam0",
                "--sessao",
                session_id,
                "--inicio",
                "2026-09-23T14:29:59.000Z",
            ],
            cwd=COMPONENT_ROOT,
            input=csv_input,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode and "ModuleNotFoundError: No module named" in result.stderr:
            self.skipTest(result.stderr.strip().splitlines()[-1])
        self.assertEqual(result.returncode, 0, result.stderr)
        messages = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        self.assertGreaterEqual(len(messages), 1, result.stdout)
        errors = validate_message(
            messages[-1],
            schema="ods.i2.tracks",
            message_type="event",
            producer="I2",
            payload_fields=("frame_index", "tracks"),
            context_fields=("captured_at", "source_id", "session_id", "sequence", "artifact_version"),
        )
        if errors:
            self.fail(f"I2 diverge de ods.i2.tracks: {'; '.join(errors)}")
