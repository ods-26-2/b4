from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "i1"
ARTIFACT_ROOT = COMPONENT_ROOT / "I1" / "artifacts" / "app-plates" / "1.0.0"
EVENTS_FILE = "events.jsonl"


class I1IntegrationTests(unittest.TestCase):
    def test_real_onnx_inference_publishes_contract_events(self) -> None:
        if shutil.which("docker") is None:
            self.skipTest("Docker CLI não está instalado.")

        daemon = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if daemon.returncode:
            self.skipTest(f"Docker Engine indisponível: {daemon.stderr.strip()}")

        if not (ARTIFACT_ROOT / "app-plates.onnx").is_file():
            self.skipTest("Modelo ONNX local do I1 não encontrado.")
        fixtures = ARTIFACT_ROOT / "fixtures"
        if not fixtures.is_dir():
            self.skipTest("Fixtures de imagem locais do I1 não encontradas.")

        with tempfile.TemporaryDirectory(prefix="b4-i1-") as temp_dir:
            events_path = Path(temp_dir) / EVENTS_FILE
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "run",
                    "--rm",
                    "--volume",
                    f"{temp_dir}:/b4-output",
                    "i1-ci-tests",
                    "python3",
                    "I1/run_detector.py",
                    "--onnx",
                    "/app/I1/artifacts/app-plates/1.0.0/app-plates.onnx",
                    "--metadata",
                    "/app/I1/artifacts/app-plates/1.0.0/metadata.json",
                    "--source",
                    "/app/I1/artifacts/app-plates/1.0.0/fixtures",
                    "--max-frames",
                    "10",
                    "--conf",
                    "0.50",
                    "--output-jsonl",
                    f"/b4-output/{EVENTS_FILE}",
                ],
                cwd=COMPONENT_ROOT,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            output = (result.stdout or "") + (result.stderr or "")
            self.assertEqual(result.returncode, 0, output)
            self.assertTrue(events_path.is_file(), output)
            messages = [
                json.loads(line)
                for line in events_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertGreater(len(messages), 0, "I1 não publicou eventos de quadro.")
        for message in messages:
            errors = validate_message(
                message,
                schema="ods.i1.detections",
                message_type="event",
                producer="I1",
                payload_fields=("frame_index", "image_width", "image_height", "detections"),
                context_fields=(
                    "captured_at",
                    "source_id",
                    "session_id",
                    "sequence",
                    "artifact_version",
                ),
            )
            self.assertFalse(errors, f"Evento I1 inválido: {'; '.join(errors)}")
            detections = message["payload"]["detections"]
            self.assertIsInstance(detections, list)
            for detection in detections:
                self.assertIsInstance(detection.get("detection_id"), int)
                self.assertIsInstance(detection.get("class"), str)
                confidence = detection.get("confidence")
                self.assertIsInstance(confidence, (int, float))
                self.assertNotIsInstance(confidence, bool)
                self.assertGreaterEqual(confidence, 0)
                self.assertLessEqual(confidence, 1)
                self.assertIsInstance(detection.get("bbox"), list)
                self.assertEqual(len(detection["bbox"]), 4)
                self.assertTrue(
                    all(
                        isinstance(value, int) and not isinstance(value, bool) and value >= 0
                        for value in detection["bbox"]
                    )
                )
