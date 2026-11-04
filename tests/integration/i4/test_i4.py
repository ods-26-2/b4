from __future__ import annotations

import sys
import unittest
from pathlib import Path

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "i4"
VIDEO_PATH = COMPONENT_ROOT / "teste.mp4"
MODEL_PATH = COMPONENT_ROOT / "model_v4.pt"
sys.path.insert(0, str(COMPONENT_ROOT))

try:
    from pipeline.pipeline import LibrasPipeline
except (ImportError, ModuleNotFoundError) as import_error:
    I4_IMPORT_ERROR = import_error
else:
    I4_IMPORT_ERROR = None


class I4IntegrationTests(unittest.TestCase):
    def test_video_output_matches_manual_sign_event(self):
        if I4_IMPORT_ERROR is not None:
            self.skipTest(f"Dependência do I4 ausente: {I4_IMPORT_ERROR}")
        if not VIDEO_PATH.is_file():
            self.skipTest(f"Vídeo de teste do I4 ausente: {VIDEO_PATH}")
        if not MODEL_PATH.is_file():
            self.skipTest(f"Checkpoint do I4 ausente: {MODEL_PATH}")

        with LibrasPipeline(MODEL_PATH, device="cpu") as pipeline:
            output = pipeline.run_from_video(VIDEO_PATH, sinalizador_id="b4-test")

        errors = validate_message(
            output.to_dict(),
            schema="ods.i4.sign",
            message_type="event",
            producer="I4",
            payload_fields=("sign", "confidence", "start_at", "end_at"),
        )
        self.assertFalse(errors, f"I4 diverge de ods.i4.sign: {'; '.join(errors)}")
