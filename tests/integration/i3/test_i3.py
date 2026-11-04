from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "i3"


class I3IntegrationTests(unittest.TestCase):
    def test_correspondences_publish_contract_calibration(self):
        csv_input = (
            "u,v,x,y,uso,ponto\n"
            "0,0,0,0,ajuste,p0\n"
            "100,0,1,0,ajuste,p1\n"
            "0,100,0,1,ajuste,p2\n"
            "100,100,1,1,ajuste,p3\n"
            "50,0,0.5,0,ajuste,p4\n"
            "0,50,0,0.5,ajuste,p5\n"
            "50,50,0.5,0.5,holdout,p6\n"
            "80,20,0.8,0.2,holdout,p7\n"
        )
        result = subprocess.run(
            [
                sys.executable,
                str(COMPONENT_ROOT / "main.py"),
                "--camera-id",
                "cam0",
                "--versao",
                "cal-cam0-test",
                "--valido-desde",
                "2026-09-23T14:00:00.000Z",
                "--espaco",
                "quadra-1",
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
        message = json.loads(result.stdout)
        errors = validate_message(
            message,
            schema="ods.i3.calibration",
            message_type="event",
            producer="I3",
            payload_fields=(
                "valid_from",
                "image_width",
                "image_height",
                "intrinsics",
                "distortion",
                "homography_px_to_m",
                "reference_frame",
                "reprojection_rms_cm",
                "holdout_points",
            ),
            context_fields=("source_id", "artifact_version"),
        )
        if errors:
            self.fail(f"I3 diverge de ods.i3.calibration: {'; '.join(errors)}")
