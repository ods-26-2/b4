from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "s1"
SESSION_ID = str(uuid.uuid4())


def _event(schema: str, message_id: str, sequence: int, timestamp: str, payload: dict) -> dict:
    return {
        "message_type": "event",
        "message_id": message_id,
        "schema": schema,
        "schema_version": "1.0",
        "producer": "P4" if schema == "ods.session.started" else "I2",
        "published_at": timestamp,
        "captured_at": timestamp,
        "source_id": "cam0",
        "session_id": SESSION_ID,
        "sequence": sequence,
        **({"artifact_version": "model-test-1"} if schema == "ods.i2.tracks" else {}),
        "payload": payload,
    }


class S1IntegrationTests(unittest.TestCase):
    def test_tracks_produce_position_transition_and_occupancy(self):
        zones = {
            "coordinates": "PIXELS",
            "source_ids": ["cam0"],
            "zones": [
                {
                    "zone_id": "zone-1",
                    "name": "Test zone",
                    "polygon": [[0, 0], [500, 0], [500, 500], [0, 500]],
                    "enter_hysteresis_ms": 100,
                    "exit_hysteresis_ms": 300,
                    "max_occupancy": 5,
                    "classes": ["person"],
                }
            ],
        }
        inputs = [
            _event(
                "ods.session.started",
                str(uuid.uuid4()),
                0,
                "2026-09-23T14:00:00.000Z",
                {"mode": "LIVE", "started_at": "2026-09-23T14:00:00.000Z", "sources": ["cam0"]},
            )
        ]
        frames = (
            (0, "2026-09-23T14:00:00.000Z", [100, 100]),
            (1, "2026-09-23T14:00:00.100Z", [100, 100]),
            (2, "2026-09-23T14:00:00.200Z", [700, 100]),
            (3, "2026-09-23T14:00:00.500Z", [700, 100]),
        )
        for frame_index, timestamp, point in frames:
            inputs.append(
                _event(
                    "ods.i2.tracks",
                    str(uuid.uuid4()),
                    frame_index + 1,
                    timestamp,
                    {
                        "frame_index": frame_index,
                        "tracks": [
                            {
                                "track_id": "track-1",
                                "class": "person",
                                "state": "CONFIRMED",
                                "predicted": False,
                                "confidence": 0.9,
                                "point_px": point,
                                "bbox": [point[0] - 10, point[1] - 30, point[0] + 10, point[1]],
                            }
                        ],
                    },
                )
            )

        with tempfile.TemporaryDirectory(prefix="b4-s1-") as temp_dir:
            temp = Path(temp_dir)
            zones_path = temp / "zones.json"
            input_path = temp / "input.json"
            output_path = temp / "output.json"
            zones_path.write_text(json.dumps(zones), encoding="utf-8")
            input_path.write_text(json.dumps(inputs), encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(COMPONENT_ROOT / "simulate.py"),
                    "--zonas",
                    str(zones_path),
                    "--entrada",
                    str(input_path),
                    "--saida",
                    str(output_path),
                ],
                cwd=COMPONENT_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            if result.returncode and "ModuleNotFoundError: No module named" in result.stderr:
                self.skipTest(result.stderr.strip().splitlines()[-1])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            outputs = json.loads(output_path.read_text(encoding="utf-8"))

        expected = {
            "ods.s1.position": ("frame_index", "positions"),
            "ods.s1.zone_transition": ("track_id", "class", "zone_id", "transition", "entered_at"),
            "ods.s1.zone_occupancy": ("zone_id", "count", "count_by_class", "track_ids"),
        }
        found = {message["schema"] for message in outputs}
        self.assertTrue(set(expected).issubset(found), f"Mensagens emitidas: {sorted(found)}")
        for message in outputs:
            if message["schema"] in expected:
                errors = validate_message(
                    message,
                    schema=message["schema"],
                    message_type="event",
                    producer="S1",
                    payload_fields=expected[message["schema"]],
                    context_fields=("captured_at", "source_id", "session_id", "sequence", "artifact_version"),
                )
                self.assertFalse(errors, f"{message['schema']}: {'; '.join(errors)}")
