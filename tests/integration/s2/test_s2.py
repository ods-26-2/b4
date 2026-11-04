from __future__ import annotations

from pathlib import Path
import sys
import unittest

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "s2"
sys.path.insert(0, str(COMPONENT_ROOT / "components" / "S2"))

try:
    from main import CamadaS2
except ModuleNotFoundError as import_error:
    CamadaS2 = None
    IMPORT_ERROR = import_error
else:
    IMPORT_ERROR = None


class S2IntegrationTests(unittest.TestCase):
    def test_normalized_position_produces_contract_entity(self):
        if IMPORT_ERROR is not None:
            self.skipTest(f"Dependência do S2 ausente: {IMPORT_ERROR}")

        input_event = {
            "entity_id": "track-1",
            "source_id": "cam0",
            "source": "cam0",
            "timestamp": 1790173799200,
            "x": 3.42,
            "y": 7.1,
            "zone_id": "vaga-12",
        }
        outputs = CamadaS2().processar_eventos([input_event])
        self.assertEqual(len(outputs), 1, outputs)
        errors = validate_message(
            outputs[0],
            schema="ods.s2.entity",
            message_type="event",
            producer="S2",
            payload_fields=("entities",),
        )
        if errors:
            self.fail(f"S2 diverge de ods.s2.entity: {'; '.join(errors)}")
