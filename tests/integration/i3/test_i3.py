from __future__ import annotations

import unittest


class I3IntegrationTests(unittest.TestCase):
    def test_calibration_has_no_kafka_input_contract(self):
        self.skipTest(
            "I3 publica ods.i3.calibration ao concluir a calibração, mas a 6c "
            "não define um comando Kafka para iniciar esse processamento."
        )
