from __future__ import annotations

import unittest


class B2IntegrationTests(unittest.TestCase):
    def test_time_service_has_no_kafka_contract(self):
        self.skipTest(
            "B2 usa MQTT e ods.system.time não consta nos contratos Kafka 6c; "
            "não é invocável pelo barramento Kafka comum."
        )
