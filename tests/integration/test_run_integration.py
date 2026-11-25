from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import run_integration


class IntegrationReportTests(unittest.TestCase):
    def test_report_contains_results_for_both_brokers(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            matrix_path = temporary_path / "component_matrix.json"
            report_path = temporary_path / "results" / "report.csv"
            matrix_path.write_text(
                json.dumps(
                    [
                        {
                            "component": "I2",
                            "test": "detections to tracks",
                            "status_without_test": "PENDENTE",
                            "interface": "Kafka",
                            "observation": "Expected I2 output",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            records = [
                {
                    "broker": "nuvem",
                    "component": "I2",
                    "test": "detections to tracks",
                    "result": "APROVADO",
                    "detail": "",
                },
                {
                    "broker": "jetson",
                    "component": "I2",
                    "test": "detections to tracks",
                    "result": "REPROVADO",
                    "detail": "No reply",
                },
            ]

            with patch.multiple(
                run_integration,
                MATRIX_PATH=matrix_path,
                REPORT_PATH=report_path,
            ):
                run_integration._write_report(records)

            self.assertTrue(report_path.is_file())
            with report_path.open(encoding="utf-8-sig", newline="") as report:
                rows = list(csv.DictReader(report, delimiter=";"))
            self.assertEqual(
                [(row["Barramento"], row["Resultado"]) for row in rows],
                [("nuvem", "APROVADO"), ("jetson", "REPROVADO")],
            )

    def test_runs_cloud_then_jetson_with_each_bootstrap_address(self):
        observed_bootstraps = []
        components = ("i2",)
        suites = []

        class FakeRunner:
            def run(self, suite):
                observed_bootstraps.append(
                    os.environ.get("ODS_KAFKA_BOOTSTRAP_SERVERS")
                )
                return SimpleNamespace(records=[], failures=[], errors=[])

        def load_suite(received_components):
            self.assertEqual(received_components, components)
            suite = object()
            suites.append(suite)
            return suite

        with patch.dict(
            os.environ,
            {"ODS_KAFKA_BOOTSTRAP_SERVERS": "previous:9092"},
        ):
            records, failed = run_integration._run_broker_suites(
                FakeRunner(),
                load_suite,
                components,
            )
            self.assertEqual(
                os.environ["ODS_KAFKA_BOOTSTRAP_SERVERS"],
                "previous:9092",
            )

        self.assertEqual(
            observed_bootstraps,
            ["34.66.235.91:9093", "172.18.4.56:9093"],
        )
        self.assertEqual(len(suites), 2)
        self.assertEqual(records, [])
        self.assertFalse(failed)
