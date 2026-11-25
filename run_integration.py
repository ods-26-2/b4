from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent
TEST_ROOT = ROOT / "tests" / "integration"
MATRIX_PATH = TEST_ROOT / "component_matrix.json"
REPORT_PATH = ROOT / "results" / "relatorio_integracao.csv"
BROKERS = (
    ("nuvem", "34.66.235.91:9093"),
    ("jetson", "172.18.4.56:9093"),
)


class IntegrationResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records: list[dict[str, str]] = []

    @staticmethod
    def _component(test: unittest.case.TestCase) -> str:
        test_id = test.id()
        marker = "tests.integration."
        if marker in test_id:
            return test_id.split(marker, 1)[1].split(".", 1)[0].upper()
        return test_id.split(".", 1)[0].upper()

    def _record(self, test, result: str, detail: str = "") -> None:
        self.records.append(
            {
                "component": self._component(test),
                "test": test.id().rsplit(".", 1)[-1],
                "result": result,
                "detail": detail.strip().replace("\r", " ").replace("\n", " ")[:1200],
            }
        )

    def addSuccess(self, test):
        super().addSuccess(test)
        self._record(test, "APROVADO")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._record(test, "REPROVADO", self._failure_summary(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self._record(test, "ERRO", self._failure_summary(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._record(test, "BLOQUEADO", reason)

    def _failure_summary(self, error, test) -> str:
        exception = error[1]
        return f"{type(exception).__name__}: {exception}"


def _write_report(records: list[dict[str, str]]) -> None:
    with MATRIX_PATH.open(encoding="utf-8") as source:
        matrix = json.load(source)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    by_broker_component: dict[str, dict[str, list[dict[str, str]]]] = {
        broker_name: {} for broker_name, _ in BROKERS
    }
    for record in records:
        by_broker_component[record["broker"]].setdefault(
            record["component"], []
        ).append(record)

    with REPORT_PATH.open("w", encoding="utf-8-sig", newline="") as destination:
        writer = csv.writer(destination, delimiter=";")
        writer.writerow(
            [
                "Barramento",
                "Componente",
                "Teste / fluxo avaliado",
                "Resultado",
                "Interface avaliada",
                "Observações",
            ]
        )
        for broker_name, _ in BROKERS:
            for item in matrix:
                outcomes = by_broker_component[broker_name].get(
                    item["component"], []
                )
                if not outcomes:
                    writer.writerow(
                        [
                            broker_name,
                            item["component"],
                            item["test"] or "—",
                            item["status_without_test"],
                            item["interface"],
                            item["observation"],
                        ]
                    )
                    continue
                for outcome in outcomes:
                    detail = outcome["detail"] or item["observation"]
                    writer.writerow(
                        [
                            broker_name,
                            item["component"],
                            outcome["test"],
                            outcome["result"],
                            item["interface"],
                            detail,
                        ]
                    )


def _load_component_suite(kafka_components: tuple[str, ...]) -> unittest.TestSuite:
    suite = unittest.TestSuite()
    for component in kafka_components:
        suite.addTests(
            unittest.defaultTestLoader.discover(
                str(TEST_ROOT / component),
                pattern="test_*.py",
                top_level_dir=str(ROOT),
            )
        )
    return suite


def _run_broker_suites(runner, suite_loader, kafka_components):
    records = []
    failed = False
    for broker_name, bootstrap_servers in BROKERS:
        print(
            f"\n=== Barramento {broker_name}: {bootstrap_servers} ===",
            flush=True,
        )
        with patch.dict(
            os.environ,
            {"ODS_KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers},
        ):
            result = runner.run(suite_loader(kafka_components))
        records.extend(
            {**record, "broker": broker_name} for record in result.records
        )
        failed = failed or bool(result.failures or result.errors)
    return records, failed


def _run_local_tests(runner):
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.defaultTestLoader.loadTestsFromName(
            "tests.integration.test_kafka_client"
        )
    )
    suite.addTests(
        unittest.defaultTestLoader.loadTestsFromName(
            "tests.integration.test_run_integration"
        )
    )
    result = runner.run(suite)
    return result.records, bool(result.failures or result.errors)


def main() -> int:
    kafka_components = (
        "b2",
        "i2",
        "i3",
        "i9",
        "p6",
        "p7",
        "s1",
        "s2",
        "s3",
        "s4",
        "s5",
        "s6",
    )
    runner = unittest.TextTestRunner(verbosity=2, resultclass=IntegrationResult)
    records, failed = _run_broker_suites(
        runner,
        _load_component_suite,
        kafka_components,
    )
    print("\n=== Testes locais do executor ===", flush=True)
    _, local_tests_failed = _run_local_tests(runner)
    _write_report(records)
    print(f"\nPlanilha atualizada: {REPORT_PATH}")
    return 1 if failed or local_tests_failed else 0


if __name__ == "__main__":
    sys.exit(main())
