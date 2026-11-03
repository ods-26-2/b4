from __future__ import annotations

import csv
import json
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = ROOT.parent
TEST_ROOT = ROOT / "tests" / "integration"
MATRIX_PATH = TEST_ROOT / "component_matrix.json"
REPORT_PATH = WORKSPACE_ROOT / "relatorio_integracao.csv"


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
    by_component: dict[str, list[dict[str, str]]] = {}
    for record in records:
        by_component.setdefault(record["component"], []).append(record)

    with REPORT_PATH.open("w", encoding="utf-8-sig", newline="") as destination:
        writer = csv.writer(destination, delimiter=";")
        writer.writerow(
            ["Componente", "Teste / fluxo avaliado", "Resultado", "Interface avaliada", "Observações"]
        )
        for item in matrix:
            outcomes = by_component.get(item["component"], [])
            if not outcomes:
                writer.writerow(
                    [
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
                        item["component"],
                        outcome["test"],
                        outcome["result"],
                        item["interface"],
                        detail,
                    ]
                )


def main() -> int:
    suite = unittest.defaultTestLoader.discover(
        str(TEST_ROOT),
        pattern="test_*.py",
        top_level_dir=str(ROOT),
    )
    runner = unittest.TextTestRunner(verbosity=2, resultclass=IntegrationResult)
    result = runner.run(suite)
    _write_report(result.records)
    print(f"\nPlanilha atualizada: {REPORT_PATH}")
    return 1 if result.failures or result.errors else 0


if __name__ == "__main__":
    sys.exit(main())
