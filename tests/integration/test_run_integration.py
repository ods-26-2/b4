from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import run_integration


class IntegrationReportTests(unittest.TestCase):
    def test_creates_results_directory_when_writing_report(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            matrix_path = temporary_path / "component_matrix.json"
            report_path = temporary_path / "results" / "report.csv"
            matrix_path.write_text("[]", encoding="utf-8")

            with patch.multiple(
                run_integration,
                MATRIX_PATH=matrix_path,
                REPORT_PATH=report_path,
            ):
                run_integration._write_report([])

            self.assertTrue(report_path.is_file())
