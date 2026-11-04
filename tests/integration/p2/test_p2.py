from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "p2"
sys.path.insert(0, str(COMPONENT_ROOT))

try:
    import numpy as np
    import onnx
    from onnx import TensorProto, helper

    p2 = importlib.import_module("p2")
except (ImportError, ModuleNotFoundError) as import_error:
    P2_IMPORT_ERROR = import_error
else:
    P2_IMPORT_ERROR = None


class P2IntegrationTests(unittest.TestCase):
    def test_cpu_package_build_infer_and_info(self):
        if P2_IMPORT_ERROR is not None:
            self.skipTest(f"Dependência do P2 ausente: {P2_IMPORT_ERROR}")

        try:
            importlib.import_module("onnxruntime")
        except (ImportError, ModuleNotFoundError) as import_error:
            self.skipTest(f"Dependência do backend CPU ausente: {import_error}")

        package_root = self._write_model_package(self._test_temp_dir / "package")
        engine_root = self._test_temp_dir / "engines"
        subprocess.run(
            [
                sys.executable,
                "-m",
                "p2.cli.build",
                str(package_root),
                "--cpu-only",
                "--engine-dir",
                str(engine_root),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        values = np.arange(28, dtype=np.float32).reshape((1, 7, 4))
        with patch.dict(
            os.environ,
            {"P2_ENGINE_DIR": str(engine_root), "P2_BACKEND": "ort"},
        ):
            with p2.load("b4-integration-1.0.0") as handle:
                outputs, stats = p2.infer(handle, values)
                self.assertEqual(
                    p2.info(handle),
                    {
                        "engine_version": "b4-integration-1.0.0-fp32",
                        "input_shape": {"images": [1, 7, 4]},
                        "output_shape": {"output0": [1, 7, 4]},
                        "precision": "FP32",
                        "max_batch": 1,
                    },
                )

        self.assertEqual(set(outputs), {"output0"})
        self.assertEqual(outputs["output0"].shape, values.shape)
        self.assertEqual(outputs["output0"].dtype, np.float32)
        np.testing.assert_array_equal(outputs["output0"], values)
        self.assertGreater(stats["latency_ms"], 0)
        self.assertEqual(stats["gpu_memory_mb"], 0)

    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp_dir.cleanup)
        self._test_temp_dir = Path(self._temp_dir.name)

    def _write_model_package(self, package_root: Path) -> Path:
        package_root.mkdir(parents=True)
        model_path = package_root / "model.onnx"
        tensor_info = helper.make_tensor_value_info(
            "images", TensorProto.FLOAT, [1, 7, 4]
        )
        output_info = helper.make_tensor_value_info(
            "output0", TensorProto.FLOAT, [1, 7, 4]
        )
        graph = helper.make_graph(
            [helper.make_node("Identity", ["images"], ["output0"])],
            "b4-integration",
            [tensor_info],
            [output_info],
        )
        model = helper.make_model(
            graph,
            opset_imports=[helper.make_opsetid("", 17)],
            ir_version=10,
        )
        onnx.save(model, str(model_path))
        metadata = {
            "artifact_id": "b4-integration",
            "artifact_version": "1.0.0",
            "onnx_file": model_path.name,
            "hash_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
            "input_shape": [1, 7, 4],
            "classes": ["person", "helmet", "vest"],
            "precision": "FP32",
        }
        (package_root / "metadata.json").write_text(
            json.dumps(metadata),
            encoding="utf-8",
        )
        return package_root
