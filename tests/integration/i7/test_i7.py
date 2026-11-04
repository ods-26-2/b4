from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
from pathlib import Path
import shutil
import tempfile
import unittest

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "i7"
PIPELINE_PATH = COMPONENT_ROOT / "scripts" / "aplicacaoModelov5-fast+rodosol.py"
MODEL_PATH = COMPONENT_ROOT / "modelos" / "modelo4classesV1" / "best.pt"
FIXTURE_PATH = (
    WORKSPACE_ROOT
    / "i1"
    / "I1"
    / "artifacts"
    / "app-plates"
    / "1.0.0"
    / "fixtures"
    / "img_000004.jpg"
)
OCR_CACHE = (
    Path.home()
    / ".cache"
    / "fast-plate-ocr"
    / "global-plates-mobile-vit-v2-model"
)
OCR_MODEL_PATH = OCR_CACHE / "global_mobile_vit_v2_ocr.onnx"
OCR_CONFIG_PATH = OCR_CACHE / "global_mobile_vit_v2_ocr_config.yaml"

try:
    import cv2
    from fast_alpr.default_ocr import DefaultOCR
    from ultralytics import YOLO
except (ImportError, ModuleNotFoundError) as import_error:
    I7_IMPORT_ERROR = import_error
else:
    I7_IMPORT_ERROR = None


class I7IntegrationTests(unittest.TestCase):
    def test_local_yolo_ocr_output_matches_manual_plate_read_event(self):
        if I7_IMPORT_ERROR is not None:
            self.skipTest(f"Dependência do I7 ausente: {I7_IMPORT_ERROR}")
        if not MODEL_PATH.is_file():
            self.skipTest(f"Checkpoint YOLO local do I7 ausente: {MODEL_PATH}")
        if not FIXTURE_PATH.is_file():
            self.skipTest(f"Imagem de teste local ausente: {FIXTURE_PATH}")
        if not OCR_MODEL_PATH.is_file() or not OCR_CONFIG_PATH.is_file():
            self.skipTest("Modelo/configuração Fast-ALPR não estão no cache local.")

        pipeline_spec = importlib.util.spec_from_file_location(
            "i7_b4_pipeline", PIPELINE_PATH
        )
        if pipeline_spec is None or pipeline_spec.loader is None:
            self.fail(f"Não foi possível carregar o pipeline do I7: {PIPELINE_PATH}")
        pipeline = importlib.util.module_from_spec(pipeline_spec)
        pipeline_spec.loader.exec_module(pipeline)

        input_directory = Path(tempfile.mkdtemp(prefix="b4-i7-input-"))
        output_directory = Path(tempfile.mkdtemp(prefix="b4-i7-output-"))
        try:
            shutil.copy2(FIXTURE_PATH, input_directory / FIXTURE_PATH.name)
            pipeline.PASTA_IMAGENS = str(input_directory)
            pipeline.PASTA_SAIDA = str(output_directory)
            pipeline.CAMINHO_MODELO_YOLO = str(MODEL_PATH)
            pipeline.DefaultOCR = lambda **_: DefaultOCR(
                model_path=OCR_MODEL_PATH,
                config_path=OCR_CONFIG_PATH,
                device="cpu",
            )
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                output = pipeline.main()

            results_path = output_directory / "resultados.csv"
            self.assertTrue(results_path.is_file(), "I7 não gerou resultados.csv.")
            with results_path.open(encoding="utf-8", newline="") as results_file:
                rows = list(csv.DictReader(results_file))
            self.assertTrue(rows, "I7 não produziu leitura para a imagem de teste.")
            valid_reads = [
                row
                for row in rows
                if len(row.get("placa_lida", "")) == 7
                and row.get("tipo_detectado")
                and "Inválida" not in row["tipo_detectado"]
                and "Sem Leitura" not in row["tipo_detectado"]
            ]
            self.assertTrue(
                valid_reads,
                f"YOLO/Fast-ALPR não produziu placa válida: {rows!r}",
            )

            errors = validate_message(
                output,
                schema="ods.i7.plate_read",
                message_type="event",
                producer="I7",
                payload_fields=(
                    "frame_index",
                    "class",
                    "plate_layout",
                    "license_plate",
                    "confidence",
                    "character_confidences",
                    "uncertain",
                    "bbox",
                ),
                context_fields=("captured_at", "source_id", "artifact_version"),
            )
            self.assertFalse(
                errors,
                "A inferência local YOLO/Fast-ALPR produziu "
                f"{valid_reads!r}, mas o I7 não retornou/publicou um evento "
                "ods.i7.plate_read; a implementação salva CSV e mosaicos. "
                f"Validação: {'; '.join(errors)}",
            )
        finally:
            shutil.rmtree(input_directory)
            shutil.rmtree(output_directory)
