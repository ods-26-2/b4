from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "i5"
AUDIO_PATH = COMPONENT_ROOT / "data" / "audio6.ogg"
MODEL_NAME = "openai/whisper-tiny"
sys.path.insert(0, str(COMPONENT_ROOT / "src"))

try:
    import librosa
    from huggingface_hub import snapshot_download
    from huggingface_hub.utils import LocalEntryNotFoundError
    from TranscriptionWhisper import TranscritorWhisper
except (ImportError, ModuleNotFoundError) as import_error:
    I5_IMPORT_ERROR = import_error
else:
    I5_IMPORT_ERROR = None


class I5IntegrationTests(unittest.TestCase):
    def test_local_whisper_output_matches_manual_transcript_event(self):
        if I5_IMPORT_ERROR is not None:
            self.skipTest(f"Dependência do I5 ausente: {I5_IMPORT_ERROR}")
        if not AUDIO_PATH.is_file():
            self.skipTest(f"Áudio local do I5 ausente: {AUDIO_PATH}")

        try:
            model_path = snapshot_download(MODEL_NAME, local_files_only=True)
        except LocalEntryNotFoundError:
            self.skipTest(f"Modelo {MODEL_NAME} não está no cache local.")

        audio, sampling_rate = librosa.load(AUDIO_PATH, sr=16000)
        transcriber = TranscritorWhisper(model_name=model_path, device="cpu")
        output = transcriber.transcrever_audio(
            audio_array=audio,
            sampling_rate=sampling_rate,
            origem="b4-test-mic",
            timestamp_inicio="2026-09-16T14:05:00.000Z",
        )
        message = json.loads(output)
        self.assertTrue(
            message.get("payload", {}).get("texto_transcrito"),
            "Whisper executou, mas não produziu texto transcrito.",
        )

        errors = validate_message(
            message,
            schema="ods.i5.transcript",
            message_type="event",
            producer="I5",
            payload_fields=(
                "start_at",
                "end_at",
                "transcribed_text",
                "normalized_text",
                "confidence",
                "uncertain",
            ),
            context_fields=("captured_at", "source_id", "artifact_version"),
        )
        self.assertFalse(
            errors,
            "Saída real do I5 diverge de ods.i5.transcript; "
            f"schema={message.get('schema')!r}, "
            f"payload_fields={sorted(message.get('payload', {}))}, "
            f"erros={'; '.join(errors)}",
        )
