from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import types
import unittest
import uuid
from unittest.mock import patch

from tests.integration.contract_validation import validate_message


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "p6"


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.flush_count = 0

    def produce(self, **message: object) -> None:
        self.messages.append(message)

    def flush(self) -> None:
        self.flush_count += 1


class P6IntegrationTests(unittest.TestCase):
    def test_replay_state_publisher_matches_manual_event_envelope(self):
        source = subprocess.run(
            ["git", "show", "origin/develop:kafka_client.py"],
            cwd=COMPONENT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if source.returncode:
            self.skipTest(
                "P6 kafka_client.py ausente no checkout e origin/develop indisponível."
            )

        fake_kafka = types.ModuleType("confluent_kafka")
        fake_kafka.Producer = object
        fake_kafka.Consumer = object
        module_globals = {"__name__": "p6_kafka_client_under_test"}
        with patch.dict(sys.modules, {"confluent_kafka": fake_kafka}):
            exec(
                compile(source.stdout, "origin/develop:kafka_client.py", "exec"),
                module_globals,
            )

        producer = FakeProducer()
        session_id = str(uuid.uuid4())
        module_globals["enviar_evento_ods"](
            producer,
            "ods.p6.replay.state",
            "ods.p6.replay.state",
            session_id,
            {
                "recording_id": "rec-b4",
                "state": "PLAYING",
                "position_ms": 1000,
                "duration_ms": 5000,
                "speed_factor": 1.0,
                "loop": False,
            },
        )

        self.assertEqual(len(producer.messages), 1)
        self.assertEqual(producer.flush_count, 1)
        message = json.loads(producer.messages[0]["value"].decode("utf-8"))
        errors = validate_message(
            message,
            schema="ods.p6.replay.state",
            message_type="event",
            producer="P6",
            payload_fields=(
                "recording_id",
                "state",
                "position_ms",
                "duration_ms",
                "speed_factor",
                "loop",
            ),
            context_fields=("captured_at", "source_id", "session_id", "sequence"),
        )
        self.assertFalse(errors, f"Publicação P6 diverge do manual 6c: {'; '.join(errors)}")

    def test_record_command_returns_contract_reply(self):
        executable_sources = list(COMPONENT_ROOT.rglob("*.py")) if COMPONENT_ROOT.is_dir() else []
        if not executable_sources:
            self.skipTest(
                "P6 ainda não tem implementação executável para receber "
                "ods.p6.record.command e produzir reply."
            )
        self.skipTest(
            "P6 ainda não expõe um handler identificável para "
            "ods.p6.record.command; conectar o teste ao runtime ao implementá-lo."
        )

    def test_replay_command_returns_state_and_session(self):
        executable_sources = list(COMPONENT_ROOT.rglob("*.py")) if COMPONENT_ROOT.is_dir() else []
        if not executable_sources:
            self.skipTest(
                "P6 ainda não tem implementação executável para receber "
                "ods.p6.replay.command e produzir reply/state."
            )
        self.skipTest(
            "P6 ainda não expõe um handler identificável para "
            "ods.p6.replay.command; conectar o teste ao runtime ao implementá-lo."
        )
