from __future__ import annotations

from pathlib import Path
import unittest


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "s6"


class S6IntegrationTests(unittest.TestCase):
    def test_synthesis_command_returns_contract_reply(self):
        executable_sources = list(COMPONENT_ROOT.rglob("*.py")) if COMPONENT_ROOT.is_dir() else []
        if not executable_sources:
            self.skipTest(
                "S6 ainda não tem implementação executável para receber "
                "ods.s6.synthesis.command e produzir reply."
            )
        self.skipTest(
            "S6 ainda não expõe um handler identificável para "
            "ods.s6.synthesis.command; conectar o teste ao runtime ao implementá-lo."
        )
