from __future__ import annotations

from pathlib import Path
import unittest


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
COMPONENT_ROOT = WORKSPACE_ROOT / "p7"
COMMAND_ADAPTER = COMPONENT_ROOT / "command_handler.py"


class P7IntegrationTests(unittest.TestCase):
    def test_move_command_returns_contract_reply_and_state(self):
        if not COMMAND_ADAPTER.is_file():
            self.skipTest(
                "P7 tem driver direto de servos, mas não implementa o handler "
                "de ods.p7.move.command nem publica reply/state conforme o manual."
            )
        self.skipTest(
            "Handler P7 encontrado; conectar este teste ao transporte e ao "
            "atuador simulado para validar reply e ods.p7.state."
        )
