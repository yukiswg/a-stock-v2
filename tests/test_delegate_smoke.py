from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2._delegate_smoke import smoke_message


class DelegateSmokeTests(unittest.TestCase):
    def test_smoke_message_matches_expected_literal(self) -> None:
        self.assertEqual(smoke_message(), "hello from codex delegation")


if __name__ == "__main__":
    unittest.main()
