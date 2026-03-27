from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.evaluation_harness.daily import evaluate_prediction_history


class PredictionEvalTests(unittest.TestCase):
    def test_prediction_history_reads_saved_states(self) -> None:
        result = evaluate_prediction_history(state_dir=ROOT / "data/output/state", cache_dir=ROOT / "data/cache/daily_bars")
        self.assertGreaterEqual(result["market_count"], 1)
        self.assertGreaterEqual(result["watch_count"], 1)
        self.assertIn("basis_breakdown", result)


if __name__ == "__main__":
    unittest.main()
