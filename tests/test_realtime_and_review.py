from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.evaluation_harness.intraday import build_intraday_review
from ashare_harness_v2.runtime_harness.realtime import run_realtime_session


class RealtimeAndReviewTests(unittest.TestCase):
    def test_synthetic_realtime_generates_alerts_and_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            monitor_plan = [
                {
                    "object_id": "000300",
                    "object_name": "沪深300",
                    "metadata": {"category": "benchmark", "thresholds": {"price_jump_threshold_5m": 0.015, "price_drop_threshold_5m": -0.015, "relative_strength_threshold_5m": 0.01}},
                },
                {
                    "object_id": "300750",
                    "object_name": "宁德时代",
                    "metadata": {"category": "watch", "thresholds": {"price_jump_threshold_5m": 0.018, "price_drop_threshold_5m": -0.015, "relative_strength_threshold_5m": 0.014}},
                },
            ]
            monitor_plan_path = tmp_path / "monitor_plan.json"
            monitor_plan_path.write_text(json.dumps(monitor_plan, ensure_ascii=False, indent=2), encoding="utf-8")
            result = run_realtime_session(
                as_of="2026-03-23",
                realtime_dir=tmp_path / "realtime",
                monitor_plan_path=monitor_plan_path,
                source_kind="synthetic",
                iterations=6,
            )
            self.assertGreater(result["session_summary"]["alert_count"], 0)
            review = build_intraday_review(session_dir=result["session_dir"], horizon_steps=2)
            self.assertGreaterEqual(review["alert_evaluation"]["evaluated_count"], 1)
            self.assertIn("event_breakdown", review["alert_evaluation"])
            self.assertIn("diagnostics", review["alert_evaluation"])
            self.assertTrue(review["headline"])


if __name__ == "__main__":
    unittest.main()
