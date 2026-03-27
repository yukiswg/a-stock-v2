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

from ashare_harness_v2.cli import build_parser
from ashare_harness_v2.evaluation_harness import render_fixed_pool_topn_report, write_fixed_pool_topn_report


class FixedPoolEvaluationTests(unittest.TestCase):
    def test_cli_parses_fixed_pool_command(self) -> None:
        args = build_parser().parse_args(
            [
                "evaluate-fixed-pool",
                "--as-of",
                "2026-03-27",
                "--pool-size",
                "20",
                "--top-n",
                "5",
            ]
        )
        self.assertEqual(args.command, "evaluate-fixed-pool")
        self.assertEqual(args.pool_size, 20)
        self.assertEqual(args.top_n, 5)

    def test_render_and_write_report_include_required_metadata(self) -> None:
        result = {
            "as_of": "2026-03-27",
            "begin": "2026-03-01",
            "end": "2026-03-27",
            "pool_size": 20,
            "top_n": 5,
            "signal_day_count": 10,
            "evaluated_day_count": 10,
            "incomplete_selection_days": 0,
            "uses_only_signal_date_data": True,
            "data_basis": "T日按当日可见数据排序，评估T+1收盘相对T日收盘收益；不使用T+1信息做选股。",
            "positive_day_rate": 0.6,
            "daily_average_next_day_return": 0.012,
            "daily_median_next_day_return": 0.01,
            "stock_pick_count": 50,
            "stock_win_rate": 0.58,
            "stock_average_next_day_return": 0.007,
            "stock_median_next_day_return": 0.005,
            "fixed_pool": [
                {"rank": 1, "code": "300750", "name": "宁德时代", "selection_score": 88.2, "trade_action": "正常仓"},
                {"rank": 2, "code": "600519", "name": "贵州茅台", "selection_score": 80.4, "trade_action": "试仓"},
            ],
            "daily_rows": [
                {
                    "as_of": "2026-03-25",
                    "selection_count": 5,
                    "average_next_day_return": 0.013,
                    "positive_pick_ratio": 0.6,
                    "picks": [
                        {"rank": 1, "code": "300750", "name": "宁德时代", "selection_score": 88.2, "next_day_return": 0.02},
                    ],
                }
            ],
        }
        markdown = render_fixed_pool_topn_report(result)
        self.assertIn("样本股池", markdown)
        self.assertIn("每日规则: 从固定池中取前 `5` 名", markdown)
        self.assertIn("是否只用前一日/当日数据: `是`", markdown)
        with tempfile.TemporaryDirectory() as tmp:
            paths = write_fixed_pool_topn_report(result, output_dir=tmp, as_of="2026-03-27")
            self.assertTrue(paths["markdown"].exists())
            self.assertTrue(paths["json"].exists())
            payload = json.loads(paths["json"].read_text(encoding="utf-8"))
            self.assertEqual(payload["top_n"], 5)


if __name__ == "__main__":
    unittest.main()
