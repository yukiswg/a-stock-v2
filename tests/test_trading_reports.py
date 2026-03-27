from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.skill_harness.trading_reports import render_investment_report


class TradingReportTests(unittest.TestCase):
    def test_investment_report_renders_factor_analysis_fields(self) -> None:
        markdown = render_investment_report(
            as_of="2026-03-27",
            evaluation={
                "security": {"code": "300750", "name": "宁德时代", "category": "watch", "sector": "电池"},
                "decision": "buy",
                "confidence": 0.71,
                "summary": "测试摘要",
                "thesis": "测试主线",
                "scorecard": {
                    "market_score": 55.0,
                    "sector_score": 60.0,
                    "stock_score": 68.0,
                    "timing_score": 62.0,
                    "risk_penalty": 9.0,
                    "coverage_score": 78.0,
                    "total_score": 66.0,
                },
                "positive_factors": ["景气度改善", "趋势未破坏"],
                "negative_factors": ["仍需等待放量确认"],
                "counter_evidence": ["短线追高赔率一般"],
                "missing_information": [],
                "position_context": {},
                "strategy_profile": {"label": "综合决策", "policy_summary": "测试", "regime_overlay": "测试", "checklist": []},
                "factor_analysis": {
                    "provider": "builtin_fallback",
                    "style_box": "质量 / 动量",
                    "dominant_factor": "质量",
                    "factor_summary": "正向贡献主要来自 质量(+2.1)、动量(+1.2)。拖累主要来自 风险(-1.5)。",
                    "factor_focus": ["ROE 12.0%", "20日收益 +9.0%"],
                    "attribution": {"headline": "风格 +3.3 | 行业 +0.8 | 资金 +0.6 | 风险 -1.5"},
                    "selection_overlay": {"rps_proxy_20d": 84.0, "trend_template_score": 72.0},
                    "factors": [
                        {"name": "质量", "exposure": 0.74, "contribution": 2.1, "signal": "positive", "reason": "ROE 12.0%"},
                        {"name": "风险", "exposure": 0.52, "contribution": -1.5, "signal": "negative", "reason": "波动率偏高"},
                    ],
                },
                "action_plan": {
                    "action": "standard_position",
                    "label": "正常仓",
                    "position_guidance": "先小仓确认",
                    "levels": {"support_price": 200.0, "breakout_price": 210.0},
                    "trigger_conditions": ["站上 210 且量能放大"],
                    "invalidation_conditions": ["跌破 200 则取消"],
                    "blockers": ["未放量前不追价"],
                    "execution_brief": ["现在先观察", "突破再升级", "跌破则取消"],
                },
                "evidence_sources": ["market", "technical"],
                "evidence_highlights": ["市场中性偏稳", "趋势未破坏"],
            },
            market_view={"reason": ["市场中性偏稳"]},
            better_candidates=[],
            pdf_payload=None,
            price_chart=None,
            momentum_chart=None,
        )
        self.assertIn("因子画像", markdown)
        self.assertIn("因子归因", markdown)
        self.assertIn("因子解释与归因", markdown)
        self.assertIn("RPS代理", markdown)


if __name__ == "__main__":
    unittest.main()
