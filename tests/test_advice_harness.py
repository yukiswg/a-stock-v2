from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.advice_harness import answer_user_query, discover_top_ideas
from ashare_harness_v2.advice_harness.query_parser import parse_user_query
from ashare_harness_v2.config import load_config
from unittest.mock import patch


class AdviceHarnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_config(ROOT / "config/default.toml")

    def test_query_parser_extracts_name_and_horizon(self) -> None:
        parsed = parse_user_query("我想问下宁德时代中线该不该买")
        self.assertEqual(parsed.stock_name_hint, "宁德时代")
        self.assertEqual(parsed.horizon, "medium_term")
        self.assertEqual(parsed.question_type, "should_buy")

    def test_query_parser_extracts_code(self) -> None:
        parsed = parse_user_query("300750 现在能买吗")
        self.assertEqual(parsed.symbol_hint, "300750")

    def test_query_parser_supports_plain_name_and_add_position(self) -> None:
        plain = parse_user_query("宁德时代")
        self.assertEqual(plain.stock_name_hint, "宁德时代")
        add = parse_user_query("宁德时代今天应该加仓吗")
        self.assertEqual(add.question_type, "add_position")
        self.assertTrue(add.has_position_hint)

    def test_query_parser_trims_colloquial_suffix(self) -> None:
        parsed = parse_user_query("三花智控怎么样")
        self.assertEqual(parsed.stock_name_hint, "三花智控")

    def test_query_parser_handles_colloquial_add_position_sentence(self) -> None:
        parsed = parse_user_query("潞安环能现在还能加仓吗？我已有仓位，偏稳健")
        self.assertEqual(parsed.stock_name_hint, "潞安环能")
        self.assertEqual(parsed.question_type, "add_position")
        self.assertTrue(parsed.has_position_hint)

    def test_query_parser_detects_strategy_style(self) -> None:
        trend = parse_user_query("宁德时代趋势跟随现在能买吗")
        pullback = parse_user_query("宁德时代想等回踩低吸，现在怎么看")
        defensive = parse_user_query("宁德时代稳健防守一点，现在能买吗")
        self.assertEqual(trend.strategy_style, "trend_following")
        self.assertEqual(trend.stock_name_hint, "宁德时代")
        self.assertEqual(pullback.strategy_style, "pullback_accumulation")
        self.assertEqual(pullback.stock_name_hint, "宁德时代")
        self.assertEqual(defensive.strategy_style, "defensive_quality")
        self.assertEqual(defensive.stock_name_hint, "宁德时代")

    def test_answer_user_query_returns_structured_decision(self) -> None:
        result = answer_user_query(self.config, question="宁德时代现在能买吗", as_of="2026-03-23")
        self.assertEqual(result["security"]["code"], "300750")
        self.assertIn(result["decision"], {"buy", "watch", "avoid", "insufficient_evidence"})
        self.assertIn("scorecard", result)
        self.assertIn("coverage_score", result["scorecard"])
        self.assertGreaterEqual(len(result["evidence_used"]), 3)
        self.assertIn("thesis", result)
        self.assertIn("action_plan", result)
        self.assertTrue(result["action_plan"]["label"])
        self.assertIn(result["action_plan"]["label"], result["summary"])
        self.assertIn(result["action_plan"]["label"], result["thesis"])
        self.assertGreaterEqual(len(result["trigger_conditions"]), 1)
        self.assertGreaterEqual(len(result["invalidation_conditions"]), 1)
        self.assertIn("position_guidance", result)
        self.assertIn("strategy_profile", result)
        self.assertEqual(result["strategy_profile"]["label"], "综合决策")
        self.assertIn("factor_analysis", result)
        self.assertTrue(result["factor_analysis"]["style_box"])
        self.assertTrue(result["factor_analysis"]["factor_summary"])
        self.assertGreaterEqual(len(result["factor_analysis"]["factors"]), 4)
        self.assertIn("attribution", result["factor_analysis"])
        self.assertIn("selection_overlay", result["factor_analysis"])
        self.assertNotIn("。，", result["thesis"])
        self.assertNotIn("。。", result["thesis"])
        self.assertNotIn(",,", result["thesis"])

    def test_style_based_query_returns_strategy_profile(self) -> None:
        result = answer_user_query(
            self.config,
            question="宁德时代趋势跟随现在能买吗",
            as_of="2026-03-23",
            allow_live_enrich=False,
        )
        self.assertEqual(result["question"]["strategy_style"], "trend_following")
        self.assertEqual(result["strategy_profile"]["label"], "趋势跟随")
        self.assertTrue(result["strategy_profile"]["checklist"])
        self.assertTrue(result["strategy_profile"]["regime_overlay"])
        self.assertIn(result["action_plan"]["label"], result["summary"])

    def test_answer_user_query_gracefully_degrades_when_supplemental_and_jqfactor_fail(self) -> None:
        empty_payload = {
            "fundamentals": {},
            "valuation": {},
            "capital_flow": {},
            "external_analysis": {},
            "company_info": {},
            "sector_map": {},
            "sector_metrics": {},
            "errors": {"qstock_bridge": ["timeout"]},
        }
        with (
            patch("ashare_harness_v2.advice_harness.evidence.ensure_supplemental_payload", return_value=empty_payload),
            patch("ashare_harness_v2.advice_harness.factor_analysis._adapt_factor_rows", return_value=None),
        ):
            result = answer_user_query(self.config, question="宁德时代现在能买吗", as_of="2026-03-23", allow_live_enrich=True)
        self.assertEqual(result["security"]["code"], "300750")
        self.assertEqual(result["factor_analysis"]["provider"], "builtin_fallback")
        self.assertIn(result["decision"], {"buy", "watch", "avoid", "insufficient_evidence"})

    def test_add_position_query_returns_position_context(self) -> None:
        result = answer_user_query(self.config, question="513310 今天应该加仓吗", as_of="2026-03-23", allow_live_enrich=False)
        self.assertIn(result["decision"], {"add", "hold", "trim", "insufficient_evidence", "watch", "avoid"})
        self.assertIn("position_context", result)
        self.assertTrue(result["position_context"]["is_holding"])
        self.assertIn(result["action_plan"]["label"], {"逢高减仓", "破位减仓", "继续持有", "继续持有，不再加仓", "趋势加仓", "等回踩再加"})

    def test_add_position_colloquial_query_resolves_security(self) -> None:
        result = answer_user_query(
            self.config,
            question="潞安环能现在还能加仓吗？我已有仓位，偏稳健",
            as_of="2026-03-23",
            allow_live_enrich=False,
        )
        self.assertEqual(result["security"]["code"], "601699")
        self.assertIn(result["action_plan"]["label"], {"逢高减仓", "破位减仓", "继续持有", "继续持有，不再加仓", "趋势加仓", "等回踩再加"})

    def test_holding_query_without_explicit_add_still_uses_position_mode(self) -> None:
        result = answer_user_query(
            self.config,
            question="中韩芯片现在怎么办？",
            as_of="2026-03-24",
            allow_live_enrich=False,
        )
        self.assertEqual(result["security"]["code"], "513310")
        self.assertEqual(result["question"]["question_type"], "add_position")
        self.assertTrue(result["position_context"]["is_holding"])
        self.assertIn(result["decision"], {"add", "hold", "trim", "insufficient_evidence"})
        self.assertIn(
            result["action_plan"]["label"],
            {"逢高减仓", "破位减仓", "继续持有", "继续持有，不再加仓", "趋势加仓", "等回踩再加", "继续持有，等待市场修复"},
        )
        self.assertNotIn("暂无关键结论", result["thesis"])

    def test_discover_top_ideas_returns_candidates(self) -> None:
        result = discover_top_ideas(self.config, as_of="2026-03-23", limit=5)
        self.assertGreaterEqual(result["idea_count"], 5)
        self.assertEqual(len(result["ideas"]), 5)
        self.assertIn("market_view", result)
        self.assertTrue(all("total_score" in item for item in result["ideas"]))
        self.assertTrue(all("trade_action" in item for item in result["ideas"]))
        self.assertTrue(all("action_plan" in item for item in result["ideas"]))
        self.assertTrue(all("trigger_conditions" in item for item in result["ideas"]))
        self.assertTrue(all((item.get("metadata") or {}).get("selection_score") is not None for item in result["ideas"]))

    def test_discover_top_ideas_respects_strategy_style(self) -> None:
        result = discover_top_ideas(self.config, as_of="2026-03-23", limit=5, strategy_style="trend_following")
        styles = [(item.get("metadata") or {}).get("strategy_style") for item in result["ideas"]]
        selection_scores = [float((item.get("metadata") or {}).get("selection_score") or 0.0) for item in result["ideas"]]
        self.assertTrue(all(style == "trend_following" for style in styles))
        self.assertEqual(selection_scores, sorted(selection_scores, reverse=True))


if __name__ == "__main__":
    unittest.main()
