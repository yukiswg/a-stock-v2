from __future__ import annotations

import sys
import types
from dataclasses import replace
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if "pdfplumber" not in sys.modules:
    pdfplumber_stub = types.ModuleType("pdfplumber")
    pdfplumber_stub.open = lambda *args, **kwargs: None
    sys.modules["pdfplumber"] = pdfplumber_stub
if "pypdf" not in sys.modules:
    pypdf_stub = types.ModuleType("pypdf")
    pypdf_stub.PdfReader = object
    sys.modules["pypdf"] = pypdf_stub

from ashare_harness_v2.advice_harness.evidence import AdviceSnapshot
from ashare_harness_v2.advice_harness.schemas import ActionPlan, ScoreCard, StrategyProfile
from ashare_harness_v2.decision_core import (
    SecurityEvaluation,
    build_action_plan,
    build_next_checks,
    build_position_context,
    candidate_selection_score,
    evaluate_security,
)


def _make_bars(closes: list[float]) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    for index, close in enumerate(closes):
        rows.append(
            {
                "trade_date": f"2026-02-{index + 1:02d}",
                "close_price": close,
                "high_price": round(close * 1.01, 3),
                "low_price": round(close * 0.99, 3),
                "volume": 100000.0 + index * 1000.0,
                "pct_change": 0.0,
            }
        )
    return rows


class DecisionCoreTests(unittest.TestCase):
    def test_candidate_selection_score_prefers_actionable_and_well_covered_setup(self) -> None:
        strategy_profile = StrategyProfile(style="general", label="综合决策", policy_summary="测试", regime_overlay="测试")
        actionable = SecurityEvaluation(
            code="300750",
            name="测试股份",
            category="watch",
            sector="电池",
            using_sector_proxy=False,
            question_type="should_buy",
            decision="buy",
            confidence=0.71,
            summary="测试",
            thesis="测试",
            scorecard=ScoreCard(
                market_score=52.0,
                sector_score=60.0,
                stock_score=72.0,
                timing_score=64.0,
                risk_penalty=10.0,
                missing_data_penalty=0.0,
                coverage_score=82.0,
                total_score=68.0,
            ),
            positive_factors=[],
            negative_factors=[],
            counter_evidence=[],
            missing_information=[],
            next_checks=[],
            trigger_conditions=[],
            invalidation_conditions=[],
            action_plan=ActionPlan(
                action="standard_position",
                label="正常仓",
                rationale="测试",
                position_guidance="测试",
                urgency="medium",
                urgency_score=70.0,
            ),
            strategy_profile=strategy_profile,
        )
        passive = SecurityEvaluation(
            code="300751",
            name="测试观察股",
            category="watch",
            sector="电池",
            using_sector_proxy=False,
            question_type="should_buy",
            decision="watch",
            confidence=0.55,
            summary="测试",
            thesis="测试",
            scorecard=ScoreCard(
                market_score=32.0,
                sector_score=60.0,
                stock_score=72.0,
                timing_score=64.0,
                risk_penalty=24.0,
                missing_data_penalty=0.0,
                coverage_score=46.0,
                total_score=68.0,
            ),
            positive_factors=[],
            negative_factors=[],
            counter_evidence=[],
            missing_information=[],
            next_checks=[],
            trigger_conditions=[],
            invalidation_conditions=[],
            action_plan=ActionPlan(
                action="watch_only",
                label="只观察",
                rationale="测试",
                position_guidance="测试",
                urgency="medium",
                urgency_score=70.0,
            ),
            strategy_profile=strategy_profile,
        )

        actionable_score = candidate_selection_score(actionable, strategy_style="general")
        passive_score = candidate_selection_score(passive, strategy_style="general")

        self.assertGreater(actionable_score, passive_score)
        self.assertGreater(actionable_score, 60.0)
        self.assertLess(passive_score, 60.0)

    def test_evaluate_security_exposes_prediction_bundle(self) -> None:
        closes = [10.0 + index * 0.08 + ((index % 5) - 2) * 0.03 for index in range(45)]
        snapshot = AdviceSnapshot(
            as_of="2026-03-25",
            state_root=None,
            holdings={},
            universe=[],
            feature_map={
                "300750": {
                    "code": "300750",
                    "name": "测试股份",
                    "as_of": "2026-03-25",
                    "category": "stock",
                    "last_close": closes[-1],
                    "ret_1d": 0.011,
                    "ret_5d": 0.026,
                    "ret_20d": 0.092,
                    "high_gap_20d": -0.041,
                    "low_gap_20d": 0.057,
                    "volume_ratio_5d": 1.12,
                    "relative_strength_20d": 0.038,
                    "volatility_20d": 0.024,
                    "trend_score": 58.0,
                    "source": "test",
                }
            },
            series_map={"300750": {"bars": _make_bars(closes)}},
            decision_bundle={
                "market_view": {
                    "action": "balanced",
                    "score": 54.0,
                    "reason": ["市场中性偏稳。"],
                    "metadata": {"label": "均衡"},
                }
            },
            news_items=[],
            announcements=[],
        )
        evaluation = evaluate_security(
            snapshot,
            config={"project": {}},
            code="300750",
            name="测试股份",
            category="stock",
            allow_supplemental_refresh=False,
            fetch_announcements=False,
        )
        self.assertEqual(evaluation.prediction_bundle["code"], "300750")
        self.assertTrue({"intraday", "dayend", "nextday", "longterm"}.issubset(evaluation.prediction_bundle))
        self.assertGreater(evaluation.prediction_bundle["reference_price"], 0.0)
        self.assertIn(evaluation.factor_analysis["provider"], {"builtin_fallback", "jqfactor_analyzer_adapted"})
        self.assertTrue(evaluation.factor_analysis["style_box"])
        self.assertGreaterEqual(len(evaluation.factor_analysis["factors"]), 4)
        self.assertIn("attribution", evaluation.factor_analysis)
        self.assertIn("selection_overlay", evaluation.factor_analysis)

    def test_external_analysis_can_change_main_decision_not_just_report_text(self) -> None:
        closes = [10.0 + index * 0.03 for index in range(45)]
        base_snapshot = AdviceSnapshot(
            as_of="2026-03-25",
            state_root=None,
            holdings={},
            universe=[],
            feature_map={
                "300750": {
                    "code": "300750",
                    "name": "测试股份",
                    "as_of": "2026-03-25",
                    "category": "stock",
                    "last_close": closes[-1],
                    "ret_1d": 0.004,
                    "ret_5d": 0.012,
                    "ret_20d": 0.045,
                    "high_gap_20d": -0.015,
                    "low_gap_20d": 0.031,
                    "volume_ratio_5d": 1.12,
                    "relative_strength_20d": 0.018,
                    "volatility_20d": 0.022,
                    "trend_score": 51.0,
                    "source": "test",
                }
            },
            series_map={"300750": {"bars": _make_bars(closes)}},
            decision_bundle={
                "market_view": {
                    "action": "balanced",
                    "score": 54.0,
                    "reason": ["市场一般。"],
                    "metadata": {"label": "均衡"},
                }
            },
            news_items=[],
            announcements=[],
            supplemental={
                "fundamentals": {"300750": {"revenue_growth_yoy": 0.06, "profit_growth_yoy": 0.08, "roe": 0.11, "eps": 1.0, "bps": 4.5}},
                "valuation": {"300750": {"pe_vs_industry": 0.98}},
                "capital_flow": {"300750": {"main_net_flow_5d": 20000000.0, "main_net_ratio_5d": 0.01}},
                "company_info": {"300750": {"industry_name": "电池"}},
                "external_analysis": {},
            },
            explicit_sector_map={"300750": "电池"},
            sector_metrics={"电池": {"score": 55.0}},
            name_map={"300750": "测试股份"},
        )
        positive_snapshot = replace(
            base_snapshot,
            supplemental={**base_snapshot.supplemental, "external_analysis": {"300750": {"capital_flow_conviction": 0.8, "trend_template_score": 82.0, "rps_proxy_20d": 92.0}}},
        )
        negative_snapshot = replace(
            base_snapshot,
            supplemental={**base_snapshot.supplemental, "external_analysis": {"300750": {"capital_flow_conviction": -0.8, "trend_template_score": 30.0, "rps_proxy_20d": 18.0}}},
        )

        positive = evaluate_security(
            positive_snapshot,
            config={"project": {}},
            code="300750",
            name="测试股份",
            category="stock",
            allow_supplemental_refresh=False,
            fetch_announcements=False,
        )
        negative = evaluate_security(
            negative_snapshot,
            config={"project": {}},
            code="300750",
            name="测试股份",
            category="stock",
            allow_supplemental_refresh=False,
            fetch_announcements=False,
        )

        self.assertGreater(positive.scorecard.stock_score, negative.scorecard.stock_score)
        self.assertGreater(positive.scorecard.total_score, negative.scorecard.total_score)
        self.assertNotEqual(positive.decision, negative.decision)

    def test_build_position_context_falls_back_to_holding_sector_weight(self) -> None:
        snapshot = AdviceSnapshot(
            as_of="2026-03-25",
            state_root=None,
            holdings={
                "positions": [
                    {
                        "code": "601699",
                        "sector": "煤炭",
                        "market_value": 3500.0,
                        "cost_price": None,
                        "last_price": None,
                    }
                ],
                "sector_weights": [{"sector": "煤炭", "weight": 0.35}],
                "total_market_value": 10000.0,
                "total_equity": 10000.0,
                "exposure_ratio": 1.0,
            },
            universe=[],
            feature_map={},
            series_map={},
            decision_bundle={},
            news_items=[],
            announcements=[],
        )
        context = build_position_context(snapshot, code="601699", sector_label="能源金属")
        self.assertEqual(context["sector_weight"], 0.35)
        self.assertEqual(context["sector_weight_basis"], "煤炭")
        self.assertFalse(context["position_pricing_complete"])

    def test_support_zone_buy_uses_support_confirmation_not_breakout_chase(self) -> None:
        closes = [9.6, 9.75, 9.9, 10.0, 10.1, 10.25, 10.5, 10.7, 10.85, 11.0, 11.2, 11.1, 10.95, 10.8, 10.6, 10.45, 10.3, 10.2, 10.1, 10.05]
        snapshot = AdviceSnapshot(
            as_of="2026-03-25",
            state_root=None,
            holdings={},
            universe=[],
            feature_map={},
            series_map={"300750": {"bars": _make_bars(closes)}},
            decision_bundle={},
            news_items=[],
            announcements=[],
        )
        feature = {
            "last_close": 10.05,
            "high_gap_20d": -0.1027,
            "volume_ratio_5d": 0.72,
            "ret_20d": 0.08,
            "ret_5d": -0.021,
        }
        scorecard = ScoreCard(
            market_score=56.0,
            sector_score=60.0,
            stock_score=72.0,
            timing_score=62.0,
            risk_penalty=8.0,
            missing_data_penalty=0.0,
            coverage_score=96.0,
            total_score=71.0,
        )
        next_checks = build_next_checks(
            decision="buy",
            question_type="should_buy",
            feature=feature,
            market_score=scorecard.market_score,
            position_context={},
        )
        plan = build_action_plan(
            security_name="测试股份",
            decision="buy",
            question_type="should_buy",
            scorecard=scorecard,
            feature=feature,
            position_context={},
            next_checks=next_checks,
            positives=["趋势仍在"],
            negatives=["短线回撤"],
            snapshot=snapshot,
            code="300750",
            strategy_style="general",
        )
        self.assertEqual(plan.label, "支撑位试仓")
        self.assertIn("10.05", plan.trigger_conditions[0])
        self.assertNotIn("放量站稳", plan.trigger_conditions[0])
        self.assertGreater(plan.levels["entry_reward_risk"], 1.8)

    def test_add_position_trim_uses_near_support_recheck_and_cost_warning(self) -> None:
        closes = [9.6, 9.75, 9.9, 10.0, 10.1, 10.25, 10.5, 10.7, 10.85, 11.0, 11.2, 11.1, 10.95, 10.8, 10.6, 10.45, 10.3, 10.2, 10.1, 10.05]
        snapshot = AdviceSnapshot(
            as_of="2026-03-25",
            state_root=None,
            holdings={
                "positions": [
                    {
                        "code": "300750",
                        "sector": "新能源",
                        "market_value": 3500.0,
                        "cost_price": None,
                        "last_price": None,
                    }
                ],
                "sector_weights": [{"sector": "新能源", "weight": 0.35}],
                "total_market_value": 10000.0,
                "total_equity": 10000.0,
                "exposure_ratio": 1.0,
            },
            universe=[],
            feature_map={},
            series_map={"300750": {"bars": _make_bars(closes)}},
            decision_bundle={},
            news_items=[],
            announcements=[],
        )
        feature = {
            "last_close": 10.05,
            "high_gap_20d": -0.1027,
            "volume_ratio_5d": 0.65,
            "ret_20d": 0.08,
            "ret_5d": -0.03,
        }
        position_context = build_position_context(snapshot, code="300750", sector_label="电池")
        scorecard = ScoreCard(
            market_score=37.0,
            sector_score=42.0,
            stock_score=20.0,
            timing_score=36.0,
            risk_penalty=18.0,
            missing_data_penalty=0.0,
            coverage_score=96.0,
            total_score=18.0,
        )
        next_checks = build_next_checks(
            decision="trim",
            question_type="add_position",
            feature=feature,
            market_score=scorecard.market_score,
            position_context=position_context,
        )
        plan = build_action_plan(
            security_name="测试股份",
            decision="trim",
            question_type="add_position",
            scorecard=scorecard,
            feature=feature,
            position_context=position_context,
            next_checks=next_checks,
            positives=["还有支撑"],
            negatives=["基本面偏弱"],
            snapshot=snapshot,
            code="300750",
            strategy_style="general",
        )
        self.assertEqual(plan.label, "破位减仓")
        self.assertIn("10.22", plan.invalidation_conditions[0])
        self.assertIn("持仓成本未知", plan.rationale)
        self.assertTrue(any("持仓成本未知" in item for item in plan.do_not))
        self.assertTrue(any("关键支撑或均线附近缩量企稳" in item for item in next_checks))


if __name__ == "__main__":
    unittest.main()
