from __future__ import annotations

import sys
import types
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

from ashare_harness_v2.config import load_universe
from ashare_harness_v2.data_harness.holdings import load_holdings_snapshot
from ashare_harness_v2.data_harness.market_data import fetch_daily_series
from ashare_harness_v2.decision_harness.engine import (
    annotate_watchlist_alternatives,
    build_decision_bundle,
    build_homepage_payload,
    serialize_homepage_decision,
)
from ashare_harness_v2.decision_harness.rendering import render_action_summary, render_daily_report, render_homepage_overview
from ashare_harness_v2.models import DailyBar, InstrumentFeatures, StructuredDecision
from ashare_harness_v2.prediction_harness.engine import build_prediction_bundle


class DecisionBundleTests(unittest.TestCase):
    def test_homepage_payload_and_rendering_expose_predictions(self) -> None:
        bars: list[DailyBar] = []
        price = 10.0
        for index in range(45):
            open_price = price
            close_price = price * (1 + 0.003 + ((index % 6) - 2) * 0.0008)
            high_price = max(open_price, close_price) * 1.01
            low_price = min(open_price, close_price) * 0.99
            bars.append(
                DailyBar(
                    trade_date=f"202602{index + 1:02d}",
                    open_price=round(open_price, 3),
                    close_price=round(close_price, 3),
                    high_price=round(high_price, 3),
                    low_price=round(low_price, 3),
                    volume=120000.0 + index * 500.0,
                    amount=1400000.0 + index * 1200.0,
                    amplitude=0.02,
                    pct_change=(close_price / open_price) - 1,
                    change_amount=close_price - open_price,
                    turnover=0.012,
                )
            )
            price = close_price

        prediction_bundle = build_prediction_bundle(
            code="300750",
            name="测试股份",
            bars=bars,
            trend_score=58.0,
            relative_strength=0.03,
        ).to_dict()
        holding = StructuredDecision(
            object_type="holding",
            object_id="300750",
            object_name="测试股份",
            at="2026-03-25",
            action="hold_no_add",
            score=66.0,
            probability=0.58,
            reason=["先持有，等待更好的加仓赔率。"],
            risk=["短线节奏一般。"],
            sources=["unit_test"],
            thesis="测试股份有预测数据可供展示。",
            priority_score=63.0,
            metadata={
                "strategy_style": "pullback_accumulation",
                "strategy_label": "回踩低吸",
                "action_plan": {"label": "持有观察", "position_guidance": "不追价。", "levels": {}},
                "prediction_bundle": prediction_bundle,
            },
        )
        market_view = StructuredDecision(
            object_type="market",
            object_id="market",
            object_name="A股市场",
            at="2026-03-25",
            action="balanced",
            score=52.0,
            probability=0.53,
            reason=["市场中性偏稳。"],
            risk=[],
            sources=["unit_test"],
            thesis="保持均衡。",
            metadata={"label": "均衡", "regime": "震荡", "policy": ["控制追价"]},
        )
        feature_map = {
            "300750": InstrumentFeatures(
                code="300750",
                name="测试股份",
                as_of="2026-03-25",
                category="stock",
                last_close=bars[-1].close_price,
                ret_1d=0.011,
                ret_5d=0.025,
                ret_20d=0.081,
                high_gap_20d=-0.04,
                low_gap_20d=0.05,
                volume_ratio_5d=1.08,
                relative_strength_20d=0.03,
                volatility_20d=0.022,
                trend_score=58.0,
                source="test",
            )
        }
        homepage = build_homepage_payload(
            as_of="2026-03-25",
            market_view=market_view,
            holdings_actions=[holding],
            watchlist=[],
            action_summary="先观察，不追价。",
            feature_map=feature_map,
        )
        serialized = serialize_homepage_decision(holding)
        rendered = render_homepage_overview(homepage)

        self.assertIn("prediction_bundle", serialized)
        self.assertTrue({"intraday", "dayend", "nextday", "longterm"}.issubset(serialized["prediction_bundle"]))
        self.assertEqual(len(homepage["predictions"]), 1)
        self.assertTrue({"intraday", "dayend", "nextday", "longterm"}.issubset(homepage["predictions"][0]["bundle"]))
        self.assertIn("## 价格预测（统计模型，仅供参考）", rendered)
        self.assertIn("次日预测", rendered)
        self.assertIn("10日期望", rendered)
        self.assertIn("测试股份(300750)", rendered)

    def test_decision_bundle_has_structured_sections(self) -> None:
        holdings = load_holdings_snapshot(ROOT / "data/input/holdings/default_holdings.csv", as_of="2026-03-17")
        universe = load_universe(ROOT / "config/universe.csv")
        required = {item.code: item.name for item in universe}
        for position in holdings.positions:
            required[position.code] = position.name
        series_map = {
            code: fetch_daily_series(code=code, name=name, cache_dir=ROOT / "data/cache/daily_bars", end="2026-03-17")
            for code, name in required.items()
        }
        bundle, feature_map = build_decision_bundle(
            as_of="2026-03-17",
            holdings=holdings,
            universe=universe,
            series_map=series_map,
            news_items=[],
            announcements=[],
            llm_summary={},
        )
        self.assertIn(bundle.market_view.action, {"risk_off", "balanced", "risk_on"})
        self.assertGreaterEqual(len(bundle.holdings_actions), 1)
        self.assertGreaterEqual(len(bundle.watchlist), 1)
        self.assertGreaterEqual(len(bundle.monitor_plan), 3)
        self.assertIn("today_action", bundle.homepage_overview)
        self.assertIn("market_state", bundle.homepage_overview)
        self.assertIn("priority_actions", bundle.homepage_overview)
        self.assertIn("holdings_risks", bundle.homepage_overview)
        self.assertIn("watch_opportunities", bundle.homepage_overview)
        self.assertIn("000300", feature_map)
        self.assertTrue(all(hasattr(item, "priority_score") for item in bundle.holdings_actions))
        self.assertTrue(all(item.action for item in bundle.watchlist))
        self.assertTrue(all((item.metadata or {}).get("strategy_style") in {"trend_following", "pullback_accumulation", "defensive_quality"} for item in bundle.watchlist))
        self.assertTrue(all((item.metadata or {}).get("selection_score") is not None for item in bundle.watchlist))

    def test_daily_report_prefers_realtime_market_state_probability(self) -> None:
        holdings = load_holdings_snapshot(ROOT / "data/input/holdings/default_holdings.csv", as_of="2026-03-17")
        universe = load_universe(ROOT / "config/universe.csv")
        required = {item.code: item.name for item in universe}
        for position in holdings.positions:
            required[position.code] = position.name
        series_map = {
            code: fetch_daily_series(code=code, name=name, cache_dir=ROOT / "data/cache/daily_bars", end="2026-03-17")
            for code, name in required.items()
        }
        bundle, _ = build_decision_bundle(
            as_of="2026-03-17",
            holdings=holdings,
            universe=universe,
            series_map=series_map,
            news_items=[],
            announcements=[],
            llm_summary={},
        )
        bundle.homepage_overview["market_state"]["regime"] = "盘中修复中"
        bundle.homepage_overview["market_state"]["probability"] = 0.51
        bundle.homepage_overview["market_state"]["score"] = 51.0
        bundle.homepage_overview["market_state"]["realtime_change_log"] = ["指数修复。", "观察名单转强。"]
        rendered = render_daily_report(bundle=bundle, holdings=holdings, news_items=[], announcements=[])
        self.assertIn("状态: `盘中修复中` | 概率 `51%` | 分数 `51.0`", rendered)
        self.assertIn("盘中变化: 指数修复。 观察名单转强。", rendered)
        self.assertIn("当前结论:", rendered)
        self.assertIn("风险/反证:", rendered)
        self.assertIn("触发条件:", rendered)
        self.assertIn("失效条件:", rendered)
        self.assertIn("仓位建议:", rendered)

    def test_action_summary_prefers_live_priorities(self) -> None:
        holdings = load_holdings_snapshot(ROOT / "data/input/holdings/default_holdings.csv", as_of="2026-03-17")
        universe = load_universe(ROOT / "config/universe.csv")
        required = {item.code: item.name for item in universe}
        for position in holdings.positions:
            required[position.code] = position.name
        series_map = {
            code: fetch_daily_series(code=code, name=name, cache_dir=ROOT / "data/cache/daily_bars", end="2026-03-17")
            for code, name in required.items()
        }
        bundle, _ = build_decision_bundle(
            as_of="2026-03-17",
            holdings=holdings,
            universe=universe,
            series_map=series_map,
            news_items=[],
            announcements=[],
            llm_summary={},
        )
        bundle.homepage_overview["market_state"]["regime"] = "盘中修复中"
        bundle.homepage_overview["market_state"]["probability"] = 0.51
        bundle.homepage_overview["priority_actions"] = [
            {
                "headline": "先处理 潞安环能 的破位减仓",
                "action_label": "破位减仓",
                "display_action_label": "先别加仓，跌破再减一点",
                "execution_urgency_label": "先处理",
                "current_status": "盘中最新价 13.95，离防线很近。",
            },
            {
                "headline": "观察 中闽能源 的等放量突破触发",
                "action_label": "等放量突破",
                "display_action_label": "先观察，站上再看",
                "execution_urgency_label": "重点确认",
                "current_status": "盘中最新价 7.54，已到触发位。",
            },
        ]
        rendered = render_action_summary(bundle)
        self.assertIn("今天怎么做: 盘中修复中，现在先处理：先处理 潞安环能 的破位减仓(先处理)；观察 中闽能源 的等放量突破触发(重点确认)。", rendered)

    def test_daily_report_shows_action_family_and_position_completeness(self) -> None:
        holdings = load_holdings_snapshot(ROOT / "data/input/holdings/default_holdings.csv", as_of="2026-03-17")
        universe = load_universe(ROOT / "config/universe.csv")
        required = {item.code: item.name for item in universe}
        for position in holdings.positions:
            required[position.code] = position.name
        series_map = {
            code: fetch_daily_series(code=code, name=name, cache_dir=ROOT / "data/cache/daily_bars", end="2026-03-17")
            for code, name in required.items()
        }
        bundle, _ = build_decision_bundle(
            as_of="2026-03-17",
            holdings=holdings,
            universe=universe,
            series_map=series_map,
            news_items=[],
            announcements=[],
            llm_summary={},
        )
        rendered = render_daily_report(bundle=bundle, holdings=holdings, news_items=[], announcements=[])
        self.assertIn("动作族 `", rendered)
        self.assertIn("数量或可卖数量缺失，不下具体手数", rendered)

    def test_watchlist_can_downgrade_to_better_alternative(self) -> None:
        leader = StructuredDecision(
            object_type="watch",
            object_id="300001",
            object_name="龙头一号",
            at="2026-03-17",
            action="wait_for_breakout",
            score=78.0,
            probability=0.62,
            reason=["等待突破", "趋势延续更完整"],
            risk=["估值偏高"],
            sources=["daily_features"],
            thesis="龙头一号更接近执行位。",
            counterpoints=["估值偏高"],
            trigger_conditions=["站上 12.50 再处理"],
            invalidation_conditions=["跌破 11.30 取消"],
            priority_score=82.0,
            metadata={
                "sector": "通信设备",
                "selection_score": 82.0,
                "action_plan": {
                    "label": "等放量突破",
                    "position_guidance": "先观察，不提前追价。",
                    "trigger_conditions": ["站上 12.50 再处理"],
                    "invalidation_conditions": ["跌破 11.30 取消"],
                    "levels": {"breakout_price": 12.5, "support_price": 11.3},
                },
            },
        )
        follower = StructuredDecision(
            object_type="watch",
            object_id="300002",
            object_name="跟随二号",
            at="2026-03-17",
            action="watch_only",
            score=61.0,
            probability=0.48,
            reason=["先观察", "还没进入可交易区间"],
            risk=["位置偏中段"],
            sources=["daily_features"],
            thesis="跟随二号暂时还没有马上执行的理由。",
            counterpoints=["位置偏中段"],
            trigger_conditions=["等待重新靠近 9.80"],
            invalidation_conditions=["跌破 8.90 继续回避"],
            priority_score=58.0,
            metadata={
                "sector": "通信设备",
                "selection_score": 58.0,
                "action_plan": {
                    "label": "只观察",
                    "position_guidance": "先观察，不分配资金。",
                    "trigger_conditions": ["等待重新靠近 9.80"],
                    "invalidation_conditions": ["跌破 8.90 继续回避"],
                    "levels": {"breakout_price": 9.8, "support_price": 8.9},
                },
            },
        )
        watchlist = [leader, follower]
        annotate_watchlist_alternatives(watchlist)
        self.assertEqual(follower.action, "switch_to_better_alternative")
        self.assertEqual(follower.metadata["preferred_alternative"]["name"], "龙头一号")
        self.assertEqual(follower.metadata["action_plan"]["label"], "替代标的")

    def test_action_summary_watch_section_prefers_alternative_hint(self) -> None:
        holdings = load_holdings_snapshot(ROOT / "data/input/holdings/default_holdings.csv", as_of="2026-03-17")
        universe = load_universe(ROOT / "config/universe.csv")
        required = {item.code: item.name for item in universe}
        for position in holdings.positions:
            required[position.code] = position.name
        series_map = {
            code: fetch_daily_series(code=code, name=name, cache_dir=ROOT / "data/cache/daily_bars", end="2026-03-17")
            for code, name in required.items()
        }
        bundle, _ = build_decision_bundle(
            as_of="2026-03-17",
            holdings=holdings,
            universe=universe,
            series_map=series_map,
            news_items=[],
            announcements=[],
            llm_summary={},
        )
        bundle.homepage_overview["watch_opportunities"] = [
            {
                "name": "跟随二号",
                "code": "300002",
                "action_label": "替代标的",
                "display_action_label": "先看更强的替代标的",
                "analysis_comparison": "同梯队更值得先看的标的是 龙头一号，当前优先级高出 24.0 分。",
            }
        ]
        rendered = render_action_summary(bundle)
        self.assertIn("同梯队更值得先看的标的是 龙头一号", rendered)


if __name__ == "__main__":
    unittest.main()
