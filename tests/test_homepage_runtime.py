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

from ashare_harness_v2.runtime_harness.homepage import update_homepage_with_session
from ashare_harness_v2.runtime_harness.homepage import update_homepage_with_live_quotes
from ashare_harness_v2.decision_harness.rendering import render_homepage_overview
from ashare_harness_v2.models import QuoteSnapshot


class HomepageRuntimeTests(unittest.TestCase):
    def test_session_priority_prefers_execution_urgency(self) -> None:
        base = {
            "as_of": "2026-03-25",
            "market_state": {"regime": "防守期", "probability": 0.38, "score": 38.0, "summary": "先控制风险。"},
            "priority_actions": [],
            "holdings_actions": [
                {
                    "code": "601699",
                    "name": "潞安环能",
                    "action": "cut_on_breakdown",
                    "action_label": "破位减仓",
                    "priority_score": 72.0,
                    "score": 55.0,
                    "reason": "先看风控位",
                    "trigger": "跌破 13.87 减仓",
                    "invalidation": "站回 14.55 暂缓",
                    "levels": {"support_price": 13.87, "pullback_price": 14.55},
                },
                {
                    "code": "513310",
                    "name": "中韩芯片",
                    "action": "cut_on_breakdown",
                    "action_label": "破位减仓",
                    "priority_score": 80.0,
                    "score": 60.0,
                    "reason": "基础分更高",
                    "trigger": "跌破 3.66 减仓",
                    "invalidation": "站回 3.92 暂缓",
                    "levels": {"support_price": 3.66, "pullback_price": 3.92},
                },
            ],
            "watchlist": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            session = Path(tmp)
            quotes = [
                {"timestamp": "2026-03-25T10:00:00", "code": "601699", "name": "潞安环能", "last_price": 13.89, "prev_close": 14.65, "freshness_seconds": 1.0},
                {"timestamp": "2026-03-25T10:00:00", "code": "513310", "name": "中韩芯片", "last_price": 3.88, "prev_close": 3.79, "freshness_seconds": 1.0},
            ]
            (session / "quotes.jsonl").write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in quotes), encoding="utf-8")
            (session / "features.jsonl").write_text("", encoding="utf-8")
            (session / "alerts.jsonl").write_text("", encoding="utf-8")
            updated = update_homepage_with_session(base, session_dir=session)
        self.assertEqual(updated["priority_actions"][0]["code"], "601699")
        self.assertEqual(updated["priority_actions"][0]["execution_urgency_label"], "先处理")
        self.assertIn("风控位", updated["priority_actions"][0]["execution_urgency_reason"])

    def test_premarket_live_quotes_replace_reference_close(self) -> None:
        base = {
            "as_of": "2026-03-25",
            "priority_actions": [{"code": "300750", "name": "宁德时代"}],
            "holdings_actions": [],
            "watchlist": [],
            "current_prices": [
                {
                    "code": "300750",
                    "name": "宁德时代",
                    "last_price": "401.000",
                    "ret_day": "-1.10%",
                    "timestamp": "2026-03-24 close",
                    "freshness": "eod",
                }
            ],
        }

        class StubLiveSource:
            def fetch(self, codes: list[str]) -> list[QuoteSnapshot]:
                self.codes = list(codes)
                return [
                    QuoteSnapshot(
                        code="300750",
                        name="宁德时代",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=412.89,
                        prev_close=409.18,
                        open_price=411.0,
                        high_price=413.2,
                        low_price=410.5,
                        volume=1000000.0,
                        amount=412890000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    )
                ]

        updated = update_homepage_with_live_quotes(base, as_of="2026-03-25", stale_after_seconds=90, live_source=StubLiveSource())
        self.assertEqual(updated["price_mode"], "realtime")
        self.assertEqual(updated["price_section_title"], "当前最新价格")
        self.assertIn("盘前已拉取实时行情", updated["price_note"])
        self.assertEqual(updated["current_prices"][0]["timestamp"], "2026-03-25T09:28:00")
        self.assertEqual(updated["current_prices"][0]["freshness"], "1s")

    def test_session_updates_reprioritize_actions(self) -> None:
        base = {
            "as_of": "2026-03-24",
            "market_state": {
                "regime": "防守期",
                "probability": 0.38,
                "score": 38.0,
                "summary": "先控制风险。",
            },
            "priority_actions": [],
            "holdings_actions": [
                {
                    "code": "513310",
                    "name": "中韩芯片",
                    "action": "cut_on_breakdown",
                    "action_label": "破位减仓",
                    "priority_score": 70.0,
                    "score": 45.0,
                    "reason": "风控优先",
                    "trigger": "若跌破 3.74 执行减仓",
                    "invalidation": "若站回 4.13 暂缓",
                },
                {
                    "code": "601699",
                    "name": "潞安环能",
                    "action": "hold_no_add",
                    "action_label": "继续持有，不再加仓",
                    "priority_score": 75.0,
                    "score": 55.0,
                    "reason": "持有观察",
                    "trigger": "先等指数修复",
                    "invalidation": "跌破 14.28",
                },
            ],
            "watchlist": [
                {
                    "code": "300750",
                    "name": "宁德时代",
                    "action": "watch_market_turn",
                    "action_label": "只观察",
                    "priority_score": 58.0,
                    "score": 52.0,
                    "reason": "市场修复后再看",
                    "trigger": "等指数修复后突破 413.00",
                    "invalidation": "跌破 376.30",
                    "levels": {"breakout_price": 413.0, "support_price": 376.3},
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            session = Path(tmp)
            quotes = [
                {
                    "timestamp": "2026-03-24T10:00:00",
                    "code": "513310",
                    "name": "中韩芯片",
                    "last_price": 3.73,
                    "prev_close": 3.80,
                    "freshness_seconds": 1.0,
                },
                {
                    "timestamp": "2026-03-24T10:00:00",
                    "code": "300750",
                    "name": "宁德时代",
                    "last_price": 414.2,
                    "prev_close": 409.0,
                    "freshness_seconds": 1.0,
                },
                {
                    "timestamp": "2026-03-24T10:00:00",
                    "code": "000300",
                    "name": "沪深300",
                    "last_price": 3400.0,
                    "prev_close": 3480.0,
                    "freshness_seconds": 1.0,
                },
            ]
            alerts = [
                {
                    "timestamp": "2026-03-24T10:00:00",
                    "code": "513310",
                    "name": "中韩芯片",
                    "severity": "high",
                    "event_type": "drawdown_break",
                    "summary": "触发回撤破位",
                },
                {
                    "timestamp": "2026-03-24T10:00:00",
                    "code": "000300",
                    "name": "沪深300",
                    "severity": "high",
                    "event_type": "benchmark_drop",
                    "summary": "指数转弱",
                },
            ]
            (session / "quotes.jsonl").write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in quotes), encoding="utf-8")
            features = [
                {
                    "timestamp": "2026-03-24T10:00:00",
                    "code": "513310",
                    "name": "中韩芯片",
                    "last_price": 3.73,
                    "return_1step": -0.018,
                    "volume_ratio": 1.9,
                    "relative_strength": -0.012,
                    "freshness_seconds": 1.0,
                },
                {
                    "timestamp": "2026-03-24T10:00:00",
                    "code": "300750",
                    "name": "宁德时代",
                    "last_price": 412.8,
                    "return_1step": 0.019,
                    "volume_ratio": 1.6,
                    "relative_strength": 0.016,
                    "freshness_seconds": 1.0,
                },
            ]
            (session / "features.jsonl").write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in features), encoding="utf-8")
            (session / "alerts.jsonl").write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in alerts), encoding="utf-8")

            updated = update_homepage_with_session(base, session_dir=session)
        self.assertEqual(updated["priority_actions"][0]["code"], "513310")
        self.assertIn("优先级说明", f"优先级说明 {updated['priority_actions'][0].get('priority_note', '')}")
        self.assertTrue(updated["holdings_risks"])
        self.assertTrue(updated["watch_opportunities"])
        self.assertEqual(updated["price_mode"], "realtime")
        self.assertEqual(updated["price_section_title"], "当前最新价格")
        self.assertIn("实时会话", updated["price_note"])
        self.assertIn("display_action_label", updated["priority_actions"][0])
        self.assertIn("current_status", updated["priority_actions"][0])
        self.assertIn("display_trigger", updated["watch_opportunities"][0])
        self.assertEqual(updated["market_state"]["regime"], "盘中偏弱")
        self.assertIn("风控", updated["market_state"]["summary"])
        self.assertTrue(
            "日内" in updated["holdings_risks"][0]["current_status"]
            or "短周期波动" in updated["holdings_risks"][0]["current_status"]
        )
        self.assertIn("已经站上", updated["watch_opportunities"][0]["current_status"])

    def test_rendering_marks_reference_close_prices_as_non_realtime(self) -> None:
        overview = {
            "as_of": "2026-03-25",
            "market_state": {},
            "today_action": "盘前观察",
            "price_section_title": "参考收盘价（非实时）",
            "price_note": "未运行实时会话；以下为最近可用收盘价，仅供盘前或离线参考。",
            "current_prices": [
                {
                    "code": "513310",
                    "name": "中韩芯片",
                    "last_price": "3.742",
                    "ret_day": "-5.81%",
                    "timestamp": "2026-03-24 close",
                    "freshness": "eod",
                }
            ],
            "priority_actions": [],
            "holdings_risks": [],
            "watch_opportunities": [],
            "latest_alerts": [],
            "holdings_actions": [],
        }
        rendered = render_homepage_overview(overview)
        self.assertIn("## 参考收盘价（非实时）", rendered)
        self.assertIn("未运行实时会话", rendered)

    def test_rendering_prefers_display_fields(self) -> None:
        overview = {
            "as_of": "2026-03-25",
            "market_state": {"regime": "防守期", "probability": 0.4, "score": 38.0, "summary": "先控制风险。"},
            "price_section_title": "当前最新价格",
            "price_note": "数据来自实时会话。",
            "current_prices": [],
            "priority_actions": [
                {
                    "headline": "先处理 潞安环能",
                    "action_label": "破位减仓",
                    "display_action_label": "先别加仓，跌破再减一点",
                    "display_reason": "潞安环能 现在先别加仓。",
                    "current_status": "盘中最新价 13.95，还在 13.87 防线之上。",
                    "display_trigger": "只有跌破 13.87，才执行减一点。",
                    "display_invalidation": "如果重新回到 14.55 上方，就先别继续减。",
                    "position_guidance": "先减 1/3 到 1/2。",
                }
            ],
            "holdings_risks": [],
            "watch_opportunities": [],
            "latest_alerts": [],
            "holdings_actions": [],
        }
        rendered = render_homepage_overview(overview)
        self.assertIn("先别加仓，跌破再减一点", rendered)
        self.assertIn("盘中最新价 13.95", rendered)
        self.assertIn("只有跌破 13.87", rendered)
        self.assertIn("行动紧急度", rendered)

    def test_premarket_live_quotes_refresh_market_state(self) -> None:
        base = {
            "as_of": "2026-03-25",
            "market_state": {
                "regime": "防守期",
                "probability": 0.38,
                "score": 38.0,
                "summary": "先控制风险。",
            },
            "priority_actions": [{"code": "300750", "name": "宁德时代", "action": "wait_for_breakout", "levels": {"breakout_price": 413.0}}],
            "holdings_actions": [],
            "watchlist": [],
            "current_prices": [
                {
                    "code": "300750",
                    "name": "宁德时代",
                    "last_price": "401.000",
                    "ret_day": "-1.10%",
                    "timestamp": "2026-03-24 close",
                    "freshness": "eod",
                }
            ],
        }

        class StubLiveSource:
            def fetch(self, codes: list[str]) -> list[QuoteSnapshot]:
                return [
                    QuoteSnapshot(
                        code="300750",
                        name="宁德时代",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=414.89,
                        prev_close=409.18,
                        open_price=411.0,
                        high_price=415.2,
                        low_price=410.5,
                        volume=1000000.0,
                        amount=412890000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                    QuoteSnapshot(
                        code="000300",
                        name="沪深300",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=3510.0,
                        prev_close=3460.0,
                        open_price=3472.0,
                        high_price=3515.0,
                        low_price=3468.0,
                        volume=1000000.0,
                        amount=3510000000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                    QuoteSnapshot(
                        code="000001",
                        name="上证指数",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=3380.0,
                        prev_close=3330.0,
                        open_price=3340.0,
                        high_price=3388.0,
                        low_price=3335.0,
                        volume=1000000.0,
                        amount=3380000000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                    QuoteSnapshot(
                        code="399006",
                        name="创业板指",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=2110.0,
                        prev_close=2065.0,
                        open_price=2072.0,
                        high_price=2115.0,
                        low_price=2070.0,
                        volume=1000000.0,
                        amount=2110000000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                ]

        updated = update_homepage_with_live_quotes(base, as_of="2026-03-25", stale_after_seconds=90, live_source=StubLiveSource())
        self.assertEqual(updated["market_state"]["regime"], "盘中修复中")
        self.assertIn("盘前偏防守", updated["market_state"]["summary"])
        self.assertGreater(updated["market_state"]["probability"], 0.38)

    def test_live_quote_refresh_does_not_compound_market_probability(self) -> None:
        base = {
            "as_of": "2026-03-25",
            "market_state": {
                "regime": "防守期",
                "baseline_regime": "防守期",
                "probability": 0.38,
                "baseline_probability": 0.38,
                "score": 38.0,
                "baseline_score": 38.0,
                "summary": "先控制风险。",
            },
            "priority_actions": [{"code": "300750", "name": "宁德时代"}],
            "holdings_actions": [],
            "watchlist": [],
            "current_prices": [],
        }

        class StubLiveSource:
            def fetch(self, codes: list[str]) -> list[QuoteSnapshot]:
                return [
                    QuoteSnapshot(
                        code="300750",
                        name="宁德时代",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=414.89,
                        prev_close=409.18,
                        open_price=411.0,
                        high_price=415.2,
                        low_price=410.5,
                        volume=1000000.0,
                        amount=412890000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                    QuoteSnapshot(
                        code="000300",
                        name="沪深300",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=3510.0,
                        prev_close=3460.0,
                        open_price=3472.0,
                        high_price=3515.0,
                        low_price=3468.0,
                        volume=1000000.0,
                        amount=3510000000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                    QuoteSnapshot(
                        code="000001",
                        name="上证指数",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=3380.0,
                        prev_close=3330.0,
                        open_price=3340.0,
                        high_price=3388.0,
                        low_price=3335.0,
                        volume=1000000.0,
                        amount=3380000000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                    QuoteSnapshot(
                        code="399006",
                        name="创业板指",
                        timestamp="2026-03-25T09:28:00",
                        fetched_at="2026-03-25T09:28:01",
                        freshness_seconds=1.0,
                        is_stale=False,
                        last_price=2110.0,
                        prev_close=2065.0,
                        open_price=2072.0,
                        high_price=2115.0,
                        low_price=2070.0,
                        volume=1000000.0,
                        amount=2110000000.0,
                        turnover=0.01,
                        source="stub_live",
                        trade_date="2026-03-25",
                    ),
                ]

        once = update_homepage_with_live_quotes(base, as_of="2026-03-25", stale_after_seconds=90, live_source=StubLiveSource())
        twice = update_homepage_with_live_quotes(once, as_of="2026-03-25", stale_after_seconds=90, live_source=StubLiveSource())
        self.assertEqual(once["market_state"]["probability"], twice["market_state"]["probability"])


if __name__ == "__main__":
    unittest.main()
