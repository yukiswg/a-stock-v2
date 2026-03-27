from __future__ import annotations

from datetime import datetime, timedelta
import importlib.util
import sys
from pathlib import Path
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.data_harness.market_data import code_to_secid, fetch_daily_series, prefer_cached_series
from ashare_harness_v2.data_harness.market_data import trend_score_from_parts
from ashare_harness_v2.models import DailyBar


def load_compute_timing_score():
    package_name = "ashare_harness_v2.advice_harness"
    package = sys.modules.get(package_name)
    if package is None:
        package = types.ModuleType(package_name)
        package.__path__ = [str(SRC / "ashare_harness_v2" / "advice_harness")]
        sys.modules[package_name] = package

    module_specs = {
        "ashare_harness_v2.advice_harness.schemas": SRC / "ashare_harness_v2" / "advice_harness" / "schemas.py",
        "ashare_harness_v2.advice_harness.scoring": SRC / "ashare_harness_v2" / "advice_harness" / "scoring.py",
    }
    for module_name, module_path in module_specs.items():
        if module_name in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        assert spec and spec.loader
        spec.loader.exec_module(module)
    scoring_module = sys.modules["ashare_harness_v2.advice_harness.scoring"]
    return scoring_module.compute_timing_score


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls(2026, 3, 25, 9, 30, 0, tzinfo=tz)


class MarketDataTests(unittest.TestCase):
    def test_index_code_maps_to_correct_secid(self) -> None:
        self.assertEqual(code_to_secid("000300"), "1.000300")
        self.assertEqual(code_to_secid("399006"), "0.399006")

    def test_cached_series_can_be_loaded_for_historical_date(self) -> None:
        series = fetch_daily_series(
            code="000300",
            name="沪深300",
            cache_dir=ROOT / "data/cache/daily_bars",
            end="2026-03-17",
        )
        self.assertEqual(series.bars[-1].trade_date, "2026-03-17")
        self.assertGreaterEqual(len(series.bars), 30)

    @patch("ashare_harness_v2.data_harness.market_data.datetime", FrozenDateTime)
    def test_prefer_cached_series_rejects_stale_daily_bar_for_current_day(self) -> None:
        stale_bars = self._bars_ending_on("2026-03-23")
        fresh_bars = self._bars_ending_on("2026-03-24")
        self.assertFalse(prefer_cached_series(stale_bars, end="2026-03-25"))
        self.assertTrue(prefer_cached_series(fresh_bars, end="2026-03-25"))

    def test_trend_score_penalizes_chasing_after_eight_percent(self) -> None:
        base = trend_score_from_parts(
            ret_5=0.08,
            ret_20=0.0,
            relative_strength=0.0,
            high_gap=-0.02,
            volume_ratio=1.0,
        )
        chase = trend_score_from_parts(
            ret_5=0.12,
            ret_20=0.0,
            relative_strength=0.0,
            high_gap=-0.02,
            volume_ratio=1.0,
        )
        self.assertLess(chase, base)

    def test_trend_score_rewards_pullback_vs_neutral_baseline(self) -> None:
        neutral = trend_score_from_parts(
            ret_5=0.0,
            ret_20=0.0,
            relative_strength=0.0,
            high_gap=-0.02,
            volume_ratio=1.0,
        )
        pullback = trend_score_from_parts(
            ret_5=-0.06,
            ret_20=0.0,
            relative_strength=0.0,
            high_gap=-0.12,
            volume_ratio=1.0,
        )
        self.assertGreater(pullback, neutral)

    def test_compute_timing_score_flags_chasing_and_pullbacks(self) -> None:
        compute_timing_score = load_compute_timing_score()
        chase_score, chase_positives, chase_negatives = compute_timing_score(
            feature={
                "ret_5d": 0.09,
                "high_gap_20d": -0.02,
                "volume_ratio_5d": 1.0,
                "volatility_20d": 0.02,
            },
            horizon="swing",
        )
        pullback_score, pullback_positives, pullback_negatives = compute_timing_score(
            feature={
                "ret_5d": -0.06,
                "high_gap_20d": -0.12,
                "volume_ratio_5d": 1.0,
                "volatility_20d": 0.02,
            },
            horizon="swing",
        )

        self.assertLess(chase_score, pullback_score)
        self.assertIn("5日涨幅过大进入超买区，均值回归风险大，勿追。", chase_negatives)
        self.assertIn("距20日高点 -2.00%，接近突破位，但注意追高风险。", chase_positives)
        self.assertIn("5日回撤且处于20日低位，均值回归概率偏高（赔率优于追涨）。", pullback_positives)
        self.assertIn("距20日高点 -12.00%，处于回调中，右侧确认不足。", pullback_negatives)

    def _bars_ending_on(self, trade_date: str) -> list[DailyBar]:
        end_day = datetime.strptime(trade_date, "%Y-%m-%d").date()
        rows: list[DailyBar] = []
        for offset in range(29, -1, -1):
            day = end_day - timedelta(days=offset)
            price = 10.0 + len(rows) * 0.1
            rows.append(
                DailyBar(
                    trade_date=day.isoformat(),
                    open_price=price,
                    close_price=price,
                    high_price=price,
                    low_price=price,
                    volume=1000.0,
                    amount=10000.0,
                    amplitude=0.0,
                    pct_change=0.0,
                    change_amount=0.0,
                    turnover=0.0,
                )
            )
        return rows


if __name__ == "__main__":
    unittest.main()
