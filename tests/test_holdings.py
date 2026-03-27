from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.data_harness.holdings import load_holdings_snapshot
from ashare_harness_v2.data_harness.holdings import hydrate_holdings_snapshot_with_prices
from ashare_harness_v2.models import DailyBar, DailySeriesSnapshot, HoldingPosition, HoldingsSnapshot


class HoldingsTests(unittest.TestCase):
    def test_load_default_holdings_snapshot(self) -> None:
        snapshot = load_holdings_snapshot(ROOT / "data/input/holdings/default_holdings.csv", as_of="2026-03-23")
        self.assertEqual(snapshot.as_of, "2026-03-23")
        self.assertEqual(len(snapshot.positions), 3)
        self.assertGreater(snapshot.total_market_value, 0)
        self.assertEqual(snapshot.positions[0].code, "601699")

    def test_hydrate_holdings_snapshot_with_series_prices(self) -> None:
        snapshot = HoldingsSnapshot(
            as_of="2026-03-25",
            source_file="test.csv",
            positions=[
                HoldingPosition(
                    code="601699",
                    name="潞安环能",
                    quantity=0.0,
                    available_quantity=0.0,
                    market_value=3500.0,
                    cost_price=None,
                    last_price=None,
                    pnl_amount=None,
                    pnl_pct=None,
                    sector="煤炭",
                )
            ],
            total_market_value=3500.0,
            total_equity=3500.0,
            exposure_ratio=1.0,
            alerts=[],
            sector_weights=[{"sector": "煤炭", "market_value": 3500.0, "weight": 1.0}],
        )
        series_map = {
            "601699": DailySeriesSnapshot(
                code="601699",
                name="潞安环能",
                secid="1.601699",
                fetched_at="2026-03-25T12:00:00",
                source="test",
                bars=[
                    DailyBar(
                        trade_date="2026-03-24",
                        open_price=14.65,
                        close_price=13.87,
                        high_price=14.4,
                        low_price=13.8,
                        volume=253354.0,
                        amount=0.0,
                        amplitude=0.0,
                        pct_change=-0.0532,
                        change_amount=-0.78,
                        turnover=0.0,
                    )
                ],
            )
        }
        enriched = hydrate_holdings_snapshot_with_prices(snapshot, series_map=series_map)
        self.assertEqual(enriched.positions[0].last_price, 13.87)
        self.assertEqual(enriched.positions[0].market_value, 3500.0)


if __name__ == "__main__":
    unittest.main()
