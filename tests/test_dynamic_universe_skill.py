from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.config import UniverseItem
from ashare_harness_v2.skill_harness.sector_rotation import build_dynamic_universe_from_sectors


class DynamicUniverseSkillTests(unittest.TestCase):
    def test_sector_skill_selects_top_sectors_and_leaders(self) -> None:
        config = {
            "dynamic_universe": {
                "enabled": True,
                "top_sector_limit": 5,
                "leaders_per_sector": 1,
                "min_sector_score": 52,
                "include_static_watch": False,
            }
        }
        base_universe = [
            UniverseItem(code="000300", name="沪深300", category="benchmark"),
            UniverseItem(code="300750", name="宁德时代", category="watch"),
        ]
        sector_metrics = {
            "电力": {"score": 72.0, "pct_change": 0.041, "net_inflow": 80.0, "leader": "迪森股份"},
            "环保设备": {"score": 70.0, "pct_change": 0.039, "net_inflow": 50.0, "leader": "雪浪环境"},
            "半导体": {"score": 68.0, "pct_change": 0.028, "net_inflow": 40.0, "leader": "中芯国际"},
            "软件开发": {"score": 66.0, "pct_change": 0.021, "net_inflow": 20.0, "leader": "金山办公"},
            "汽车整车": {"score": 64.0, "pct_change": 0.018, "net_inflow": 10.0, "leader": "赛力斯"},
            "煤炭": {"score": 58.0, "pct_change": 0.012, "net_inflow": 8.0, "leader": "潞安环能"},
        }
        resolver_map = {
            "迪森股份": {"code": "300335", "zwjc": "迪森股份"},
            "雪浪环境": {"code": "300385", "zwjc": "雪浪环境"},
            "中芯国际": {"code": "688981", "zwjc": "中芯国际"},
            "金山办公": {"code": "688111", "zwjc": "金山办公"},
            "赛力斯": {"code": "601127", "zwjc": "赛力斯"},
            "潞安环能": {"code": "601699", "zwjc": "潞安环能"},
        }

        result = build_dynamic_universe_from_sectors(
            config=config,
            base_universe=base_universe,
            holdings_codes={"601699"},
            sector_metrics=sector_metrics,
            resolver=lambda name: resolver_map.get(name),
        )

        self.assertEqual(result["selection_mode"], "dynamic_sector_leaders")
        self.assertEqual(len(result["top_sectors"]), 5)
        self.assertEqual(len(result["leaders"]), 5)
        self.assertTrue(all(item["code"] != "601699" for item in result["leaders"]))
        self.assertEqual(result["universe"][0]["code"], "000300")
        self.assertTrue(any(item["code"] == "300335" for item in result["universe"]))
        self.assertFalse(any(item["code"] == "300750" and item["category"] == "watch" for item in result["universe"]))

    def test_sector_skill_falls_back_to_static_watch_when_dynamic_empty(self) -> None:
        config = {"dynamic_universe": {"enabled": True, "include_static_watch": False}}
        base_universe = [
            UniverseItem(code="000300", name="沪深300", category="benchmark"),
            UniverseItem(code="300750", name="宁德时代", category="watch"),
        ]
        result = build_dynamic_universe_from_sectors(
            config=config,
            base_universe=base_universe,
            holdings_codes=set(),
            sector_metrics={},
            resolver=lambda _: None,
        )
        self.assertEqual(len(result["leaders"]), 0)
        self.assertTrue(any(item["code"] == "300750" for item in result["universe"]))


if __name__ == "__main__":
    unittest.main()
