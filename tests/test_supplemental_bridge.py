from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import types
from unittest.mock import patch
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

SCRIPT_PATH = ROOT / "scripts/fetch_akshare_supplemental.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("fetch_akshare_supplemental", SCRIPT_PATH)
assert SCRIPT_SPEC and SCRIPT_SPEC.loader
SUPPLEMENTAL_SCRIPT = importlib.util.module_from_spec(SCRIPT_SPEC)
sys.modules.setdefault("akshare", types.SimpleNamespace())
SCRIPT_SPEC.loader.exec_module(SUPPLEMENTAL_SCRIPT)

from ashare_harness_v2.advice_harness.evidence import build_advice_snapshot, maybe_enrich_snapshot_with_live_supplemental
from ashare_harness_v2.advice_harness.scoring import compute_stock_score
from ashare_harness_v2.config import load_config
from ashare_harness_v2.data_harness import supplemental_bridge as bridge_helper
from ashare_harness_v2.data_harness.supplemental import (
    filter_supplemental_codes,
    merge_payloads,
    parse_provider_names,
    run_supplemental_bridge,
)


class SupplementalBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_config(ROOT / "config/default.toml")

    def test_live_supplemental_payload_merges_into_snapshot(self) -> None:
        snapshot = build_advice_snapshot(self.config, as_of="2026-03-23")
        payload = {
            "fundamentals": {"300750": {"revenue_growth_yoy": 0.2, "profit_growth_yoy": 0.3, "roe": 0.18, "eps": 12.0, "bps": 48.0}},
            "valuation": {"300750": {"industry_name": "电气机械和器材制造业", "industry_pe": 28.0}},
            "capital_flow": {"300750": {"main_net_flow_5d": 200000000.0, "main_net_ratio_5d": 0.03}},
            "external_analysis": {"300750": {"capital_flow_conviction": 0.42, "capital_flow_style": "accumulation"}},
            "company_info": {"300750": {"name": "宁德时代", "business": "动力电池", "industry_name": "电气机械和器材制造业"}},
            "sector_map": {"300750": "电池"},
            "sector_metrics": {"电池": {"score": 64.0, "pct_change": 0.012}},
        }
        with patch("ashare_harness_v2.advice_harness.evidence.ensure_supplemental_payload", return_value=payload):
            applied = maybe_enrich_snapshot_with_live_supplemental(snapshot, config=self.config, codes=["300750"])
        self.assertTrue(applied)
        self.assertEqual(snapshot.explicit_sector_map["300750"], "电池")
        self.assertIn("300750", snapshot.supplemental["fundamentals"])
        self.assertEqual(snapshot.supplemental["external_analysis"]["300750"]["capital_flow_style"], "accumulation")
        self.assertIn("电池", snapshot.sector_metrics)

    def test_stock_score_accepts_industry_relative_valuation_and_main_flow(self) -> None:
        score, positives, negatives, missing, _value_factor = compute_stock_score(
            feature={"trend_score": 62.0, "ret_20d": 0.12, "relative_strength_20d": 0.08},
            fundamentals={"revenue_growth_yoy": 0.18, "profit_growth_yoy": 0.25, "roe": 0.16, "operating_cashflow_margin": 0.14, "gross_margin": 0.22, "roic": 0.12, "debt_to_asset": 0.45},
            valuation={"pe_ttm": 18.0, "pb": 2.1, "pe_vs_industry": 0.82},
            capital_flow={"main_net_flow_5d": 350000000.0, "main_net_ratio_5d": 0.04},
        )
        self.assertGreater(score, 70.0)
        self.assertTrue(any("行业" in item or "主力" in item for item in positives))
        self.assertFalse(any("PE" in item for item in missing))
        self.assertEqual(negatives, [])

    def test_filter_supplemental_codes_skips_benchmarks_and_etfs(self) -> None:
        codes = filter_supplemental_codes(["300750", "513310", "588000", "510300", "601699"])
        self.assertEqual(codes, ["300750", "601699"])

    def test_script_filter_supported_stock_codes_skips_benchmarks_and_etfs(self) -> None:
        codes = SUPPLEMENTAL_SCRIPT.filter_supported_stock_codes(["300750", "513310", "588000", "510300", "601699", "399006"])
        self.assertEqual(codes, ["300750", "601699"])

    def test_parse_provider_names_supports_delimiters_and_fallbacks(self) -> None:
        settings = {
            "provider": "akshare_bridge+qstock_bridge",
            "fallback_providers": "capitalfarmer_bridge",
        }
        self.assertEqual(
            parse_provider_names(settings),
            ["akshare_bridge", "qstock_bridge", "capitalfarmer_bridge"],
        )

    def test_merge_payloads_keeps_bridge_errors(self) -> None:
        merged = merge_payloads(
            {"capital_flow": {"300750": {"main_net_flow_5d": 1.0}}, "errors": {"alpha": ["warn-a"]}},
            {"company_info": {"300750": {"industry_name": "电池"}}, "errors": {"beta": ["warn-b"]}},
        )
        self.assertEqual(merged["capital_flow"]["300750"]["main_net_flow_5d"], 1.0)
        self.assertEqual(merged["company_info"]["300750"]["industry_name"], "电池")
        self.assertEqual(merged["errors"]["alpha"], ["warn-a"])
        self.assertEqual(merged["errors"]["beta"], ["warn-b"])

    def test_merge_payloads_deep_merges_same_code_across_providers(self) -> None:
        merged = merge_payloads(
            {"capital_flow": {"300750": {"main_net_flow_5d": 1.0, "provider": "akshare"}}, "external_analysis": {"300750": {"provider_status": {"akshare": "available"}}}},
            {"capital_flow": {"300750": {"northbound_net_flow_5d": 2.0, "provider": "capitalfarmer"}}, "external_analysis": {"300750": {"provider_status": {"capitalfarmer": "available"}}}},
        )
        self.assertEqual(merged["capital_flow"]["300750"]["main_net_flow_5d"], 1.0)
        self.assertEqual(merged["capital_flow"]["300750"]["northbound_net_flow_5d"], 2.0)
        self.assertEqual(merged["external_analysis"]["300750"]["provider_status"]["akshare"], "available")
        self.assertEqual(merged["external_analysis"]["300750"]["provider_status"]["capitalfarmer"], "available")

    def test_run_supplemental_bridge_merges_multiple_provider_scripts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            alpha = temp_root / "alpha.py"
            beta = temp_root / "beta.py"
            alpha.write_text(
                "import json, sys\n"
                "json.dump({'capital_flow': {'300750': {'main_net_flow_5d': 123.0}}, 'errors': {'alpha': ['ok']}}, sys.stdout)\n",
                encoding="utf-8",
            )
            beta.write_text(
                "import json, sys\n"
                "json.dump({'company_info': {'300750': {'industry_name': '电池'}}, 'errors': {'beta': ['ok']}}, sys.stdout)\n",
                encoding="utf-8",
            )
            config = {
                "project": {"state_dir": str(temp_root / "state")},
                "supplemental": {
                    "enabled": True,
                    "python": sys.executable,
                    "providers": ["alpha", "beta"],
                    "provider_scripts": {
                        "alpha": str(alpha),
                        "beta": str(beta),
                    },
                    "timeout_seconds": 5,
                },
            }
            payload = run_supplemental_bridge(config, as_of="2026-03-27", codes=["300750"])
        self.assertEqual(payload["capital_flow"]["300750"]["main_net_flow_5d"], 123.0)
        self.assertEqual(payload["company_info"]["300750"]["industry_name"], "电池")
        self.assertEqual(payload["errors"]["alpha"], ["ok"])
        self.assertEqual(payload["errors"]["beta"], ["ok"])

    def test_qstock_payload_builder_combines_trend_and_sector(self) -> None:
        with (
            patch.object(bridge_helper, "fetch_hist_moneyflow", return_value={"main_net_flow_5d": 5.0}),
            patch.object(bridge_helper, "fetch_trend_snapshot", return_value={"trend_ret_20d": 0.12}),
            patch.object(bridge_helper, "fetch_sector_snapshot", return_value={"label": "电池", "candidates": [{"label": "电池", "pct_change": 0.02}]}),
        ):
            payload = bridge_helper.build_qstock_payload("2026-03-27", ["300750"])
        self.assertEqual(payload["capital_flow"]["300750"]["main_net_flow_5d"], 5.0)
        self.assertEqual(payload["capital_flow"]["300750"]["provider"], "qstock_adapter")
        self.assertIn("300750", payload["external_analysis"])
        self.assertEqual(payload["sector_map"]["300750"], "电池")
        self.assertEqual(payload["company_info"]["300750"]["sector_candidates"][0]["label"], "电池")

    def test_capitalfarmer_payload_builder_combines_northbound_and_billboard(self) -> None:
        with (
            patch.object(
                bridge_helper,
                "fetch_hist_moneyflow",
                return_value={"main_net_flow_5d": 8000000.0},
            ),
            patch.object(
                bridge_helper,
                "fetch_northbound_stats",
                return_value={"northbound_net_flow_5d": 12000000.0, "industry_name": "电池", "concept_names": ["锂电池"]},
            ),
            patch.object(
                bridge_helper,
                "fetch_billboard_stats",
                return_value={"longhu_appearances_90d": 3},
            ),
        ):
            payload = bridge_helper.build_capitalfarmer_payload("2026-03-27", ["300750"])
        self.assertEqual(payload["capital_flow"]["300750"]["main_net_flow_5d"], 8000000.0)
        self.assertEqual(payload["capital_flow"]["300750"]["northbound_net_flow_5d"], 12000000.0)
        self.assertEqual(payload["capital_flow"]["300750"]["provider"], "capitalfarmer_adapter")
        self.assertIn("300750", payload["external_analysis"])
        self.assertEqual(payload["company_info"]["300750"]["industry_name"], "电池")
        self.assertEqual(payload["company_info"]["300750"]["concept_names"], ["锂电池"])

    def test_qstock_payload_builder_returns_errors_without_crashing_when_requests_fail(self) -> None:
        with (
            patch.object(bridge_helper, "fetch_hist_moneyflow", side_effect=RuntimeError("money failed")),
            patch.object(bridge_helper, "fetch_trend_snapshot", side_effect=RuntimeError("trend failed")),
            patch.object(bridge_helper, "fetch_sector_snapshot", side_effect=RuntimeError("sector failed")),
        ):
            payload = bridge_helper.build_qstock_payload("2026-03-27", ["300750"])
        self.assertIn("300750", payload["errors"])
        self.assertEqual(payload["capital_flow"], {})


if __name__ == "__main__":
    unittest.main()
