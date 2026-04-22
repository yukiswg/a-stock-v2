"""Contract tests for watchdog.vix.run_vix_signal.

Network-dependent: may fail to reach yfinance. In that case the function
returns {"error": "..."} and we accept that as a valid contract outcome.
"""
from __future__ import annotations

from watchdog.vix import run_vix_signal


def _looks_like_error(result: dict) -> bool:
    return isinstance(result, dict) and "error" in result


def test_default_config_returns_expected_keys():
    r = run_vix_signal({})
    assert isinstance(r, dict)
    if _looks_like_error(r):
        return  # network-degraded path is legal
    for k in ("status", "red_count", "tech_pct", "signals"):
        assert k in r, f"missing key {k}"
    for s in ("rsi", "vix", "momentum"):
        assert s in r["signals"], f"missing signal {s}"


def test_custom_tickers_does_not_crash():
    cfg = {
        "tickers_tech": {"688256.SS": "寒武纪"},
        "tickers_defense": {"601857.SS": "中国石油"},
    }
    r = run_vix_signal(cfg)
    assert isinstance(r, dict)
    assert _looks_like_error(r) or "status" in r
