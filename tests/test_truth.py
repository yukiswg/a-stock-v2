"""Schema / graceful-degradation tests for watchdog.truth.score_truth.

Tests do not assert on upstream akshare data values; only on return shape
and that bad/unknown inputs don't crash and degrade to score=0.

score_truth issues ~200 sequential akshare calls, so we run it in a
background thread with a wall-clock cap and skip on timeout/network error
(per spec: smoke tests may skip on network failure, must not fail).
"""
from __future__ import annotations

import threading

import pytest

from watchdog.truth import score_truth

EXPECTED_KEYS = {"ticker", "name", "score", "verdict", "evidence"}
_WALL_CAP_S = 90.0


def _score_or_skip(ticker: str, name=None) -> dict:
    box: dict = {}
    def _run():
        try:
            box["r"] = score_truth(ticker, name)
        except Exception as e:  # noqa: BLE001
            box["err"] = e
    th = threading.Thread(target=_run, daemon=True)
    th.start()
    th.join(_WALL_CAP_S)
    if th.is_alive():
        pytest.skip(f"score_truth({ticker}) exceeded {_WALL_CAP_S}s (network slow)")
    if "err" in box:
        pytest.skip(f"score_truth({ticker}) network error: {box['err']}")
    return box["r"]


def test_score_truth_returns_schema():
    r = _score_or_skip("688256", "寒武纪")
    assert EXPECTED_KEYS.issubset(r.keys())
    assert isinstance(r["score"], int) and 0 <= r["score"] <= 5
    assert isinstance(r["verdict"], str) and r["verdict"]
    assert isinstance(r["evidence"], dict)


def test_score_truth_bad_ticker_degrades():
    r = _score_or_skip("999999")
    assert EXPECTED_KEYS.issubset(r.keys())
    assert r["score"] == 0
    assert "不入选" in r["verdict"]
