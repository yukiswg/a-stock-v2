"""End-to-end contract test for watchdog.gate.apply_truth_gate.

We monkeypatch watchdog.gate.score_truth to a fast fake so the test does NOT
hit akshare. This stays offline and finishes in milliseconds.
"""
from __future__ import annotations

from watchdog.gate import apply_truth_gate


def _fake_score(ticker, name=None, config=None):
    return {
        "ticker": ticker, "name": name, "score": 3, "verdict": "🔥 重点推荐",
        "evidence": {
            "announcements": {"hit": True}, "shareholder_changes": {"hit": False},
            "dragon_tiger": {"hit": True}, "earnings_forecast": {"hit": True},
            "institutional_research": {"hit": False},
        },
    }


def _mini_hotspots():
    return {
        "as_of": "2026-04-22",
        "sources": {},
        "themes": [
            {"theme": "半导体", "strength": "strong", "sources_hit": ["industry_boards", "zt_pool"],
             "tickers": ["688256", "688981"]},
            {"theme": "煤炭", "strength": "medium", "sources_hit": ["industry_boards"],
             "tickers": ["601699", "601088"]},
        ],
    }


def test_gate_end_to_end(monkeypatch):
    monkeypatch.setattr("watchdog.gate.score_truth", _fake_score)
    r = apply_truth_gate(_mini_hotspots(), config={"universe": {"holdings": ["601699"]}})
    assert {"key_recommendations", "candidates", "rejected", "stats", "thresholds"}.issubset(r.keys())
    total = len(r["key_recommendations"]) + len(r["candidates"]) + len(r["rejected"])
    assert total == 4  # 4 unique tickers across themes
    assert len(r["key_recommendations"]) == 4  # score=3 >= key_thr
    assert r["stats"]["total_tickers_evaluated"] == 4


def test_gate_empty_hotspots_does_not_crash(monkeypatch):
    monkeypatch.setattr("watchdog.gate.score_truth", _fake_score)
    r = apply_truth_gate({"as_of": "2026-04-22", "themes": []})
    assert r["stats"]["total_tickers_evaluated"] == 0
    assert r["key_recommendations"] == [] and r["candidates"] == [] and r["rejected"] == []
