"""Smoke / schema tests for watchdog.find_hotspots.

The inbox-empty test is fully offline: we point inbox_dir at a tmp path
and only care that _fetch_inbox doesn't crash on an empty dir.
"""
from __future__ import annotations

from pathlib import Path

from watchdog.find_hotspots import find_hotspots, _fetch_inbox


def test_find_hotspots_smoke():
    r = find_hotspots(config=None)
    assert {"as_of", "sources", "themes"}.issubset(r.keys())
    assert isinstance(r["sources"], dict) and r["sources"]
    assert isinstance(r["themes"], list)
    # at least one source must report ok=True (inbox always does locally)
    assert any(v.get("ok") for v in r["sources"].values())


def test_inbox_empty_does_not_crash(tmp_path: Path):
    items = _fetch_inbox(tmp_path, {"601699": "潞安环能"})
    assert items == []
    # also tolerate non-existent dir
    assert _fetch_inbox(tmp_path / "nope", {}) == []
