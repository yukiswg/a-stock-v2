from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for Python < 3.11
    import tomli as tomllib


@dataclass(slots=True)
class UniverseItem:
    code: str
    name: str
    category: str


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("rb") as handle:
        return tomllib.load(handle)


def load_universe(path: str | Path) -> list[UniverseItem]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            UniverseItem(
                code=str(row.get("code") or "").strip(),
                name=str(row.get("name") or "").strip(),
                category=str(row.get("category") or "watch").strip(),
            )
            for row in reader
            if str(row.get("code") or "").strip()
        ]
