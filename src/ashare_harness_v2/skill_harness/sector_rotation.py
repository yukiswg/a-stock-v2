from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Callable

from ..config import UniverseItem
from ..data_harness.news import resolve_security


@dataclass(slots=True)
class DynamicLeaderCandidate:
    code: str
    name: str
    sector: str
    sector_score: float
    sector_pct_change: float | None
    sector_net_inflow: float | None
    leader_name_raw: str
    rank: int
    rank_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_dynamic_universe_from_sectors(
    *,
    config: dict[str, Any],
    base_universe: list[UniverseItem],
    holdings_codes: set[str],
    sector_metrics: dict[str, dict[str, Any]],
    resolver: Callable[[str], dict[str, str] | None] = resolve_security,
) -> dict[str, Any]:
    settings = config.get("dynamic_universe") or {}
    enabled = bool(settings.get("enabled", False))
    top_sector_limit = max(int(settings.get("top_sector_limit") or 5), 1)
    leaders_per_sector = max(int(settings.get("leaders_per_sector") or 1), 1)
    min_sector_score = float(settings.get("min_sector_score") or 50.0)
    include_static_watch = bool(settings.get("include_static_watch", True))
    if not enabled:
        return {
            "enabled": False,
            "selection_mode": "static_only",
            "top_sectors": [],
            "leaders": [],
            "resolver_failures": [],
            "universe": [asdict_universe(item) for item in base_universe],
            "universe_items": list(base_universe),
        }

    ranked_sectors = rank_sectors(
        sector_metrics=sector_metrics,
        top_sector_limit=top_sector_limit,
        min_sector_score=min_sector_score,
    )
    candidates: list[DynamicLeaderCandidate] = []
    failures: list[dict[str, str]] = []
    seen_codes = {item.code for item in base_universe if item.category == "watch"}
    for index, sector in enumerate(ranked_sectors, start=1):
        leader_names = split_leader_names(str(sector.get("leader") or ""))
        if not leader_names:
            failures.append({"sector": str(sector["sector"]), "reason": "missing_leader_name"})
            continue
        selected_in_sector = 0
        for leader_name in leader_names:
            if selected_in_sector >= leaders_per_sector:
                break
            try:
                resolved = resolver(leader_name)
            except Exception:
                resolved = None
            if not resolved:
                failures.append({"sector": str(sector["sector"]), "reason": f"resolve_failed:{leader_name}"})
                continue
            code = str(resolved.get("code") or "").strip()
            name = str(resolved.get("zwjc") or leader_name).strip()
            if not code:
                failures.append({"sector": str(sector["sector"]), "reason": f"empty_code:{leader_name}"})
                continue
            if code in holdings_codes:
                failures.append({"sector": str(sector["sector"]), "reason": f"skip_holding:{code}"})
                continue
            if code in seen_codes:
                continue
            seen_codes.add(code)
            selected_in_sector += 1
            candidates.append(
                DynamicLeaderCandidate(
                    code=code,
                    name=name or leader_name,
                    sector=str(sector["sector"]),
                    sector_score=float(sector.get("score") or 0.0),
                    sector_pct_change=to_float(sector.get("pct_change")),
                    sector_net_inflow=to_float(sector.get("net_inflow")),
                    leader_name_raw=leader_name,
                    rank=index,
                    rank_score=float(sector.get("rank_score") or 0.0),
                )
            )

    universe_items = merge_universe(
        base_universe=base_universe,
        dynamic_candidates=candidates,
        include_static_watch=include_static_watch,
    )
    return {
        "enabled": True,
        "selection_mode": "dynamic_sector_leaders",
        "top_sectors": ranked_sectors,
        "leaders": [item.to_dict() for item in candidates],
        "resolver_failures": failures[:40],
        "universe": [asdict_universe(item) for item in universe_items],
        "universe_items": universe_items,
    }


def rank_sectors(
    *,
    sector_metrics: dict[str, dict[str, Any]],
    top_sector_limit: int,
    min_sector_score: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for label, payload in (sector_metrics or {}).items():
        sector = str(label or "").strip()
        if not sector:
            continue
        score = float(payload.get("score") or 0.0)
        if score < min_sector_score:
            continue
        pct_change = to_float(payload.get("pct_change"))
        net_inflow = to_float(payload.get("net_inflow"))
        rank_score = score
        if isinstance(pct_change, float):
            rank_score += max(min(pct_change * 140.0, 8.0), -8.0)
        if isinstance(net_inflow, float):
            rank_score += max(min(net_inflow / 20.0, 6.0), -6.0)
        rows.append(
            {
                "sector": sector,
                "score": round(score, 2),
                "pct_change": pct_change,
                "net_inflow": net_inflow,
                "leader": str(payload.get("leader") or "").strip(),
                "leader_pct_change": to_float(payload.get("leader_pct_change")),
                "rank_score": round(rank_score, 2),
            }
        )
    rows.sort(
        key=lambda item: (
            float(item.get("rank_score") or 0.0),
            float(item.get("score") or 0.0),
            float(item.get("pct_change") or -99.0),
            float(item.get("net_inflow") or -999.0),
        ),
        reverse=True,
    )
    for index, item in enumerate(rows, start=1):
        item["rank"] = index
    return rows[:top_sector_limit]


def merge_universe(
    *,
    base_universe: list[UniverseItem],
    dynamic_candidates: list[DynamicLeaderCandidate],
    include_static_watch: bool,
) -> list[UniverseItem]:
    static_non_watch = [item for item in base_universe if item.category != "watch"]
    static_watch = [item for item in base_universe if item.category == "watch"] if include_static_watch else []
    dynamic_watch = [UniverseItem(code=item.code, name=item.name, category="watch") for item in dynamic_candidates]
    if not dynamic_watch and not static_watch:
        static_watch = [item for item in base_universe if item.category == "watch"]
    merged: list[UniverseItem] = []
    seen: set[str] = set()
    for item in [*static_non_watch, *static_watch, *dynamic_watch]:
        if item.code in seen:
            continue
        seen.add(item.code)
        merged.append(item)
    return merged


def split_leader_names(raw: str) -> list[str]:
    value = str(raw or "").strip()
    if not value:
        return []
    rows = [item.strip() for item in re.split(r"[、,，/|;；\s]+", value) if item.strip()]
    seen: set[str] = set()
    result: list[str] = []
    for item in rows:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def to_float(value: Any) -> float | None:
    if value in (None, "", "-", "--"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def asdict_universe(item: UniverseItem) -> dict[str, str]:
    return {"code": item.code, "name": item.name, "category": item.category}
