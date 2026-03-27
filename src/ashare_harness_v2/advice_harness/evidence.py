from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config import load_universe
from ..data_harness.holdings import load_holdings_snapshot
from ..data_harness.market_data import (
    code_to_secid,
    compute_series_features,
    fetch_daily_series,
    filter_series_by_date,
    load_cached_series,
)
from ..data_harness.news import collect_news, fetch_security_announcements, resolve_security
from ..data_harness.supplemental import (
    SUPPLEMENTAL_STATE_FILES,
    ensure_sector_metrics_payload,
    ensure_supplemental_payload,
    filter_supplemental_codes,
    supplemental_enabled,
)
from ..models import AnnouncementItem, DailyBar, DailySeriesSnapshot, NewsItem
from ..skill_harness.sector_rotation import build_dynamic_universe_from_sectors
from ..utils import average, clamp, load_json
from .schemas import EvidenceItem


NEGATIVE_KEYWORDS = ("减持", "处罚", "问询", "风险", "波动", "违约", "减速", "辞职", "诉讼", "质押")
POSITIVE_KEYWORDS = ("增长", "回购", "订单", "景气", "突破", "扩产", "增持", "预增", "中标", "高景气")
OPTIONAL_STATE_FILES = SUPPLEMENTAL_STATE_FILES
BENCHMARK_CODES = {"000300", "510300", "000001", "399006"}


@dataclass(slots=True)
class AdviceSnapshot:
    as_of: str
    state_root: Path | None
    holdings: dict[str, Any]
    universe: list[dict[str, Any]]
    feature_map: dict[str, dict[str, Any]]
    series_map: dict[str, dict[str, Any]]
    decision_bundle: dict[str, Any]
    news_items: list[dict[str, Any]]
    announcements: list[dict[str, Any]]
    supplemental: dict[str, dict[str, Any]] = field(default_factory=dict)
    explicit_sector_map: dict[str, str] = field(default_factory=dict)
    sector_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    name_map: dict[str, str] = field(default_factory=dict)


def build_advice_snapshot(
    config: dict[str, Any],
    *,
    as_of: str,
    holdings_file: str | None = None,
    cash: float = 0.0,
) -> AdviceSnapshot:
    project = config["project"]
    state_root = Path(project["state_dir"]) / as_of
    base_universe = load_universe(project["universe_file"])
    universe = load_universe_rows_from_state(state_root=state_root, fallback=base_universe)
    if _has_state_root(state_root):
        return _load_snapshot_from_state(as_of=as_of, state_root=state_root, universe=universe)
    return _build_snapshot_from_sources(config, as_of=as_of, holdings_file=holdings_file, cash=cash, universe=universe)


def ensure_security_feature(
    snapshot: AdviceSnapshot,
    *,
    config: dict[str, Any],
    code: str,
    name: str | None = None,
) -> dict[str, Any]:
    if code in snapshot.feature_map:
        feature = snapshot.feature_map[code]
        if name and feature.get("name") in {"", None, code}:
            feature["name"] = name
        snapshot.name_map[code] = str(feature.get("name") or name or code)
        return feature
    project = config["project"]
    security_name = name or snapshot.name_map.get(code) or code
    benchmark_series = _benchmark_series(snapshot, config=config, as_of=snapshot.as_of)
    series = fetch_daily_series(code=code, name=security_name, cache_dir=project["daily_bar_cache_dir"], end=snapshot.as_of)
    feature = compute_series_features(series, benchmark_series=benchmark_series, category="query")
    snapshot.feature_map[code] = feature
    snapshot.series_map[code] = series.to_dict()
    snapshot.name_map[code] = security_name
    return feature


def extend_snapshot_with_cached_candidates(
    snapshot: AdviceSnapshot,
    *,
    config: dict[str, Any],
    limit: int = 120,
) -> None:
    cache_dir = Path(config["project"]["daily_bar_cache_dir"])
    benchmark_series = _benchmark_series(snapshot, config=config, as_of=snapshot.as_of)
    known_codes = set(snapshot.feature_map)
    added = 0
    for path in sorted(cache_dir.glob("*.json")):
        if added >= limit:
            break
        code = path.stem.split("_", 1)[1] if "_" in path.stem else path.stem
        if code in known_codes or code in BENCHMARK_CODES:
            continue
        secid = path.stem.replace("_", ".", 1)
        bars = load_cached_series(cache_dir, secid=secid)
        bars = filter_series_by_date(bars, begin="20240101", end=snapshot.as_of)
        if len(bars) < 30:
            continue
        name = snapshot.name_map.get(code) or code
        series = DailySeriesSnapshot(
            code=code,
            name=name,
            secid=code_to_secid(code),
            fetched_at=datetime.now().isoformat(timespec="seconds"),
            source="cache_scan",
            bars=bars,
            used_cache=True,
            degraded=False,
        )
        snapshot.feature_map[code] = compute_series_features(series, benchmark_series=benchmark_series, category="discovery")
        snapshot.series_map[code] = series.to_dict()
        snapshot.name_map[code] = name
        known_codes.add(code)
        added += 1


def resolve_security_from_query(snapshot: AdviceSnapshot, *, symbol_hint: str | None, name_hint: str | None) -> dict[str, Any] | None:
    if symbol_hint:
        code = symbol_hint
        return {
            "code": code,
            "name": snapshot.name_map.get(code) or code,
            "category": _category_for_code(snapshot, code),
            "sector": snapshot.explicit_sector_map.get(code),
        }
    if name_hint:
        needle = name_hint.strip().lower()
        if needle:
            normalized_needle = _normalize_security_name(needle)
            best_match: tuple[str, str] | None = None
            for code, name in snapshot.name_map.items():
                normalized_name = str(name or "").strip().lower()
                comparable_name = _normalize_security_name(normalized_name)
                if not normalized_name:
                    continue
                if (
                    needle == normalized_name
                    or needle in normalized_name
                    or normalized_name in needle
                    or normalized_needle == comparable_name
                    or normalized_needle in comparable_name
                    or comparable_name in normalized_needle
                ):
                    if best_match is None or len(normalized_name) > len(best_match[1]):
                        best_match = (code, str(name))
            if best_match is not None:
                return {
                    "code": best_match[0],
                    "name": best_match[1],
                    "category": _category_for_code(snapshot, best_match[0]),
                    "sector": snapshot.explicit_sector_map.get(best_match[0]),
                }
        for code, name in snapshot.name_map.items():
            if needle == name.lower() or needle in name.lower():
                return {"code": code, "name": name, "category": _category_for_code(snapshot, code), "sector": snapshot.explicit_sector_map.get(code)}
        for item in snapshot.universe:
            if needle == str(item.get("name") or "").lower():
                code = str(item["code"])
                return {"code": code, "name": str(item.get("name") or code), "category": str(item.get("category") or "watch"), "sector": snapshot.explicit_sector_map.get(code)}
        try:
            remote = resolve_security(name_hint)
        except Exception:
            remote = None
        if remote:
            code = str(remote.get("code") or "")
            name = str(remote.get("zwjc") or name_hint)
            if code:
                snapshot.name_map[code] = name
                return {"code": code, "name": name, "category": _category_for_code(snapshot, code), "sector": snapshot.explicit_sector_map.get(code)}
    return None


def _normalize_security_name(value: str) -> str:
    text = str(value or "").strip().lower()
    for token in ("etf", "基金", "华泰柏瑞", "华夏", "易方达", "国泰", "富国", "广发", "南方"):
        text = text.replace(token.lower(), "")
    replacements = {
        "半导体": "芯片",
        "科创50": "科创",
        "中韩半导体": "中韩芯片",
    }
    for source, target in replacements.items():
        text = text.replace(source.lower(), target.lower())
    return text.strip()


def ensure_security_announcements(
    snapshot: AdviceSnapshot,
    *,
    code: str,
    name: str,
    limit_per_stock: int = 4,
) -> list[dict[str, Any]]:
    existing = [item for item in snapshot.announcements if str(item.get("code") or "") == code]
    if existing:
        return sorted(existing, key=lambda item: str(item.get("published_at") or ""), reverse=True)
    rows = [
        item.to_dict()
        for item in fetch_security_announcements(
            [{"code": code, "name": name}],
            limit_per_stock=limit_per_stock,
        )
    ]
    snapshot.announcements.extend(rows)
    return rows


def maybe_enrich_snapshot_with_live_supplemental(
    snapshot: AdviceSnapshot,
    *,
    config: dict[str, Any],
    codes: list[str],
    refresh: bool = False,
) -> bool:
    if not supplemental_enabled(config):
        return False
    payload = ensure_supplemental_payload(config, as_of=snapshot.as_of, codes=codes, refresh=refresh)
    if not payload:
        return False
    if snapshot.state_root is None:
        snapshot.state_root = Path(config["project"]["state_dir"]) / snapshot.as_of
    for key in ("fundamentals", "valuation", "capital_flow", "external_analysis", "company_info"):
        snapshot.supplemental.setdefault(key, {}).update((payload.get(key) or {}))
    snapshot.explicit_sector_map.update({str(code): str(label) for code, label in (payload.get("sector_map") or {}).items()})
    snapshot.sector_metrics.update({str(label): value for label, value in (payload.get("sector_metrics") or {}).items()})
    for code, item in (payload.get("company_info") or {}).items():
        name = str(item.get("name") or item.get("company_name") or snapshot.name_map.get(code) or code)
        snapshot.name_map[str(code)] = name
        if str(code) in snapshot.feature_map and snapshot.feature_map[str(code)].get("name") in {None, "", str(code)}:
            snapshot.feature_map[str(code)]["name"] = name
    return True


def infer_sector_context(snapshot: AdviceSnapshot, *, code: str, feature: dict[str, Any]) -> tuple[str | None, float, bool, list[str]]:
    explicit_sector = snapshot.explicit_sector_map.get(code)
    if explicit_sector:
        sector_metrics = snapshot.sector_metrics.get(explicit_sector) or {}
        peer_scores = [
            float(snapshot.feature_map[item_code].get("trend_score") or 50.0)
            for item_code, sector in snapshot.explicit_sector_map.items()
            if sector == explicit_sector and item_code in snapshot.feature_map
        ]
        external_score = sector_metrics.get("score")
        if isinstance(external_score, (int, float)):
            if peer_scores:
                merged_score = (float(external_score) * 0.7) + ((average(peer_scores) or float(external_score)) * 0.3)
                return explicit_sector, merged_score, True, []
            return explicit_sector, float(external_score), True, []
        return explicit_sector, average(peer_scores) or float(feature.get("trend_score") or 50.0), True, []

    group = infer_security_group(code=code, name=str(feature.get("name") or code))
    peer_scores = [
        float(item.get("trend_score") or 50.0)
        for item_code, item in snapshot.feature_map.items()
        if infer_security_group(code=item_code, name=str(item.get("name") or item_code)) == group
    ]
    notes = [f"未拿到明确行业映射，当前以 `{group}` 同类证券表现代替板块强度。"]
    return group, average(peer_scores) or float(feature.get("trend_score") or 50.0), False, notes


def build_market_evidence(snapshot: AdviceSnapshot) -> EvidenceItem:
    market = snapshot.decision_bundle.get("market_view") or {}
    score = float(market.get("score") or 50.0)
    signal = "positive" if score >= 58 else "negative" if score <= 42 else "neutral"
    reasons = market.get("reason") or []
    summary = reasons[0] if reasons else "缺少市场背景。"
    return EvidenceItem(
        category="market",
        signal=signal,
        strength=score,
        summary=summary,
        source="decision_bundle.market_view",
        verified=True,
        freshness=snapshot.as_of,
        metadata={"action": market.get("action"), "label": ((market.get("metadata") or {}).get("label"))},
    )


def build_technical_evidence(feature: dict[str, Any]) -> list[EvidenceItem]:
    return [
        EvidenceItem(
            category="technical",
            signal="positive" if float(feature.get("trend_score") or 0.0) >= 60 else "negative" if float(feature.get("trend_score") or 0.0) < 40 else "neutral",
            strength=float(feature.get("trend_score") or 50.0),
            summary=f"趋势分 {float(feature.get('trend_score') or 50.0):.1f}，20日相对强弱 {format_pct(feature.get('relative_strength_20d'))}。",
            source="daily_features",
            verified=True,
            freshness=str(feature.get("as_of") or "unknown"),
            metadata={"code": feature.get("code")},
        ),
        EvidenceItem(
            category="timing",
            signal="positive" if is_positive_timing(feature) else "negative" if is_negative_timing(feature) else "neutral",
            strength=timing_strength(feature),
            summary=f"距20日高点 {format_pct(feature.get('high_gap_20d'))}，5日量比 {format_ratio(feature.get('volume_ratio_5d'))}。",
            source="daily_features",
            verified=True,
            freshness=str(feature.get("as_of") or "unknown"),
            metadata={"code": feature.get("code")},
        ),
    ]


def build_announcement_evidence(announcements: list[dict[str, Any]]) -> tuple[list[EvidenceItem], int, int]:
    rows: list[EvidenceItem] = []
    positive_count = 0
    negative_count = 0
    for item in sorted(announcements, key=lambda row: str(row.get("published_at") or ""), reverse=True)[:3]:
        title = str(item.get("title") or "")
        signal = "neutral"
        strength = 50.0
        if any(keyword in title for keyword in POSITIVE_KEYWORDS):
            signal = "positive"
            strength = 68.0
            positive_count += 1
        if any(keyword in title for keyword in NEGATIVE_KEYWORDS):
            signal = "negative"
            strength = 30.0
            negative_count += 1
        rows.append(
            EvidenceItem(
                category="announcement",
                signal=signal,
                strength=strength,
                summary=f"{item.get('published_at') or '未知日期'} 公告：{title}",
                source="cninfo",
                verified=True,
                freshness=str(item.get("published_at") or "unknown"),
                metadata={"url": item.get("url")},
            )
        )
    return rows, positive_count, negative_count


def build_sector_evidence(
    *,
    label: str | None,
    score: float,
    is_explicit: bool,
    notes: list[str],
    as_of: str,
    metrics: dict[str, Any] | None = None,
) -> list[EvidenceItem]:
    summary = f"{'行业' if is_explicit else '同类证券'} `{label or '未知'}` 强度 {score:.1f}。"
    if metrics:
        pct_change = metrics.get("pct_change")
        breadth = metrics.get("breadth")
        leader = str(metrics.get("leader") or "")
        parts = [f"行业 `{label or '未知'}` 强度 {score:.1f}"]
        if isinstance(pct_change, (int, float)):
            parts.append(f"涨跌 {pct_change:+.2%}")
        if isinstance(breadth, (int, float)):
            parts.append(f"广度 {breadth:.0%}")
        if leader:
            parts.append(f"领涨股 {leader}")
        summary = "，".join(parts) + "。"
    rows = [
        EvidenceItem(
            category="sector",
            signal="positive" if score >= 60 else "negative" if score < 40 else "neutral",
            strength=score,
            summary=summary,
            source="sector_context",
            verified=is_explicit,
            freshness=as_of,
            metadata={"label": label, "is_explicit": is_explicit},
        )
    ]
    for note in notes:
        rows.append(
            EvidenceItem(
                category="coverage",
                signal="neutral",
                strength=45.0,
                summary=note,
                source="sector_context",
                verified=False,
                freshness=as_of,
            )
        )
    return rows


def build_supplemental_evidence(
    snapshot: AdviceSnapshot,
    *,
    code: str,
    as_of: str,
    feature: dict[str, Any],
) -> tuple[list[EvidenceItem], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    fundamentals = ((snapshot.supplemental.get("fundamentals") or {}).get(code)) or {}
    valuation = finalize_valuation(
        feature=feature,
        fundamentals=fundamentals,
        valuation=((snapshot.supplemental.get("valuation") or {}).get(code)) or {},
    )
    capital_flow = ((snapshot.supplemental.get("capital_flow") or {}).get(code)) or {}
    external_analysis = ((snapshot.supplemental.get("external_analysis") or {}).get(code)) or {}
    company_info = ((snapshot.supplemental.get("company_info") or {}).get(code)) or {}
    rows: list[EvidenceItem] = []
    if fundamentals:
        pieces = []
        if isinstance(fundamentals.get("revenue_growth_yoy"), (int, float)):
            pieces.append(f"营收同比 {float(fundamentals['revenue_growth_yoy']):+.2%}")
        if isinstance(fundamentals.get("profit_growth_yoy"), (int, float)):
            pieces.append(f"利润同比 {float(fundamentals['profit_growth_yoy']):+.2%}")
        if isinstance(fundamentals.get("roe"), (int, float)):
            pieces.append(f"ROE {float(fundamentals['roe']):.1%}")
        rows.append(
            EvidenceItem(
                category="fundamental",
                signal="positive",
                strength=65.0,
                summary="，".join(pieces) + "。" if pieces else "已加载财务指标，可用于盈利质量判断。",
                source="supplemental.fundamentals",
                verified=True,
                freshness=as_of,
                metadata={"keys": sorted(fundamentals)},
            )
        )
    if valuation:
        text = []
        if isinstance(valuation.get("pe_ttm"), (int, float)):
            text.append(f"PE {float(valuation['pe_ttm']):.1f}x")
        if isinstance(valuation.get("pb"), (int, float)):
            text.append(f"PB {float(valuation['pb']):.2f}x")
        if isinstance(valuation.get("pe_vs_industry"), (int, float)):
            text.append(f"相对行业 {float(valuation['pe_vs_industry']):.2f}x")
        rows.append(
            EvidenceItem(
                category="valuation",
                signal="neutral",
                strength=55.0,
                summary="，".join(text) + "。" if text else "已加载估值数据，可用于判断贵不贵。",
                source="supplemental.valuation",
                verified=True,
                freshness=as_of,
                metadata={"keys": sorted(valuation)},
            )
        )
    if capital_flow:
        flow_text = []
        if isinstance(capital_flow.get("main_net_flow_5d"), (int, float)):
            flow_text.append(f"5日主力净流入 {capital_flow['main_net_flow_5d'] / 100000000:.2f} 亿")
        if isinstance(capital_flow.get("main_net_ratio_5d"), (int, float)):
            flow_text.append(f"5日主力净占比 {float(capital_flow['main_net_ratio_5d']):+.2%}")
        rows.append(
            EvidenceItem(
                category="capital_flow",
                signal="neutral",
                strength=55.0,
                summary="，".join(flow_text) + "。" if flow_text else "已加载资金行为数据，可用于验证是否有增量资金。",
                source="supplemental.capital_flow",
                verified=True,
                freshness=as_of,
                metadata={"keys": sorted(capital_flow)},
            )
        )
    if external_analysis:
        text = []
        conviction = str(external_analysis.get("capital_flow_style") or "").strip()
        conviction_score = external_analysis.get("capital_flow_conviction")
        provider_status = external_analysis.get("provider_status") or {}
        if conviction:
            text.append(f"资金行为 `{conviction}`")
        if isinstance(conviction_score, (int, float)):
            text.append(f"信号强度 {float(conviction_score):+.2f}")
        available = [name for name, status in provider_status.items() if str(status).startswith("available")]
        if available:
            text.append(f"外部适配 {', '.join(available)}")
        rows.append(
            EvidenceItem(
                category="external_analysis",
                signal="positive" if float(external_analysis.get("capital_flow_conviction") or 0.0) >= 0.25 else "negative" if float(external_analysis.get("capital_flow_conviction") or 0.0) <= -0.25 else "neutral",
                strength=58.0,
                summary="，".join(text) + "。" if text else "已加载外部分析适配信号。",
                source="supplemental.external_analysis",
                verified=bool(available),
                freshness=as_of,
                metadata={"keys": sorted(external_analysis)},
            )
        )
    if company_info:
        business = str(company_info.get("business") or company_info.get("business_outline") or "")
        business = business[:80] + ("..." if len(business) > 80 else "")
        industry = str(company_info.get("industry_name") or company_info.get("industry_large") or company_info.get("sector_match") or "")
        details = []
        if industry:
            details.append(f"所属行业 {industry}")
        if business:
            details.append(f"主营 {business}")
        rows.append(
            EvidenceItem(
                category="company",
                signal="neutral",
                strength=52.0,
                summary="，".join(details) + "。" if details else "已加载公司概况。",
                source="supplemental.company_info",
                verified=True,
                freshness=as_of,
                metadata={"keys": sorted(company_info)},
            )
        )
    return rows, fundamentals, valuation, capital_flow, company_info, external_analysis


def infer_security_group(*, code: str, name: str) -> str:
    if code in BENCHMARK_CODES:
        return "benchmark"
    if code.startswith("5") or "ETF" in name.upper():
        return "ETF"
    if code.startswith("688"):
        return "科创板"
    if code.startswith("300"):
        return "创业板"
    if code.startswith(("000", "001", "002", "003")):
        return "深市主板"
    if code.startswith(("600", "601", "603", "605")):
        return "沪市主板"
    return "A股"


def maybe_enrich_names(snapshot: AdviceSnapshot, *, codes: list[str]) -> None:
    for code in codes:
        name = snapshot.name_map.get(code)
        if name and name != code:
            continue
        try:
            remote = resolve_security(code)
        except Exception:
            remote = None
        if remote and remote.get("zwjc"):
            snapshot.name_map[code] = str(remote["zwjc"])
            if code in snapshot.feature_map:
                snapshot.feature_map[code]["name"] = snapshot.name_map[code]


def is_positive_timing(feature: dict[str, Any]) -> bool:
    return (feature.get("high_gap_20d") or -1.0) >= -0.03 and (feature.get("volume_ratio_5d") or 0.0) >= 1.0


def is_negative_timing(feature: dict[str, Any]) -> bool:
    return (feature.get("high_gap_20d") or 0.0) <= -0.1 or (feature.get("volume_ratio_5d") or 1.0) <= 0.85


def timing_strength(feature: dict[str, Any]) -> float:
    score = 50.0
    high_gap = feature.get("high_gap_20d")
    volume_ratio = feature.get("volume_ratio_5d")
    if isinstance(high_gap, (int, float)):
        score += (0.03 + float(high_gap)) * 200
    if isinstance(volume_ratio, (int, float)):
        score += clamp(float(volume_ratio) - 1.0, -0.5, 1.5) * 20
    return max(0.0, min(score, 100.0))


def format_pct(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "无"
    return f"{float(value):+.2%}"


def format_ratio(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "无"
    return f"{float(value):.2f}x"


def finalize_valuation(*, feature: dict[str, Any], fundamentals: dict[str, Any], valuation: dict[str, Any]) -> dict[str, Any]:
    row = dict(valuation or {})
    last_close = feature.get("last_close")
    eps = fundamentals.get("eps")
    bps = fundamentals.get("bps")
    industry_pe = row.get("industry_pe")
    if isinstance(last_close, (int, float)) and isinstance(eps, (int, float)) and eps > 0:
        row.setdefault("pe_ttm", float(last_close) / float(eps))
    if isinstance(last_close, (int, float)) and isinstance(bps, (int, float)) and bps > 0:
        row.setdefault("pb", float(last_close) / float(bps))
    if isinstance(row.get("pe_ttm"), (int, float)) and isinstance(industry_pe, (int, float)) and industry_pe > 0:
        row.setdefault("pe_vs_industry", float(row["pe_ttm"]) / float(industry_pe))
    return row


def _has_state_root(state_root: Path) -> bool:
    required = ["decision_bundle.json", "features.json", "holdings_snapshot.json", "news.json", "announcements.json", "series_snapshots.json"]
    return all((state_root / item).exists() for item in required)


def _load_snapshot_from_state(*, as_of: str, state_root: Path, universe: list[dict[str, Any]]) -> AdviceSnapshot:
    holdings = load_json(state_root / "holdings_snapshot.json", default={}) or {}
    feature_map = load_json(state_root / "features.json", default={}) or {}
    decision_bundle = load_json(state_root / "decision_bundle.json", default={}) or {}
    news_items = load_json(state_root / "news.json", default=[]) or []
    announcements = load_json(state_root / "announcements.json", default=[]) or []
    series_map = load_json(state_root / "series_snapshots.json", default={}) or {}
    supplemental = {
        key: load_json(state_root / filename, default={}) or {}
        for key, filename in OPTIONAL_STATE_FILES.items()
        if key not in {"sector_map", "sector_metrics"}
    }
    explicit_sector_map: dict[str, str] = {}
    for position in holdings.get("positions", []):
        code = str(position.get("code") or "")
        sector = str(position.get("sector") or "").strip()
        if code and sector:
            explicit_sector_map[code] = sector
    extra_sector_map = load_json(state_root / OPTIONAL_STATE_FILES["sector_map"], default={}) or {}
    for code, sector in extra_sector_map.items():
        explicit_sector_map[str(code)] = str(sector)
    sector_metrics = load_json(state_root / OPTIONAL_STATE_FILES["sector_metrics"], default={}) or {}
    name_map = {str(item.get("code") or ""): str(item.get("name") or item.get("code") or "") for item in universe}
    for code, feature in feature_map.items():
        name_map[str(code)] = str((feature or {}).get("name") or code)
    for position in holdings.get("positions", []):
        code = str(position.get("code") or "")
        if code:
            name_map[code] = str(position.get("name") or code)
    return AdviceSnapshot(
        as_of=as_of,
        state_root=state_root,
        holdings=holdings,
        universe=universe,
        feature_map=feature_map,
        series_map=series_map,
        decision_bundle=decision_bundle,
        news_items=news_items,
        announcements=announcements,
        supplemental=supplemental,
        explicit_sector_map=explicit_sector_map,
        sector_metrics=sector_metrics,
        name_map=name_map,
    )


def _build_snapshot_from_sources(
    config: dict[str, Any],
    *,
    as_of: str,
    holdings_file: str | None,
    cash: float,
    universe: list[dict[str, Any]],
) -> AdviceSnapshot:
    from ..decision_harness.engine import build_decision_bundle

    project = config["project"]
    analysis = config["analysis"]
    holdings = load_holdings_snapshot(holdings_file or project["holdings_file"], as_of=as_of, cash=cash)
    base_universe = load_universe(project["universe_file"])
    sector_payload = ensure_sector_metrics_payload(config, as_of=as_of)
    dynamic_universe = build_dynamic_universe_from_sectors(
        config=config,
        base_universe=base_universe,
        holdings_codes={position.code for position in holdings.positions},
        sector_metrics=sector_payload.get("sector_metrics") or {},
    )
    universe_items = dynamic_universe["universe_items"]
    required = {item.code: item.name for item in universe_items}
    for position in holdings.positions:
        required[position.code] = position.name
    supplemental_payload = ensure_supplemental_payload(
        config,
        as_of=as_of,
        codes=filter_supplemental_codes(
            [item.code for item in universe_items if item.category != "benchmark"] + [position.code for position in holdings.positions]
        ),
    )
    concrete_map = {
        code: fetch_daily_series(code=code, name=name, cache_dir=project["daily_bar_cache_dir"], end=as_of)
        for code, name in required.items()
    }
    news_items = [
        item.to_dict()
        for item in collect_news(
            config["news"]["sources"],
            max_items_per_source=int(analysis["news_max_items_per_source"]),
            max_age_days=int(analysis["news_max_age_days"]),
        )
        if item.published_at is None or item.published_at <= as_of
    ]
    announcement_items = fetch_security_announcements(
        [{"code": position.code, "name": position.name} for position in holdings.positions],
        limit_per_stock=int(analysis["announcement_limit_per_stock"]),
    )
    announcement_items = [item for item in announcement_items if item.published_at is None or item.published_at <= as_of]
    bundle, feature_map = build_decision_bundle(
        as_of=as_of,
        holdings=holdings,
        universe=universe_items,
        series_map=concrete_map,
        news_items=[NewsItem(**item) for item in news_items],
        announcements=announcement_items,
        llm_summary={},
        config=config,
        supplemental={key: (supplemental_payload.get(key) or {}) for key in ("fundamentals", "valuation", "capital_flow", "company_info")},
        sector_map=supplemental_payload.get("sector_map") or {},
        sector_metrics=supplemental_payload.get("sector_metrics") or {},
    )
    explicit_sector_map = {position.code: str(position.sector or "") for position in holdings.positions if position.sector}
    explicit_sector_map.update({str(code): str(label) for code, label in (supplemental_payload.get("sector_map") or {}).items()})
    name_map = {code: name for code, name in required.items()}
    bundle.homepage_overview["dynamic_universe"] = {
        "selection_mode": dynamic_universe.get("selection_mode"),
        "top_sectors": list(dynamic_universe.get("top_sectors") or []),
        "leaders": list(dynamic_universe.get("leaders") or []),
    }
    return AdviceSnapshot(
        as_of=as_of,
        state_root=None,
        holdings=holdings.to_dict(),
        universe=[{"code": item.code, "name": item.name, "category": item.category} for item in universe_items],
        feature_map={code: value.to_dict() for code, value in feature_map.items()},
        series_map={code: series.to_dict() for code, series in concrete_map.items()},
        decision_bundle=bundle.to_dict(),
        news_items=news_items,
        announcements=[item.to_dict() for item in announcement_items],
        supplemental={key: (supplemental_payload.get(key) or {}) for key in ("fundamentals", "valuation", "capital_flow", "company_info")},
        explicit_sector_map=explicit_sector_map,
        sector_metrics=supplemental_payload.get("sector_metrics") or {},
        name_map=name_map,
    )


def _benchmark_series(snapshot: AdviceSnapshot, *, config: dict[str, Any], as_of: str) -> DailySeriesSnapshot | None:
    for code in ("000300", "510300"):
        payload = snapshot.series_map.get(code)
        if payload:
            return _restore_series(payload)
    for code, name in (("000300", "沪深300"), ("510300", "沪深300ETF")):
        try:
            return fetch_daily_series(code=code, name=name, cache_dir=config["project"]["daily_bar_cache_dir"], end=as_of)
        except Exception:
            continue
    return None


def _restore_series(payload: dict[str, Any]) -> DailySeriesSnapshot:
    return DailySeriesSnapshot(
        code=str(payload["code"]),
        name=str(payload["name"]),
        secid=str(payload["secid"]),
        fetched_at=str(payload["fetched_at"]),
        source=str(payload["source"]),
        bars=[DailyBar(**row) for row in (payload.get("bars") or []) if isinstance(row, dict)],
        used_cache=bool(payload.get("used_cache")),
        degraded=bool(payload.get("degraded")),
    )


def _category_for_code(snapshot: AdviceSnapshot, code: str) -> str:
    if code in BENCHMARK_CODES:
        return "benchmark"
    for item in snapshot.universe:
        if str(item.get("code") or "") == code:
            return str(item.get("category") or "watch")
    for position in snapshot.holdings.get("positions", []):
        if str(position.get("code") or "") == code:
            return "holding"
    return "query"


def load_universe_rows_from_state(*, state_root: Path, fallback: list[Any]) -> list[dict[str, Any]]:
    payload = load_json(state_root / "universe_effective.json", default=[]) or []
    if not payload:
        return [{"code": item.code, "name": item.name, "category": item.category} for item in fallback]
    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or "").strip()
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "name": str(item.get("name") or code),
                "category": str(item.get("category") or "watch"),
            }
        )
    if rows:
        return rows
    return [{"code": item.code, "name": item.name, "category": item.category} for item in fallback]
