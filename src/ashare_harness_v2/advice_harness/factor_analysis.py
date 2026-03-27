from __future__ import annotations

from typing import Any

from ..utils import clamp


def build_factor_analysis(
    *,
    feature: dict[str, Any],
    fundamentals: dict[str, Any] | None,
    valuation: dict[str, Any] | None,
    capital_flow: dict[str, Any] | None,
    external_analysis: dict[str, Any] | None,
    sector_label: str | None,
    sector_score: float,
    market_score: float,
    peer_features: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    factors = [
        _quality_factor(fundamentals),
        _value_factor(valuation),
        _momentum_factor(feature),
        _flow_factor(capital_flow, external_analysis=external_analysis),
        _rps_factor(feature, peer_features=peer_features, external_analysis=external_analysis),
        _sector_factor(sector_label=sector_label, sector_score=sector_score),
        _risk_factor(feature=feature, market_score=market_score),
    ]
    provider = "builtin_fallback"
    adapted = _adapt_factor_rows(factors)
    if adapted is not None:
        factors = adapted
        provider = "jqfactor_analyzer_adapted"
    positives = [item for item in factors if float(item["contribution"]) > 0]
    negatives = [item for item in factors if float(item["contribution"]) < 0]
    sorted_factors = sorted(factors, key=lambda item: abs(float(item["contribution"])), reverse=True)
    dominant = max(factors, key=lambda item: abs(float(item["contribution"])))
    style_box = " / ".join(item["name"] for item in sorted(factors, key=lambda row: float(row["exposure"]), reverse=True)[:2]) or "均衡"
    summary_lines = []
    if positives:
        summary_lines.append(
            "正向贡献主要来自 "
            + "、".join(f"{item['name']}({float(item['contribution']):+.1f})" for item in sorted(positives, key=lambda row: float(row["contribution"]), reverse=True)[:3])
            + "。"
        )
    if negatives:
        summary_lines.append(
            "拖累主要来自 "
            + "、".join(f"{item['name']}({float(item['contribution']):+.1f})" for item in sorted(negatives, key=lambda row: float(row["contribution"]))[:3])
            + "。"
        )
    factor_focus = [item["reason"] for item in sorted_factors[:3] if str(item.get("reason") or "").strip()]
    attribution = _build_attribution(factors)
    selection_overlay = _build_selection_overlay(
        feature=feature,
        factors=factors,
        external_analysis=external_analysis,
        peer_features=peer_features,
    )
    return {
        "provider": provider,
        "style_box": style_box,
        "dominant_factor": dominant["name"],
        "factor_summary": " ".join(summary_lines) or "当前因子驱动不集中，更多依赖后续量价确认。",
        "factor_focus": factor_focus,
        "attribution": attribution,
        "selection_overlay": selection_overlay,
        "factors": factors,
    }


def _adapt_factor_rows(factors: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
    try:
        import pandas as pd
        from jqfactor_analyzer import standardlize, winsorize_med
    except Exception:
        return None

    names = [str(item.get("name") or "") for item in factors]
    if not names:
        return None
    raw = pd.Series([float(item.get("contribution") or 0.0) for item in factors], index=names, dtype="float64")
    if raw.abs().sum() <= 0:
        return None
    try:
        normalized = standardlize(winsorize_med(raw, scale=5, axis=0), axis=0).fillna(0.0)
    except Exception:
        return None

    adapted: list[dict[str, Any]] = []
    for item in factors:
        name = str(item.get("name") or "")
        normalized_value = float(normalized.get(name, 0.0))
        base_exposure = float(item.get("exposure") or 0.0)
        contribution = round(clamp(normalized_value * 2.4, -5.0, 5.0), 2)
        exposure = round(clamp(base_exposure * 0.7 + min(abs(normalized_value) / 3.0, 1.0) * 0.3, 0.0, 1.0), 3)
        adapted.append(
            {
                **item,
                "exposure": exposure,
                "contribution": contribution,
                "signal": "positive" if contribution > 0.6 else "negative" if contribution < -0.6 else "neutral",
            }
        )
    return adapted


def _quality_factor(fundamentals: dict[str, Any] | None) -> dict[str, Any]:
    if not fundamentals:
        return _factor_row("质量", 0.2, -1.0, "财务质量数据不足。")
    score = 0.0
    reasons: list[str] = []
    roe = fundamentals.get("roe")
    roic = fundamentals.get("roic")
    gross_margin = fundamentals.get("gross_margin")
    rev = fundamentals.get("revenue_growth_yoy")
    profit = fundamentals.get("profit_growth_yoy")
    if isinstance(roe, (int, float)):
        score += clamp((float(roe) - 0.08) * 4.5, -1.2, 1.2)
        reasons.append(f"ROE {float(roe):.1%}")
    if isinstance(roic, (int, float)):
        score += clamp((float(roic) - 0.08) * 4.0, -1.0, 1.0)
        reasons.append(f"ROIC {float(roic):.1%}")
    if isinstance(gross_margin, (int, float)):
        score += clamp((float(gross_margin) - 0.18) * 2.5, -0.8, 0.8)
        reasons.append(f"毛利率 {float(gross_margin):.1%}")
    if isinstance(rev, (int, float)):
        score += clamp(float(rev) * 1.5, -0.9, 0.9)
        reasons.append(f"营收同比 {float(rev):+.1%}")
    if isinstance(profit, (int, float)):
        score += clamp(float(profit) * 1.8, -1.0, 1.0)
        reasons.append(f"利润同比 {float(profit):+.1%}")
    exposure = _exposure_from_score(score, base=0.45)
    contribution = round(score * 4.0, 2)
    return _factor_row("质量", exposure, contribution, "，".join(reasons[:3]) or "质量因子中性。")


def _value_factor(valuation: dict[str, Any] | None) -> dict[str, Any]:
    if not valuation:
        return _factor_row("估值", 0.25, -0.8, "估值数据不足。")
    score = 0.0
    reasons: list[str] = []
    pe_vs_industry = valuation.get("pe_vs_industry")
    pe_pct = valuation.get("pe_percentile")
    pe_ttm = valuation.get("pe_ttm")
    pb = valuation.get("pb")
    if isinstance(pe_vs_industry, (int, float)):
        score += clamp((1.0 - float(pe_vs_industry)) * 2.5, -1.2, 1.2)
        reasons.append(f"PE/行业 {float(pe_vs_industry):.2f}x")
    elif isinstance(pe_pct, (int, float)):
        score += clamp((0.5 - float(pe_pct)) * 2.0, -1.0, 1.0)
        reasons.append(f"PE分位 {float(pe_pct):.0%}")
    elif isinstance(pe_ttm, (int, float)):
        score += clamp((20.0 - float(pe_ttm)) / 15.0, -1.0, 1.0)
        reasons.append(f"PE {float(pe_ttm):.1f}x")
    if isinstance(pb, (int, float)):
        score += clamp((2.5 - float(pb)) / 2.5, -0.8, 0.8)
        reasons.append(f"PB {float(pb):.1f}x")
    exposure = _exposure_from_score(score, base=0.4)
    contribution = round(score * 3.0, 2)
    return _factor_row("估值", exposure, contribution, "，".join(reasons[:3]) or "估值因子中性。")


def _momentum_factor(feature: dict[str, Any]) -> dict[str, Any]:
    score = 0.0
    reasons: list[str] = []
    ret_20 = feature.get("ret_20d")
    ret_5 = feature.get("ret_5d")
    rs = feature.get("relative_strength_20d")
    trend = feature.get("trend_score")
    if isinstance(ret_20, (int, float)):
        score += clamp(float(ret_20) * 3.0, -1.5, 1.5)
        reasons.append(f"20日收益 {float(ret_20):+.1%}")
    if isinstance(rs, (int, float)):
        score += clamp(float(rs) * 3.5, -1.3, 1.3)
        reasons.append(f"相对强弱 {float(rs):+.1%}")
    if isinstance(ret_5, (int, float)):
        score += clamp(float(ret_5) * 1.5, -0.8, 0.8)
        reasons.append(f"5日收益 {float(ret_5):+.1%}")
    if isinstance(trend, (int, float)):
        score += clamp((float(trend) - 50.0) / 30.0, -0.8, 0.8)
        reasons.append(f"趋势分 {float(trend):.1f}")
    exposure = _exposure_from_score(score, base=0.42)
    contribution = round(score * 3.8, 2)
    return _factor_row("动量", exposure, contribution, "，".join(reasons[:3]) or "动量因子中性。")


def _flow_factor(capital_flow: dict[str, Any] | None, *, external_analysis: dict[str, Any] | None) -> dict[str, Any]:
    if not capital_flow:
        if external_analysis:
            conviction = float(external_analysis.get("capital_flow_conviction") or 0.0)
            style = str(external_analysis.get("capital_flow_style") or "neutral")
            return _factor_row("资金", _exposure_from_score(conviction, base=0.3), round(conviction * 3.2, 2), f"外部资金风格 {style}")
        return _factor_row("资金", 0.25, -0.6, "资金面数据不足。")
    score = 0.0
    reasons: list[str] = []
    main_flow = capital_flow.get("main_net_flow_5d")
    main_ratio = capital_flow.get("main_net_ratio_5d")
    north = capital_flow.get("northbound_net_flow_5d")
    if isinstance(main_flow, (int, float)):
        score += clamp(float(main_flow) / 300000000.0, -1.2, 1.2)
        reasons.append(f"5日主力净流 {float(main_flow) / 100000000:.2f}亿")
    if isinstance(main_ratio, (int, float)):
        score += clamp(float(main_ratio) * 10.0, -1.0, 1.0)
        reasons.append(f"主力净占比 {float(main_ratio):+.1%}")
    if isinstance(north, (int, float)):
        score += clamp(float(north), -0.8, 0.8)
        reasons.append(f"北向净流 {float(north):+.2f}")
    conviction = (external_analysis or {}).get("capital_flow_conviction")
    conviction_style = str((external_analysis or {}).get("capital_flow_style") or "").strip()
    if isinstance(conviction, (int, float)):
        score += clamp(float(conviction), -0.8, 0.8)
        reasons.append(f"外部信号 {conviction_style or 'neutral'} {float(conviction):+.2f}")
    exposure = _exposure_from_score(score, base=0.35)
    contribution = round(score * 2.8, 2)
    return _factor_row("资金", exposure, contribution, "，".join(reasons[:3]) or "资金因子中性。")


def _rps_factor(
    feature: dict[str, Any],
    *,
    peer_features: list[dict[str, Any]] | None,
    external_analysis: dict[str, Any] | None,
) -> dict[str, Any]:
    proxy = (external_analysis or {}).get("rps_proxy_20d")
    if not isinstance(proxy, (int, float)):
        proxy = _cross_section_percentile(feature.get("ret_20d"), peer_features=peer_features, field="ret_20d")
    if not isinstance(proxy, (int, float)):
        return _factor_row("相对排名", 0.25, 0.0, "缺少跨标的相对排名。")
    contribution = round(clamp((float(proxy) - 50.0) / 12.0, -4.0, 4.0), 2)
    exposure = clamp(0.35 + abs(float(proxy) - 50.0) / 100.0, 0.0, 1.0)
    return _factor_row("相对排名", exposure, contribution, f"20日 RPS 代理 {float(proxy):.1f}")


def _sector_factor(*, sector_label: str | None, sector_score: float) -> dict[str, Any]:
    score = clamp((float(sector_score) - 50.0) / 20.0, -1.2, 1.2)
    exposure = _exposure_from_score(score, base=0.35)
    contribution = round(score * 2.4, 2)
    label = sector_label or "行业映射不足"
    return _factor_row("行业", exposure, contribution, f"{label} 强度 {float(sector_score):.1f}")


def _risk_factor(*, feature: dict[str, Any], market_score: float) -> dict[str, Any]:
    score = 0.0
    reasons: list[str] = []
    volatility = feature.get("volatility_20d")
    high_gap = feature.get("high_gap_20d")
    if isinstance(volatility, (int, float)):
        score -= clamp(float(volatility) * 8.0, 0.0, 1.5)
        reasons.append(f"20日波动率 {float(volatility):.1%}")
    if isinstance(high_gap, (int, float)) and float(high_gap) < -0.1:
        score -= clamp(abs(float(high_gap)) * 2.0, 0.0, 1.0)
        reasons.append(f"距20日高点 {float(high_gap):+.1%}")
    score -= clamp((45.0 - float(market_score)) / 15.0, 0.0, 1.6)
    reasons.append(f"市场分 {float(market_score):.1f}")
    exposure = _exposure_from_score(abs(score), base=0.5)
    contribution = round(score * 3.2, 2)
    return _factor_row("风险", exposure, contribution, "，".join(reasons[:3]))


def _factor_row(name: str, exposure: float, contribution: float, reason: str) -> dict[str, Any]:
    signal = "positive" if contribution > 0.6 else "negative" if contribution < -0.6 else "neutral"
    return {
        "name": name,
        "exposure": round(clamp(exposure, 0.0, 1.0), 3),
        "contribution": round(contribution, 2),
        "signal": signal,
        "reason": reason,
    }


def _exposure_from_score(score: float, *, base: float) -> float:
    return clamp(base + score / 4.0, 0.0, 1.0)


def _cross_section_percentile(value: Any, *, peer_features: list[dict[str, Any]] | None, field: str) -> float | None:
    if not isinstance(value, (int, float)) or not peer_features:
        return None
    peers = [float(item.get(field)) for item in peer_features if isinstance(item.get(field), (int, float))]
    if not peers:
        return None
    rank = sum(1 for item in peers if item <= float(value))
    return round(rank / max(len(peers), 1) * 100.0, 2)


def _build_attribution(factors: list[dict[str, Any]]) -> dict[str, Any]:
    style = sum(float(item.get("contribution") or 0.0) for item in factors if item.get("name") in {"质量", "估值", "动量", "相对排名"})
    industry = sum(float(item.get("contribution") or 0.0) for item in factors if item.get("name") == "行业")
    capital = sum(float(item.get("contribution") or 0.0) for item in factors if item.get("name") == "资金")
    risk = sum(float(item.get("contribution") or 0.0) for item in factors if item.get("name") == "风险")
    specific = round(sum(float(item.get("contribution") or 0.0) for item in factors) - style - industry - capital - risk, 2)
    parts = [
        f"风格 {style:+.1f}",
        f"行业 {industry:+.1f}",
        f"资金 {capital:+.1f}",
        f"风险 {risk:+.1f}",
    ]
    return {
        "style": round(style, 2),
        "industry": round(industry, 2),
        "capital": round(capital, 2),
        "risk": round(risk, 2),
        "specific": specific,
        "headline": " | ".join(parts),
    }


def _build_selection_overlay(
    *,
    feature: dict[str, Any],
    factors: list[dict[str, Any]],
    external_analysis: dict[str, Any] | None,
    peer_features: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    trend_template_score = 50.0
    if float(feature.get("ret_20d") or 0.0) > 0.05:
        trend_template_score += 12.0
    if float(feature.get("relative_strength_20d") or 0.0) > 0.03:
        trend_template_score += 10.0
    if float(feature.get("high_gap_20d") or -1.0) > -0.08:
        trend_template_score += 8.0
    if float(feature.get("volume_ratio_5d") or 0.0) >= 1.05:
        trend_template_score += 6.0
    if float(feature.get("ret_5d") or 0.0) > 0.10:
        trend_template_score -= 10.0
    rps_proxy = (external_analysis or {}).get("rps_proxy_20d")
    if not isinstance(rps_proxy, (int, float)):
        rps_proxy = _cross_section_percentile(feature.get("ret_20d"), peer_features=peer_features, field="ret_20d")
    capital_conviction = float((external_analysis or {}).get("capital_flow_conviction") or 0.0)
    total = sum(float(item.get("contribution") or 0.0) for item in factors)
    return {
        "rps_proxy_20d": round(float(rps_proxy or 0.0), 2),
        "trend_template_score": round(clamp(trend_template_score, 0.0, 100.0), 2),
        "capital_conviction": round(capital_conviction, 3),
        "factor_total_contribution": round(total, 2),
    }
