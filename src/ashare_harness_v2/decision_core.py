from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from .advice_harness.evidence import (
    AdviceSnapshot,
    build_announcement_evidence,
    build_market_evidence,
    build_sector_evidence,
    build_supplemental_evidence,
    build_technical_evidence,
    ensure_security_announcements,
    ensure_security_feature,
    infer_sector_context,
    maybe_enrich_snapshot_with_live_supplemental,
)
from .advice_harness.factor_analysis import build_factor_analysis
from .advice_harness.scoring import (
    combine_scores,
    compute_coverage_score,
    compute_missing_data_penalty,
    compute_risk_penalty,
    compute_sector_score,
    compute_stock_score,
    compute_timing_score,
    decide_action,
)
from .advice_harness.schemas import ActionPlan, EvidenceItem, ScoreCard, StrategyProfile
from .prediction_harness.engine import build_prediction_bundle
from .utils import clamp


def _rows_to_daily_bars(rows: list[dict[str, Any]]) -> list:
    """Convert dict rows from series_map into DailyBar objects for prediction engine."""
    try:
        from .models import DailyBar
    except Exception:
        return []

    bars: list[DailyBar] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        close_price = row.get("close_price")
        if not isinstance(close_price, (int, float)) or close_price <= 0:
            continue
        open_price = row.get("open_price")
        if not isinstance(open_price, (int, float)) or open_price <= 0:
            open_price = float(close_price)
        high_price = row.get("high_price")
        if not isinstance(high_price, (int, float)) or high_price <= 0:
            high_price = max(float(open_price), float(close_price))
        low_price = row.get("low_price")
        if not isinstance(low_price, (int, float)) or low_price <= 0:
            low_price = min(float(open_price), float(close_price))
        volume = row.get("volume")
        volume_value = float(volume) if isinstance(volume, (int, float)) else 0.0
        amount = row.get("amount")
        amount_value = float(amount) if isinstance(amount, (int, float)) else float(close_price) * volume_value
        pct_change = row.get("pct_change")
        pct_change_value = float(pct_change) if isinstance(pct_change, (int, float)) else 0.0
        change_amount = row.get("change_amount")
        if not isinstance(change_amount, (int, float)):
            change_amount = float(close_price) - float(open_price)
        turnover = row.get("turnover")
        turnover_value = float(turnover) if isinstance(turnover, (int, float)) else 0.0
        amplitude = row.get("amplitude")
        if isinstance(amplitude, (int, float)):
            amplitude_value = float(amplitude)
        elif float(close_price):
            amplitude_value = (float(high_price) - float(low_price)) / float(close_price)
        else:
            amplitude_value = 0.0
        bars.append(
            DailyBar(
                trade_date=str(row.get("trade_date") or ""),
                open_price=float(open_price),
                close_price=float(close_price),
                high_price=float(high_price),
                low_price=float(low_price),
                volume=volume_value,
                amount=amount_value,
                amplitude=amplitude_value,
                pct_change=pct_change_value,
                change_amount=float(change_amount),
                turnover=turnover_value,
                source=str(row.get("source") or "series_map"),
            )
        )
    return bars

STYLE_LABELS = {
    "general": "综合决策",
    "trend_following": "趋势跟随",
    "pullback_accumulation": "回踩低吸",
    "defensive_quality": "防守质量",
}
MARKET_STYLE_MAP = {
    "risk_on": "trend_following",
    "balanced": "pullback_accumulation",
    "risk_off": "defensive_quality",
}


@dataclass(slots=True)
class SecurityEvaluation:
    code: str
    name: str
    category: str
    sector: str | None
    using_sector_proxy: bool
    question_type: str
    decision: str
    confidence: float
    summary: str
    thesis: str
    scorecard: ScoreCard
    positive_factors: list[str]
    negative_factors: list[str]
    counter_evidence: list[str]
    missing_information: list[str]
    next_checks: list[str]
    trigger_conditions: list[str]
    invalidation_conditions: list[str]
    action_plan: ActionPlan
    strategy_profile: StrategyProfile
    factor_analysis: dict[str, Any] = field(default_factory=dict)
    evidence_used: list[EvidenceItem] = field(default_factory=list)
    position_context: dict[str, Any] = field(default_factory=dict)
    pdf_insights: list[dict[str, Any]] = field(default_factory=list)
    prediction_bundle: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "category": self.category,
            "sector": self.sector,
            "using_sector_proxy": self.using_sector_proxy,
            "question_type": self.question_type,
            "decision": self.decision,
            "confidence": self.confidence,
            "summary": self.summary,
            "thesis": self.thesis,
            "scorecard": self.scorecard.to_dict(),
            "positive_factors": list(self.positive_factors),
            "negative_factors": list(self.negative_factors),
            "counter_evidence": list(self.counter_evidence),
            "missing_information": list(self.missing_information),
            "next_checks": list(self.next_checks),
            "trigger_conditions": list(self.trigger_conditions),
            "invalidation_conditions": list(self.invalidation_conditions),
            "action_plan": self.action_plan.to_dict(),
            "strategy_profile": self.strategy_profile.to_dict(),
            "factor_analysis": dict(self.factor_analysis),
            "evidence_used": [item.to_dict() for item in self.evidence_used],
            "position_context": dict(self.position_context),
            "pdf_insights": list(self.pdf_insights),
            "prediction_bundle": self.prediction_bundle,
        }


def infer_market_strategy_style(*, action: str | None = None, regime: str | None = None) -> str:
    normalized_action = str(action or "").strip().lower()
    if normalized_action in MARKET_STYLE_MAP:
        return MARKET_STYLE_MAP[normalized_action]
    normalized_regime = str(regime or "").strip()
    if "进攻" in normalized_regime:
        return "trend_following"
    if "防守" in normalized_regime:
        return "defensive_quality"
    if "选择" in normalized_regime or "震荡" in normalized_regime:
        return "pullback_accumulation"
    return "general"


def candidate_style_fit(evaluation: SecurityEvaluation, *, strategy_style: str) -> float:
    action = evaluation.action_plan.action
    scorecard = evaluation.scorecard
    if strategy_style == "trend_following":
        bonus = 0.0
        if action in {"standard_position", "trial_position", "wait_for_breakout", "add_on_strength"}:
            bonus += 8.0
        if action in {"wait_for_pullback", "buy_on_pullback"}:
            bonus -= 6.0
        bonus += max(min((float(scorecard.timing_score) - 50.0) * 0.18, 5.0), -5.0)
        bonus += max(min((float(scorecard.stock_score) - 50.0) * 0.08, 3.0), -3.0)
        return round(bonus, 2)
    if strategy_style == "pullback_accumulation":
        bonus = 0.0
        if action in {"wait_for_pullback", "buy_on_pullback", "trial_position"}:
            bonus += 8.0
        if action in {"wait_for_breakout", "add_on_strength", "standard_position"}:
            bonus -= 4.0
        bonus += max(min((float(scorecard.stock_score) - 50.0) * 0.1, 3.0), -3.0)
        return round(bonus, 2)
    if strategy_style == "defensive_quality":
        bonus = 0.0
        if action in {"watch_market_turn", "watch_only", "trial_position", "hold_no_add"}:
            bonus += 7.0
        if action in {"standard_position", "add_on_strength"}:
            bonus -= 5.0
        bonus += max(min((float(scorecard.stock_score) - 55.0) * 0.12, 4.0), -4.0)
        bonus -= max(min((float(scorecard.risk_penalty) - 15.0) * 0.08, 3.0), 0.0)
        return round(bonus, 2)
    return 0.0


def candidate_selection_score(
    evaluation: SecurityEvaluation,
    *,
    strategy_style: str,
    feature: dict[str, Any] | None = None,
) -> float:
    scorecard = evaluation.scorecard
    action = str(evaluation.action_plan.action or "")
    base_score = (
        float(evaluation.action_plan.urgency_score) * 0.45
        + float(scorecard.total_score) * 0.30
        + float(scorecard.coverage_score) * 0.10
        + float(scorecard.stock_score) * 0.08
        + float(scorecard.timing_score) * 0.07
    )
    score = base_score + candidate_style_fit(evaluation, strategy_style=strategy_style)

    if evaluation.question_type == "add_position":
        if action in {"add_on_strength", "buy_on_pullback"}:
            score += 5.0
        elif action in {"hold_core", "hold_no_add", "hold_core_wait_market"}:
            score += 1.5
        elif action in {"trim_into_strength", "cut_on_breakdown"}:
            score -= 8.0
    else:
        if action in {"standard_position", "trial_position", "buy_on_pullback"}:
            score += 6.0
        elif action in {"wait_for_breakout", "wait_for_pullback"}:
            score += 2.0
        elif action in {"watch_only", "watch_market_turn", "stay_out"}:
            score -= 6.0

    if float(scorecard.coverage_score) < 55.0:
        score -= min((55.0 - float(scorecard.coverage_score)) * 0.35, 8.0)
    if float(scorecard.market_score) < 35.0 and action not in {"watch_market_turn", "watch_only", "hold_core_wait_market", "stay_out"}:
        score -= 6.0
    if float(scorecard.risk_penalty) > 18.0:
        score -= min((float(scorecard.risk_penalty) - 18.0) * 0.45, 7.0)
    score += _feature_selection_adjustment(feature, action=action)
    overlay = (evaluation.factor_analysis or {}).get("selection_overlay") or {}
    rps_proxy = overlay.get("rps_proxy_20d")
    trend_template_score = overlay.get("trend_template_score")
    capital_conviction = overlay.get("capital_conviction")
    if isinstance(rps_proxy, (int, float)):
        if float(rps_proxy) >= 80.0:
            score += 3.0
        elif float(rps_proxy) <= 35.0:
            score -= 3.0
    if isinstance(trend_template_score, (int, float)):
        if action in {"wait_for_breakout", "add_on_strength", "standard_position"} and float(trend_template_score) < 48.0:
            score -= 4.0
        elif action in {"wait_for_pullback", "buy_on_pullback", "trial_position"} and 45.0 <= float(trend_template_score) <= 72.0:
            score += 2.0
    if isinstance(capital_conviction, (int, float)):
        score += max(min(float(capital_conviction) * 5.0, 3.0), -3.0)

    return round(clamp(score, 0.0, 100.0), 2)


def _feature_selection_adjustment(feature: dict[str, Any] | None, *, action: str) -> float:
    if not feature:
        return 0.0
    ret_5 = float(feature.get("ret_5d") or 0.0)
    ret_20 = float(feature.get("ret_20d") or 0.0)
    relative_strength = float(feature.get("relative_strength_20d") or 0.0)
    volume_ratio = float(feature.get("volume_ratio_5d") or 0.0)

    adjustment = 0.0
    if ret_5 > 0.12:
        adjustment -= 10.0
    elif ret_5 > 0.08:
        adjustment -= 6.0
    elif ret_5 > 0.05:
        adjustment -= 3.0
    elif action in {"wait_for_pullback", "buy_on_pullback", "trial_position"} and -0.04 <= ret_5 <= 0.01 and relative_strength > 0:
        adjustment += 3.0

    if ret_20 > 0.25 and volume_ratio < 1.0:
        adjustment -= 3.0
    elif action in {"wait_for_breakout", "standard_position", "add_on_strength"} and volume_ratio >= 1.25 and ret_5 <= 0.08:
        adjustment += 2.0

    return adjustment


def evaluate_security(
    snapshot: AdviceSnapshot,
    *,
    config: dict[str, Any],
    code: str,
    name: str,
    category: str,
    question_type: str = "should_buy",
    horizon: str = "swing",
    risk_profile: str = "balanced",
    strategy_style: str = "general",
    pdf_payload: dict[str, Any] | None = None,
    allow_supplemental_refresh: bool = True,
    fetch_announcements: bool = True,
) -> SecurityEvaluation:
    feature = ensure_security_feature(snapshot, config=config, code=code, name=name)
    if allow_supplemental_refresh:
        maybe_enrich_snapshot_with_live_supplemental(snapshot, config=config, codes=[code])

    sector_label, peer_score, has_sector_mapping, sector_notes = infer_sector_context(snapshot, code=code, feature=feature)
    if fetch_announcements:
        announcements = ensure_security_announcements(snapshot, code=code, name=str(feature.get("name") or name))
    else:
        announcements = [item for item in snapshot.announcements if str(item.get("code") or "") == code]
    announcement_evidence, positive_count, negative_count = build_announcement_evidence(announcements)

    pdf_evidence, pdf_positive, pdf_negative, pdf_missing = build_pdf_evidence(pdf_payload)
    supplemental_evidence, fundamentals, valuation, capital_flow, company_info, external_analysis = build_supplemental_evidence(
        snapshot,
        code=code,
        as_of=snapshot.as_of,
        feature=feature,
    )

    evidence = [build_market_evidence(snapshot)]
    evidence.extend(build_technical_evidence(feature))
    evidence.extend(announcement_evidence)
    evidence.extend(pdf_evidence)
    evidence.extend(supplemental_evidence)
    evidence.extend(
        build_sector_evidence(
            label=sector_label,
            score=peer_score,
            is_explicit=has_sector_mapping,
            notes=sector_notes,
            as_of=snapshot.as_of,
            metrics=snapshot.sector_metrics.get(sector_label or ""),
        )
    )

    sector_score, using_sector_proxy = compute_sector_score(feature=feature, peer_score=peer_score, has_sector_mapping=has_sector_mapping)
    stock_score, stock_positive, stock_negative, stock_missing, _value_factor = compute_stock_score(
        feature=feature,
        fundamentals=fundamentals,
        valuation=valuation,
        capital_flow=capital_flow,
        external_analysis=external_analysis,
    )
    timing_score, timing_positive, timing_negative = compute_timing_score(feature=feature, horizon=horizon)
    market_score = float((snapshot.decision_bundle.get("market_view") or {}).get("score") or 50.0)
    factor_analysis = build_factor_analysis(
        feature=feature,
        fundamentals=fundamentals,
        valuation=valuation,
        capital_flow=capital_flow,
        external_analysis=external_analysis,
        sector_label=sector_label,
        sector_score=sector_score,
        market_score=market_score,
        peer_features=list(snapshot.feature_map.values()),
    )

    risk_penalty, risk_details = compute_risk_penalty(
        market_score=market_score,
        announcement_negative_count=negative_count + len(pdf_negative),
        feature=feature,
    )

    missing_information = list(stock_missing)
    if using_sector_proxy:
        missing_information.append("缺少明确行业映射")
    if not announcements:
        missing_information.append("缺少个股公告")
    if not company_info:
        missing_information.append("缺少公司概况")
    missing_information.extend(pdf_missing)

    coverage_score = compute_coverage_score(
        evidence_count=len([item for item in evidence if item.category != "coverage"]),
        missing_information=missing_information,
        has_sector_mapping=has_sector_mapping,
        has_announcements=bool(announcements),
        supplemental_count=sum(1 for bucket in (fundamentals, valuation, capital_flow, external_analysis, company_info) if bucket),
    )
    scorecard = combine_scores(
        market_score=market_score,
        sector_score=sector_score,
        stock_score=stock_score,
        timing_score=timing_score,
        risk_penalty=risk_penalty,
        missing_data_penalty=compute_missing_data_penalty(missing_information=missing_information),
        coverage_score=coverage_score,
    )

    position_context = build_position_context(snapshot, code=code, sector_label=sector_label)
    if question_type == "add_position":
        decision, confidence, position_notes = decide_position_action(
            scorecard=scorecard,
            position_context=position_context,
            risk_profile=risk_profile,
        )
    else:
        decision, confidence = decide_action(scorecard)
        position_notes = []

    negatives = (
        stock_negative
        + timing_negative
        + risk_details
        + [item.summary for item in announcement_evidence if item.signal == "negative"]
        + pdf_negative
        + position_notes
    )
    positives = (
        stock_positive
        + timing_positive
        + [item.summary for item in announcement_evidence if item.signal == "positive"]
        + pdf_positive
    )
    positive_lines = _top_lines(positives, limit=6)
    negative_lines = _top_lines(negatives, limit=6)
    next_checks = build_next_checks(
        decision=decision,
        question_type=question_type,
        feature=feature,
        market_score=market_score,
        position_context=position_context,
    )
    action_plan = build_action_plan(
        security_name=str(feature.get("name") or name),
        decision=decision,
        question_type=question_type,
        scorecard=scorecard,
        feature=feature,
        position_context=position_context,
        next_checks=next_checks,
        positives=positive_lines,
        negatives=negative_lines,
        snapshot=snapshot,
        code=code,
        strategy_style=strategy_style,
    )
    strategy_profile = build_strategy_profile(
        strategy_style=strategy_style,
        action_plan=action_plan,
        scorecard=scorecard,
        question_type=question_type,
        security_name=str(feature.get("name") or name),
        position_context=position_context,
    )
    thesis = build_thesis(
        security_name=str(feature.get("name") or name),
        action_plan=action_plan,
        positives=positive_lines,
        negatives=negative_lines,
    )
    counter_evidence = _top_lines(negative_lines + next_checks, limit=6)

    # --- Build price predictions ---
    series_payload = snapshot.series_map.get(code) or {}
    bar_rows = (series_payload.get("bars") or []) if isinstance(series_payload, dict) else []
    bars = _rows_to_daily_bars(bar_rows)
    prediction_bundle_dict: dict[str, Any] = {}
    if bars:
        try:
            bundle = build_prediction_bundle(
                code=code,
                name=str(feature.get("name") or name),
                bars=bars,
                trend_score=float(feature.get("trend_score") or 50.0),
                relative_strength=float(feature.get("relative_strength_20d") or 0.0),
            )
            prediction_bundle_dict = bundle.to_dict()
        except Exception:
            pass

    return SecurityEvaluation(
        code=code,
        name=str(feature.get("name") or name),
        category=category,
        sector=sector_label,
        using_sector_proxy=using_sector_proxy,
        question_type=question_type,
        decision=decision,
        confidence=confidence,
        summary=build_summary(
            security_name=str(feature.get("name") or name),
            action_label=action_plan.label,
            score=scorecard.total_score,
            market_score=market_score,
            coverage_score=coverage_score,
        ),
        thesis=thesis,
        scorecard=scorecard,
        positive_factors=positive_lines,
        negative_factors=negative_lines,
        counter_evidence=counter_evidence,
        missing_information=_top_lines(missing_information, limit=8),
        next_checks=next_checks,
        trigger_conditions=list(action_plan.trigger_conditions),
        invalidation_conditions=list(action_plan.invalidation_conditions),
        action_plan=action_plan,
        strategy_profile=strategy_profile,
        factor_analysis=factor_analysis,
        evidence_used=evidence[:12],
        position_context=position_context,
        pdf_insights=(pdf_payload or {}).get("insights") or [],
        prediction_bundle=prediction_bundle_dict,
    )


def build_position_context(snapshot: AdviceSnapshot, *, code: str, sector_label: str | None) -> dict[str, Any]:
    holdings = snapshot.holdings or {}
    total_market_value = float(holdings.get("total_market_value") or 0.0)
    total_equity = float(holdings.get("total_equity") or 0.0)
    exposure_ratio = float(holdings.get("exposure_ratio") or 0.0)
    positions = holdings.get("positions") or []
    target = next((item for item in positions if str(item.get("code") or "") == code), None) or {}
    sector_weights = holdings.get("sector_weights") or []
    holding_sector = str(target.get("sector") or "")
    effective_sector = sector_label or holding_sector
    sector_weight = 0.0
    sector_weight_basis = None
    for candidate_sector in [effective_sector, holding_sector]:
        if not candidate_sector:
            continue
        match = next(
            (float(item.get("weight") or 0.0) for item in sector_weights if str(item.get("sector") or "") == candidate_sector),
            None,
        )
        if match is not None:
            sector_weight = match
            sector_weight_basis = candidate_sector
            break
    market_value = float(target.get("market_value") or 0.0)
    position_weight = (market_value / total_market_value) if total_market_value else 0.0
    quantity = float(target.get("quantity") or 0.0)
    available_quantity = float(target.get("available_quantity") or 0.0)
    cost_price = target.get("cost_price")
    last_price = target.get("last_price")
    execution_pricing_complete = cost_price is not None and last_price is not None
    execution_size_complete = quantity > 0 and available_quantity > 0
    return {
        "is_holding": bool(target),
        "quantity": round(quantity, 2),
        "available_quantity": round(available_quantity, 2),
        "market_value": round(market_value, 2),
        "position_weight": round(position_weight, 4),
        "sector_weight": round(sector_weight, 4),
        "pnl_pct": target.get("pnl_pct"),
        "cost_price": cost_price,
        "last_price": last_price,
        "total_equity": round(total_equity, 2),
        "exposure_ratio": round(exposure_ratio, 4),
        "effective_sector": effective_sector or None,
        "holding_sector": holding_sector or None,
        "sector_weight_basis": sector_weight_basis,
        "position_pricing_complete": execution_pricing_complete,
        "position_size_complete": execution_size_complete,
        "position_execution_complete": execution_pricing_complete and execution_size_complete,
    }


def build_entry_context(*, levels: dict[str, float], feature: dict[str, Any]) -> dict[str, Any]:
    current_price = float(feature.get("last_close") or 0.0)
    breakout_price = float(levels.get("breakout_price") or 0.0)
    support_price = float(levels.get("support_price") or 0.0)
    pullback_price = float(levels.get("pullback_price") or 0.0)
    box_width = max(breakout_price - support_price, 0.0) if breakout_price and support_price and breakout_price > support_price else 0.0
    relative_position = 0.5
    if current_price > 0 and box_width > 0:
        relative_position = clamp((current_price - support_price) / box_width, 0.0, 1.0)
    near_support = bool(current_price > 0 and support_price > 0 and current_price <= support_price * 1.025)
    near_breakout = bool(current_price > 0 and breakout_price > 0 and current_price >= breakout_price * 0.97)
    stop_price = round(support_price * 0.97, 3) if support_price > 0 else round(current_price * 0.95, 3) if current_price > 0 else 0.0
    upside_pct = round(max((breakout_price - current_price) / current_price, 0.0), 4) if current_price > 0 and breakout_price > 0 else 0.0
    downside_pct = round(max((current_price - stop_price) / current_price, 0.0), 4) if current_price > 0 and stop_price > 0 else 0.0
    reward_risk = round(upside_pct / max(downside_pct, 0.01), 2) if current_price > 0 else 0.0
    relative_strength = feature.get("relative_strength_20d")
    relative_strength_value = float(relative_strength) if isinstance(relative_strength, (int, float)) else 0.0
    trend_score = feature.get("trend_score")
    trend_score_value = float(trend_score) if isinstance(trend_score, (int, float)) else 50.0
    if near_support:
        zone = "support_zone"
        zone_label = "支撑区"
    elif near_breakout:
        zone = "breakout_zone"
        zone_label = "阻力/突破前沿"
    elif 0.35 <= relative_position <= 0.65:
        zone = "mid_box"
        zone_label = "箱体中段"
    elif relative_position < 0.35:
        zone = "lower_box"
        zone_label = "箱体下沿"
    elif relative_position > 0.65:
        zone = "upper_box"
        zone_label = "箱体上沿"
    else:
        zone = "unclassified"
        zone_label = "无明确区间"
    favorable_pullback = bool(
        near_support
        and reward_risk >= 2.2
        and upside_pct >= 0.05
        and relative_strength_value >= -0.01
        and trend_score_value >= 45.0
    )
    breakout_chase_risk = bool(
        near_breakout
        and (reward_risk < 1.2 or upside_pct < 0.04 or relative_strength_value < 0.0)
    )
    return {
        "current_price": round(current_price, 3),
        "stop_price": stop_price,
        "upside_pct": upside_pct,
        "downside_pct": downside_pct,
        "reward_risk": reward_risk,
        "relative_position": round(relative_position, 4),
        "zone": zone,
        "zone_label": zone_label,
        "near_support": near_support,
        "near_breakout": near_breakout,
        "favorable_pullback": favorable_pullback,
        "breakout_chase_risk": breakout_chase_risk,
        "near_pullback_anchor": bool(current_price > 0 and pullback_price > 0 and abs(current_price - pullback_price) / current_price <= 0.02),
    }


def build_action_plan(
    *,
    security_name: str,
    decision: str,
    question_type: str,
    scorecard: ScoreCard,
    feature: dict[str, Any],
    position_context: dict[str, Any],
    next_checks: list[str],
    positives: list[str],
    negatives: list[str],
    snapshot: AdviceSnapshot,
    code: str,
    strategy_style: str,
) -> ActionPlan:
    levels = build_trade_levels(snapshot=snapshot, code=code, feature=feature)
    entry_context = build_entry_context(levels=levels, feature=feature)
    breakout_price = format_price_level(levels.get("breakout_price"))
    support_price = format_price_level(levels.get("support_price"))
    pullback_price = format_price_level(levels.get("pullback_price"))
    retest_price = format_price_level(levels.get("retest_price"))
    high_gap = feature.get("high_gap_20d")
    high_gap_value = float(high_gap) if isinstance(high_gap, (int, float)) else None
    volume_ratio = feature.get("volume_ratio_5d")
    volume_ratio_value = float(volume_ratio) if isinstance(volume_ratio, (int, float)) else 0.0
    ret_20 = feature.get("ret_20d")
    ret_20_value = float(ret_20) if isinstance(ret_20, (int, float)) else 0.0
    ret_5 = feature.get("ret_5d")
    ret_5_value = float(ret_5) if isinstance(ret_5, (int, float)) else 0.0
    relative_strength = feature.get("relative_strength_20d")
    relative_strength_value = float(relative_strength) if isinstance(relative_strength, (int, float)) else 0.0
    timing_confirmed = bool(
        (high_gap_value is not None and high_gap_value >= -0.03)
        and volume_ratio_value >= 1.15
        and relative_strength_value >= 0.0
    )
    pullback_friendly = bool((high_gap_value is not None and high_gap_value <= -0.05) and ret_20_value > 0.05)
    strong_stock = bool(float(scorecard.stock_score) >= 65 and float(scorecard.timing_score) >= 52)
    overweight = bool(
        float(position_context.get("position_weight") or 0.0) >= 0.2
        or float(position_context.get("sector_weight") or 0.0) >= 0.3
    )
    weak_tape = float(scorecard.market_score) < 40
    cautious_tape = float(scorecard.market_score) < 50
    execution_context_incomplete = bool(
        question_type == "add_position"
        and position_context.get("is_holding")
        and not position_context.get("position_execution_complete", True)
    )
    favorable_pullback = bool(entry_context.get("favorable_pullback"))
    breakout_chase_risk = bool(entry_context.get("breakout_chase_risk"))
    zone_label = str(entry_context.get("zone_label") or "无明确区间")

    if question_type == "add_position":
        if decision == "trim":
            if ret_5_value > 0:
                action = "trim_into_strength"
                label = "逢高减仓"
                rationale = f"{security_name} 当前更适合借反弹降风险，不再为弱势仓位追加筹码。"
                position_guidance = "优先把仓位降到单票 8%-12% 区间。"
                trigger_conditions = [f"若反弹接近 {retest_price} 但量价没有继续扩张，执行减仓。"] + next_checks[:1]
                invalidation_conditions = [f"若重新站回 {breakout_price} 并保持相对强势，可暂停继续减仓。"]
                urgency_score = 92.0
            else:
                action = "cut_on_breakdown"
                label = "破位减仓"
                rationale = f"{security_name} 已经进入弱趋势防守阶段，先处理风险，再谈加仓。"
                position_guidance = "先减 1/3 到 1/2，保留观察仓。"
                trigger_conditions = [f"若跌破 {support_price} 且相对强弱继续走弱，立即执行减仓。"] + next_checks[:1]
                if favorable_pullback:
                    invalidation_conditions = [f"若 {support_price} 一带止跌并重新收回 {pullback_price}，先降级为观察，不急着继续减仓。"]
                else:
                    invalidation_conditions = [f"若重新站回 {retest_price} 且成交恢复，可转回观察。"]
                urgency_score = 97.0
        elif decision == "add":
            if favorable_pullback and not cautious_tape and not overweight:
                action = "buy_on_pullback"
                label = "支撑附近低吸加仓"
                rationale = f"{security_name} 已靠近关键支撑，当前位置的赔率优于追突破。"
                position_guidance = "只补 2%-3% 试探仓，守不住支撑就撤。"
                trigger_conditions = [f"在 {support_price} 附近缩量企稳，或重新收回 {pullback_price} 后，再小幅补仓。"] + next_checks[:1]
                invalidation_conditions = [f"若有效跌破 {support_price}，取消低吸加仓。"]
                urgency_score = 70.0
            elif timing_confirmed and not cautious_tape and not overweight and not breakout_chase_risk:
                action = "add_on_strength"
                label = "趋势加仓"
                rationale = f"{security_name} 的趋势和时点都支持在强势确认后小幅加仓。"
                position_guidance = "只增加 2%-4% 的试探仓，不做一次性重仓。"
                trigger_conditions = [f"放量站稳 {breakout_price} 后，再执行小幅加仓。"] + next_checks[:1]
                invalidation_conditions = [f"若回落跌破 {support_price}，停止加仓并回到原仓位。"]
                urgency_score = 76.0
            else:
                action = "buy_on_pullback"
                label = "等回踩再加"
                rationale = (
                    f"{security_name} 的方向仍可跟踪，但当前接近阻力区、剩余利润空间不够，不值得追着加仓。"
                    if breakout_chase_risk
                    else f"{security_name} 的方向仍可跟踪，但当前不值得追着加仓。"
                )
                position_guidance = "预留 2%-3% 机动仓，只在更优价位补。"
                trigger_conditions = [f"回踩到 {pullback_price} 附近且不放量破位时，再考虑补仓。"] + next_checks[:1]
                invalidation_conditions = [f"若跌破 {support_price}，取消补仓计划。"]
                urgency_score = 63.0
        else:
            if overweight:
                action = "hold_no_add"
                label = "继续持有，不再加仓"
                rationale = f"{security_name} 还没有差到必须卖，但仓位条件已经不支持继续加。"
                position_guidance = "保持当前仓位，等风险敞口下降后再评估。"
                trigger_conditions = next_checks[:2] or [f"只有重新站上 {breakout_price} 并降低主题暴露后，才重新评估加仓。"]
                invalidation_conditions = [f"若跌破 {support_price}，转为破位减仓。"]
                urgency_score = 61.0
            elif weak_tape and strong_stock:
                action = "hold_core_wait_market"
                label = "继续持有，等待市场修复"
                rationale = f"{security_name} 自身并不差，但市场环境不支持继续冒进。"
                position_guidance = "保留核心仓，暂不追加。"
                trigger_conditions = [f"等指数企稳且股价重新靠近 {breakout_price} 再评估加仓。"] + next_checks[:1]
                invalidation_conditions = [f"若跌破 {support_price}，转入减仓预案。"]
                urgency_score = 58.0
            else:
                action = "hold_core"
                label = "继续持有"
                rationale = (
                    f"{security_name} 当前更像持有型仓位，而不是主动进攻型仓位。"
                    if not favorable_pullback
                    else f"{security_name} 已进入 {zone_label}，但在总仓位和胜率没有同步改善前，仍先持有观察。"
                )
                position_guidance = "维持原仓位，等待更清晰信号。"
                trigger_conditions = (
                    [f"若 {support_price} 一带缩量企稳并重新收回 {pullback_price}，再评估是否小幅加仓。"] + next_checks[:1]
                    if favorable_pullback
                    else next_checks[:2] or [f"只有重新站上 {breakout_price} 才考虑主动加仓。"]
                )
                invalidation_conditions = [f"若连续走弱并跌破 {support_price}，转入减仓。"]
                urgency_score = 50.0
    else:
        if decision == "buy":
            if favorable_pullback:
                action = "trial_position"
                label = "支撑位试仓"
                rationale = f"{security_name} 已靠近支撑区，当前位置的赔率优于追涨，可先做支撑确认型试仓。"
                position_guidance = "首笔 2%-4%，等支撑确认后再决定是否放大。"
                trigger_conditions = [f"在 {support_price} 附近缩量企稳，或收回 {pullback_price} 后试仓。"] + next_checks[:1]
                invalidation_conditions = [f"若跌破 {support_price}，撤销试仓并等待下一结构。"]
                urgency_score = 74.0
            elif timing_confirmed and not cautious_tape and not breakout_chase_risk:
                action = "standard_position"
                label = "正常仓"
                rationale = f"{security_name} 已经具备可执行买点，可以按纪律建立正常仓位。"
                position_guidance = "首笔 5%-8%，不追求一次到位。"
                trigger_conditions = [f"放量站稳 {breakout_price} 后，按计划开仓。"] + next_checks[:1]
                invalidation_conditions = [f"若买入后回落跌破 {support_price}，退出观察仓。"]
                urgency_score = 79.0
            else:
                action = "trial_position"
                label = "试仓"
                rationale = (
                    f"{security_name} 有做多逻辑，但当前位置更接近阻力区，剩余空间不足，不支持直接追价。"
                    if breakout_chase_risk
                    else f"{security_name} 有做多逻辑，但环境和时点只支持先试错，不支持直接放大仓位。"
                )
                position_guidance = "先用 2%-4% 的试仓验证，不重仓追价。"
                trigger_conditions = (
                    [f"回踩 {pullback_price} 并获得承接时再试仓，不在阻力前沿追单。"] + next_checks[:1]
                    if breakout_chase_risk
                    else [f"站上 {breakout_price} 或回踩 {pullback_price} 获得承接时，试仓。"] + next_checks[:1]
                )
                invalidation_conditions = [f"若跌破 {support_price}，撤销试仓计划。"]
                urgency_score = 71.0
        elif decision == "watch":
            if weak_tape and strong_stock:
                action = "watch_market_turn"
                label = "只观察"
                rationale = f"{security_name} 自身质量不差，但当前更大的约束来自市场而不是个股。"
                position_guidance = "先不出手，把它放在市场修复后的第一梯队。"
                trigger_conditions = [f"等指数修复后，再看是否放量突破 {breakout_price}。"] + next_checks[:1]
                invalidation_conditions = [f"若相对强弱转负或跌破 {support_price}，降出重点观察名单。"]
                urgency_score = 57.0
            elif zone_label == "箱体中段":
                action = "watch_only"
                label = "箱中观望"
                rationale = f"{security_name} 当前处在箱体中段，既不贴近支撑，也没有突破确认，赔率一般。"
                position_guidance = "不在区间中部出手，等靠近支撑或有效突破后再评估。"
                trigger_conditions = [f"靠近 {support_price} 再看承接，或放量突破 {breakout_price} 再升级计划。"] + next_checks[:1]
                invalidation_conditions = [f"若跌破 {support_price}，继续回避。"]
                urgency_score = 46.0
            elif pullback_friendly:
                action = "wait_for_pullback"
                label = "等回踩"
                rationale = f"{security_name} 方向还在，但更优赔率不在当前位置。"
                position_guidance = "等待更舒服的回踩位，再决定是否试仓。"
                trigger_conditions = [f"回踩 {pullback_price} 一带且量能不失真时，再考虑出手。"] + next_checks[:1]
                invalidation_conditions = [f"若直接跌破 {support_price}，取消计划。"]
                urgency_score = 60.0
            elif high_gap_value is not None and high_gap_value > -0.03:
                action = "wait_for_breakout"
                label = "等放量突破"
                rationale = f"{security_name} 接近关键位，但还没给出值得执行的确认。"
                position_guidance = "先观察，不提前追价。"
                trigger_conditions = [f"只有放量突破 {breakout_price}，才升级为试仓。"] + next_checks[:1]
                invalidation_conditions = [f"若回落跌破 {support_price}，取消突破预案。"]
                urgency_score = 62.0
            else:
                action = "watch_only"
                label = "只观察"
                rationale = f"{security_name} 还没有进入可交易区间，现在更适合跟踪，不适合执行。"
                position_guidance = "不分配资金，保留观察位。"
                trigger_conditions = next_checks[:2] or [f"等待价格重新回到 {breakout_price} 附近再评估。"]
                invalidation_conditions = [f"若跌破 {support_price}，从候选名单降级。"]
                urgency_score = 44.0
        else:
            action = "stay_out"
            label = "暂不参与"
            rationale = f"{security_name} 当前胜率和赔率都不够，不值得占用资金。"
            position_guidance = "不建立仓位，把额度留给更清晰的机会。"
            trigger_conditions = next_checks[:2] or [f"只有重新站上 {breakout_price} 并修复相对强弱，才重新评估。"]
            invalidation_conditions = [f"若继续跌破 {support_price}，维持回避。"]
            urgency_score = 38.0

    market_view = snapshot.decision_bundle.get("market_view") or {}
    market_action = str(market_view.get("action") or "")
    risk_off_market = market_action == "risk_off"
    trend_score_value = float(feature.get("trend_score") or 50.0)
    rel_strength_value = relative_strength_value if isinstance(relative_strength_value, float) else None
    weak_trend = bool(
        trend_score_value <= 45.0
        or (rel_strength_value is not None and rel_strength_value <= -0.03)
        or (isinstance(ret_20, (int, float)) and ret_20_value <= -0.05)
    )
    near_high = bool(high_gap_value is not None and high_gap_value > -0.03)
    volume_confirmed = volume_ratio_value >= 1.2 and relative_strength_value is not None and relative_strength_value >= 0.0
    short_term_overheat = bool(
        (isinstance(ret_5, (int, float)) and ret_5_value >= 0.10)
        or (isinstance(ret_20, (int, float)) and ret_20_value >= 0.20)
    )

    if question_type == "add_position" and weak_trend and action not in {"cut_on_breakdown", "trim_into_strength"}:
        if ret_5_value > 0:
            action = "trim_into_strength"
            label = "逢高减仓"
            rationale = f"{security_name} 趋势和相对强弱已转弱，反弹只用于降风险，不再加仓。"
            position_guidance = "优先把仓位降到单票 8%-12% 区间。"
            trigger_conditions = [f"若反弹接近 {retest_price} 但量价没有继续扩张，执行减仓。"] + next_checks[:1]
            invalidation_conditions = [f"若重新站回 {breakout_price} 并恢复相对强势，可暂停继续减仓。"]
            urgency_score = max(urgency_score, 90.0)
        else:
            action = "cut_on_breakdown"
            label = "破位减仓"
            rationale = f"{security_name} 趋势明显转弱，先处理风险，再谈加仓。"
            position_guidance = "先减 1/3 到 1/2，保留观察仓。"
            trigger_conditions = [f"若跌破 {support_price} 且相对强弱继续走弱，立即执行减仓。"] + next_checks[:1]
            if favorable_pullback:
                invalidation_conditions = [f"若 {support_price} 一带止跌并重新收回 {pullback_price}，先降级为观察。"]
            else:
                invalidation_conditions = [f"若重新站回 {retest_price} 且成交恢复，可转回观察。"]
            urgency_score = max(urgency_score, 94.0)

    if question_type != "add_position" and weak_trend and action in {"standard_position", "trial_position"}:
        action = "watch_only"
        label = "只观察"
        rationale = f"{security_name} 趋势与相对强弱转弱，当前不适合主动开仓。"
        position_guidance = "先观望，不建立新仓。"
        trigger_conditions = [f"只有重新站回 {breakout_price} 且相对强弱修复，再评估。"] + next_checks[:1]
        invalidation_conditions = [f"若继续走弱并跌破 {support_price}，继续回避。"]
        urgency_score = min(urgency_score, 52.0)

    if risk_off_market:
        if question_type == "add_position" and action in {"add_on_strength", "buy_on_pullback"}:
            action = "hold_core_wait_market"
            label = "继续持有，等待市场修复"
            rationale = f"{security_name} 市场偏空，新开仓/加仓暂缓，先守已有仓位。"
            position_guidance = "保持现有仓位，不做加仓。"
            trigger_conditions = [f"等市场修复并重新站稳 {breakout_price} 再评估加仓。"] + next_checks[:1]
            invalidation_conditions = [f"若跌破 {support_price}，转入减仓预案。"]
            urgency_score = min(urgency_score, 60.0)
        elif question_type != "add_position" and action in {"standard_position", "trial_position"}:
            action = "watch_market_turn"
            label = "只观察"
            rationale = f"{security_name} 质量不差，但市场偏空，新开仓一律暂停。"
            position_guidance = "不建立新仓，等待市场修复。"
            trigger_conditions = [f"等市场修复并放量站稳 {breakout_price} 再评估。"] + next_checks[:1]
            invalidation_conditions = [f"若回落跌破 {support_price}，继续观望。"]
            urgency_score = min(urgency_score, 58.0)

    if near_high and not volume_confirmed:
        if question_type == "add_position" and action in {"add_on_strength", "buy_on_pullback"}:
            action = "hold_no_add"
            label = "继续持有，不再加仓"
            rationale = f"{security_name} 距20日高点过近但量能不足，先不追着加仓。"
            position_guidance = "保持现有仓位，等回踩或放量确认后再评估。"
            trigger_conditions = [f"回踩到 {pullback_price} 附近企稳，或放量站稳 {breakout_price} 再考虑加仓。"] + next_checks[:1]
            invalidation_conditions = [f"若跌破 {support_price}，转入减仓预案。"]
            urgency_score = min(urgency_score, 60.0)
        elif question_type != "add_position" and action in {"standard_position", "trial_position"}:
            action = "wait_for_breakout"
            label = "等放量突破"
            rationale = f"{security_name} 距20日高点过近但量能不足，先不追价。"
            position_guidance = "不追高，等放量确认后再考虑。"
            trigger_conditions = [f"只有放量突破 {breakout_price}，才升级为试仓。"] + next_checks[:1]
            invalidation_conditions = [f"若回落跌破 {support_price}，取消突破预案。"]
            urgency_score = min(urgency_score, 60.0)

    if short_term_overheat:
        if question_type == "add_position" and action in {"add_on_strength", "buy_on_pullback"}:
            action = "hold_no_add"
            label = "继续持有，不再加仓"
            rationale = f"{security_name} 短期涨幅偏大，先不追着加仓。"
            position_guidance = "保持仓位，等回踩或消化后再评估。"
            trigger_conditions = [f"回踩到 {pullback_price} 一带企稳，再评估是否补仓。"] + next_checks[:1]
            invalidation_conditions = [f"若跌破 {support_price}，转入减仓预案。"]
            urgency_score = min(urgency_score, 58.0)
        elif question_type != "add_position" and action in {"standard_position", "trial_position"}:
            action = "wait_for_pullback"
            label = "等回踩"
            rationale = f"{security_name} 短期涨幅过大，先等回踩消化再考虑。"
            position_guidance = "不追价，等待更舒服的回撤位置。"
            trigger_conditions = [f"回踩 {pullback_price} 一带且不放量破位时，再考虑试仓。"] + next_checks[:1]
            invalidation_conditions = [f"若跌破 {support_price}，取消计划。"]
            urgency_score = min(urgency_score, 58.0)

    if question_type != "add_position" and action in {"standard_position", "trial_position"} and not timing_confirmed and not favorable_pullback:
        action = "wait_for_breakout"
        label = "等放量突破"
        rationale = f"{security_name} 还没有明确突破确认信号，宁可先等确认。"
        position_guidance = "不提前买入，等放量确认后再考虑。"
        trigger_conditions = [f"只有放量突破 {breakout_price}，才升级为试仓。"] + next_checks[:1]
        invalidation_conditions = [f"若回落跌破 {support_price}，取消突破预案。"]
        urgency_score = min(urgency_score, 58.0)

    urgency = "high" if urgency_score >= 85 else "medium" if urgency_score >= 60 else "low"
    monitoring_focus = _top_lines(
        [
            f"当前更像 {zone_label}，重点看价格相对 {breakout_price} / {support_price} 的反应。",
            positives[0] if positives else "",
            negatives[0] if negatives else "",
        ],
        limit=3,
    )
    serialized_levels = {key: round(float(value), 3) for key, value in levels.items() if isinstance(value, (int, float)) and float(value) > 0}
    serialized_levels.update(
        {
            "stop_price": float(entry_context.get("stop_price") or 0.0),
            "entry_reward_risk": float(entry_context.get("reward_risk") or 0.0),
            "entry_upside_pct": round(float(entry_context.get("upside_pct") or 0.0) * 100, 2),
            "entry_downside_pct": round(float(entry_context.get("downside_pct") or 0.0) * 100, 2),
            "box_position_pct": round(float(entry_context.get("relative_position") or 0.0) * 100, 2),
        }
    )
    do_not_lines = [
        "不要在没有触发条件前提前追单。",
        "不要把单票仓位一次性打满。",
        "不要忽略市场状态对个股执行的影响。",
    ]
    if execution_context_incomplete:
        if not position_context.get("position_pricing_complete", True):
            do_not_lines.insert(0, "持仓成本未知时，不把摊低成本当成默认动作。")
        if not position_context.get("position_size_complete", True):
            do_not_lines.insert(0, "持仓数量或可卖数量缺失时，不给精确手数。")
        if not position_context.get("position_pricing_complete", True):
            rationale = f"{rationale} 持仓成本未知，本次只按结构与风险预算给出比例级建议。"
        else:
            rationale = f"{rationale} 当前持仓字段不完整，本次只按结构与风险预算给出比例级建议。"
        position_guidance = f"{position_guidance} 由于持仓数量或成本字段缺失，本次不下具体手数。"
    blockers = build_decision_blockers(
        scorecard=scorecard,
        feature=feature,
        position_context=position_context,
        question_type=question_type,
        action=action,
        entry_context=entry_context,
    )
    execution_brief = build_execution_brief(
        action=action,
        question_type=question_type,
        trigger_conditions=trigger_conditions,
        invalidation_conditions=invalidation_conditions,
    )
    plan = ActionPlan(
        action=action,
        label=label,
        rationale=rationale,
        position_guidance=position_guidance,
        urgency=urgency,
        urgency_score=urgency_score,
        levels=serialized_levels,
        trigger_conditions=_top_lines(trigger_conditions, limit=3),
        invalidation_conditions=_top_lines(invalidation_conditions, limit=3),
        blockers=blockers,
        execution_brief=execution_brief,
        do_not=_top_lines(do_not_lines, limit=3),
        monitoring_focus=monitoring_focus,
    )
    return adapt_action_plan_to_strategy(
        action_plan=plan,
        strategy_style=strategy_style,
        question_type=question_type,
        security_name=security_name,
        scorecard=scorecard,
        breakout_price=breakout_price,
        support_price=support_price,
        pullback_price=pullback_price,
        next_checks=next_checks,
        position_context=position_context,
    )


def build_decision_blockers(
    *,
    scorecard: ScoreCard,
    feature: dict[str, Any],
    position_context: dict[str, Any],
    question_type: str,
    action: str,
    entry_context: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    coverage_score = float(scorecard.coverage_score)
    market_score = float(scorecard.market_score)
    trend_score = float(feature.get("trend_score") or 50.0)
    ret_20 = float(feature.get("ret_20d") or 0.0)
    relative_strength = feature.get("relative_strength_20d")
    relative_strength_value = float(relative_strength) if isinstance(relative_strength, (int, float)) else 0.0
    volume_ratio = float(feature.get("volume_ratio_5d") or 0.0)
    reward_risk = float(entry_context.get("reward_risk") or 0.0)
    near_breakout = bool(entry_context.get("near_breakout"))
    position_weight = float(position_context.get("position_weight") or 0.0)
    sector_weight = float(position_context.get("sector_weight") or 0.0)
    exposure_ratio = float(position_context.get("exposure_ratio") or 0.0)
    pnl_pct = position_context.get("pnl_pct")
    pnl_value = float(pnl_pct) if isinstance(pnl_pct, (int, float)) else None

    if coverage_score < 45:
        blockers.append(f"覆盖度只有 {coverage_score:.1f}，证据还不够完整。")
    elif coverage_score < 60:
        blockers.append(f"覆盖度 {coverage_score:.1f}，当前最多只适合观察或试仓。")
    if market_score < 40:
        blockers.append(f"市场分 {market_score:.1f} 低于 40，当前不支持主动进攻。")
    elif market_score < 50 and question_type != "add_position":
        blockers.append(f"市场分 {market_score:.1f} 仍偏谨慎，最多只适合试仓。")
    if trend_score <= 45.0 or ret_20 <= -0.08 or relative_strength_value <= -0.03:
        blockers.append("中期趋势或相对强弱还没修复，先别把反弹当主剧本。")
    if near_breakout and volume_ratio < 1.15:
        blockers.append(f"临近关键位但量比只有 {volume_ratio:.2f}x，容易演变成追价。")
    if reward_risk < 2.2 and action not in {"cut_on_breakdown", "trim_into_strength", "stay_out"}:
        blockers.append(f"当前位置赔率只有 {reward_risk:.2f}，还不够支持积极出手。")
    if question_type == "add_position":
        if position_weight >= 0.20:
            blockers.append(f"单票仓位 {position_weight:.1%} 已偏高，继续加仓容错率下降。")
        if sector_weight >= 0.30:
            blockers.append(f"同主题暴露 {sector_weight:.1%} 偏高，继续加仓会放大主题风险。")
        if exposure_ratio >= 0.92:
            blockers.append(f"总仓位 {exposure_ratio:.1%} 偏满，现金缓冲不足。")
        if pnl_value is not None and pnl_value <= -0.08:
            blockers.append(f"当前浮亏 {pnl_value:+.2%}，弱势里不主动摊低成本。")
    return _top_lines(blockers, limit=3)


def build_execution_brief(
    *,
    action: str,
    question_type: str,
    trigger_conditions: list[str],
    invalidation_conditions: list[str],
) -> list[str]:
    if action in {"cut_on_breakdown", "trim_into_strength", "stay_out"}:
        now_text = "现在先做风险控制，不做主动加仓。"
    elif action in {"hold_no_add", "hold_core_wait_market", "hold_core", "watch_only", "watch_market_turn"}:
        now_text = "现在以持有/观察为主，不主动出手。"
    elif question_type == "add_position":
        now_text = "现在不抢着补仓，只在条件确认后小幅加。"
    else:
        now_text = "现在不提前开仓，只等确认信号。"
    upgrade_text = trigger_conditions[0] if trigger_conditions else "只有触发条件成立，才升级动作。"
    cancel_text = invalidation_conditions[0] if invalidation_conditions else "若结构失效，当前计划自动取消。"
    return [now_text, upgrade_text, cancel_text]


def adapt_action_plan_to_strategy(
    *,
    action_plan: ActionPlan,
    strategy_style: str,
    question_type: str,
    security_name: str,
    scorecard: ScoreCard,
    breakout_price: str,
    support_price: str,
    pullback_price: str,
    next_checks: list[str],
    position_context: dict[str, Any],
) -> ActionPlan:
    if strategy_style == "trend_following":
        return adapt_trend_following_plan(
            action_plan=action_plan,
            question_type=question_type,
            security_name=security_name,
            breakout_price=breakout_price,
            support_price=support_price,
            next_checks=next_checks,
        )
    if strategy_style == "pullback_accumulation":
        return adapt_pullback_plan(
            action_plan=action_plan,
            question_type=question_type,
            security_name=security_name,
            breakout_price=breakout_price,
            support_price=support_price,
            pullback_price=pullback_price,
            next_checks=next_checks,
        )
    if strategy_style == "defensive_quality":
        return adapt_defensive_plan(
            action_plan=action_plan,
            question_type=question_type,
            security_name=security_name,
            scorecard=scorecard,
            breakout_price=breakout_price,
            support_price=support_price,
            next_checks=next_checks,
            position_context=position_context,
        )
    return action_plan


def adapt_trend_following_plan(
    *,
    action_plan: ActionPlan,
    question_type: str,
    security_name: str,
    breakout_price: str,
    support_price: str,
    next_checks: list[str],
) -> ActionPlan:
    if question_type == "add_position" and action_plan.action == "buy_on_pullback":
        return replace(
            action_plan,
            action="hold_no_add",
            label="继续持有，不追左侧补仓",
            rationale=f"{security_name} 若按趋势跟随做，不在回踩不明朗时主动补仓，先等右侧确认。",
            position_guidance="不做左侧摊低，只在放量重新转强后再考虑加仓。",
            urgency="medium",
            urgency_score=max(action_plan.urgency_score, 64.0),
            trigger_conditions=[f"只有重新放量站稳 {breakout_price}，才恢复趋势加仓。"] + list(next_checks[:1]),
            invalidation_conditions=[f"若跌破 {support_price}，转回减仓或继续观望。"],
            do_not=_top_lines(["不要在弱势回踩里补仓。"] + list(action_plan.do_not), limit=3),
            monitoring_focus=_top_lines([f"重点看是否重新站回 {breakout_price}。"] + list(action_plan.monitoring_focus), limit=3),
        )
    if question_type != "add_position" and action_plan.action == "wait_for_pullback":
        return replace(
            action_plan,
            action="wait_for_breakout",
            label="等放量突破",
            rationale=f"{security_name} 若按趋势跟随做，优先等右侧确认，不提前埋伏回踩。",
            position_guidance="只在突破确认后试仓，不做左侧埋伏单。",
            urgency="medium",
            urgency_score=max(action_plan.urgency_score, 62.0),
            trigger_conditions=[f"放量站稳 {breakout_price} 后，再进入趋势试仓。"] + list(next_checks[:1]),
            invalidation_conditions=[f"若跌破 {support_price}，取消趋势跟随预案。"],
            do_not=_top_lines(["不要把回踩假设当成趋势确认。"] + list(action_plan.do_not), limit=3),
            monitoring_focus=_top_lines([f"跟踪突破 {breakout_price} 时的量能扩张。"] + list(action_plan.monitoring_focus), limit=3),
        )
    return action_plan


def adapt_pullback_plan(
    *,
    action_plan: ActionPlan,
    question_type: str,
    security_name: str,
    breakout_price: str,
    support_price: str,
    pullback_price: str,
    next_checks: list[str],
) -> ActionPlan:
    if question_type == "add_position" and action_plan.action == "add_on_strength":
        return replace(
            action_plan,
            action="buy_on_pullback",
            label="等回踩再加",
            rationale=f"{security_name} 若按回踩低吸做，不追强拉后的加仓点，优先等更舒服的回撤。",
            position_guidance="保留机动仓，只在回踩承接成立时补仓。",
            urgency="medium",
            urgency_score=min(max(action_plan.urgency_score, 65.0), 72.0),
            trigger_conditions=[f"回踩 {pullback_price} 附近并确认承接后，再补仓。"] + list(next_checks[:1]),
            invalidation_conditions=[f"若跌破 {support_price}，取消低吸计划。"],
            do_not=_top_lines(["不要在放量拉升日追着补仓。"] + list(action_plan.do_not), limit=3),
            monitoring_focus=_top_lines([f"观察 {pullback_price} 一带是否缩量企稳。"] + list(action_plan.monitoring_focus), limit=3),
        )
    if question_type != "add_position" and action_plan.action in {"standard_position", "wait_for_breakout"}:
        return replace(
            action_plan,
            action="wait_for_pullback",
            label="等回踩",
            rationale=f"{security_name} 若按回踩低吸做，现在不追突破，优先等更好的赔率位置。",
            position_guidance="先不追价，等回踩确认后用小仓试错。",
            urgency="medium",
            urgency_score=min(max(action_plan.urgency_score, 60.0), 68.0),
            trigger_conditions=[f"回踩 {pullback_price} 一带且不放量破位时，再考虑试仓。"] + list(next_checks[:1]),
            invalidation_conditions=[f"若跌破 {support_price}，取消低吸预案。"],
            do_not=_top_lines(["不要把突破追价当成低吸策略。"] + list(action_plan.do_not), limit=3),
            monitoring_focus=_top_lines([f"观察价格回撤到 {pullback_price} 一带时的承接。"] + list(action_plan.monitoring_focus), limit=3),
        )
    return action_plan


def adapt_defensive_plan(
    *,
    action_plan: ActionPlan,
    question_type: str,
    security_name: str,
    scorecard: ScoreCard,
    breakout_price: str,
    support_price: str,
    next_checks: list[str],
    position_context: dict[str, Any],
) -> ActionPlan:
    if question_type == "add_position" and action_plan.action in {"add_on_strength", "buy_on_pullback"}:
        return replace(
            action_plan,
            action="hold_no_add",
            label="继续持有，不再加仓",
            rationale=f"{security_name} 若按防守质量做，当前优先稳住已有仓位，不扩张风险暴露。",
            position_guidance="保留现有仓位，等待市场和个股共振改善后再看。",
            urgency="medium",
            urgency_score=max(action_plan.urgency_score, 66.0),
            trigger_conditions=[f"只有市场修复且重新站稳 {breakout_price}，才恢复加仓评估。"] + list(next_checks[:1]),
            invalidation_conditions=[f"若跌破 {support_price}，转入更强风控。"],
            do_not=_top_lines(["不要在总仓位紧张时继续加同类风险。"] + list(action_plan.do_not), limit=3),
            monitoring_focus=_top_lines([f"先看市场风险预算是否允许恢复进攻。"] + list(action_plan.monitoring_focus), limit=3),
        )
    if question_type != "add_position" and action_plan.action in {"standard_position", "trial_position"}:
        if float(scorecard.market_score) < 48 or float(position_context.get("exposure_ratio") or 0.0) >= 0.85:
            return replace(
                action_plan,
                action="watch_market_turn",
                label="只观察",
                rationale=f"{security_name} 即便质量不差，防守策略也不在弱环境里主动扩大试错。",
                position_guidance="先不建仓，把它放在市场修复后的备选名单。",
                urgency="medium",
                urgency_score=59.0,
                trigger_conditions=[f"等市场修复后，再看是否重新站稳 {breakout_price}。"] + list(next_checks[:1]),
                invalidation_conditions=[f"若跌破 {support_price}，降出重点观察。"],
                do_not=_top_lines(["不要把防守策略做成追涨策略。"] + list(action_plan.do_not), limit=3),
                monitoring_focus=_top_lines([f"先看市场风险预算，再看 {security_name} 自身强度。"] + list(action_plan.monitoring_focus), limit=3),
            )
        return replace(
            action_plan,
            action="trial_position",
            label="试仓",
            rationale=f"{security_name} 可以纳入防守型试仓，但仍只允许小仓确认，不做进攻式建仓。",
            position_guidance="首笔控制在 1%-3%，确认后再决定是否提高。",
            urgency="medium",
            urgency_score=min(action_plan.urgency_score, 68.0),
            do_not=_top_lines(["不要一次性打到标准仓。"] + list(action_plan.do_not), limit=3),
        )
    return action_plan


def build_strategy_profile(
    *,
    strategy_style: str,
    action_plan: ActionPlan,
    scorecard: ScoreCard,
    question_type: str,
    security_name: str,
    position_context: dict[str, Any],
) -> StrategyProfile:
    label = STYLE_LABELS.get(strategy_style, STYLE_LABELS["general"])
    if strategy_style == "trend_following":
        regime_overlay = (
            "当前市场偏弱，趋势策略只保留右侧确认后的轻仓试错。"
            if float(scorecard.market_score) < 45
            else "市场没有明显压制趋势策略，可按确认程度分批执行。"
        )
        return StrategyProfile(
            style=strategy_style,
            label=label,
            policy_summary=f"{label}视角下，当前不是先猜低点，而是等趋势确认后再执行“{action_plan.label}”。",
            regime_overlay=regime_overlay,
            checklist=[
                "只在放量突破或相对强弱修复后执行。",
                "仓位随确认程度递增，先试后放大。",
                "跌破关键支撑时不做左侧摊低。",
            ],
            do_not=[
                "不要把回踩猜测当成趋势确认。",
                "不要在弱市场里用趋势策略重仓抢反弹。",
            ],
            preferred_actions=["等放量突破", "试仓", "正常仓", "趋势加仓"],
        )
    if strategy_style == "pullback_accumulation":
        regime_overlay = (
            "当前市场偏弱，低吸只能做缩量回踩，不做接飞刀。"
            if float(scorecard.market_score) < 45
            else "市场中性时，可优先等待回踩承接，而不是追突破。"
        )
        return StrategyProfile(
            style=strategy_style,
            label=label,
            policy_summary=f"{label}视角下，当前优先等赔率更好的回撤位置，再执行“{action_plan.label}”。",
            regime_overlay=regime_overlay,
            checklist=[
                "优先看回踩关键均线或支撑位时是否缩量企稳。",
                "首笔仓位小，不在长阳拉升日追高。",
                "跌破支撑立即取消低吸预案。",
            ],
            do_not=[
                "不要把突破追价包装成低吸。",
                "不要在放量下跌中机械补仓。",
            ],
            preferred_actions=["等回踩", "试仓", "等回踩再加"],
        )
    if strategy_style == "defensive_quality":
        exposure_ratio = float(position_context.get("exposure_ratio") or 0.0)
        regime_overlay = (
            "当前市场和仓位都偏紧，防守策略优先保留现金与确定性。"
            if float(scorecard.market_score) < 50 or exposure_ratio >= 0.85
            else "市场没有明显转差，但防守策略仍只接受小仓、慢节奏执行。"
        )
        return StrategyProfile(
            style=strategy_style,
            label=label,
            policy_summary=f"{label}视角下，核心是先控制风险预算，再决定是否执行“{action_plan.label}”。",
            regime_overlay=regime_overlay,
            checklist=[
                "优先质量和风险预算，不追求最高弹性。",
                "同主题暴露过高时，不继续放大仓位。",
                "先看市场状态，再看个股是否值得试仓。",
            ],
            do_not=[
                "不要在满仓或弱市场里扩大试错。",
                "不要把防守风格做成短线进攻风格。",
            ],
            preferred_actions=["只观察", "试仓", "继续持有，不再加仓"],
        )
    base_overlay = (
        "当前市场偏弱，执行上先看风控与触发条件。"
        if float(scorecard.market_score) < 45
        else "当前市场中性偏可控，按触发条件执行即可。"
    )
    return StrategyProfile(
        style="general",
        label=STYLE_LABELS["general"],
        policy_summary=f"综合决策视角下，当前建议执行“{action_plan.label}”，同时绑定触发与失效条件。",
        regime_overlay=base_overlay,
        checklist=[
            "先看市场状态，再看个股质量与时点。",
            "动作必须和触发条件绑定，不凭主观感觉执行。",
            "失效条件触发后，优先回到风险动作。",
        ],
        do_not=[
            "不要忽略仓位与市场环境约束。",
            "不要脱离触发条件提前交易。",
        ],
        preferred_actions=[action_plan.label],
    )


def build_trade_levels(*, snapshot: AdviceSnapshot, code: str, feature: dict[str, Any]) -> dict[str, float]:
    series_payload = snapshot.series_map.get(code) or {}
    bars = (series_payload.get("bars") or []) if isinstance(series_payload, dict) else []
    closes = [float(row.get("close_price") or 0.0) for row in bars if float(row.get("close_price") or 0.0) > 0]
    last_close = float(feature.get("last_close") or (closes[-1] if closes else 0.0))
    recent_20 = closes[-20:] if len(closes) >= 20 else closes
    recent_10 = closes[-10:] if len(closes) >= 10 else closes
    recent_5 = closes[-5:] if len(closes) >= 5 else closes
    breakout = max(recent_20) if recent_20 else last_close
    support = min(recent_10) if recent_10 else last_close
    pullback = (sum(recent_5) / len(recent_5)) if recent_5 else last_close
    retest = max(recent_5) if recent_5 else last_close
    return {
        "breakout_price": round(float(breakout), 3) if breakout else 0.0,
        "support_price": round(float(support), 3) if support else 0.0,
        "pullback_price": round(float(pullback), 3) if pullback else 0.0,
        "retest_price": round(float(retest), 3) if retest else 0.0,
    }


def build_thesis(*, security_name: str, action_plan: ActionPlan, positives: list[str], negatives: list[str]) -> str:
    positive = _normalize_clause(positives[0] if positives else "基本面和走势没有形成明确优势")
    secondary = _normalize_clause(positives[1]) if len(positives) > 1 else ""
    constraint = _normalize_clause(negatives[0] if negatives else "当前没有明显反证")
    if secondary:
        return f"{security_name} 的主逻辑是 {positive}；{secondary}，但 {constraint}，所以当前策略是“{action_plan.label}”。"
    return f"{security_name} 的主逻辑是 {positive}，但 {constraint}，所以当前策略是“{action_plan.label}”。"


def decide_position_action(
    *,
    scorecard: ScoreCard,
    position_context: dict[str, Any],
    risk_profile: str,
) -> tuple[str, float, list[str]]:
    if not position_context.get("is_holding"):
        decision, confidence = decide_action(scorecard)
        notes = ["当前持仓中没有这只证券，无法给出真正的加仓结论。"]
        return ("watch" if decision == "buy" else decision), confidence, notes

    notes: list[str] = []
    position_weight = float(position_context.get("position_weight") or 0.0)
    sector_weight = float(position_context.get("sector_weight") or 0.0)
    exposure_ratio = float(position_context.get("exposure_ratio") or 0.0)
    pnl_pct = position_context.get("pnl_pct")
    if isinstance(pnl_pct, str):
        pnl_pct = None

    add_cap = 0.22 if risk_profile != "aggressive" else 0.28
    sector_cap = 0.35 if risk_profile != "aggressive" else 0.45
    if position_weight >= add_cap:
        notes.append(f"当前仓位占比 {position_weight:.1%}，已经接近单票上限。")
    if sector_weight >= sector_cap:
        notes.append(f"当前主题暴露 {sector_weight:.1%}，继续加仓会放大同主题风险。")
    if exposure_ratio >= 0.92:
        notes.append(f"当前总仓位 {exposure_ratio:.1%}，现金缓冲偏低。")
    if isinstance(pnl_pct, (int, float)) and pnl_pct <= -0.08:
        notes.append(f"当前浮亏 {pnl_pct:+.2%}，避免在弱趋势里摊低成本。")

    if scorecard.coverage_score < 45:
        return "insufficient_evidence", round(clamp(scorecard.coverage_score / 100.0, 0.05, 0.6), 4), notes
    if scorecard.stock_score < 40 or scorecard.timing_score < 35:
        return "trim", round(clamp((100.0 - scorecard.total_score) / 160.0, 0.05, 0.92), 4), notes
    if scorecard.market_score < 40 and scorecard.timing_score < 60:
        return "hold", round(clamp((scorecard.coverage_score + 50.0) / 200.0, 0.05, 0.82), 4), notes
    if scorecard.coverage_score < 60:
        notes.append(f"覆盖度 {scorecard.coverage_score:.1f}，当前不支持积极加仓。")
    if notes:
        return "hold", round(clamp((scorecard.total_score + scorecard.coverage_score) / 200.0, 0.05, 0.82), 4), notes
    if (
        scorecard.total_score >= 72
        and scorecard.market_score >= 50
        and scorecard.timing_score >= 60
        and scorecard.coverage_score >= 60
    ):
        return "add", round(clamp((scorecard.total_score + scorecard.coverage_score) / 180.0, 0.05, 0.95), 4), notes
    if scorecard.total_score >= 50:
        return "hold", round(clamp((scorecard.total_score + scorecard.coverage_score) / 210.0, 0.05, 0.86), 4), notes
    return "trim", round(clamp((100.0 - scorecard.total_score) / 180.0, 0.05, 0.9), 4), notes


def build_pdf_evidence(pdf_payload: dict[str, Any] | None) -> tuple[list[EvidenceItem], list[str], list[str], list[str]]:
    if not pdf_payload:
        return [], [], [], []
    rows: list[EvidenceItem] = []
    positives: list[str] = []
    negatives: list[str] = []
    missing: list[str] = []
    for insight in (pdf_payload.get("insights") or [])[:2]:
        signal = str(insight.get("signal") or "neutral")
        summary = str(insight.get("summary") or "").strip()
        if not summary:
            continue
        rows.append(
            EvidenceItem(
                category="pdf_insight",
                signal=signal,
                strength=float(insight.get("strength") or 55.0),
                summary=summary,
                source="pdf_skill",
                verified=bool(insight.get("verified", True)),
                freshness=str(insight.get("published_at") or pdf_payload.get("as_of") or "unknown"),
                metadata={
                    "title": insight.get("title"),
                    "pdf_path": insight.get("pdf_path"),
                    "text_path": insight.get("text_path"),
                },
            )
        )
        key_lines = [str(line) for line in (insight.get("key_lines") or []) if str(line).strip()]
        if signal == "positive":
            positives.extend(key_lines[:2] or [summary])
        elif signal == "negative":
            negatives.extend(key_lines[:2] or [summary])
    if not rows:
        missing.append("缺少公告 PDF 深读")
    return rows, _top_lines(positives, limit=4), _top_lines(negatives, limit=4), _top_lines(missing, limit=2)


def build_summary(*, security_name: str, action_label: str, score: float, market_score: float, coverage_score: float) -> str:
    return (
        f"{security_name} 当前策略为“{action_label}”。"
        f" 综合分 {score:.1f}，市场分 {market_score:.1f}，覆盖度 {coverage_score:.1f}。"
    )


def build_next_checks(
    *,
    decision: str,
    question_type: str,
    feature: dict[str, Any],
    market_score: float,
    position_context: dict[str, Any],
) -> list[str]:
    checks: list[str] = []
    if market_score < 45:
        checks.append("先等指数和市场广度修复，再看个股确认。")
    if (feature.get("volume_ratio_5d") or 0.0) < 1.0:
        checks.append("关注下一次放量是否能站稳关键价位。")
    if (feature.get("high_gap_20d") or 0.0) < -0.05:
        checks.append("先观察是否在关键支撑或均线附近缩量企稳，再看修复力度。")
    if question_type == "add_position" and position_context.get("is_holding"):
        if float(position_context.get("position_weight") or 0.0) >= 0.2:
            checks.append("单票仓位已经不低，后续只在趋势确认后再考虑微调。")
        if float(position_context.get("sector_weight") or 0.0) >= 0.3:
            checks.append("同主题暴露偏高，先确认其他相关持仓是否需要同步降权。")
        if not position_context.get("position_pricing_complete", True):
            checks.append("当前持仓成本未知，本次不把摊低成本当成默认动作。")
        if not position_context.get("position_size_complete", True):
            checks.append("当前持仓数量或可卖数量缺失，本次只给比例建议，不下具体手数。")
    if decision in {"trim", "avoid"}:
        checks.append("若跌破关键支撑且量能放大，优先执行减仓计划。")
    return _top_lines(checks, limit=5)


def holding_action_from_evaluation(evaluation: SecurityEvaluation) -> tuple[str, list[str], list[str]]:
    return (
        evaluation.action_plan.action,
        [evaluation.action_plan.rationale] + (evaluation.positive_factors[:1] or ["继续跟踪个股与市场的共振情况。"]),
        evaluation.invalidation_conditions[:2] or evaluation.counter_evidence[:2] or ["若条件恶化，优先执行风险动作。"],
    )


def watch_action_from_evaluation(evaluation: SecurityEvaluation) -> tuple[str, list[str], list[str]]:
    return (
        evaluation.action_plan.action,
        [evaluation.action_plan.rationale] + (evaluation.positive_factors[:1] or ["先保留观察，再等条件满足。"]),
        evaluation.invalidation_conditions[:2] or evaluation.counter_evidence[:2] or ["条件不成立时，不进行交易。"],
    )


def format_price_level(value: float | None) -> str:
    if value in (None, 0):
        return "关键位"
    return f"{float(value):.2f}"


def _top_lines(items: list[str], *, limit: int) -> list[str]:
    rows: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
        if len(rows) >= limit:
            break
    return rows


def _normalize_clause(text: str) -> str:
    value = str(text or "").strip()
    value = value.rstrip("，。；;,.!?！？ ")
    return value or "暂无关键结论"
