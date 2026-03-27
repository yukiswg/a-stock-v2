from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..advice_harness.evidence import AdviceSnapshot
from ..config import UniverseItem
from ..decision_core import (
    candidate_selection_score,
    candidate_style_fit,
    evaluate_security,
    holding_action_from_evaluation,
    infer_market_strategy_style,
    watch_action_from_evaluation,
)
from ..models import (
    AnnouncementItem,
    DailyDecisionBundle,
    DailySeriesSnapshot,
    HoldingsSnapshot,
    InstrumentFeatures,
    NewsItem,
    StructuredDecision,
)
from ..utils import average, clamp, ensure_dir, write_json
from ..data_harness.market_data import compute_series_features
from .display import enrich_display_fields
from .rendering import (
    render_action_summary,
    render_comprehensive_report,
    render_daily_report,
    render_integrated_report,
    render_homepage_overview,
)


BENCHMARK_CODES = {"000300", "510300", "000001", "399006"}
NEGATIVE_KEYWORDS = ("减持", "处罚", "问询", "风险", "波动", "违约", "减速")
POSITIVE_KEYWORDS = ("增长", "回购", "订单", "景气", "突破", "扩产", "增持")


def build_decision_bundle(
    *,
    as_of: str,
    holdings: HoldingsSnapshot,
    universe: list[UniverseItem],
    series_map: dict[str, DailySeriesSnapshot],
    news_items: list[NewsItem],
    announcements: list[AnnouncementItem],
    llm_summary: dict[str, Any],
    config: dict[str, Any] | None = None,
    supplemental: dict[str, dict[str, Any]] | None = None,
    sector_map: dict[str, str] | None = None,
    sector_metrics: dict[str, dict[str, Any]] | None = None,
) -> tuple[DailyDecisionBundle, dict[str, InstrumentFeatures]]:
    benchmark_series = series_map.get("000300") or series_map.get("510300")
    feature_map: dict[str, InstrumentFeatures] = {}
    universe_map = {item.code: item for item in universe}
    for code, snapshot in series_map.items():
        category = universe_map.get(code).category if code in universe_map else ("holding" if code in {item.code for item in holdings.positions} else "watch")
        feature_map[code] = InstrumentFeatures(**compute_series_features(snapshot, benchmark_series=benchmark_series, category=category))

    market_view = build_market_decision(
        as_of=as_of,
        feature_map=feature_map,
        news_items=news_items,
        announcements=announcements,
        sector_metrics=sector_metrics or {},
    )
    snapshot = _build_shared_snapshot(
        as_of=as_of,
        holdings=holdings,
        universe=universe,
        feature_map=feature_map,
        series_map=series_map,
        market_view=market_view,
        news_items=news_items,
        announcements=announcements,
        supplemental=supplemental or {},
        sector_map=sector_map or {},
        sector_metrics=sector_metrics or {},
    )
    runtime_config = config or {"project": {"daily_bar_cache_dir": "data/cache/daily_bars"}, "supplemental": {"enabled": False}}
    holdings_actions = build_holdings_actions(as_of=as_of, holdings=holdings, snapshot=snapshot, config=runtime_config)
    watchlist = build_watchlist(as_of=as_of, holdings=holdings, snapshot=snapshot, config=runtime_config)
    monitor_plan = build_monitor_plan(as_of=as_of, market_view=market_view, holdings_actions=holdings_actions, watchlist=watchlist, feature_map=feature_map)
    action_summary = build_action_summary_line(market_view, holdings_actions, watchlist)
    homepage_overview = build_homepage_payload(
        as_of=as_of,
        market_view=market_view,
        holdings_actions=holdings_actions,
        watchlist=watchlist,
        action_summary=action_summary,
        feature_map=feature_map,
    )
    bundle = DailyDecisionBundle(
        as_of=as_of,
        market_view=market_view,
        holdings_actions=holdings_actions,
        watchlist=watchlist,
        monitor_plan=monitor_plan,
        final_action_summary=action_summary,
        homepage_overview=homepage_overview,
        llm_summary=llm_summary,
    )
    return bundle, feature_map


def build_market_decision(
    *,
    as_of: str,
    feature_map: dict[str, InstrumentFeatures],
    news_items: list[NewsItem],
    announcements: list[AnnouncementItem],
    sector_metrics: dict[str, dict[str, Any]] | None = None,
) -> StructuredDecision:
    benchmark = feature_map.get("000300") or feature_map.get("510300") or next(iter(feature_map.values()))
    opportunity_set = [feature for feature in feature_map.values() if feature.category != "benchmark"]
    broad_feature_values = [feature.trend_score for feature in opportunity_set]
    breadth = average(broad_feature_values) or 50.0
    leadership_count = sum(
        1
        for feature in opportunity_set
        if feature.trend_score >= 68 and isinstance(feature.relative_strength_20d, (int, float)) and feature.relative_strength_20d >= 0.05
    )
    breakdown_count = sum(
        1
        for feature in opportunity_set
        if feature.trend_score <= 35 or (isinstance(feature.ret_20d, (int, float)) and feature.ret_20d <= -0.08)
    )
    news_bias = news_sentiment_score(news_items)
    announcement_bias = announcement_risk_score(announcements)
    raw_probability = 0.5
    raw_probability += ((benchmark.ret_5d or 0.0) * 3.0)
    raw_probability += ((benchmark.ret_20d or 0.0) * 1.5)
    raw_probability += ((breadth - 50.0) / 200.0)
    raw_probability += news_bias
    raw_probability += announcement_bias
    sector_scores = [float((item or {}).get("score") or 0.0) for item in (sector_metrics or {}).values() if isinstance((item or {}).get("score"), (int, float))]
    if sector_scores:
        raw_probability += ((average(sector_scores) or 50.0) - 50.0) / 260.0
    probability = round(clamp(raw_probability, 0.05, 0.95), 4)
    if probability >= 0.58:
        action = "risk_on"
        label = "偏多"
        regime = "进攻期"
        policy = ["允许新开仓", "优先强趋势与放量突破", "单票可从试仓升级到正常仓"]
    elif probability <= 0.42:
        action = "risk_off"
        label = "偏空"
        regime = "防守期"
        policy = ["新开仓从严控制", "优先处理弱持仓和破位风险", "只保留少量观察名单"]
    else:
        action = "balanced"
        label = "震荡"
        regime = "选择期"
        policy = ["只做最强标的", "优先等回踩或等确认", "不追高，不扩散仓位"]
    reasons = [
        f"沪深300 5日收益 {format_pct(benchmark.ret_5d)}，20日收益 {format_pct(benchmark.ret_20d)}。",
        f"候选池平均趋势分 {breadth:.1f}。",
        f"强势候选 {leadership_count} 个，明显走弱对象 {breakdown_count} 个。",
        f"新闻偏移 {news_bias:+.2f}，公告偏移 {announcement_bias:+.2f}。",
    ]
    risks = [
        "宏观新闻和指数拐点可能快速反转。",
        "公告标题判断只反映增量信息，不替代正文核验。",
    ]
    return StructuredDecision(
        object_type="market",
        object_id=benchmark.code,
        object_name=f"{label}市场",
        at=as_of,
        action=action,
        score=round(probability * 100, 2),
        probability=probability,
        reason=reasons,
        risk=risks,
        thesis=f"{regime}下，当前更适合执行“{policy[0]}”而不是平均分配火力。",
        counterpoints=risks[:2],
        trigger_conditions=policy[:2],
        invalidation_conditions=["若指数与广度快速修复，需上调风险预算。", "若强势股同步失去相对强度，继续收缩进攻动作。"],
        priority_score=round((1.0 - abs(probability - 0.5)) * 50 + (55 if action == "risk_off" else 35), 2),
        sources=["benchmark_daily_bars", "official_news", "cninfo_announcements"],
        metadata={
            "label": label,
            "regime": regime,
            "policy": policy,
            "benchmark_code": benchmark.code,
            "benchmark_ret_5d": benchmark.ret_5d,
            "benchmark_ret_20d": benchmark.ret_20d,
            "breadth_score": breadth,
            "leadership_count": leadership_count,
            "breakdown_count": breakdown_count,
        },
    )


def build_holdings_actions(
    *,
    as_of: str,
    holdings: HoldingsSnapshot,
    snapshot: AdviceSnapshot,
    config: dict[str, Any],
) -> list[StructuredDecision]:
    rows: list[StructuredDecision] = []
    for position in holdings.positions:
        evaluation = evaluate_security(
            snapshot,
            config=config,
            code=position.code,
            name=position.name,
            category="holding",
            question_type="add_position",
            horizon="swing",
            risk_profile="balanced",
            pdf_payload=None,
            allow_supplemental_refresh=False,
            fetch_announcements=False,
        )
        action, reasons, risks = holding_action_from_evaluation(evaluation)
        rows.append(
            StructuredDecision(
                object_type="holding",
                object_id=position.code,
                object_name=position.name,
                at=as_of,
                action=action,
                score=round(evaluation.scorecard.total_score, 2),
                probability=evaluation.confidence,
                reason=reasons,
                risk=risks,
                thesis=evaluation.thesis,
                counterpoints=evaluation.counter_evidence[:3],
                trigger_conditions=evaluation.trigger_conditions[:3],
                invalidation_conditions=evaluation.invalidation_conditions[:3],
                priority_score=evaluation.action_plan.urgency_score,
                sources=_sources_from_evaluation(evaluation),
                metadata={
                    "sector": evaluation.sector or position.sector,
                    "market_value": position.market_value,
                    "decision": evaluation.decision,
                    "coverage_score": round(evaluation.scorecard.coverage_score, 2),
                    "positive_factors": list(evaluation.positive_factors[:3]),
                    "negative_factors": list(evaluation.negative_factors[:3]),
                    "factor_analysis": dict(evaluation.factor_analysis),
                    "position_context": dict(evaluation.position_context),
                    "action_plan": evaluation.action_plan.to_dict(),
                    "position_guidance": evaluation.action_plan.position_guidance,
                    "prediction_bundle": dict(evaluation.prediction_bundle),
                },
            )
        )
    rows.sort(key=lambda item: (item.priority_score, item.score), reverse=True)
    return rows


def build_watchlist(
    *,
    as_of: str,
    holdings: HoldingsSnapshot,
    snapshot: AdviceSnapshot,
    config: dict[str, Any],
) -> list[StructuredDecision]:
    holding_codes = {item.code for item in holdings.positions}
    market_view = snapshot.decision_bundle.get("market_view") or {}
    strategy_style = infer_market_strategy_style(
        action=str(market_view.get("action") or ""),
        regime=str((market_view.get("metadata") or {}).get("regime") or ""),
    )
    candidates = [
        feature
        for feature in (InstrumentFeatures(**item) if isinstance(item, dict) else item for item in snapshot.feature_map.values())
        if feature.code not in holding_codes and feature.category != "benchmark"
    ]
    watchlist: list[StructuredDecision] = []
    for feature in candidates:
        evaluation = evaluate_security(
            snapshot,
            config=config,
            code=feature.code,
            name=feature.name,
            category=feature.category,
            question_type="should_buy",
            horizon="swing",
            risk_profile="balanced",
            strategy_style=strategy_style,
            pdf_payload=None,
            allow_supplemental_refresh=False,
            fetch_announcements=False,
        )
        action, reasons, risks = watch_action_from_evaluation(evaluation)
        style_fit_score = candidate_style_fit(evaluation, strategy_style=strategy_style)
        selection_score = candidate_selection_score(evaluation, strategy_style=strategy_style, feature=asdict(feature))
        watchlist.append(
            StructuredDecision(
                object_type="watch",
                object_id=feature.code,
                object_name=feature.name,
                at=as_of,
                action=action,
                score=evaluation.scorecard.total_score,
                probability=evaluation.confidence,
                reason=reasons,
                risk=risks,
                thesis=evaluation.thesis,
                counterpoints=evaluation.counter_evidence[:3],
                trigger_conditions=evaluation.trigger_conditions[:3],
                invalidation_conditions=evaluation.invalidation_conditions[:3],
                priority_score=selection_score,
                sources=_sources_from_evaluation(evaluation),
                metadata={
                    "category": feature.category,
                    "decision": evaluation.decision,
                    "sector": evaluation.sector,
                    "strategy_style": strategy_style,
                    "strategy_label": evaluation.strategy_profile.label,
                    "style_fit_score": round(style_fit_score, 2),
                    "selection_score": round(selection_score, 2),
                    "coverage_score": round(evaluation.scorecard.coverage_score, 2),
                    "positive_factors": list(evaluation.positive_factors[:3]),
                    "negative_factors": list(evaluation.negative_factors[:3]),
                    "factor_analysis": dict(evaluation.factor_analysis),
                    "position_context": dict(evaluation.position_context),
                    "action_plan": evaluation.action_plan.to_dict(),
                    "position_guidance": evaluation.action_plan.position_guidance,
                    "prediction_bundle": dict(evaluation.prediction_bundle),
                },
            )
        )
    watchlist.sort(
        key=lambda item: (
            float((item.metadata or {}).get("selection_score") or item.priority_score),
            float((item.metadata or {}).get("style_fit_score") or 0.0),
            item.score,
        ),
        reverse=True,
    )
    annotate_watchlist_alternatives(watchlist)
    return watchlist[:4]


def build_monitor_plan(
    *,
    as_of: str,
    market_view: StructuredDecision,
    holdings_actions: list[StructuredDecision],
    watchlist: list[StructuredDecision],
    feature_map: dict[str, InstrumentFeatures],
) -> list[StructuredDecision]:
    rows: list[StructuredDecision] = []
    benchmark_codes = ["000300", "000001", "399006"]
    for code in benchmark_codes:
        feature = feature_map.get(code)
        if feature is None:
            continue
        rows.append(
            StructuredDecision(
                object_type="monitor",
                object_id=feature.code,
                object_name=feature.name,
                at=as_of,
                action="monitor_index",
                score=feature.trend_score,
                probability=market_view.probability,
                reason=[f"作为风格和系统性风险锚点，跟踪 {feature.name}。"],
                risk=["指数转弱会降低个股告警价值。"],
                sources=["daily_bars"],
                metadata={
                    "category": "benchmark",
                    "thresholds": {
                        "price_jump_threshold_5m": 0.015,
                        "price_drop_threshold_5m": -0.015,
                        "relative_strength_threshold_5m": 0.01,
                    },
                },
            )
        )
    for item in holdings_actions[:5]:
        rows.append(
            StructuredDecision(
                object_type="monitor",
                object_id=item.object_id,
                object_name=item.object_name,
                at=as_of,
                action="monitor_holding",
                score=item.score,
                probability=item.probability,
                reason=[f"持仓动作 `{item.action}`，盘中需要验证是否符合预期。"] + item.reason[:1],
                risk=item.risk[:1],
                sources=item.sources,
                metadata={
                    "category": "holding",
                    "parent_action": item.action,
                    "thresholds": {
                        "price_jump_threshold_5m": 0.02,
                        "price_drop_threshold_5m": -0.02,
                        "relative_strength_threshold_5m": 0.012,
                    },
                },
            )
        )
    for item in watchlist[:4]:
        rows.append(
            StructuredDecision(
                object_type="monitor",
                object_id=item.object_id,
                object_name=item.object_name,
                at=as_of,
                action="monitor_watch",
                score=item.score,
                probability=item.probability,
                reason=[f"观察动作 `{item.action}`，盘中只在出现确认信号时提醒。"] + item.reason[:1],
                risk=item.risk[:1],
                sources=item.sources,
                metadata={
                    "category": "watch",
                    "parent_action": item.action,
                    "thresholds": {
                        "price_jump_threshold_5m": 0.018,
                        "price_drop_threshold_5m": -0.015,
                        "relative_strength_threshold_5m": 0.014,
                    },
                },
            )
        )
    return rows


def build_action_summary_line(
    market_view: StructuredDecision,
    holdings_actions: list[StructuredDecision],
    watchlist: list[StructuredDecision],
) -> str:
    regime = str(market_view.metadata.get("regime") or market_view.metadata.get("label") or market_view.action)
    priorities = build_priority_actions(holdings_actions=holdings_actions, watchlist=watchlist)
    if priorities:
        headlines = "；".join(item["headline"] for item in priorities[:2])
        return f"{regime}，今天先处理：{headlines}。"
    return f"{regime}，先看指数和强势股是否给出新的执行信号。"


def _build_shared_snapshot(
    *,
    as_of: str,
    holdings: HoldingsSnapshot,
    universe: list[UniverseItem],
    feature_map: dict[str, InstrumentFeatures],
    series_map: dict[str, DailySeriesSnapshot],
    market_view: StructuredDecision,
    news_items: list[NewsItem],
    announcements: list[AnnouncementItem],
    supplemental: dict[str, dict[str, Any]],
    sector_map: dict[str, str],
    sector_metrics: dict[str, dict[str, Any]],
) -> AdviceSnapshot:
    explicit_sector_map = {position.code: str(position.sector or "") for position in holdings.positions if position.sector}
    explicit_sector_map.update({str(code): str(label) for code, label in (sector_map or {}).items()})
    name_map = {item.code: item.name for item in universe}
    name_map.update({code: feature.name for code, feature in feature_map.items()})
    for position in holdings.positions:
        name_map[position.code] = position.name
    return AdviceSnapshot(
        as_of=as_of,
        state_root=None,
        holdings=holdings.to_dict(),
        universe=[{"code": item.code, "name": item.name, "category": item.category} for item in universe],
        feature_map={code: feature.to_dict() for code, feature in feature_map.items()},
        series_map={code: series.to_dict() for code, series in series_map.items()},
        decision_bundle={"market_view": market_view.to_dict()},
        news_items=[item.to_dict() for item in news_items],
        announcements=[item.to_dict() for item in announcements],
        supplemental=supplemental or {},
        explicit_sector_map=explicit_sector_map,
        sector_metrics=sector_metrics or {},
        name_map=name_map,
    )


def _sources_from_evaluation(evaluation: Any) -> list[str]:
    sources: list[str] = []
    for item in evaluation.evidence_used:
        source = str(item.source or "").strip()
        if not source or source in sources:
            continue
        sources.append(source)
    return sources or ["shared_decision_core"]


def build_homepage_payload(
    *,
    as_of: str,
    market_view: StructuredDecision,
    holdings_actions: list[StructuredDecision],
    watchlist: list[StructuredDecision],
    action_summary: str,
    feature_map: dict[str, InstrumentFeatures],
) -> dict[str, Any]:
    current_prices = []
    ordered_codes: list[tuple[str, str]] = []
    priority_actions = build_priority_actions(holdings_actions=holdings_actions, watchlist=watchlist)
    for item in priority_actions[:3]:
        ordered_codes.append((str(item["code"]), str(item["name"])))
    for item in holdings_actions[:3]:
        ordered_codes.append((item.object_id, item.object_name))
    for item in watchlist[:2]:
        ordered_codes.append((item.object_id, item.object_name))
    seen: set[str] = set()
    for code, name in ordered_codes:
        if code in seen:
            continue
        seen.add(code)
        feature = feature_map.get(code)
        if feature is None:
            continue
        current_prices.append(
            {
                "code": code,
                "name": name,
                "last_price": f"{feature.last_close:.3f}".rstrip("0").rstrip("."),
                "ret_day": format_pct(feature.ret_1d),
                "timestamp": f"{feature.as_of} close",
                "freshness": "eod",
            }
        )
    return {
        "as_of": as_of,
        "today_action": action_summary,
        "market_state": {
            "label": market_view.metadata.get("label"),
            "regime": market_view.metadata.get("regime"),
            "score": market_view.score,
            "probability": market_view.probability,
            "baseline_regime": market_view.metadata.get("regime"),
            "baseline_score": market_view.score,
            "baseline_probability": market_view.probability,
            "summary": market_view.thesis or (market_view.reason[0] if market_view.reason else ""),
            "policy": list(market_view.metadata.get("policy") or []),
        },
        "market_label": market_view.metadata.get("label"),
        "market_probability": market_view.probability,
        "price_mode": "reference_close",
        "price_section_title": "参考收盘价（非实时）",
        "price_note": "未运行实时会话；以下为最近可用收盘价，仅供盘前或离线参考。",
        "current_prices": current_prices,
        "latest_alerts": [],
        "priority_actions": priority_actions[:3],
        "holdings_risks": [serialize_homepage_decision(item) for item in holdings_actions if is_risk_action(item.action)][:3],
        "watch_opportunities": [serialize_homepage_decision(item) for item in watchlist if is_opportunity_action(item.action)][:4],
        "holdings_actions": [
            serialize_homepage_decision(item)
            for item in holdings_actions
        ],
        "watchlist": [
            serialize_homepage_decision(item)
            for item in watchlist
        ],
        "predictions": _build_predictions_list(holdings_actions, watchlist),
    }


def build_priority_actions(*, holdings_actions: list[StructuredDecision], watchlist: list[StructuredDecision]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in holdings_actions:
        if is_risk_action(item.action):
            headline = f"先处理 {item.object_name} 的{action_label(item)}"
            bucket = "holding_risk"
            priority = item.priority_score + 8.0
        elif is_supportive_holding_action(item.action):
            headline = f"确认 {item.object_name} 是否具备{action_label(item)}条件"
            bucket = "holding_follow_up"
            priority = item.priority_score
        else:
            continue
        rows.append(priority_row(item=item, bucket=bucket, headline=headline, priority=priority))
    for item in watchlist:
        if not is_opportunity_action(item.action):
            continue
        headline = f"观察 {item.object_name} 的{action_label(item)}触发"
        rows.append(priority_row(item=item, bucket="watch_opportunity", headline=headline, priority=item.priority_score))
    rows.sort(key=lambda row: (float(row["priority_score"]), float(row["score"])), reverse=True)
    return rows


def priority_row(*, item: StructuredDecision, bucket: str, headline: str, priority: float) -> dict[str, Any]:
    action_plan = item.metadata.get("action_plan") or {}
    row = {
        "bucket": bucket,
        "headline": headline,
        "code": item.object_id,
        "name": item.object_name,
        "action": item.action,
        "action_label": action_label(item),
        "priority_score": round(priority, 2),
        "score": round(item.score, 2),
        "reason": item.reason[0] if item.reason else item.thesis,
        "thesis": item.thesis,
        "trigger": item.trigger_conditions[0] if item.trigger_conditions else "",
        "invalidation": item.invalidation_conditions[0] if item.invalidation_conditions else "",
        "strategy_style": str(item.metadata.get("strategy_style") or ""),
        "strategy_label": str(item.metadata.get("strategy_label") or ""),
        "positive_factors": list((item.metadata or {}).get("positive_factors") or item.reason[1:3]),
        "negative_factors": list((item.metadata or {}).get("negative_factors") or item.risk[:2]),
        "counterpoints": list(item.counterpoints or item.risk[:2]),
        "risk": list(item.risk[:2]),
        "coverage_score": (item.metadata or {}).get("coverage_score"),
        "position_context": dict((item.metadata or {}).get("position_context") or {}),
        "preferred_alternative": dict((item.metadata or {}).get("preferred_alternative") or {}),
        "position_guidance": str(action_plan.get("position_guidance") or item.metadata.get("position_guidance") or ""),
        "levels": dict(action_plan.get("levels") or {}),
    }
    return enrich_display_fields(row)


def serialize_homepage_decision(item: StructuredDecision) -> dict[str, Any]:
    action_plan = item.metadata.get("action_plan") or {}
    row = {
        "code": item.object_id,
        "name": item.object_name,
        "action": item.action,
        "action_label": action_label(item),
        "score": round(item.score, 2),
        "priority_score": round(item.priority_score, 2),
        "reason": item.reason[0] if item.reason else "",
        "thesis": item.thesis,
        "trigger": item.trigger_conditions[0] if item.trigger_conditions else "",
        "invalidation": item.invalidation_conditions[0] if item.invalidation_conditions else "",
        "strategy_style": str(item.metadata.get("strategy_style") or ""),
        "strategy_label": str(item.metadata.get("strategy_label") or ""),
        "positive_factors": list((item.metadata or {}).get("positive_factors") or item.reason[1:3]),
        "negative_factors": list((item.metadata or {}).get("negative_factors") or item.risk[:2]),
        "counterpoints": list(item.counterpoints or item.risk[:2]),
        "risk": list(item.risk[:2]),
        "coverage_score": (item.metadata or {}).get("coverage_score"),
        "position_context": dict((item.metadata or {}).get("position_context") or {}),
        "preferred_alternative": dict((item.metadata or {}).get("preferred_alternative") or {}),
        "position_guidance": str(action_plan.get("position_guidance") or item.metadata.get("position_guidance") or ""),
        "levels": dict(action_plan.get("levels") or {}),
        # Include prediction bundle if available (set by evaluate_security)
        "prediction_bundle": dict((item.metadata or {}).get("prediction_bundle") or {}),
    }
    return enrich_display_fields(row)


def is_risk_action(action: str) -> bool:
    return action in {"cut_on_breakdown", "trim_into_strength"}


def is_supportive_holding_action(action: str) -> bool:
    return action in {"add_on_strength", "buy_on_pullback", "hold_no_add", "hold_core_wait_market", "hold_core"}


def is_opportunity_action(action: str) -> bool:
    return action in {
        "standard_position",
        "trial_position",
        "wait_for_pullback",
        "wait_for_breakout",
        "watch_market_turn",
        "watch_only",
        "switch_to_better_alternative",
        "stay_out",
    }


def _build_predictions_list(
    holdings_actions: list[StructuredDecision],
    watchlist: list[StructuredDecision],
) -> list[dict[str, Any]]:
    """Extract prediction bundles from all decisions for homepage display."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in list(holdings_actions)[:3] + list(watchlist)[:4]:
        code = item.object_id
        if code in seen:
            continue
        seen.add(code)
        bundle = (item.metadata or {}).get("prediction_bundle") or {}
        if not bundle:
            continue
        rows.append({
            "code": code,
            "name": item.object_name,
            "bundle": bundle,
        })
    return rows


def annotate_watchlist_alternatives(watchlist: list[StructuredDecision]) -> None:
    if len(watchlist) < 2:
        return
    overall_leader = watchlist[0]
    sector_leaders: dict[str, StructuredDecision] = {}
    for item in watchlist:
        sector = str((item.metadata or {}).get("sector") or "").strip()
        if sector and sector not in sector_leaders:
            sector_leaders[sector] = item
    for item in watchlist[1:]:
        if item.action not in {"watch_only", "watch_market_turn", "wait_for_pullback", "wait_for_breakout", "stay_out"}:
            continue
        sector = str((item.metadata or {}).get("sector") or "").strip()
        if item.action in {"watch_only", "watch_market_turn", "stay_out"}:
            alternative = overall_leader
            min_gap = 10.0
        else:
            alternative = sector_leaders.get(sector) if sector and sector_leaders.get(sector) is not item else overall_leader
            min_gap = 12.0
        if alternative is None or alternative is item:
            continue
        gap = float((alternative.metadata or {}).get("selection_score") or alternative.priority_score) - float(
            (item.metadata or {}).get("selection_score") or item.priority_score
        )
        if gap < min_gap:
            continue
        promote_better_alternative(item=item, alternative=alternative, gap=gap)


def promote_better_alternative(*, item: StructuredDecision, alternative: StructuredDecision, gap: float) -> None:
    action_plan = dict(item.metadata.get("action_plan") or {})
    alternative_label = action_label(alternative)
    action_plan["action"] = "switch_to_better_alternative"
    action_plan["label"] = "替代标的"
    action_plan["rationale"] = f"{item.object_name} 当前不是最优资金去处，优先看 {alternative.object_name}。"
    action_plan["position_guidance"] = f"不给 {item.object_name} 分配新仓，候选额度优先留给 {alternative.object_name}。"
    action_plan["urgency_score"] = round(max(float(action_plan.get("urgency_score") or item.priority_score), 66.0), 2)
    existing_triggers = list(action_plan.get("trigger_conditions") or [])
    existing_invalidations = list(action_plan.get("invalidation_conditions") or [])
    action_plan["trigger_conditions"] = [f"若同梯队必须选一只，先等 {alternative.object_name} 满足“{alternative_label}”条件。"] + existing_triggers[:1]
    action_plan["invalidation_conditions"] = [f"只有当 {item.object_name} 重新强于 {alternative.object_name}，才恢复优先级。"] + existing_invalidations[:1]
    item.action = "switch_to_better_alternative"
    item.priority_score = min(item.priority_score, max(alternative.priority_score - 6.0, 0.0))
    item.reason = [
        str(action_plan["rationale"]),
        f"同梯队更强的是 {alternative.object_name}，当前优先级领先 {gap:.1f} 分。",
    ]
    item.thesis = f"{item.object_name} 不是完全不能做，但当前执行位和资金效率都弱于 {alternative.object_name}。"
    item.counterpoints = [f"与 {alternative.object_name} 相比，当前更缺少马上执行的理由。"] + list(item.counterpoints[:2])
    item.trigger_conditions = list(action_plan["trigger_conditions"][:3])
    item.invalidation_conditions = list(action_plan["invalidation_conditions"][:3])
    item.metadata["preferred_alternative"] = {
        "code": alternative.object_id,
        "name": alternative.object_name,
        "action_label": alternative_label,
        "priority_gap": round(gap, 2),
    }
    item.metadata["action_plan"] = action_plan
    item.metadata["position_guidance"] = str(action_plan["position_guidance"])


def action_label(item: StructuredDecision) -> str:
    action_plan = item.metadata.get("action_plan") or {}
    return str(action_plan.get("label") or item.action)


def write_daily_outputs(
    *,
    output_dir: str | Path,
    bundle: DailyDecisionBundle,
    holdings: HoldingsSnapshot,
    feature_map: dict[str, InstrumentFeatures],
    news_items: list[NewsItem],
    announcements: list[AnnouncementItem],
) -> dict[str, Path]:
    root = ensure_dir(output_dir)
    as_of = bundle.as_of
    daily_report_path = root / f"{as_of}_daily_report.md"
    daily_report_path.write_text(
        render_daily_report(bundle=bundle, holdings=holdings, news_items=news_items, announcements=announcements),
        encoding="utf-8",
    )
    comprehensive_report_path = root / f"{as_of}_comprehensive_report.md"
    comprehensive_report_path.write_text(
        render_comprehensive_report(bundle=bundle, holdings=holdings, feature_map=feature_map, news_items=news_items, announcements=announcements),
        encoding="utf-8",
    )
    integrated_report_path = root / f"{as_of}_integrated_report.md"
    integrated_report_path.write_text(
        render_integrated_report(bundle=bundle, holdings=holdings, feature_map=feature_map, news_items=news_items, announcements=announcements),
        encoding="utf-8",
    )
    action_summary_path = root / f"{as_of}_action_summary.md"
    action_summary_path.write_text(render_action_summary(bundle), encoding="utf-8")
    homepage_path = root / f"{as_of}_homepage_overview.md"
    homepage_path.write_text(render_homepage_overview(bundle.homepage_overview), encoding="utf-8")
    return {
        "daily_report": daily_report_path,
        "comprehensive_report": comprehensive_report_path,
        "integrated_report": integrated_report_path,
        "action_summary": action_summary_path,
        "homepage_overview": homepage_path,
        "action_summary_json": write_json(root / f"{as_of}_action_summary.json", bundle.to_dict()),
        "homepage_overview_json": write_json(root / f"{as_of}_homepage_overview.json", bundle.homepage_overview),
        "monitor_plan_json": write_json(root / f"{as_of}_monitor_plan.json", [item.to_dict() for item in bundle.monitor_plan]),
        "feature_snapshot_json": write_json(root / f"{as_of}_features.json", {code: feature.to_dict() for code, feature in feature_map.items()}),
    }


def news_sentiment_score(items: list[NewsItem]) -> float:
    score = 0.0
    for item in items[:8]:
        if any(keyword in item.title for keyword in POSITIVE_KEYWORDS):
            score += 0.01
        if any(keyword in item.title for keyword in NEGATIVE_KEYWORDS):
            score -= 0.01
    return clamp(score, -0.05, 0.05)


def announcement_risk_score(items: list[AnnouncementItem]) -> float:
    score = 0.0
    for item in items[:8]:
        if any(keyword in item.title for keyword in POSITIVE_KEYWORDS):
            score += 0.01
        if any(keyword in item.title for keyword in NEGATIVE_KEYWORDS):
            score -= 0.015
    return clamp(score, -0.08, 0.04)


def format_pct(value: float | None) -> str:
    return "无" if value is None else f"{value:+.2%}"


def format_ratio(value: float | None) -> str:
    return "无" if value is None else f"{value:.2f}x"
