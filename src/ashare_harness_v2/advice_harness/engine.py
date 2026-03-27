from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from ..decision_core import (
    SecurityEvaluation,
    build_strategy_profile,
    build_summary,
    build_thesis,
    candidate_selection_score,
    candidate_style_fit,
    evaluate_security,
)
from ..skill_harness import analyze_announcement_pdfs, build_investment_report, build_portfolio_candidate_workbook
from ..utils import ensure_dir, load_json, normalize_whitespace, write_json
from .evidence import (
    build_advice_snapshot,
    ensure_security_announcements,
    extend_snapshot_with_cached_candidates,
    maybe_enrich_snapshot_with_live_supplemental,
    maybe_enrich_names,
    resolve_security_from_query,
)
from .planner import build_execution_plan
from .query_parser import parse_user_query
from .schemas import ActionPlan, AdviceDecision, CandidateIdea


def answer_user_query(
    config: dict[str, Any],
    *,
    question: str,
    as_of: str,
    holdings_file: str | None = None,
    cash: float = 0.0,
    discovery_limit: int = 5,
    write_output: bool = False,
    allow_live_enrich: bool = True,
) -> dict[str, Any]:
    snapshot = build_advice_snapshot(config, as_of=as_of, holdings_file=holdings_file, cash=cash)
    parsed = parse_user_query(question)
    security = resolve_security_from_query(snapshot, symbol_hint=parsed.symbol_hint, name_hint=parsed.stock_name_hint)
    if security is None:
        return {
            "as_of": as_of,
            "question": parsed.to_dict(),
            "decision": "need_symbol",
            "summary": "问题里缺少明确股票名称或代码，当前无法判断该不该买。",
            "missing_information": ["请提供 6 位代码或股票名称。"],
            "better_candidates": discover_top_ideas(config, as_of=as_of, limit=discovery_limit, write_output=False)["ideas"],
        }

    effective_query = _normalize_query_for_security(parsed, security)

    if write_output or allow_live_enrich:
        ensure_security_announcements(snapshot, code=str(security["code"]), name=str(security["name"]))
    pdf_payload = _load_or_build_pdf_payload(
        config,
        as_of=as_of,
        code=str(security["code"]),
        announcements=[item for item in snapshot.announcements if str(item.get("code") or "") == str(security["code"])],
        write_output=write_output,
    )
    evaluation = evaluate_security(
        snapshot,
        config=config,
        code=str(security["code"]),
        name=str(security["name"]),
        category=str(security.get("category") or "query"),
        question_type=effective_query.question_type,
        horizon=effective_query.horizon,
        risk_profile=effective_query.risk_profile,
        strategy_style=effective_query.strategy_style,
        pdf_payload=pdf_payload,
        allow_supplemental_refresh=allow_live_enrich,
        fetch_announcements=allow_live_enrich,
    )
    plan = build_execution_plan(effective_query, sector_available=not evaluation.using_sector_proxy, supplemental_available=True)

    better_candidates = _better_candidates(
        config,
        evaluation=evaluation,
        current_code=evaluation.code,
        as_of=as_of,
        limit=discovery_limit,
        strategy_style=effective_query.strategy_style,
    )
    action_plan = _apply_better_alternative_hint(evaluation=evaluation, better_candidates=better_candidates)
    strategy_profile = build_strategy_profile(
        strategy_style=effective_query.strategy_style,
        action_plan=action_plan,
        scorecard=evaluation.scorecard,
        question_type=effective_query.question_type,
        security_name=evaluation.name,
        position_context=evaluation.position_context,
    )
    summary = build_summary(
        security_name=evaluation.name,
        action_label=action_plan.label,
        score=evaluation.scorecard.total_score,
        market_score=evaluation.scorecard.market_score,
        coverage_score=evaluation.scorecard.coverage_score,
    )
    thesis = build_thesis(
        security_name=evaluation.name,
        action_plan=action_plan,
        positives=evaluation.positive_factors,
        negatives=evaluation.negative_factors,
    )

    artifacts: dict[str, str] = {}
    if write_output:
        report_paths = build_investment_report(
            config,
            as_of=as_of,
            evaluation=_evaluation_payload(
                evaluation,
                action_plan=action_plan,
                strategy_profile=strategy_profile,
                summary=summary,
                thesis=thesis,
            ),
            market_view=snapshot.decision_bundle.get("market_view") or {},
            series_payload=snapshot.series_map.get(evaluation.code) or {},
            better_candidates=[item.to_dict() for item in better_candidates],
            pdf_payload=pdf_payload,
        )
        artifacts.update(report_paths)

    advice = AdviceDecision(
        as_of=as_of,
        question=effective_query,
        security={
            "code": evaluation.code,
            "name": evaluation.name,
            "category": evaluation.category,
            "sector": evaluation.sector,
        },
        plan=plan,
        decision=evaluation.decision,
        confidence=evaluation.confidence,
        summary=summary,
        thesis=thesis,
        scorecard=evaluation.scorecard,
        positive_factors=evaluation.positive_factors,
        negative_factors=evaluation.negative_factors,
        counter_evidence=evaluation.counter_evidence,
        missing_information=evaluation.missing_information,
        next_checks=evaluation.next_checks,
        trigger_conditions=list(action_plan.trigger_conditions),
        invalidation_conditions=list(action_plan.invalidation_conditions),
        action_plan=action_plan,
        strategy_profile=strategy_profile,
        factor_analysis=dict(evaluation.factor_analysis),
        position_guidance=action_plan.position_guidance,
        evidence_used=evaluation.evidence_used,
        better_candidates=better_candidates,
        position_context=evaluation.position_context,
        pdf_insights=evaluation.pdf_insights,
        artifacts=artifacts,
    )
    payload = advice.to_dict()
    if write_output:
        write_json(_advice_output_dir(config, as_of=as_of) / f"{evaluation.code}_advice.json", payload)
    return payload


def _normalize_query_for_security(parsed: Any, security: dict[str, Any]) -> Any:
    category = str(security.get("category") or "")
    if category != "holding":
        return parsed
    if getattr(parsed, "question_type", "") == "add_position":
        return parsed
    return replace(
        parsed,
        question_type="add_position",
        has_position_hint=True if parsed.has_position_hint is None else parsed.has_position_hint,
    )


def discover_top_ideas(
    config: dict[str, Any],
    *,
    as_of: str,
    limit: int = 5,
    strategy_style: str = "general",
    holdings_file: str | None = None,
    cash: float = 0.0,
    write_output: bool = False,
) -> dict[str, Any]:
    snapshot = build_advice_snapshot(config, as_of=as_of, holdings_file=holdings_file, cash=cash)
    min_candidate_pool = max(limit * 3, 12)
    current_candidate_pool = len(_eligible_discovery_features(snapshot))
    if current_candidate_pool < min_candidate_pool:
        extend_snapshot_with_cached_candidates(
            snapshot,
            config=config,
            limit=max(min_candidate_pool - current_candidate_pool, limit * 2),
        )
    candidates = _build_discovery_candidates(snapshot, config=config, strategy_style=strategy_style)
    if write_output:
        enrichment_limit = int((config.get("supplemental") or {}).get("discovery_enrich_limit", max(limit * 2, 8)))
        maybe_enrich_snapshot_with_live_supplemental(
            snapshot,
            config=config,
            codes=[item.code for item in candidates[:enrichment_limit]],
        )
        candidates = _build_discovery_candidates(snapshot, config=config, strategy_style=strategy_style)
    maybe_enrich_names(snapshot, codes=[item.code for item in candidates[: limit * 2]])
    for item in candidates[: limit * 2]:
        previous_name = item.name
        item.name = str(snapshot.name_map.get(item.code) or item.name)
        if item.name != previous_name:
            item.summary = item.summary.replace(previous_name, item.name, 1)
            item.thesis = item.thesis.replace(previous_name, item.name, 1)
            if item.action_plan is not None:
                item.action_plan.rationale = item.action_plan.rationale.replace(previous_name, item.name)
                item.action_plan.position_guidance = item.action_plan.position_guidance.replace(previous_name, item.name)
    payload = {
        "as_of": as_of,
        "idea_count": len(candidates),
        "ideas": [item.to_dict() for item in candidates[:limit]],
        "market_view": snapshot.decision_bundle.get("market_view"),
        "artifacts": {},
    }
    if write_output:
        write_json(_advice_output_dir(config, as_of=as_of) / "discovery.json", payload)
        workbook_paths = build_portfolio_candidate_workbook(
            config,
            as_of=as_of,
            market_view=snapshot.decision_bundle.get("market_view") or {},
            holdings_rows=_evaluate_holdings(snapshot, config=config),
            candidate_rows=[item.to_dict() for item in candidates[:limit]],
        )
        payload["artifacts"] = workbook_paths
        write_json(_advice_output_dir(config, as_of=as_of) / "discovery.json", payload)
    return payload


def decision_rank(decision: str) -> int:
    order = {"add": 4, "buy": 3, "hold": 2, "watch": 2, "insufficient_evidence": 1, "trim": 0, "avoid": 0}
    return order.get(decision, 0)


def _eligible_discovery_features(snapshot: Any) -> list[dict[str, Any]]:
    holding_codes = {
        str(position.get("code") or "")
        for position in (snapshot.holdings.get("positions") or [])
        if isinstance(position, dict)
    }
    rows: list[dict[str, Any]] = []
    for feature in snapshot.feature_map.values():
        code = str((feature or {}).get("code") or "")
        if not code or code in {"000300", "510300", "000001", "399006"} or code in holding_codes:
            continue
        rows.append(feature)
    return rows


def _build_discovery_candidates(snapshot: Any, *, config: dict[str, Any], strategy_style: str) -> list[CandidateIdea]:
    candidates: list[CandidateIdea] = []
    for feature in _eligible_discovery_features(snapshot):
        code = str(feature.get("code") or "")
        evaluation = evaluate_security(
            snapshot,
            config=config,
            code=code,
            name=str(snapshot.name_map.get(code) or feature.get("name") or code),
            category="watch",
            question_type="should_buy",
            horizon="swing",
            risk_profile="balanced",
            strategy_style=strategy_style,
            pdf_payload=None,
            allow_supplemental_refresh=False,
            fetch_announcements=False,
        )
        style_fit_score = candidate_style_fit(evaluation, strategy_style=strategy_style)
        selection_score = candidate_selection_score(evaluation, strategy_style=strategy_style, feature=feature)
        candidates.append(
            CandidateIdea(
                code=evaluation.code,
                name=evaluation.name,
                decision=evaluation.decision,
                trade_action=evaluation.action_plan.label,
                total_score=evaluation.scorecard.total_score,
                coverage_score=evaluation.scorecard.coverage_score,
                market_score=evaluation.scorecard.market_score,
                summary=evaluation.summary,
                thesis=evaluation.thesis,
                catalysts=evaluation.positive_factors[:3],
                risks=evaluation.negative_factors[:3],
                trigger_conditions=evaluation.trigger_conditions[:3],
                invalidation_conditions=evaluation.invalidation_conditions[:3],
                position_guidance=evaluation.action_plan.position_guidance,
                priority_score=round(selection_score, 2),
                action_plan=evaluation.action_plan,
                metadata={
                    "sector": evaluation.sector,
                    "sector_proxy": evaluation.using_sector_proxy,
                    "strategy_label": evaluation.strategy_profile.label,
                    "style_fit_score": round(style_fit_score, 2),
                    "selection_score": round(selection_score, 2),
                    "strategy_style": strategy_style,
                    "factor_analysis": dict(evaluation.factor_analysis),
                },
            )
        )
    candidates.sort(
        key=lambda item: (
            float((item.metadata or {}).get("selection_score") or item.priority_score),
            float((item.metadata or {}).get("style_fit_score") or 0.0),
            decision_rank(item.decision),
            item.total_score,
            item.coverage_score,
        ),
        reverse=True,
    )
    return candidates


def _evaluate_holdings(snapshot: Any, *, config: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position in (snapshot.holdings.get("positions") or []):
        evaluation = evaluate_security(
            snapshot,
            config=config,
            code=str(position.get("code") or ""),
            name=str(position.get("name") or position.get("code") or ""),
            category="holding",
            question_type="add_position",
            horizon="swing",
            risk_profile="balanced",
            strategy_style="general",
            pdf_payload=None,
            allow_supplemental_refresh=False,
            fetch_announcements=False,
        )
        row = _evaluation_payload(evaluation)
        row["action"] = evaluation.action_plan.label
        rows.append(row)
    rows.sort(
        key=lambda item: (
            float((item.get("action_plan") or {}).get("urgency_score") or 0.0),
            float((item.get("scorecard") or {}).get("total_score") or 0.0),
        ),
        reverse=True,
    )
    return rows


def _better_candidates(
    config: dict[str, Any],
    *,
    evaluation: SecurityEvaluation,
    current_code: str,
    as_of: str,
    limit: int,
    strategy_style: str,
) -> list[CandidateIdea]:
    discovery = discover_top_ideas(config, as_of=as_of, limit=limit + 3, strategy_style=strategy_style, write_output=False)
    rows: list[CandidateIdea] = []
    current_selection_score = candidate_selection_score(evaluation, strategy_style=strategy_style)
    for item in discovery["ideas"]:
        if str(item.get("code") or "") == current_code:
            continue
        selection_score = float((item.get("metadata") or {}).get("selection_score") or item.get("total_score") or 0.0)
        if selection_score <= current_selection_score:
            continue
        payload = dict(item)
        if isinstance(payload.get("action_plan"), dict):
            payload["action_plan"] = ActionPlan(**payload["action_plan"])
        rows.append(CandidateIdea(**payload))
        if len(rows) >= limit:
            break
    return rows


def _load_or_build_pdf_payload(
    config: dict[str, Any],
    *,
    as_of: str,
    code: str,
    announcements: list[dict[str, Any]],
    write_output: bool,
) -> dict[str, Any] | None:
    pdf_root = Path(config["project"].get("pdf_dir") or "data/output/pdf") / as_of / code
    payload_path = pdf_root / f"{code}_announcement_insights.json"
    cached = load_json(payload_path, default=None)
    if cached and (cached.get("insights") or []):
        return _normalize_cached_pdf_payload(cached)
    if not write_output:
        return _normalize_cached_pdf_payload(cached) if cached else cached
    built = analyze_announcement_pdfs(config, as_of=as_of, code=code, announcements=announcements)
    return _normalize_cached_pdf_payload(built)


def _evaluation_payload(
    evaluation: SecurityEvaluation,
    *,
    action_plan: ActionPlan | None = None,
    strategy_profile: dict[str, Any] | Any | None = None,
    summary: str | None = None,
    thesis: str | None = None,
) -> dict[str, Any]:
    final_action_plan = action_plan or evaluation.action_plan
    final_strategy_profile = strategy_profile or evaluation.strategy_profile
    return {
        "security": {
            "code": evaluation.code,
            "name": evaluation.name,
            "category": evaluation.category,
            "sector": evaluation.sector,
        },
        "code": evaluation.code,
        "name": evaluation.name,
        "decision": evaluation.decision,
        "confidence": evaluation.confidence,
        "summary": summary or evaluation.summary,
        "thesis": thesis or evaluation.thesis,
        "scorecard": evaluation.scorecard.to_dict(),
        "positive_factors": list(evaluation.positive_factors),
        "negative_factors": list(evaluation.negative_factors),
        "counter_evidence": list(evaluation.counter_evidence),
        "missing_information": list(evaluation.missing_information),
        "next_checks": list(evaluation.next_checks),
        "trigger_conditions": list(final_action_plan.trigger_conditions),
        "invalidation_conditions": list(final_action_plan.invalidation_conditions),
        "action_plan": final_action_plan.to_dict(),
        "strategy_profile": final_strategy_profile.to_dict() if hasattr(final_strategy_profile, "to_dict") else dict(final_strategy_profile),
        "factor_analysis": dict(evaluation.factor_analysis),
        "sector": evaluation.sector,
        "position_context": dict(evaluation.position_context),
        "evidence_sources": sorted({str(item.source or "").strip() for item in evaluation.evidence_used if str(item.source or "").strip()}),
        "evidence_highlights": [str(item.summary) for item in evaluation.evidence_used[:4] if str(item.summary or "").strip()],
    }


def _apply_better_alternative_hint(*, evaluation: SecurityEvaluation, better_candidates: list[CandidateIdea]):
    action_plan = evaluation.action_plan
    if evaluation.position_context.get("is_holding") or not better_candidates:
        return action_plan
    top_candidate = better_candidates[0]
    if top_candidate.total_score < evaluation.scorecard.total_score + 5:
        return action_plan
    if action_plan.action not in {"stay_out", "watch_only", "watch_market_turn", "wait_for_pullback"}:
        return action_plan
    return action_plan.__class__(
        action="switch_to_better_alternative",
        label="替代更优标的",
        rationale=f"{evaluation.name} 当前不是最优资金去处，优先关注 {top_candidate.name}。",
        position_guidance=f"当前不分配仓位给 {evaluation.name}，把候选额度留给 {top_candidate.name}。",
        urgency=action_plan.urgency,
        urgency_score=max(action_plan.urgency_score, 66.0),
        trigger_conditions=[f"若必须在同类标的里选一只，优先看 {top_candidate.name} 的触发条件。"] + list(action_plan.trigger_conditions[:1]),
        invalidation_conditions=[f"只有当 {evaluation.name} 重新强于 {top_candidate.name}，才恢复优先级。"] + list(action_plan.invalidation_conditions[:1]),
        do_not=list(action_plan.do_not),
        monitoring_focus=[f"对比 {evaluation.name} 与 {top_candidate.name} 的相对强弱。"] + list(action_plan.monitoring_focus[:1]),
    )


def _normalize_cached_pdf_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not payload:
        return payload
    rows = []
    for item in payload.get("insights") or []:
        title = str(item.get("title") or "")
        published_at = str(item.get("published_at") or payload.get("as_of") or "")
        key_lines = [_compact_pdf_line(line) for line in (item.get("key_lines") or []) if str(line).strip()]
        signal = str(item.get("signal") or "neutral")
        strength = float(item.get("strength") or 52.0)
        if "综合授信" in title and signal == "negative":
            signal = "neutral"
            strength = 52.0
        takeaway = _pdf_takeaway(signal=signal, title=title, key_lines=key_lines)
        summary = f"{published_at} PDF 深读：{title}。提炼结论：{takeaway}"
        rows.append({**item, "signal": signal, "strength": strength, "summary": summary, "key_lines": key_lines[:4]})
    return {**payload, "insights": rows}


def _compact_pdf_line(value: Any, *, limit: int = 56) -> str:
    compact = normalize_whitespace(str(value or "")).replace("□", "").replace("■", "")
    for token in ("本公司董事会", "以下简称", "详见", "特此公告", "网站", "www.", "http://", "https://"):
        compact = compact.replace(token, "")
    compact = normalize_whitespace(compact)
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "…"


def _pdf_takeaway(*, signal: str, title: str, key_lines: list[str]) -> str:
    text = " ".join(key_lines).lower()
    if signal == "positive":
        if any(token in text for token in ("订单", "中标", "回购", "增持", "增长", "扩产")):
            return "公告释放了偏正面的经营催化，但仍需等价格与量能确认。"
        return "公告信息偏正面，可作为加分项，但不足以单独触发交易。"
    if signal == "negative":
        if any(token in text for token in ("诉讼", "处罚", "问询", "违规", "调查")):
            return "监管或法律风险上升，需收缩预期并跟踪后续披露。"
        if any(token in text for token in ("减持", "质押")):
            return "股东层面信号偏负面，需防范估值与流动性压力。"
        if any(token in text for token in ("不确定", "风险提示", "审批", "进展", "投建周期")):
            return "关键事项推进存在不确定性，执行上应降低仓位和胜率预期。"
        if any(token in text for token in ("亏损", "下滑", "下降")):
            return "经营指标存在走弱信号，短期基本面承压。"
        return "公告中包含风险措辞，当前应保持防守式执行。"
    if any(token in title for token in ("综合授信", "董事会工作报告", "股东大会决议", "投资者关系活动记录表")):
        return "本公告以常规披露为主，未形成明确交易催化。"
    return "本公告信息中性，主要用于更新事实背景，不改变当前交易计划。"


def _advice_output_dir(config: dict[str, Any], *, as_of: str) -> Path:
    advice_dir = Path(config["project"].get("advice_dir", "data/output/advice"))
    return ensure_dir(advice_dir / as_of)
