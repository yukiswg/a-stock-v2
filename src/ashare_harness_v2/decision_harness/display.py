from __future__ import annotations

from typing import Any


ACTION_DISPLAY_LABELS = {
    "cut_on_breakdown": "先别加仓，跌破再减一点",
    "trim_into_strength": "反弹时先减一点",
    "buy_on_pullback": "等回踩稳住再加",
    "add_on_strength": "站稳关键价再加",
    "hold_no_add": "继续拿着，先别加仓",
    "hold_core_wait_market": "先拿着，等市场好转",
    "hold_core": "继续拿着",
    "standard_position": "确认后再买一点",
    "trial_position": "先小仓试一下",
    "wait_for_pullback": "先等回踩再看",
    "wait_for_breakout": "先观察，站上再看",
    "watch_market_turn": "先等市场回暖",
    "watch_only": "先观察",
    "switch_to_better_alternative": "先看更强的替代标的",
    "stay_out": "先别买",
}

ACTION_BUCKET_LABELS = {
    "cut_on_breakdown": "破位减仓",
    "trim_into_strength": "逢高减仓",
    "buy_on_pullback": "等回踩",
    "add_on_strength": "突破跟随",
    "hold_no_add": "观察",
    "hold_core_wait_market": "观察",
    "hold_core": "观察",
    "standard_position": "正常仓",
    "trial_position": "试仓",
    "wait_for_pullback": "等回踩",
    "wait_for_breakout": "突破跟随",
    "watch_market_turn": "观察",
    "watch_only": "观察",
    "switch_to_better_alternative": "替代标的",
    "stay_out": "不参与",
}


def enrich_display_fields(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    action = str(data.get("action") or "")
    levels = data.get("levels") or {}
    fallback_label = str(data.get("action_label") or action)
    data.setdefault("technical_action_label", fallback_label)
    data["display_action_label"] = user_facing_action_label(action, fallback=fallback_label)
    data["canonical_action_label"] = canonical_action_label(action, fallback=fallback_label)
    data["display_reason"] = user_facing_reason(
        action=action,
        name=str(data.get("name") or data.get("code") or "该标的"),
        levels=levels,
        reason=str(data.get("reason") or data.get("thesis") or ""),
    )
    data["display_trigger"] = user_facing_trigger(
        action=action,
        levels=levels,
        fallback=str(data.get("trigger") or ""),
    )
    data["display_invalidation"] = user_facing_invalidation(
        action=action,
        levels=levels,
        fallback=str(data.get("invalidation") or ""),
    )
    analysis = build_analysis_snapshot(data)
    data["analysis_summary"] = analysis["summary"]
    data["analysis_core_reason"] = analysis["core_reason"]
    data["analysis_risk"] = analysis["risk_counter_case"]
    data["analysis_position"] = analysis["position_text"]
    data["analysis_comparison"] = analysis["comparison_text"]
    data["analysis_setup"] = analysis["setup_text"]
    data["analysis_evidence_balance"] = analysis["evidence_balance_text"]
    data["analysis_execution_window"] = analysis["execution_window_text"]
    data["analysis_capital_action"] = analysis["capital_action_text"]
    data["analysis_evidence_quality"] = analysis["evidence_quality_text"]
    data["analysis_blockers"] = analysis["blocker_text"]
    data["analysis_execution_triplet"] = analysis["execution_triplet_text"]
    data["analysis_factor_profile"] = analysis["factor_profile_text"]
    data["analysis_factor_attribution"] = analysis["factor_attribution_text"]
    return data


def canonical_action_label(action: str, *, fallback: str) -> str:
    return ACTION_BUCKET_LABELS.get(action, fallback)


def user_facing_action_label(action: str, *, fallback: str) -> str:
    if fallback and fallback.strip() and fallback.strip() != action:
        return fallback
    return ACTION_DISPLAY_LABELS.get(action, fallback)


def user_facing_reason(*, action: str, name: str, levels: dict[str, Any], reason: str) -> str:
    support = _pick_level(levels, "support_price")
    breakout = _pick_level(levels, "breakout_price")
    pullback = _pick_level(levels, "pullback_price")
    recovery = _pick_level(levels, "retest_price", "pullback_price", "breakout_price")
    if action == "cut_on_breakdown":
        parts = [f"{name} 现在先别加仓。"]
        if support:
            parts.append(f"重点盯住 {support} 这条防线，跌破再减一点。")
        else:
            parts.append("如果继续走弱，就先减一点，别硬扛。")
        if recovery:
            parts.append(f"只有重新回到 {recovery} 上方，才先改回观察。")
        return " ".join(parts)
    if action == "trim_into_strength":
        target = _pick_level(levels, "retest_price", "breakout_price")
        if target:
            return f"{name} 现在不是追着加的时候。 如果反弹到 {target} 附近但量能跟不上，先减一点。"
        return f"{name} 现在不适合追高，反弹时优先减一点。"
    if action == "buy_on_pullback":
        entry = pullback or support
        if entry and support:
            return f"{name} 现在不用追高。 只有回踩到 {entry} 一带稳住，且不跌破 {support}，才考虑小幅加仓。"
        if entry:
            return f"{name} 现在不用追高。 只有回踩到 {entry} 一带稳住，才考虑小幅加仓。"
        return f"{name} 现在先别追，等它回踩稳住再考虑加仓。"
    if action == "add_on_strength":
        target = breakout or recovery
        if target:
            return f"{name} 现在先别急着加。 只有站上 {target} 且量能跟上，才考虑加一点。"
        return f"{name} 现在先别急着加，等走势真正转强再说。"
    if action == "hold_no_add":
        return f"{name} 现在先拿着，但不主动加仓。"
    if action == "hold_core_wait_market":
        return f"{name} 现在先拿着，等市场整体好转后再看要不要加。"
    if action == "hold_core":
        return f"{name} 现在先继续拿着，不需要立刻动作。"
    if action == "standard_position":
        target = breakout or support
        if target:
            return f"{name} 如果要买，只能等关键价 {target} 附近出现确认，不要提前冲进去。"
        return f"{name} 还没到直接出手的时候，先等确认。"
    if action == "trial_position":
        entry = pullback or support or breakout
        if entry:
            return f"{name} 如果要试，只能用小仓位，并在 {entry} 附近确认承接后再动手。"
        return f"{name} 只能先小仓试，不适合重仓。"
    if action == "wait_for_pullback":
        entry = pullback or support
        if entry:
            return f"{name} 现在先观察，不追高。 只有回踩到 {entry} 一带稳住了再考虑。"
        return f"{name} 现在先观察，不追高，等回踩确认。"
    if action == "wait_for_breakout":
        if breakout:
            return f"{name} 现在先观察。 只有站上关键价 {breakout} 且量能放大，才考虑买。"
        return f"{name} 现在先观察，只有走势真正突破时再考虑。"
    if action == "watch_market_turn":
        return f"{name} 现在先观察市场，不急着动手。"
    if action == "watch_only":
        return f"{name} 现在先观察，不急着交易。"
    if action == "switch_to_better_alternative":
        return f"{name} 不是当前最优先的选择，先看更强的替代标的。"
    if action == "stay_out":
        return f"{name} 现在先别碰。"
    return reason or f"{name} 当前先保持观察。"


def user_facing_trigger(*, action: str, levels: dict[str, Any], fallback: str) -> str:
    support = _pick_level(levels, "support_price")
    breakout = _pick_level(levels, "breakout_price")
    pullback = _pick_level(levels, "pullback_price")
    recovery = _pick_level(levels, "pullback_price", "retest_price", "breakout_price")
    if action == "cut_on_breakdown" and support:
        return f"只有跌破 {support}，才执行减一点。"
    if action == "trim_into_strength":
        target = _pick_level(levels, "retest_price", "breakout_price")
        if target:
            return f"如果反弹到 {target} 附近但量能跟不上，就先减一点。"
    if action == "buy_on_pullback" and (pullback or support):
        return f"只在 {(pullback or support)} 一带稳住时再加。"
    if action == "add_on_strength" and (breakout or recovery):
        return f"只在站上 {(breakout or recovery)} 且量能放大时再加。"
    if action == "wait_for_pullback" and (pullback or support):
        return f"只在 {(pullback or support)} 一带稳住时再考虑。"
    if action in {"wait_for_breakout", "standard_position", "trial_position"} and breakout:
        return f"只在站上 {breakout} 且量能放大时再考虑。"
    if action in {"hold_no_add", "hold_core_wait_market", "hold_core", "watch_market_turn", "watch_only"}:
        return "现在先观察，不主动加仓。"
    return fallback or "先观察，等条件满足再执行。"


def user_facing_invalidation(*, action: str, levels: dict[str, Any], fallback: str) -> str:
    support = _pick_level(levels, "support_price")
    breakout = _pick_level(levels, "breakout_price")
    pullback = _pick_level(levels, "pullback_price")
    recovery = _pick_level(levels, "retest_price", "pullback_price", "breakout_price")
    if action == "cut_on_breakdown" and recovery:
        return f"如果重新回到 {recovery} 上方，就先别继续减。"
    if action == "trim_into_strength" and (breakout or recovery):
        return f"如果直接站稳 {(breakout or recovery)}，就暂停减仓。"
    if action in {"buy_on_pullback", "wait_for_pullback"} and support:
        return f"如果跌破 {support}，这次计划就取消。"
    if action == "add_on_strength" and support:
        return f"如果站上后又跌回 {support} 下方，就取消加仓。"
    if action in {"wait_for_breakout", "standard_position", "trial_position"}:
        if support:
            return f"如果没站上关键价前先跌破 {support}，就继续观察。"
        return "如果没有确认突破，就继续观察。"
    return fallback or "条件不成立时，先不执行。"


def build_analysis_snapshot(row: dict[str, Any]) -> dict[str, str]:
    action = str(row.get("action") or "")
    name = str(row.get("name") or row.get("code") or "该标的")
    levels = row.get("levels") or {}
    positives = _as_list(row.get("positive_factors")) or _as_list(row.get("reason"))
    counterpoints = _as_list(row.get("counterpoints")) or _as_list(row.get("risk"))
    market_regime = str(row.get("market_regime") or "").strip()
    preferred_alternative = row.get("preferred_alternative") or {}
    factor_analysis = row.get("factor_analysis") or {}
    breakout = _pick_level(levels, "breakout_price")
    support = _pick_level(levels, "support_price")
    pullback = _pick_level(levels, "pullback_price")
    action_bucket = canonical_action_label(action, fallback=str(row.get("action_label") or action or "观察"))
    market_note = f"{market_regime}下，" if market_regime else ""
    main_signal = positives[0] if positives else str(row.get("thesis") or row.get("reason") or "当前缺少更强的正向优势。")
    second_signal = positives[1] if len(positives) > 1 else ""
    counter_case = "；".join(counterpoints[:2] or ["当前缺少明确反证，但仍需按触发条件执行。"])
    if action == "cut_on_breakdown":
        price_note = f"执行位在 {support} 一带，重点是先守住风控。" if support else "当前先把风控放在反弹预期前面。"
        summary = f"{market_note}{name}先做风险处理，不把反弹当默认剧本。"
        core_reason = _join_parts([summary, main_signal, price_note])
    elif action == "trim_into_strength":
        price_note = f"如果反弹到 {(_pick_level(levels, 'retest_price', 'breakout_price') or '关键阻力')} 一带但量能跟不上，只把反弹当减仓窗口。"
        summary = f"{market_note}{name}反弹只用于降风险，不用于追回仓位。"
        core_reason = _join_parts([summary, main_signal, price_note])
    elif action in {"wait_for_breakout", "add_on_strength", "standard_position"}:
        trigger_price = breakout or pullback or support or "关键价"
        summary = f"{market_note}{name}不是因为涨了才买，而是等 {trigger_price} 的确认再执行。"
        core_reason = _join_parts([summary, main_signal, second_signal])
    elif action in {"wait_for_pullback", "buy_on_pullback", "trial_position"}:
        anchor = pullback or support or breakout or "更舒服的回踩位"
        summary = f"{market_note}{name}逻辑可以继续跟，但更优赔率在 {anchor}，不在当前位置追价。"
        core_reason = _join_parts([summary, main_signal, second_signal])
    elif action == "switch_to_better_alternative":
        alternative_name = str(preferred_alternative.get("name") or "更强标的")
        summary = f"{market_note}{name}不是完全没逻辑，但当前资金效率不如 {alternative_name}。"
        core_reason = _join_parts([summary, f"同阶段先把候选额度留给 {alternative_name}。", main_signal])
    elif action == "stay_out":
        summary = f"{market_note}{name}当前胜率和赔率都不够，不值得占用仓位预算。"
        core_reason = _join_parts([summary, main_signal])
    else:
        monitor_note = f"先看 {support or breakout or pullback or '关键位'} 附近是否出现新的确认信号。"
        summary = f"{market_note}{name}当前以观察为主，暂时没有必须立刻执行的理由。"
        core_reason = _join_parts([summary, main_signal, monitor_note])
    position_text = build_position_note(
        action=action,
        default_text=str(row.get("position_guidance") or ""),
        position_context=row.get("position_context") or {},
    )
    trigger_conditions = _as_list(row.get("trigger_conditions"))
    invalidation_conditions = _as_list(row.get("invalidation_conditions"))
    setup_text = build_setup_note(levels=levels, action=action)
    evidence_balance_text = build_evidence_balance_note(
        positives=positives,
        counterpoints=counterpoints,
        triggers=trigger_conditions,
    )
    execution_window_text = build_execution_window_note(
        action=action,
        triggers=trigger_conditions,
        invalidations=invalidation_conditions,
    )
    capital_action_text = build_capital_action_note(action=action, position_context=row.get("position_context") or {})
    evidence_quality_text = build_evidence_quality_note(
        coverage_score=_coverage_score_from_row(row),
        missing_information=_as_list(row.get("missing_information")),
    )
    blocker_text = build_blocker_note(
        blockers=_as_list(row.get("blockers")) or _as_list((row.get("action_plan") or {}).get("blockers")),
        action=action,
        coverage_score=_coverage_score_from_row(row),
    )
    execution_triplet_text = build_execution_triplet_note(
        execution_brief=_as_list(row.get("execution_brief")) or _as_list((row.get("action_plan") or {}).get("execution_brief")),
        action=action,
        triggers=trigger_conditions,
        invalidations=invalidation_conditions,
    )
    factor_profile_text = build_factor_profile_note(factor_analysis=factor_analysis)
    factor_attribution_text = build_factor_attribution_note(factor_analysis=factor_analysis)
    comparison_text = ""
    if preferred_alternative:
        alternative_name = str(preferred_alternative.get("name") or preferred_alternative.get("code") or "").strip()
        if alternative_name:
            gap = preferred_alternative.get("priority_gap")
            gap_note = f"，当前优先级高出 {float(gap):.1f} 分" if isinstance(gap, (int, float)) else ""
            comparison_text = f"同梯队更值得先看的标的是 {alternative_name}{gap_note}。"
    return {
        "summary": summary,
        "core_reason": core_reason,
        "risk_counter_case": counter_case,
        "position_text": position_text,
        "comparison_text": comparison_text,
        "action_bucket": action_bucket,
        "setup_text": setup_text,
        "evidence_balance_text": evidence_balance_text,
        "execution_window_text": execution_window_text,
        "capital_action_text": capital_action_text,
        "evidence_quality_text": evidence_quality_text,
        "blocker_text": blocker_text,
        "execution_triplet_text": execution_triplet_text,
        "factor_profile_text": factor_profile_text,
        "factor_attribution_text": factor_attribution_text,
    }


def build_setup_note(*, levels: dict[str, Any], action: str) -> str:
    box_position = float(levels.get("box_position_pct") or 0.0)
    reward_risk = float(levels.get("entry_reward_risk") or 0.0)
    upside_pct = float(levels.get("entry_upside_pct") or 0.0)
    downside_pct = float(levels.get("entry_downside_pct") or 0.0)
    if box_position <= 20:
        zone = "靠近支撑"
    elif box_position <= 40:
        zone = "偏下沿"
    elif box_position <= 65:
        zone = "箱体中段"
    elif box_position <= 85:
        zone = "偏上沿"
    else:
        zone = "接近突破/高位"
    action_bias = {
        "cut_on_breakdown": "先看防守",
        "trim_into_strength": "反弹偏减仓",
        "buy_on_pullback": "回踩偏加仓",
        "add_on_strength": "突破偏加仓",
        "trial_position": "小仓试错",
        "standard_position": "等待确认",
        "wait_for_pullback": "等更优赔率",
        "wait_for_breakout": "等量价确认",
    }.get(action, "先观察")
    return (
        f"位置 `{zone}` | 上行空间 `{upside_pct:.2f}%` | 下行风险 `{downside_pct:.2f}%` | "
        f"赔率 `{reward_risk:.2f}` | 当前策略 `{action_bias}`"
    )


def build_evidence_balance_note(
    *,
    positives: list[str],
    counterpoints: list[str],
    triggers: list[str],
) -> str:
    positive = positives[0] if positives else "当前缺少更强的正向证据。"
    risk = counterpoints[0] if counterpoints else "当前没有更强的反证，但仍需按触发条件执行。"
    verify = triggers[0] if triggers else "等待下一次量价确认。"
    return f"多头主证据: {positive} | 主要风险: {risk} | 下一验证: {verify}"


def build_execution_window_note(
    *,
    action: str,
    triggers: list[str],
    invalidations: list[str],
) -> str:
    trigger_text = triggers[0] if triggers else "条件未触发前不执行。"
    invalidation_text = invalidations[0] if invalidations else "若结构被破坏，先撤回计划。"
    mode = "持仓处理" if action in {"cut_on_breakdown", "trim_into_strength", "hold_no_add", "hold_core", "hold_core_wait_market"} else "新开/加仓"
    return f"{mode}窗口: {trigger_text} | 失效线: {invalidation_text}"


def build_capital_action_note(*, action: str, position_context: dict[str, Any]) -> str:
    is_holding = bool(position_context.get("is_holding")) if isinstance(position_context, dict) else False
    if action in {"cut_on_breakdown", "trim_into_strength", "stay_out", "watch_only", "watch_market_turn"}:
        return "现在不适合加仓，优先看风险控制或继续观察。"
    if action in {"hold_no_add", "hold_core_wait_market"}:
        return "当前以持有观察为主，不主动加仓。"
    if action == "hold_core":
        return "可以继续持有，但没有新的主动加仓优势。"
    if action in {"wait_for_pullback", "wait_for_breakout", "standard_position", "trial_position"}:
        prefix = "这是条件式加仓/开仓信号" if not is_holding else "这是条件式加仓信号"
        return f"{prefix}，未触发前不动手。"
    if action in {"buy_on_pullback", "add_on_strength"}:
        return "只有触发条件成立后，才适合小幅加仓。"
    return "先按触发条件执行，不做主观加仓。"


def build_evidence_quality_note(*, coverage_score: float, missing_information: list[str]) -> str:
    missing_count = len(missing_information)
    if coverage_score < 40:
        return f"证据完整度偏低（覆盖度 {coverage_score:.1f}），当前结论按防守口径理解；仍缺 {missing_count} 项关键信息。"
    if coverage_score < 60:
        return f"证据完整度一般（覆盖度 {coverage_score:.1f}），下决定前最好再补一轮验证；仍缺 {missing_count} 项信息。"
    return f"证据完整度尚可（覆盖度 {coverage_score:.1f}），但仍需按触发条件执行。"


def build_blocker_note(*, blockers: list[str], action: str, coverage_score: float) -> str:
    if blockers:
        return " | ".join(blockers[:3])
    if coverage_score < 45:
        return "证据覆盖不足，当前结论只能按防守口径理解。"
    if action in {"hold_no_add", "hold_core_wait_market", "hold_core", "watch_only", "watch_market_turn"}:
        return "当前没有足够强的市场、位置或赔率优势去支持主动出手。"
    return "当前没有额外阻断因素，但仍必须等触发条件。"


def build_execution_triplet_note(
    *,
    execution_brief: list[str],
    action: str,
    triggers: list[str],
    invalidations: list[str],
) -> str:
    if len(execution_brief) >= 3:
        return f"现在: {execution_brief[0]} | 升级: {execution_brief[1]} | 取消: {execution_brief[2]}"
    now_text = "现在先观察，不主动交易。"
    if action in {"cut_on_breakdown", "trim_into_strength", "stay_out"}:
        now_text = "现在先做风险控制，不做主动加仓。"
    elif action in {"hold_no_add", "hold_core_wait_market", "hold_core"}:
        now_text = "现在先持有，不主动加仓。"
    upgrade_text = triggers[0] if triggers else "只有触发条件成立，才升级动作。"
    cancel_text = invalidations[0] if invalidations else "若结构失效，当前计划自动取消。"
    return f"现在: {now_text} | 升级: {upgrade_text} | 取消: {cancel_text}"


def build_factor_profile_note(*, factor_analysis: dict[str, Any]) -> str:
    if not isinstance(factor_analysis, dict) or not factor_analysis:
        return "当前未形成稳定的因子画像，先按价格和证据链执行。"
    style_box = str(factor_analysis.get("style_box") or "均衡")
    dominant_factor = str(factor_analysis.get("dominant_factor") or "未识别")
    provider = str(factor_analysis.get("provider") or "builtin_fallback")
    overlay = factor_analysis.get("selection_overlay") or {}
    provider_label = "jqfactor 适配" if provider == "jqfactor_analyzer_adapted" else "内建归因"
    pieces = [f"风格箱 `{style_box}`", f"主导因子 `{dominant_factor}`", f"来源 `{provider_label}`"]
    if isinstance(overlay.get("rps_proxy_20d"), (int, float)):
        pieces.append(f"RPS代理 `{float(overlay['rps_proxy_20d']):.1f}`")
    if isinstance(overlay.get("trend_template_score"), (int, float)):
        pieces.append(f"趋势模板 `{float(overlay['trend_template_score']):.1f}`")
    return " | ".join(pieces)


def build_factor_attribution_note(*, factor_analysis: dict[str, Any]) -> str:
    if not isinstance(factor_analysis, dict) or not factor_analysis:
        return "当前没有可用的因子归因。"
    summary = str(factor_analysis.get("factor_summary") or "").strip()
    focus = _as_list(factor_analysis.get("factor_focus"))
    attribution = factor_analysis.get("attribution") or {}
    headline = str(attribution.get("headline") or "").strip()
    if summary and focus:
        if headline:
            return f"{summary} {headline}。重点跟踪: {' | '.join(focus[:2])}"
        return f"{summary} 重点跟踪: {' | '.join(focus[:2])}"
    if summary:
        if headline:
            return f"{summary} {headline}。"
        return summary
    factors = factor_analysis.get("factors") or []
    rows = []
    for item in factors[:3]:
        if not isinstance(item, dict):
            continue
        rows.append(f"{item.get('name') or '未知'}({float(item.get('contribution') or 0.0):+.1f})")
    return "主要因子贡献: " + "、".join(rows) if rows else "当前没有可用的因子归因。"


def _coverage_score_from_row(row: dict[str, Any]) -> float:
    value = row.get("coverage_score")
    if isinstance(value, (int, float)):
        return float(value)
    scorecard = row.get("scorecard") or {}
    coverage = scorecard.get("coverage_score")
    return float(coverage) if isinstance(coverage, (int, float)) else 0.0


def build_position_note(*, action: str, default_text: str, position_context: dict[str, Any]) -> str:
    if not isinstance(position_context, dict) or not position_context:
        return default_text or "按纪律控制仓位。"
    if not position_context.get("is_holding"):
        return default_text or "先从小仓位试错，不抢一次到位。"
    weight = float(position_context.get("position_weight") or 0.0)
    sector_weight = float(position_context.get("sector_weight") or 0.0)
    notes = []
    if weight > 0:
        notes.append(f"当前单票约占仓位 {weight:.1%}")
    if sector_weight > 0:
        notes.append(f"同主题合计约 {sector_weight:.1%}")
    if not position_context.get("position_pricing_complete", True):
        notes.append("成本价或最新价缺失，只给比例级建议")
    if not position_context.get("position_size_complete", True):
        notes.append("数量或可卖数量缺失，不下具体手数")
    if default_text:
        notes.insert(0, default_text)
    return "；".join(notes or ["按纪律控制仓位。"])


def _as_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value.strip() else []
    if not isinstance(value, (list, tuple)):
        return []
    rows: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            rows.append(text)
    return rows


def _join_parts(parts: list[str]) -> str:
    rows = [str(item or "").strip() for item in parts if str(item or "").strip()]
    return " ".join(rows) if rows else "待补充"


def _pick_level(levels: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = levels.get(key)
        if isinstance(value, (int, float)) and float(value) > 0:
            return _format_price(float(value))
    return None


def _format_price(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")
