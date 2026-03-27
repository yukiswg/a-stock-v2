from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "matplotlib"))

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from ..decision_harness.display import build_analysis_snapshot, canonical_action_label
from ..utils import ensure_dir, write_json, write_text


def build_investment_report(
    config: dict[str, Any],
    *,
    as_of: str,
    evaluation: dict[str, Any],
    market_view: dict[str, Any],
    series_payload: dict[str, Any] | None,
    better_candidates: list[dict[str, Any]],
    pdf_payload: dict[str, Any] | None = None,
) -> dict[str, str]:
    code = str((evaluation.get("security") or {}).get("code") or evaluation.get("code") or "unknown")
    root = ensure_dir(Path(config["project"]["report_dir"]) / as_of / "advice")
    chart_dir = ensure_dir(root / "charts")
    price_chart = _build_price_chart(chart_dir=chart_dir, code=code, series_payload=series_payload)
    momentum_chart = _build_momentum_chart(chart_dir=chart_dir, code=code, series_payload=series_payload)
    markdown_path = root / f"{code}_investment_report.md"
    json_path = root / f"{code}_investment_report.json"
    markdown = render_investment_report(
        as_of=as_of,
        evaluation=evaluation,
        market_view=market_view,
        better_candidates=better_candidates,
        pdf_payload=pdf_payload,
        price_chart=price_chart,
        momentum_chart=momentum_chart,
    )
    write_text(markdown_path, markdown)
    write_json(
        json_path,
        {
            "as_of": as_of,
            "evaluation": evaluation,
            "market_view": market_view,
            "better_candidates": better_candidates,
            "pdf_payload": pdf_payload,
            "charts": {"price_chart": price_chart, "momentum_chart": momentum_chart},
        },
    )
    return {"markdown": str(markdown_path), "json": str(json_path), "price_chart": price_chart, "momentum_chart": momentum_chart}


def render_investment_report(
    *,
    as_of: str,
    evaluation: dict[str, Any],
    market_view: dict[str, Any],
    better_candidates: list[dict[str, Any]],
    pdf_payload: dict[str, Any] | None,
    price_chart: str | None,
    momentum_chart: str | None,
) -> str:
    security = evaluation.get("security") or {}
    scorecard = evaluation.get("scorecard") or {}
    action_plan = evaluation.get("action_plan") or {}
    evidence_sources = evaluation.get("evidence_sources") or []
    evidence_highlights = evaluation.get("evidence_highlights") or []
    position_context = evaluation.get("position_context") or {}
    preferred_alternative = better_candidates[0] if better_candidates else {}
    analysis = build_analysis_snapshot(
        {
            "action": action_plan.get("action") or "",
            "action_label": action_plan.get("label") or evaluation.get("decision") or "无",
            "name": security.get("name") or security.get("code") or "该标的",
            "thesis": evaluation.get("thesis") or evaluation.get("summary") or "",
            "reason": list(evaluation.get("positive_factors") or []),
            "positive_factors": list(evaluation.get("positive_factors") or []),
            "counterpoints": list(evaluation.get("counter_evidence") or []),
            "risk": list(evaluation.get("negative_factors") or []),
            "position_context": dict(position_context),
            "position_guidance": action_plan.get("position_guidance") or "",
            "preferred_alternative": preferred_alternative,
            "levels": dict(action_plan.get("levels") or {}),
            "coverage_score": (evaluation.get("scorecard") or {}).get("coverage_score"),
            "factor_analysis": dict(evaluation.get("factor_analysis") or {}),
            "missing_information": list(evaluation.get("missing_information") or []),
            "blockers": list(action_plan.get("blockers") or []),
            "execution_brief": list(action_plan.get("execution_brief") or []),
            "trigger_conditions": list(action_plan.get("trigger_conditions") or []),
            "invalidation_conditions": list(action_plan.get("invalidation_conditions") or []),
        }
    )
    action_bucket = canonical_action_label(
        str(action_plan.get("action") or ""),
        fallback=str(action_plan.get("label") or evaluation.get("decision") or "无"),
    )
    lines = [
        f"# {security.get('name') or security.get('code')} 个股分析",
        "",
        f"- 截止日期: `{as_of}`",
        f"- 当前结论: `{action_plan.get('label') or evaluation.get('decision') or '无'}`",
        f"- 动作族: `{action_bucket}`",
        f"- 置信度: `{evaluation.get('confidence')}`",
        f"- 一句话结论: {evaluation.get('summary')}",
        f"- 核心理由: {analysis['core_reason']}",
        f"- 因子画像: {analysis['factor_profile_text']}",
        f"- 因子归因: {analysis['factor_attribution_text']}",
        f"- 加仓判断: {analysis['capital_action_text']}",
        f"- 证据完整度: {analysis['evidence_quality_text']}",
        "",
        "## 执行卡片",
        "",
        f"- 当前结论: `{action_plan.get('label') or evaluation.get('decision') or '无'}`",
        f"- 动作族: `{action_bucket}`",
        f"- 核心理由: {analysis['core_reason']}",
        f"- 位置与赔率: {analysis['setup_text']}",
        f"- 证据平衡: {analysis['evidence_balance_text']}",
        f"- 因子画像: {analysis['factor_profile_text']}",
        f"- 因子归因: {analysis['factor_attribution_text']}",
        f"- 证据完整度: {analysis['evidence_quality_text']}",
        f"- 核心阻断因素: {analysis['blocker_text']}",
        f"- 加仓判断: {analysis['capital_action_text']}",
        f"- 执行前提: {analysis['execution_window_text']}",
        f"- 执行三段式: {analysis['execution_triplet_text']}",
        f"- 风险/反证: {analysis['risk_counter_case']}",
        f"- 触发条件: {((action_plan.get('trigger_conditions') or ['待补充']))[0]}",
        f"- 失效条件: {((action_plan.get('invalidation_conditions') or ['待补充']))[0]}",
        f"- 仓位建议: {analysis['position_text']}",
        *( [f"- 替代标的: {analysis['comparison_text']}"] if analysis.get("comparison_text") else [] ),
        "",
        "## 评分拆解",
        "",
        f"- 市场: `{scorecard.get('market_score')}`",
        f"- 行业: `{scorecard.get('sector_score')}`",
        f"- 个股: `{scorecard.get('stock_score')}`",
        f"- 时机: `{scorecard.get('timing_score')}`",
        f"- 风险扣分: `{scorecard.get('risk_penalty')}`",
        f"- 覆盖度: `{scorecard.get('coverage_score')}`",
        f"- 总分: `{scorecard.get('total_score')}`",
        "",
        "## 因子解释与归因",
        "",
        f"- 风格画像: {analysis['factor_profile_text']}",
        f"- 归因摘要: {analysis['factor_attribution_text']}",
        "",
        "## 市场上下文",
        "",
    ]
    for item in (market_view.get("reason") or [])[:3]:
        lines.append(f"- {item}")
    lines.extend(["", "## 正向因素", ""])
    for item in evaluation.get("positive_factors") or ["暂无明显正面催化。"]:
        lines.append(f"- {item}")
    lines.extend(["", "## 风险因素", ""])
    for item in evaluation.get("negative_factors") or ["暂无明确负面。"]:
        lines.append(f"- {item}")
    if evaluation.get("counter_evidence"):
        lines.extend(["", "## 反证", ""])
        for item in evaluation.get("counter_evidence") or []:
            lines.append(f"- {item}")
    lines.extend(["", "## 证据来源", ""])
    if evidence_sources:
        lines.append(f"- 来源: {', '.join(evidence_sources[:6])}")
    for item in evidence_highlights or ["当前未提炼出额外证据摘要。"]:
        lines.append(f"- {item}")
    strategy_profile = evaluation.get("strategy_profile") or {}
    if strategy_profile:
        lines.extend(["", "## 策略约束", ""])
        lines.append(f"- 风格: `{strategy_profile.get('label')}`")
        lines.append(f"- 执行政策: {strategy_profile.get('policy_summary')}")
        lines.append(f"- 市场覆盖: {strategy_profile.get('regime_overlay')}")
        for item in strategy_profile.get("checklist") or []:
            lines.append(f"- 检查项: {item}")
        for item in strategy_profile.get("do_not") or []:
            lines.append(f"- 不要做: {item}")
    if position_context:
        lines.extend(["", "## 持仓上下文", ""])
        lines.append(f"- 是否持仓: `{position_context.get('is_holding')}`")
        lines.append(f"- 持仓权重: `{position_context.get('position_weight')}` | 主题权重 `{position_context.get('sector_weight')}`")
        lines.append(f"- 成本价: `{position_context.get('cost_price')}` | 最新价 `{position_context.get('last_price')}`")
    if evaluation.get("missing_information"):
        lines.extend(["", "## 缺失信息", ""])
        for item in evaluation.get("missing_information") or []:
            lines.append(f"- {item}")
    if pdf_payload and (pdf_payload.get("insights") or []):
        lines.extend(["", "## PDF 深读", ""])
        for item in pdf_payload.get("insights")[:2]:
            lines.append(f"- `{item.get('signal')}` {item.get('summary')}")
            for detail in (item.get("key_lines") or [])[:2]:
                lines.append(f"  - {detail}")
    if better_candidates:
        lines.extend(["", "## 替代标的", ""])
        for item in better_candidates[:5]:
            lines.append(f"- {item.get('name')}({item.get('code')}): {item.get('summary')}")
    if price_chart:
        lines.extend(["", "## 图表", "", f"![Price Chart]({price_chart})"])
    if momentum_chart:
        lines.append(f"![Momentum Chart]({momentum_chart})")
    lines.extend(
        [
            "",
            "## 说明",
            "",
            "- 本报告来自 harness 评分系统与补充数据，不等于个性化投资建议。",
            "- 如果持仓字段不完整，仓位和加减仓建议应按降级模式理解。",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_price_chart(*, chart_dir: Path, code: str, series_payload: dict[str, Any] | None) -> str | None:
    bars = (series_payload or {}).get("bars") or []
    if len(bars) < 20:
        return None
    dates = [row.get("trade_date") for row in bars[-60:]]
    closes = [float(row.get("close_price") or 0.0) for row in bars[-60:]]
    ma20 = []
    for index in range(len(closes)):
        sample = closes[max(0, index - 19) : index + 1]
        ma20.append(sum(sample) / len(sample))
    path = chart_dir / f"{code}_price_chart.png"
    plt.figure(figsize=(10, 4))
    plt.plot(dates, closes, label="Close", linewidth=1.8)
    plt.plot(dates, ma20, label="MA20", linewidth=1.2)
    plt.xticks(rotation=45, ha="right")
    plt.title(f"{code} Price And MA20")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()
    return str(path)


def _build_momentum_chart(*, chart_dir: Path, code: str, series_payload: dict[str, Any] | None) -> str | None:
    bars = (series_payload or {}).get("bars") or []
    if len(bars) < 20:
        return None
    dates = [row.get("trade_date") for row in bars[-60:]]
    pct = [float(row.get("pct_change") or 0.0) * 100 for row in bars[-60:]]
    volume = [float(row.get("volume") or 0.0) / 1000000.0 for row in bars[-60:]]
    path = chart_dir / f"{code}_momentum_chart.png"
    fig, axes = plt.subplots(2, 1, figsize=(10, 5), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
    axes[0].bar(dates, pct, color=["#2E8B57" if value >= 0 else "#C00000" for value in pct])
    axes[0].set_title(f"{code} Daily Change")
    axes[0].set_ylabel("%")
    axes[1].plot(dates, volume, color="#1F4E78", linewidth=1.5)
    axes[1].set_ylabel("Vol (mn)")
    axes[1].tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return str(path)


def build_best_stock_report(config: dict[str, Any], *, as_of: str, payload: dict[str, Any]) -> dict[str, str]:
    report_payload = _normalize_best_stock_payload(payload, as_of=as_of)
    pick = dict(report_payload.get("pick") or {})
    code = str(pick.get("code") or "unknown")
    root = ensure_dir(Path(config["project"]["report_dir"]) / as_of / "best_stock")
    markdown_path = root / f"{code}_best_stock_report.md"
    json_path = root / f"{code}_best_stock_report.json"
    write_text(markdown_path, render_best_stock_report(report_payload))
    write_json(json_path, report_payload)
    return {"markdown": str(markdown_path), "json": str(json_path)}


def render_best_stock_report(payload: dict[str, Any]) -> str:
    normalized = _normalize_best_stock_payload(payload, as_of=str(payload.get("as_of") or ""))
    as_of = str(normalized.get("as_of") or "")
    pick = dict(normalized.get("pick") or {})
    backtest = dict(normalized.get("backtest") or {})
    candidate_rankings = [dict(item) for item in (normalized.get("candidate_rankings") or [])]
    market_view = dict(normalized.get("market_view") or {})
    metadata = dict(pick.get("metadata") or {})
    selection_score = _coerce_float(
        metadata.get("selection_score"),
        pick.get("selection_score"),
        pick.get("priority_score"),
        backtest.get("selection_score"),
    )
    trade_action = str(pick.get("trade_action") or pick.get("decision") or pick.get("action_label") or "待确认")
    signal_count = int(_coerce_float(backtest.get("signal_count"), backtest.get("trade_count")))
    skipped_signal_count = int(_coerce_float(backtest.get("skipped_signal_count")))
    is_actionable = bool(pick.get("is_actionable") or metadata.get("is_actionable"))
    analysis = build_analysis_snapshot(
        {
            "action": str((pick.get("action_plan") or {}).get("action") or ""),
            "action_label": trade_action,
            "name": pick.get("name") or pick.get("code") or "未知标的",
            "thesis": pick.get("thesis") or pick.get("summary") or "",
            "positive_factors": list(pick.get("catalysts") or []),
            "counterpoints": list(pick.get("risks") or []),
            "risk": list(pick.get("risks") or []),
            "position_guidance": pick.get("position_guidance") or "",
            "levels": dict((pick.get("action_plan") or {}).get("levels") or {}),
            "blockers": list((pick.get("action_plan") or {}).get("blockers") or []),
            "execution_brief": list((pick.get("action_plan") or {}).get("execution_brief") or []),
            "trigger_conditions": list((pick.get("action_plan") or {}).get("trigger_conditions") or []),
            "invalidation_conditions": list((pick.get("action_plan") or {}).get("invalidation_conditions") or []),
            "coverage_score": pick.get("coverage_score"),
            "factor_analysis": dict(pick.get("factor_analysis") or metadata.get("factor_analysis") or {}),
        }
    )
    lines = [
        "# 明日最优标的报告",
        "",
        "## 明日最优标的",
        "",
        f"- 截止日期: `{as_of}`",
        f"- 策略风格: `{payload.get('strategy_style') or metadata.get('strategy_style') or 'general'}`",
        f"- 最优标的: {pick.get('name') or pick.get('code') or '未知标的'}({pick.get('code') or 'unknown'})",
        f"- 交易结论: `{trade_action}`",
        f"- 当前可执行: `{'是' if is_actionable else '否'}`",
        f"- 选择分: `{selection_score:.2f}`",
        f"- 总分: `{_coerce_float(pick.get('total_score')):.2f}` | 覆盖度 `{_coerce_float(pick.get('coverage_score')):.2f}` | 市场分 `{_coerce_float(pick.get('market_score')):.2f}`",
        f"- 结论说明: 当前展示的是明日候选里综合分最高者，不等于无条件立即买入。",
        "",
        "## 结论摘要",
        "",
        f"- 摘要: {pick.get('summary') or '暂无摘要。'}",
        f"- 交易主线: {pick.get('thesis') or '暂无主线描述。'}",
        f"- 位置与赔率: {analysis['setup_text']}",
        f"- 因子画像: {analysis['factor_profile_text']}",
        f"- 因子归因: {analysis['factor_attribution_text']}",
        f"- 核心阻断因素: {analysis['blocker_text']}",
        f"- 执行前提: {analysis['execution_window_text']}",
        f"- 执行三段式: {analysis['execution_triplet_text']}",
        f"- 仓位建议: {pick.get('position_guidance') or '暂无仓位建议。'}",
    ]
    for item in (pick.get("catalysts") or [])[:3]:
        lines.append(f"- 催化: {item}")
    for item in (pick.get("risks") or [])[:3]:
        lines.append(f"- 风险: {item}")
    if market_view.get("reason"):
        lines.extend(["", "## 市场上下文", ""])
        for item in (market_view.get("reason") or [])[:3]:
            lines.append(f"- {item}")
    lines.extend(["", "## 候选对比", ""])
    if candidate_rankings:
        for index, row in enumerate(candidate_rankings[:5], start=1):
            lines.append(
                f"- TOP{index} {row.get('name') or row.get('code') or '未知'}({row.get('code') or 'unknown'}) | "
                f"选择分 `{_coerce_float(row.get('selection_score')):.2f}` | "
                f"交易数 `{int(_coerce_float(row.get('trade_count'))):d}` | "
                f"胜率 `{_coerce_float(row.get('win_rate')):.2%}` | "
                f"平均收益 `{_coerce_float(row.get('average_return')):+.2%}` | "
                f"综合分 `{_coerce_float(row.get('composite_score')):.2f}`"
            )
    else:
        lines.append("- 当前没有可展示的候选对比。")
    lines.extend(["", "## 历史回测", ""])
    lines.append(
        f"- 有效交易 `{int(backtest.get('trade_count') or 0)}` / 信号 `{signal_count}` | 跳过 `{skipped_signal_count}` | "
        f"胜率 `{_coerce_float(backtest.get('win_rate')):.2%}` | 平均收益 `{_coerce_float(backtest.get('average_return')):+.2%}`"
    )
    lines.append(
        f"- 中位收益 `{_coerce_float(backtest.get('median_return')):+.2%}` | 累计收益 `{_coerce_float(backtest.get('cumulative_return')):+.2%}` | 最大回撤 `{_coerce_float(backtest.get('max_drawdown')):+.2%}`"
    )
    if int(_coerce_float(backtest.get("trade_count"))):
        lines.append("- 回测口径: 历史同策略第一名在次日的表现，只用于验证排序质量，不等于当前个股未来会复制。")
    if not is_actionable:
        lines.append("- 说明: 当前第一名是观察优先级最高的候选，不代表下一交易日应直接买入。")
    trades = list(backtest.get("trades") or [])
    if trades:
        lines.extend(["", "### 最近样本", ""])
        for row in trades[:10]:
            trade_score = _coerce_float(row.get("selection_score"), (row.get("metadata") or {}).get("selection_score"))
            trade_action = str(row.get("trade_action") or row.get("decision") or "待确认")
            lines.append(
                f"- {row.get('as_of') or 'unknown'} | {row.get('name') or row.get('code') or '未知'}({row.get('code') or 'unknown'}) | 动作 `{trade_action}` | 分数 `{trade_score:.2f}` | 次日 `{_coerce_float(row.get('next_day_return')):+.2%}`"
            )
    else:
        lines.append("- 暂无可展示的历史样本。")
    return "\n".join(lines) + "\n"


def _normalize_best_stock_payload(payload: dict[str, Any], *, as_of: str) -> dict[str, Any]:
    candidate = payload.get("best_stock") if isinstance(payload.get("best_stock"), dict) else {}
    pick_source = payload.get("pick") or candidate.get("pick") or candidate
    pick = dict(pick_source or {})
    pick_metadata = dict(pick.get("metadata") or {})
    strategy_style = str(
        payload.get("strategy_style")
        or candidate.get("strategy_style")
        or pick.get("strategy_style")
        or pick_metadata.get("strategy_style")
        or "general"
    )
    selection_score = _coerce_float(
        pick.get("selection_score"),
        candidate.get("selection_score"),
        pick_metadata.get("selection_score"),
        pick.get("priority_score"),
        pick.get("total_score"),
    )
    pick_metadata["strategy_style"] = strategy_style
    pick_metadata["selection_score"] = round(selection_score, 2)
    pick_metadata["is_actionable"] = bool(pick.get("is_actionable") or pick_metadata.get("is_actionable"))
    normalized_pick = {
        **pick,
        "name": str(pick.get("name") or pick.get("code") or ""),
        "selection_score": round(selection_score, 2),
        "strategy_style": strategy_style,
        "is_actionable": bool(pick_metadata.get("is_actionable")),
        "metadata": pick_metadata,
    }
    backtest = dict(payload.get("backtest") or candidate.get("backtest") or pick.get("backtest") or {})
    trades = [dict(item) for item in (backtest.get("trades") or [])]
    normalized_backtest = {**backtest, "trades": trades}
    candidate_rankings = _normalize_best_stock_rankings(
        payload=payload,
        candidate=candidate,
        pick=normalized_pick,
        backtest=normalized_backtest,
    )
    return {
        "as_of": str(as_of or payload.get("as_of") or ""),
        "strategy_style": strategy_style,
        "pick": normalized_pick,
        "backtest": normalized_backtest,
        "candidate_rankings": candidate_rankings,
        "market_view": dict(payload.get("market_view") or {}),
    }


def _coerce_float(*values: Any) -> float:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _normalize_best_stock_rankings(
    *,
    payload: dict[str, Any],
    candidate: dict[str, Any],
    pick: dict[str, Any],
    backtest: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = payload.get("candidate_rankings") or candidate.get("candidate_rankings") or []
    normalized: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        row_backtest = dict(item.get("backtest") or {})
        normalized.append(
            {
                "code": str(item.get("code") or ""),
                "name": str(item.get("name") or item.get("code") or ""),
                "selection_score": round(_coerce_float(item.get("selection_score"), item.get("priority_score")), 2),
                "trade_count": int(_coerce_float(item.get("trade_count"), row_backtest.get("trade_count"))),
                "win_rate": _coerce_float(item.get("win_rate"), row_backtest.get("win_rate")),
                "average_return": _coerce_float(item.get("average_return"), row_backtest.get("average_return")),
                "composite_score": round(
                    _coerce_float(
                        item.get("composite_score"),
                        item.get("selection_score"),
                        row_backtest.get("win_rate"),
                    ),
                    2,
                ),
            }
        )
    if normalized:
        normalized.sort(key=lambda item: _coerce_float(item.get("composite_score")), reverse=True)
        return normalized
    return [
        {
            "code": str(pick.get("code") or ""),
            "name": str(pick.get("name") or pick.get("code") or ""),
            "selection_score": round(_coerce_float(pick.get("selection_score")), 2),
            "trade_count": int(_coerce_float(backtest.get("trade_count"))),
            "win_rate": _coerce_float(backtest.get("win_rate")),
            "average_return": _coerce_float(backtest.get("average_return")),
            "composite_score": round(_coerce_float(pick.get("selection_score"), backtest.get("win_rate")), 2),
        }
    ]
