from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from ..models import QuoteSnapshot
from ..utils import load_json, load_jsonl, write_json

RISK_HINTS = {"reduce_risk", "review_risk", "cut_loss", "trim", "risk_exit"}
ENTRY_HINTS = {"watch_entry", "entry", "buy_probe", "breakout_entry"}


def evaluate_alerts(*, session_dir: str | Path, horizon_steps: int = 3) -> dict[str, Any]:
    root = Path(session_dir)
    alerts = load_jsonl(root / "alerts.jsonl")
    quotes = [QuoteSnapshot(**row) for row in load_jsonl(root / "quotes.jsonl")]
    by_code: dict[str, list[QuoteSnapshot]] = defaultdict(list)
    for quote in quotes:
        by_code[quote.code].append(quote)
    evaluations: list[dict[str, Any]] = []
    event_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"alerts": 0, "evaluated": 0, "useful": 0, "avg_follow_return": 0.0})
    action_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"alerts": 0, "evaluated": 0, "useful": 0})
    severity_stats: dict[str, int] = defaultdict(int)
    risk_alert_count = 0
    entry_alert_count = 0
    for alert in alerts:
        code = str(alert["code"])
        event_type = str(alert.get("event_type") or "unknown")
        action_hint = str(alert.get("action_hint") or "unknown")
        severity = str(alert.get("severity") or "unknown")
        event_stats[event_type]["alerts"] += 1
        action_stats[action_hint]["alerts"] += 1
        severity_stats[severity] += 1
        if action_hint in RISK_HINTS:
            risk_alert_count += 1
        elif action_hint in ENTRY_HINTS:
            entry_alert_count += 1
        series = by_code.get(code, [])
        evaluation = evaluate_single_alert(alert=alert, quotes=series, horizon_steps=horizon_steps)
        if evaluation is not None:
            evaluations.append(evaluation)
            event_stats[event_type]["evaluated"] += 1
            action_stats[action_hint]["evaluated"] += 1
            if evaluation["is_useful"]:
                event_stats[event_type]["useful"] += 1
                action_stats[action_hint]["useful"] += 1
            event_stats[event_type]["avg_follow_return"] += float(evaluation["follow_return"] or 0.0)
    useful_rate = (sum(1 for item in evaluations if item["is_useful"]) / len(evaluations)) if evaluations else 0.0
    event_breakdown = _build_event_breakdown(event_stats)
    action_breakdown = _build_action_breakdown(action_stats)
    diagnostics = build_rule_diagnostics(event_breakdown=event_breakdown, action_breakdown=action_breakdown)
    result = {
        "session_dir": str(root),
        "alert_count": len(alerts),
        "evaluated_count": len(evaluations),
        "useful_rate": round(useful_rate, 4),
        "average_follow_return": round(sum(item["follow_return"] for item in evaluations) / len(evaluations), 4) if evaluations else 0.0,
        "evaluations": evaluations,
        "severity_breakdown": dict(severity_stats),
        "event_breakdown": event_breakdown,
        "action_breakdown": action_breakdown,
        "diagnostics": diagnostics,
        "risk_alert_count": risk_alert_count,
        "entry_alert_count": entry_alert_count,
        "sample_gap_count": max(len(alerts) - len(evaluations), 0),
    }
    return result


def evaluate_single_alert(*, alert: dict[str, Any], quotes: list[QuoteSnapshot], horizon_steps: int) -> dict[str, Any] | None:
    alert_ts = str(alert.get("timestamp") or "")
    if not alert_ts:
        return None
    ordered = sorted(quotes, key=lambda item: item.timestamp)
    try:
        index = next(idx for idx, quote in enumerate(ordered) if quote.timestamp >= alert_ts)
    except StopIteration:
        return None
    if index + horizon_steps >= len(ordered):
        return None
    entry = ordered[index]
    exit_quote = ordered[index + horizon_steps]
    follow_return = (exit_quote.last_price / entry.last_price) - 1.0 if entry.last_price else 0.0
    direction = str(alert.get("action_hint") or "")
    is_useful = follow_return > 0.003 if "买" in direction or "观察" in direction or "entry" in direction else follow_return < -0.003 if "减" in direction or "risk" in direction else abs(follow_return) > 0.004
    why = "告警后的后续价格方向与动作提示一致。" if is_useful else "告警后的后续价格方向没有验证这条提示。"
    return {
        "code": alert.get("code"),
        "name": alert.get("name"),
        "timestamp": alert_ts,
        "event_type": alert.get("event_type"),
        "severity": alert.get("severity"),
        "action_hint": alert.get("action_hint"),
        "entry_price": entry.last_price,
        "exit_price": exit_quote.last_price,
        "follow_return": round(follow_return, 4),
        "is_useful": is_useful,
        "why": why,
    }


def build_intraday_review(*, session_dir: str | Path, horizon_steps: int = 3) -> dict[str, Any]:
    session_summary = load_json(Path(session_dir) / "session_summary.json", default={}) or {}
    alert_evaluation = evaluate_alerts(session_dir=session_dir, horizon_steps=horizon_steps)
    return {
        "session_summary": session_summary,
        "alert_evaluation": alert_evaluation,
        "headline": build_intraday_headline(alert_evaluation),
    }


def write_intraday_review(review: dict[str, Any], *, output_dir: str | Path, as_of: str) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    markdown_path = root / f"{as_of}_intraday_review.md"
    markdown_path.write_text(render_intraday_review(review), encoding="utf-8")
    return {
        "markdown": markdown_path,
        "json": write_json(root / f"{as_of}_intraday_review.json", review),
    }


def render_intraday_review(review: dict[str, Any]) -> str:
    evaluation = review["alert_evaluation"]
    lines = [
        "# Intraday Review",
        "",
        f"- 结论: {review['headline']}",
        "",
        "## Alert Evaluation",
    ]
    lines.append(f"- 告警数 `{evaluation['alert_count']}` | 可评估 `{evaluation['evaluated_count']}` | 有效率 `{evaluation['useful_rate']:.2%}`")
    lines.append(
        f"- 风险类告警 `{evaluation.get('risk_alert_count', 0)}` | 入场类告警 `{evaluation.get('entry_alert_count', 0)}` | "
        f"样本缺口 `{evaluation.get('sample_gap_count', 0)}`"
    )
    for item in evaluation["evaluations"][:10]:
        lines.append(f"- {item['timestamp']} | {item['name']}({item['code']}) | `{item['event_type']}` | 收益 `{item['follow_return']:+.2%}` | {item['why']}")
    lines.extend(["", "## Rule Diagnostics"])
    for row in evaluation.get("event_breakdown", [])[:8]:
        lines.append(
            f"- `{row['event_type']}` | 状态 `{row['status']}` | 命中 `{row['useful']}/{row['evaluated']}` | "
            f"触发 `{row['alerts']}` | 均值 `{row['avg_follow_return']:+.2%}`"
        )
    lines.extend(["", "## Policy Feedback"])
    for note in evaluation.get("diagnostics", {}).get("policy_feedback", []):
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def build_intraday_headline(alert_evaluation: dict[str, Any]) -> str:
    if alert_evaluation["evaluated_count"] == 0:
        risk_count = int(alert_evaluation.get("risk_alert_count") or 0)
        entry_count = int(alert_evaluation.get("entry_alert_count") or 0)
        if risk_count > entry_count and risk_count > 0:
            return "样本不足，但盘中以风控告警为主，明日优先复核风险阈值是否过敏。"
        if entry_count > 0:
            return "样本不足，但盘中出现了入场类信号，明日需核对这些信号的后续兑现率。"
        return "今天样本不足，需延长回看窗口后再判断告警规则有效性。"
    if alert_evaluation["useful_rate"] >= 0.6:
        misleading = alert_evaluation.get("diagnostics", {}).get("misleading_rules") or []
        if misleading:
            return "今日告警整体有效，但有局部规则表现偏弱，明日需定向调阈值。"
        return "今日盘中告警整体有效，可继续沿用当前规则。"
    if alert_evaluation["useful_rate"] <= 0.3:
        return "今日盘中告警噪音偏高，需要下调触发频率或提高阈值。"
    return "今日盘中告警有效性一般，需结合具体规则继续分层。"


def _build_event_breakdown(event_stats: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event_type, item in event_stats.items():
        evaluated = int(item.get("evaluated") or 0)
        useful = int(item.get("useful") or 0)
        alerts = int(item.get("alerts") or 0)
        avg_follow_return = (float(item.get("avg_follow_return") or 0.0) / evaluated) if evaluated else 0.0
        useful_rate = (useful / evaluated) if evaluated else None
        status = "unvalidated"
        if evaluated >= 1:
            if useful_rate is not None and useful_rate >= 0.6:
                status = "effective"
            elif useful_rate is not None and useful_rate <= 0.3:
                status = "misleading"
            else:
                status = "mixed"
        rows.append(
            {
                "event_type": event_type,
                "alerts": alerts,
                "evaluated": evaluated,
                "useful": useful,
                "useful_rate": round(useful_rate, 4) if useful_rate is not None else None,
                "avg_follow_return": round(avg_follow_return, 4),
                "status": status,
            }
        )
    rows.sort(key=lambda row: (row["status"] == "misleading", row["alerts"], row["evaluated"]), reverse=True)
    return rows


def _build_action_breakdown(action_stats: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for action_hint, item in action_stats.items():
        evaluated = int(item.get("evaluated") or 0)
        useful = int(item.get("useful") or 0)
        alerts = int(item.get("alerts") or 0)
        useful_rate = (useful / evaluated) if evaluated else None
        rows.append(
            {
                "action_hint": action_hint,
                "alerts": alerts,
                "evaluated": evaluated,
                "useful": useful,
                "useful_rate": round(useful_rate, 4) if useful_rate is not None else None,
            }
        )
    rows.sort(key=lambda row: (row["alerts"], row["evaluated"]), reverse=True)
    return rows


def build_rule_diagnostics(*, event_breakdown: list[dict[str, Any]], action_breakdown: list[dict[str, Any]]) -> dict[str, Any]:
    effective = [row["event_type"] for row in event_breakdown if row["status"] == "effective"]
    misleading = [row["event_type"] for row in event_breakdown if row["status"] == "misleading"]
    unvalidated = [row["event_type"] for row in event_breakdown if row["status"] == "unvalidated" and row["alerts"] >= 2]
    feedback: list[str] = []
    for event_type in misleading[:3]:
        feedback.append(f"`{event_type}` 当天反向率偏高，建议提高触发阈值并增加市场方向过滤。")
    for event_type in effective[:3]:
        feedback.append(f"`{event_type}` 有效性较好，可保留为高优先级规则。")
    for event_type in unvalidated[:3]:
        feedback.append(f"`{event_type}` 触发较多但样本不足，建议延长回看窗口再定是否降权。")
    if not feedback:
        top_action = action_breakdown[0]["action_hint"] if action_breakdown else "unknown"
        feedback.append(f"当前样本有限，先围绕 `{top_action}` 的触发条件做手工复核。")
    return {
        "effective_rules": effective,
        "misleading_rules": misleading,
        "unvalidated_rules": unvalidated,
        "policy_feedback": feedback,
    }
