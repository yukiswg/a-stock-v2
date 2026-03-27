from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from ..data_harness.realtime import LiveQuoteSource, ReplayQuoteSource, SyntheticQuoteSource
from ..models import AlertEvent, QuoteSnapshot
from ..utils import append_jsonl, ensure_dir, load_json, write_json


def run_realtime_session(
    *,
    as_of: str,
    realtime_dir: str | Path,
    monitor_plan_path: str | Path,
    source_kind: str = "live",
    replay_path: str | Path | None = None,
    feature_snapshot_path: str | Path | None = None,
    iterations: int = 3,
    interval_seconds: float = 5.0,
    stale_after_seconds: int = 90,
    sleep_enabled: bool = False,
) -> dict[str, Any]:
    session_id = datetime.now().strftime("session_%H%M%S")
    session_dir = ensure_dir(Path(realtime_dir) / as_of / session_id)
    monitor_plan = load_json(monitor_plan_path, default=[]) or []
    codes = [str(item["object_id"]) for item in monitor_plan]
    names = {str(item.get("object_id") or ""): str(item.get("object_name") or item.get("object_id") or "") for item in monitor_plan}
    quotes_path = session_dir / "quotes.jsonl"
    features_path = session_dir / "features.jsonl"
    alerts_path = session_dir / "alerts.jsonl"
    write_json(session_dir / f"{as_of}_monitor_plan.json", monitor_plan)

    history: dict[str, list[QuoteSnapshot]] = defaultdict(list)
    emitted_at: dict[tuple[str, str], str] = {}
    all_alerts: list[dict[str, Any]] = []
    latest_quote_timestamp = None
    latest_fetched_at = None
    max_freshness = None
    stale_count = 0
    batch_count = 0

    if source_kind == "replay":
        source = ReplayQuoteSource(replay_path or "")
        batches = source.fetch_batches()
    else:
        if source_kind == "live":
            provider = LiveQuoteSource(stale_after_seconds=stale_after_seconds)
        else:
            provider = SyntheticQuoteSource(
                stale_after_seconds=stale_after_seconds,
                base_price_map=load_synthetic_base_prices(
                    as_of=as_of,
                    monitor_plan_path=monitor_plan_path,
                    feature_snapshot_path=feature_snapshot_path,
                ),
                name_map=names,
            )
        batches = []
        for _ in range(iterations):
            batches.append(provider.fetch(codes))
            if sleep_enabled and interval_seconds > 0 and len(batches) < iterations:
                time.sleep(interval_seconds)

    benchmark_codes = {str(item["object_id"]) for item in monitor_plan if str((item.get("metadata") or {}).get("category") or "") == "benchmark"}

    for quotes in batches:
        if not quotes:
            continue
        batch_count += 1
        append_jsonl(quotes_path, [quote.to_dict() for quote in quotes])
        latest_quote_timestamp = max((quote.timestamp for quote in quotes), default=latest_quote_timestamp)
        latest_fetched_at = max((quote.fetched_at for quote in quotes), default=latest_fetched_at)
        stale_count += sum(1 for quote in quotes if quote.is_stale)
        batch_freshness = [quote.freshness_seconds for quote in quotes if quote.freshness_seconds is not None]
        if batch_freshness:
            batch_max = max(batch_freshness)
            max_freshness = batch_max if max_freshness is None else max(max_freshness, batch_max)
        features = []
        benchmark_returns = {
            quote.code: intrabatch_return(history[quote.code], quote)
            for quote in quotes
            if quote.code in benchmark_codes
        }
        benchmark_anchor = next(iter(benchmark_returns.values()), None)
        alerts = []
        for quote in quotes:
            history[quote.code].append(quote)
            feature = build_realtime_feature(history[quote.code], quote, benchmark_return=benchmark_anchor)
            features.append(feature)
            event = evaluate_monitor_item(quote=quote, feature=feature, monitor_plan=monitor_plan, emitted_at=emitted_at)
            if event is not None:
                alert_row = event.to_dict()
                alerts.append(alert_row)
                all_alerts.append(alert_row)
        append_jsonl(features_path, features)
        append_jsonl(alerts_path, alerts)

    summary = {
        "session_id": session_id,
        "as_of": as_of,
        "monitor_plan_path": str(monitor_plan_path),
        "source_kind": source_kind,
        "quotes_path": str(quotes_path),
        "features_path": str(features_path),
        "alerts_path": str(alerts_path),
        "alert_count": len(all_alerts),
        "iterations_executed": batch_count,
        "latest_quote_timestamp": latest_quote_timestamp,
        "latest_fetched_at": latest_fetched_at,
        "max_freshness_seconds": max_freshness,
        "stale_quote_count": stale_count,
    }
    write_json(session_dir / "session_summary.json", summary)
    return {
        "session_dir": session_dir,
        "session_summary": summary,
        "quotes_path": quotes_path,
        "features_path": features_path,
        "alerts_path": alerts_path,
    }


def build_realtime_feature(history: list[QuoteSnapshot], quote: QuoteSnapshot, *, benchmark_return: float | None) -> dict[str, Any]:
    previous = history[-2] if len(history) >= 2 else None
    last_return = ((quote.last_price / previous.last_price) - 1.0) if previous and previous.last_price else 0.0
    last_volumes = [item.volume or 0.0 for item in history[-5:]]
    avg_volume = (sum(last_volumes) / len(last_volumes)) if last_volumes else 0.0
    volume_ratio = ((quote.volume or 0.0) / avg_volume) if avg_volume else None
    relative_strength = (last_return - benchmark_return) if benchmark_return is not None else None
    return {
        "code": quote.code,
        "name": quote.name,
        "timestamp": quote.timestamp,
        "last_price": quote.last_price,
        "return_1step": round(last_return, 4),
        "volume_ratio": round(volume_ratio, 4) if volume_ratio is not None else None,
        "relative_strength": round(relative_strength, 4) if relative_strength is not None else None,
        "freshness_seconds": quote.freshness_seconds,
    }


def intrabatch_return(history: list[QuoteSnapshot], quote: QuoteSnapshot) -> float:
    previous = history[-1] if history else None
    if previous is None or previous.last_price == 0:
        return 0.0
    return (quote.last_price / previous.last_price) - 1.0


def evaluate_monitor_item(
    *,
    quote: QuoteSnapshot,
    feature: dict[str, Any],
    monitor_plan: list[dict[str, Any]],
    emitted_at: dict[tuple[str, str], str],
) -> AlertEvent | None:
    plan = next((item for item in monitor_plan if str(item["object_id"]) == quote.code), None)
    if plan is None:
        return None
    metadata = plan.get("metadata") or {}
    thresholds = metadata.get("thresholds") or {}
    price_jump = float(thresholds.get("price_jump_threshold_5m", 0.015))
    price_drop = float(thresholds.get("price_drop_threshold_5m", -0.015))
    relative_threshold = float(thresholds.get("relative_strength_threshold_5m", 0.01))
    last_return = float(feature.get("return_1step") or 0.0)
    relative_strength = float(feature.get("relative_strength") or 0.0)
    volume_ratio = float(feature.get("volume_ratio") or 0.0)
    event_type = None
    severity = "info"
    action_hint = "observe"
    rationale = []
    if metadata.get("category") == "benchmark" and last_return <= -0.01:
        event_type = "benchmark_drop"
        severity = "high"
        action_hint = "reduce_risk"
        rationale.append("基准指数快速转弱。")
    elif last_return >= price_jump and relative_strength >= relative_threshold:
        event_type = "relative_breakout"
        severity = "medium"
        action_hint = "watch_entry"
        rationale.append("价格和相对强弱同时上行。")
    elif last_return <= price_drop:
        event_type = "drawdown_break"
        severity = "medium"
        action_hint = "review_risk"
        rationale.append("短周期快速下破。")
    elif volume_ratio >= 1.8 and abs(last_return) >= 0.006:
        event_type = "volume_anomaly"
        severity = "low"
        action_hint = "inspect"
        rationale.append("成交量明显放大。")
    if event_type is None:
        return None
    dedupe_key = (quote.code, event_type)
    if emitted_at.get(dedupe_key) == quote.timestamp:
        return None
    emitted_at[dedupe_key] = quote.timestamp
    summary = f"{quote.name}({quote.code}) 触发 {event_type}，价格 {quote.last_price:.3f}。"
    return AlertEvent(
        session_id="realtime",
        timestamp=quote.timestamp,
        code=quote.code,
        name=quote.name,
        event_type=event_type,
        severity=severity,
        score=round(abs(last_return) * 1000 + max(volume_ratio - 1.0, 0.0) * 5, 2),
        action_hint=action_hint,
        summary=summary,
        explanation="; ".join(rationale),
        rationale=rationale,
        source=quote.source,
        price=quote.last_price,
        benchmark_return=feature.get("relative_strength"),
        freshness_seconds=quote.freshness_seconds,
    )


def load_synthetic_base_prices(
    *,
    as_of: str,
    monitor_plan_path: str | Path,
    feature_snapshot_path: str | Path | None,
) -> dict[str, float]:
    rows: dict[str, float] = {}
    if feature_snapshot_path:
        feature_payload = load_json(feature_snapshot_path, default={}) or {}
        if isinstance(feature_payload, dict):
            for code, item in feature_payload.items():
                if not isinstance(item, dict):
                    continue
                price = item.get("last_close")
                if isinstance(price, (int, float)) and float(price) > 0:
                    rows[str(code)] = float(price)
    report_root = Path(monitor_plan_path).parent
    homepage_path = report_root / f"{as_of}_homepage_overview.json"
    homepage = load_json(homepage_path, default={}) or {}
    for item in homepage.get("current_prices") or []:
        code = str(item.get("code") or "")
        raw_price = item.get("last_price")
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            continue
        if code and price > 0:
            rows[code] = price
    return rows
